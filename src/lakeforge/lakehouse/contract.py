"""Check a landing run against the published dataset contracts."""

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, cast

from lakeforge.config import Settings
from lakeforge.ingest.landing import read_manifest
from lakeforge.lakehouse.catalog import BUSINESS_KEYS, DATASETS, ColumnKind
from lakeforge.lakehouse.paths import resolve_landing_run

_LOGGER = logging.getLogger("lakeforge.contract")
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_KINDS = {"string", "double", "timestamp", "date"}


@dataclass(frozen=True, slots=True)
class ContractField:
    """One field in a published contract version."""

    name: str
    kind: ColumnKind
    required: bool


@dataclass(frozen=True, slots=True)
class ContractVersion:
    """One published version of a dataset contract."""

    version: int
    fields: tuple[ContractField, ...]


@dataclass(frozen=True, slots=True)
class DatasetContract:
    """Every published version of one landing dataset."""

    dataset: str
    compatibility: str
    versions: tuple[ContractVersion, ...]

    def at(self, version: int) -> ContractVersion:
        """Return one published version."""
        for item in self.versions:
            if item.version == version:
                return item
        raise ValueError(f"{self.dataset} has no contract version {version}")

    def latest(self) -> ContractVersion:
        """Return the highest published version."""
        return max(self.versions, key=lambda item: item.version)


@dataclass(frozen=True, slots=True)
class ContractViolation:
    """One landing value that breaks the selected contract."""

    dataset: str
    line: int
    rule_id: str
    field: str
    record_id: str | None


@dataclass(frozen=True, slots=True)
class ContractResult:
    """Outcome of one contract check."""

    landing_run_id: str
    landing_dir: Path
    report_dir: Path
    contract_versions: tuple[tuple[str, int], ...]
    row_counts: tuple[tuple[str, int], ...]
    violation_counts: tuple[tuple[str, int], ...]


def catalog_dir() -> Path:
    """Return the checked-in contract catalog."""
    found = Path(__file__).resolve().parents[3] / "contracts"
    if not found.is_dir():
        raise ValueError("contract catalog is missing")
    return found


def load_catalog(directory: Path | None = None) -> dict[str, DatasetContract]:
    """Load the published contracts for the five landing datasets."""
    root = catalog_dir() if directory is None else directory
    loaded: dict[str, DatasetContract] = {}
    for dataset in DATASETS:
        path = root / f"{dataset}.json"
        if not path.is_file():
            raise ValueError(f"contract catalog is missing {dataset}.json")
        loaded[dataset] = _parse_contract(json.loads(path.read_text(encoding="utf-8")), dataset)
    return loaded


def check_dataset(
    dataset: str,
    text: str,
    version: ContractVersion,
) -> tuple[int, tuple[ContractViolation, ...]]:
    """Return the row count and violations for one JSONL file."""
    fields = {field.name: field for field in version.fields}
    key = BUSINESS_KEYS[dataset]
    violations: list[ContractViolation] = []
    rows = 0
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        if not raw_line.strip():
            continue
        rows += 1
        try:
            payload = json.loads(raw_line)
        except json.JSONDecodeError:
            violations.append(ContractViolation(dataset, line_number, "invalid_json", "", None))
            continue
        if not isinstance(payload, dict):
            violations.append(ContractViolation(dataset, line_number, "invalid_json", "", None))
            continue
        record_id = payload.get(key)
        identity = record_id if isinstance(record_id, str) else None
        for name in sorted(set(payload) - set(fields)):
            violations.append(
                ContractViolation(dataset, line_number, "unknown_field", name, identity)
            )
        for field in version.fields:
            violations.extend(_field_violations(dataset, line_number, field, payload, identity))
    return rows, tuple(violations)


def run_contract(settings: Settings, landing_run: Path) -> ContractResult:
    """Write the contract report for one landing run."""
    landing_dir = resolve_landing_run(settings.landing_dir, landing_run)
    manifest = read_manifest(landing_dir / "manifest.json")
    run_id = _manifest_run_id(manifest, landing_dir)
    catalog = load_catalog()
    selected = tuple((dataset, catalog[dataset].latest()) for dataset in DATASETS)
    _LOGGER.info("contract started", extra={"run_id": run_id, "landing_path": str(landing_dir)})
    row_counts: list[tuple[str, int]] = []
    violation_counts: list[tuple[str, int]] = []
    violations: list[ContractViolation] = []
    for dataset, version in selected:
        text = (landing_dir / f"{dataset}.jsonl").read_text(encoding="utf-8")
        rows, found = check_dataset(dataset, text, version)
        row_counts.append((dataset, rows))
        violation_counts.append((dataset, len(found)))
        violations.extend(found)
    report_dir = settings.contracts_dir.resolve() / landing_dir.name
    report_dir.mkdir(parents=True, exist_ok=True)
    _write_violations(report_dir / "schema_violation.jsonl", violations)
    _write_manifest(
        report_dir / "manifest.json",
        run_id=run_id,
        landing_dir=landing_dir,
        versions=tuple((dataset, version.version) for dataset, version in selected),
        row_counts=tuple(row_counts),
        violation_counts=tuple(violation_counts),
    )
    _LOGGER.info(
        "contract finished",
        extra={
            "run_id": run_id,
            "row_count": sum(count for _, count in row_counts),
            "landing_path": str(report_dir),
        },
    )
    return ContractResult(
        landing_run_id=run_id,
        landing_dir=landing_dir,
        report_dir=report_dir,
        contract_versions=tuple((dataset, version.version) for dataset, version in selected),
        row_counts=tuple(row_counts),
        violation_counts=tuple(violation_counts),
    )


def _field_violations(
    dataset: str,
    line_number: int,
    field: ContractField,
    payload: dict[str, Any],
    record_id: str | None,
) -> tuple[ContractViolation, ...]:
    if field.name not in payload:
        if field.required:
            return (
                ContractViolation(dataset, line_number, "missing_field", field.name, record_id),
            )
        return ()
    value = payload[field.name]
    if value is None:
        if field.required:
            return (
                ContractViolation(dataset, line_number, "null_required", field.name, record_id),
            )
        return ()
    if _matches(field.kind, value):
        return ()
    return (ContractViolation(dataset, line_number, "type_mismatch", field.name, record_id),)


def _matches(kind: ColumnKind, value: object) -> bool:
    if kind == "string":
        return isinstance(value, str)
    if kind == "double":
        return isinstance(value, int | float) and not isinstance(value, bool)
    if kind == "timestamp":
        return isinstance(value, str) and _valid_timestamp(value)
    return isinstance(value, str) and _valid_date(value)


def _valid_timestamp(value: str) -> bool:
    if _TIMESTAMP.fullmatch(value) is None:
        return False
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _valid_date(value: str) -> bool:
    if _DATE.fullmatch(value) is None:
        return False
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _parse_contract(payload: object, dataset: str) -> DatasetContract:
    if not isinstance(payload, dict):
        raise ValueError(f"{dataset} contract must be a JSON object")
    compatibility = payload.get("compatibility")
    versions = payload.get("versions")
    if payload.get("dataset") != dataset or compatibility != "backward":
        raise ValueError(f"{dataset} contract is not a backward-compatible dataset contract")
    if not isinstance(versions, list) or not versions:
        raise ValueError(f"{dataset} contract has no versions")
    parsed = tuple(_parse_version(item, dataset) for item in versions)
    return DatasetContract(dataset=dataset, compatibility="backward", versions=parsed)


def _parse_version(payload: object, dataset: str) -> ContractVersion:
    if not isinstance(payload, dict):
        raise ValueError(f"{dataset} contract version must be a JSON object")
    version = payload.get("version")
    columns = payload.get("columns")
    if not isinstance(version, int) or not isinstance(columns, list) or not columns:
        raise ValueError(f"{dataset} contract version is incomplete")
    fields = tuple(_parse_field(item, dataset) for item in columns)
    return ContractVersion(version=version, fields=fields)


def _parse_field(payload: object, dataset: str) -> ContractField:
    if not isinstance(payload, dict):
        raise ValueError(f"{dataset} contract field must be a JSON object")
    name = payload.get("name")
    kind = payload.get("type")
    required = payload.get("required")
    if (
        not isinstance(name, str)
        or not isinstance(kind, str)
        or kind not in _KINDS
        or not isinstance(required, bool)
    ):
        raise ValueError(f"{dataset} contract field is incomplete")
    return ContractField(name=name, kind=cast(ColumnKind, kind), required=required)


def _manifest_run_id(manifest: dict[str, Any], landing_dir: Path) -> str:
    run_id = manifest.get("run_id")
    if not isinstance(run_id, str) or landing_dir.name != f"run_id={run_id}":
        raise ValueError("manifest run_id does not match the landing directory")
    return run_id


def _write_violations(
    path: Path,
    violations: tuple[ContractViolation, ...] | list[ContractViolation],
) -> None:
    if not violations:
        if path.exists():
            path.unlink()
        return
    lines = [
        json.dumps(
            {
                "dataset": item.dataset,
                "line": item.line,
                "rule_id": item.rule_id,
                "field": item.field,
                "record_id": item.record_id,
            },
            separators=(",", ":"),
        )
        for item in violations
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_manifest(
    path: Path,
    *,
    run_id: str,
    landing_dir: Path,
    versions: tuple[tuple[str, int], ...],
    row_counts: tuple[tuple[str, int], ...],
    violation_counts: tuple[tuple[str, int], ...],
) -> None:
    violations = dict(violation_counts)
    payload = {
        "landing_run_id": run_id,
        "landing_path": str(landing_dir),
        "compatibility": "backward",
        "contracts": {dataset: version for dataset, version in versions},
        "datasets": {
            dataset: {"rows": rows, "violations": violations[dataset]}
            for dataset, rows in row_counts
        },
        "status": "succeeded" if sum(violations.values()) == 0 else "violations",
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
