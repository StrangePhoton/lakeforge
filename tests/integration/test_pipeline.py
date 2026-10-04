import json

import pytest

from lakeforge.cli import main
from lakeforge.config import Settings
from lakeforge.ingest.pipeline import run_pipeline
from lakeforge.logging import configure_logging

pytestmark = pytest.mark.integration


def test_pipeline_lands_the_default_snapshot(
    settings: Settings,
    source_connection: object,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del source_connection
    configure_logging("INFO")
    result = run_pipeline(settings, run_id="pipeline-test")

    assert result.vehicle_count == 4
    assert result.telemetry_count == 96
    assert result.seed == 42
    manifest = json.loads((result.landing_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "succeeded"
    assert manifest["run_id"] == "pipeline-test"
    assert manifest["seed"] == 42
    assert manifest["datasets"]["vehicles"]["rows"] == 4
    assert manifest["datasets"]["telemetry_events"]["rows"] == 96
    telemetry_lines = (result.landing_dir / "telemetry_events.jsonl").read_text(encoding="utf-8")
    assert len(telemetry_lines.splitlines()) == 96
    logged = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert any(
        entry.get("run_id") == "pipeline-test" and entry.get("row_count") == 96 for entry in logged
    )


def test_cli_pipeline_uses_the_same_slice(
    settings: Settings,
    source_connection: object,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del source_connection
    monkeypatch.setenv("LAKEFORGE_POSTGRES_HOST", settings.postgres_host)
    monkeypatch.setenv("LAKEFORGE_POSTGRES_PORT", str(settings.postgres_port))
    monkeypatch.setenv("LAKEFORGE_POSTGRES_DB", settings.postgres_db)
    monkeypatch.setenv("LAKEFORGE_POSTGRES_USER", settings.postgres_user)
    monkeypatch.setenv(
        "LAKEFORGE_POSTGRES_PASSWORD",
        settings.postgres_password.get_secret_value(),
    )
    code = main(
        [
            "pipeline",
            "--seed",
            "42",
            "--vehicles",
            "4",
            "--hours",
            "6",
            "--interval-minutes",
            "15",
            "--landing-dir",
            str(settings.landing_dir / "cli"),
        ]
    )

    assert code == 0
    output = capsys.readouterr().out
    logged = [json.loads(line) for line in output.splitlines() if line.strip()]
    finished = [entry for entry in logged if entry["message"] == "pipeline finished"]
    assert len(finished) == 1
    assert finished[0]["row_count"] == 96
    assert "run_id" in finished[0]
    landing = settings.landing_dir / "cli" / f"run_id={finished[0]['run_id']}"
    assert (landing / "manifest.json").is_file()
    assert (landing / "vehicles.jsonl").is_file()
    assert (landing / "telemetry_events.jsonl").is_file()
