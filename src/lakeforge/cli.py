"""Command line for the local source, landing, and medallion slice."""

import argparse
import logging
import sys
from pathlib import Path

from psycopg import OperationalError
from pydantic import ValidationError

from lakeforge.config import ProfileName, Settings, load_settings
from lakeforge.ingest.pipeline import run_generate, run_ingest, run_pipeline
from lakeforge.lakehouse.bronze import run_bronze
from lakeforge.lakehouse.gold import run_gold
from lakeforge.lakehouse.silver import run_silver
from lakeforge.logging import configure_logging


def main(argv: list[str] | None = None) -> int:
    """Run generate, ingest, the landing pipeline, or a medallion build."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        settings = _settings_from_args(args)
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    configure_logging(settings.log_level)
    try:
        if args.command == "generate":
            run_generate(settings)
        elif args.command == "ingest":
            run_ingest(settings)
        elif args.command == "pipeline":
            run_pipeline(settings)
        elif args.command == "bronze":
            landing_run = _optional_path(args, "landing_run")
            if landing_run is None:
                raise ValueError("landing run is required")
            run_bronze(settings, landing_run)
        elif args.command == "silver":
            bronze_run = _optional_path(args, "bronze_run")
            if bronze_run is None:
                raise ValueError("bronze run is required")
            run_silver(settings, bronze_run)
        elif args.command == "gold":
            silver_run = _optional_path(args, "silver_run")
            if silver_run is None:
                raise ValueError("silver run is required")
            run_gold(settings, silver_run)
        else:
            parser.error(f"unknown command {args.command}")
    except (OperationalError, ValueError, OSError) as exc:
        logging.getLogger("lakeforge.cli").error(str(exc))
        return 1
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakeforge")
    commands = parser.add_subparsers(dest="command", required=True)

    generate = commands.add_parser("generate", help="Load a deterministic snapshot into PostgreSQL")
    _add_generation_arguments(generate)

    ingest = commands.add_parser("ingest", help="Extract the current source snapshot to JSONL")
    _add_landing_argument(ingest)

    pipeline = commands.add_parser("pipeline", help="Generate the source snapshot and extract it")
    _add_generation_arguments(pipeline)
    _add_landing_argument(pipeline)

    bronze = commands.add_parser("bronze", help="Load one landing run into local Delta tables")
    bronze.add_argument("--landing-run", type=Path, required=True)
    _add_landing_argument(bronze)
    bronze.add_argument("--bronze-dir", type=Path, default=None)

    silver = commands.add_parser("silver", help="Conform one Bronze run into Silver Delta tables")
    silver.add_argument("--bronze-run", type=Path, required=True)
    silver.add_argument("--bronze-dir", type=Path, default=None)
    silver.add_argument("--silver-dir", type=Path, default=None)

    gold = commands.add_parser("gold", help="Aggregate one Silver run into Gold activity tables")
    gold.add_argument("--silver-run", type=Path, required=True)
    gold.add_argument("--silver-dir", type=Path, default=None)
    gold.add_argument("--gold-dir", type=Path, default=None)
    return parser


def _add_generation_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", choices=("tiny", "dev", "large"), default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--vehicles", type=int, default=None)
    parser.add_argument("--hours", type=int, default=None)
    parser.add_argument("--interval-minutes", type=int, default=None)
    parser.add_argument(
        "--no-anomalies",
        action="store_true",
        help="Generate the operational story without injected telemetry defects",
    )


def _add_landing_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--landing-dir", type=Path, default=None)


def _settings_from_args(args: argparse.Namespace) -> Settings:
    current = load_settings()
    seed = _optional_int(args, "seed")
    vehicles = _optional_int(args, "vehicles")
    hours = _optional_int(args, "hours")
    interval_minutes = _optional_int(args, "interval_minutes")
    landing_dir = _optional_path(args, "landing_dir")
    bronze_dir = _optional_path(args, "bronze_dir")
    silver_dir = _optional_path(args, "silver_dir")
    gold_dir = _optional_path(args, "gold_dir")
    profile = _optional_profile(args)
    anomalies = current.anomalies and not bool(getattr(args, "no_anomalies", False))
    return Settings(
        postgres_host=current.postgres_host,
        postgres_port=current.postgres_port,
        postgres_db=current.postgres_db,
        postgres_user=current.postgres_user,
        postgres_password=current.postgres_password,
        landing_dir=current.landing_dir if landing_dir is None else landing_dir,
        bronze_dir=current.bronze_dir if bronze_dir is None else bronze_dir,
        silver_dir=current.silver_dir if silver_dir is None else silver_dir,
        gold_dir=current.gold_dir if gold_dir is None else gold_dir,
        log_level=current.log_level,
        seed=current.seed if seed is None else seed,
        profile=current.profile if profile is None else profile,
        anomalies=anomalies,
        vehicle_count=current.vehicle_count if vehicles is None else vehicles,
        hours=current.hours if hours is None else hours,
        interval_minutes=(
            current.interval_minutes if interval_minutes is None else interval_minutes
        ),
    )


def _optional_int(args: argparse.Namespace, name: str) -> int | None:
    value = getattr(args, name, None)
    if value is None:
        return None
    if isinstance(value, int):
        return value
    raise ValueError(f"{name} must be an integer")


def _optional_profile(args: argparse.Namespace) -> ProfileName | None:
    value: object = getattr(args, "profile", None)
    if value is None:
        return None
    if value == "tiny":
        return "tiny"
    if value == "dev":
        return "dev"
    if value == "large":
        return "large"
    raise ValueError("profile must be tiny, dev, or large")


def _optional_path(args: argparse.Namespace, name: str) -> Path | None:
    value = getattr(args, name, None)
    if value is None:
        return None
    if isinstance(value, Path):
        return value
    raise ValueError(f"{name} must be a path")
