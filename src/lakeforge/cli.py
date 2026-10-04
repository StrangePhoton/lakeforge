"""Command line for the Milestone 0 source-to-landing slice."""

import argparse
import logging
import sys
from pathlib import Path

from psycopg import OperationalError
from pydantic import ValidationError

from lakeforge.config import Settings, load_settings
from lakeforge.ingest.pipeline import run_generate, run_ingest, run_pipeline
from lakeforge.logging import configure_logging


def main(argv: list[str] | None = None) -> int:
    """Run generate, ingest, or the full local pipeline."""
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
    return parser


def _add_generation_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--vehicles", type=int, default=None)
    parser.add_argument("--hours", type=int, default=None)
    parser.add_argument("--interval-minutes", type=int, default=None)


def _add_landing_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--landing-dir", type=Path, default=None)


def _settings_from_args(args: argparse.Namespace) -> Settings:
    current = load_settings()
    seed = _optional_int(args, "seed")
    vehicles = _optional_int(args, "vehicles")
    hours = _optional_int(args, "hours")
    interval_minutes = _optional_int(args, "interval_minutes")
    landing_dir = _optional_path(args, "landing_dir")
    return Settings(
        postgres_host=current.postgres_host,
        postgres_port=current.postgres_port,
        postgres_db=current.postgres_db,
        postgres_user=current.postgres_user,
        postgres_password=current.postgres_password,
        landing_dir=current.landing_dir if landing_dir is None else landing_dir,
        log_level=current.log_level,
        seed=current.seed if seed is None else seed,
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


def _optional_path(args: argparse.Namespace, name: str) -> Path | None:
    value = getattr(args, name, None)
    if value is None:
        return None
    if isinstance(value, Path):
        return value
    raise ValueError(f"{name} must be a path")
