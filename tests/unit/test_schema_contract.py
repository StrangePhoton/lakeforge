from pathlib import Path

from lakeforge.source.postgres import (
    ALARM_COLUMNS,
    SCHEMA_PATH,
    SITE_COLUMNS,
    TELEMETRY_COLUMNS,
    VEHICLE_COLUMNS,
    WORK_ORDER_COLUMNS,
    split_sql_script,
)


def test_compose_and_loader_share_one_schema_file() -> None:
    root = Path(__file__).resolve().parents[2]
    schema = root / "src" / "lakeforge" / "source" / "schema.sql"
    compose = (root / "docker-compose.yml").read_text(encoding="utf-8")

    assert SCHEMA_PATH.resolve() == schema.resolve()
    assert "src/lakeforge/source/schema.sql:/docker-entrypoint-initdb.d/01-schema.sql:ro" in compose
    text = schema.read_text(encoding="utf-8")
    for column in (
        *SITE_COLUMNS,
        *VEHICLE_COLUMNS,
        *TELEMETRY_COLUMNS,
        *ALARM_COLUMNS,
        *WORK_ORDER_COLUMNS,
    ):
        assert column in text
    assert text.count("CREATE TABLE") == 5
    telemetry = text.split("CREATE TABLE IF NOT EXISTS source.telemetry_events", 1)[1]
    telemetry = telemetry.split("CREATE TABLE", 1)[0]
    assert "REFERENCES" not in telemetry
    assert "event_time TIMESTAMPTZ NULL" in telemetry
    assert "received_at TIMESTAMPTZ NOT NULL" in telemetry


def test_split_sql_script_ignores_comments() -> None:
    script = "-- comment\nCREATE SCHEMA source;\n\n-- tail\nSELECT 1;\n"
    assert split_sql_script(script) == ["CREATE SCHEMA source", "SELECT 1"]
