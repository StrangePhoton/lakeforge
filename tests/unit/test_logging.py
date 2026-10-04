import json
import logging

import pytest

from lakeforge.logging import configure_logging


def test_log_record_is_one_json_object(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    logging.getLogger("lakeforge.ingest").info(
        "extract completed",
        extra={"run_id": "run-1", "dataset": "telemetry_events", "row_count": 96},
    )

    output = capsys.readouterr().out.strip().splitlines()
    assert len(output) == 1
    payload = json.loads(output[0])
    assert payload["level"] == "INFO"
    assert payload["logger"] == "lakeforge.ingest"
    assert payload["message"] == "extract completed"
    assert payload["run_id"] == "run-1"
    assert payload["dataset"] == "telemetry_events"
    assert payload["row_count"] == 96
    assert "timestamp" in payload


def test_debug_records_are_omitted_at_info(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    logging.getLogger("lakeforge.ingest").debug("hidden detail")

    assert capsys.readouterr().out == ""
