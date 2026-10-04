"""Single-line JSON logging on standard output."""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

_STANDARD_LOG_RECORD_ATTRS = frozenset(logging.makeLogRecord({}).__dict__) | {"message"}


class JsonFormatter(logging.Formatter):
    """Format one log record as one JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_LOG_RECORD_ATTRS:
                payload[key] = value
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(level: str) -> None:
    """Replace the root handlers with one JSON handler writing to stdout."""
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level)
