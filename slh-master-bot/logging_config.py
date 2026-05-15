"""Structured JSON logging configuration for slh-master-bot."""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Any

try:
    from pythonjsonlogger import jsonlogger  # type: ignore

    class _SLHJsonFormatter(jsonlogger.JsonFormatter):
        """Adds service name and ISO-8601 timestamp to every log record."""

        SERVICE = "slh-master-bot"

        def add_fields(
            self,
            log_record: dict[str, Any],
            record: logging.LogRecord,
            message_dict: dict[str, Any],
        ) -> None:
            super().add_fields(log_record, record, message_dict)
            log_record["service"] = self.SERVICE
            log_record["timestamp"] = datetime.now(timezone.utc).isoformat()
            log_record["level"] = record.levelname
            # Remove the default 'asctime' key added by the base formatter
            log_record.pop("asctime", None)

    _JSON_AVAILABLE = True

except ImportError:  # pragma: no cover
    _JSON_AVAILABLE = False


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure root logger with JSON output (falls back to plain text).

    Returns the root logger so callers can do::

        log = setup_logging()
        log.info("started", extra={"user_id": 123})
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(numeric_level)

    # Remove any handlers already attached (e.g. from basicConfig calls)
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(numeric_level)

    if _JSON_AVAILABLE:
        fmt = _SLHJsonFormatter(
            fmt="%(timestamp)s %(level)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    else:
        fmt = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

    handler.setFormatter(fmt)
    root.addHandler(handler)

    # Silence noisy third-party loggers
    for noisy in ("httpx", "httpcore", "telegram", "apscheduler"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return root


def get_logger(name: str) -> logging.Logger:
    """Return a named child logger (call after setup_logging)."""
    return logging.getLogger(name)
