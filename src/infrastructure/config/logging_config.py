"""
structlog configuration.

Produces structured JSON logs on stdout, suitable for container log
collectors (e.g. Fluentd, Datadog, CloudWatch). Adds ISO-8601 timestamps,
log level, and supports per-context correlation IDs via `bind_contextvars`.
"""
from __future__ import annotations

import logging
import sys
import uuid
from contextvars import ContextVar

import structlog

_correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")


def new_correlation_id() -> str:
    """Generate a fresh correlation id, e.g. one per consumed message."""
    return str(uuid.uuid4())


def bind_correlation_id(correlation_id: str | None = None) -> str:
    """Bind a correlation id to the current async context so every log
    line emitted while processing a message carries it automatically."""
    cid = correlation_id or new_correlation_id()
    _correlation_id_var.set(cid)
    structlog.contextvars.bind_contextvars(correlation_id=cid)
    return cid


def configure_logging(log_level: str = "INFO", app_name: str = "events-worker") -> None:
    """Must be called once, at process startup, before any logger is used."""

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level.upper(),
    )

    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    structlog.configure(
        processors=shared_processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(log_level.upper())

    structlog.contextvars.bind_contextvars(app=app_name)


def get_logger(*names: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(*names)
