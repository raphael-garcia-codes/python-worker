"""
Domain entities.

This module contains the core business objects of the application. They are
plain Python objects with zero knowledge of infrastructure concerns
(no SQLAlchemy, no aio-pika, no aiokafka). This is what makes the domain
layer testable in complete isolation.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class EventSource(str, Enum):
    """Identifies the origin of a processed event."""

    RABBITMQ_USERS_CREATION = "RABBITMQ_USERS_CREATION"
    KAFKA_API_MESSAGES = "KAFKA_API_MESSAGES"


class EventStatus(str, Enum):
    """Possible outcomes for the processing of an event."""

    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


@dataclass(slots=True)
class ProcessedEvent:
    """
    Domain representation of a message that has gone through the
    processing pipeline. This entity is persisted by the repository, but
    the domain itself has no idea *how* it is persisted.
    """

    source: EventSource
    payload: dict[str, Any]
    status: EventStatus
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    error_message: str | None = None
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def success(cls, source: EventSource, payload: dict[str, Any]) -> "ProcessedEvent":
        return cls(source=source, payload=payload, status=EventStatus.PROCESSED)

    @classmethod
    def failure(
        cls, source: EventSource, payload: dict[str, Any], error_message: str
    ) -> "ProcessedEvent":
        return cls(
            source=source,
            payload=payload,
            status=EventStatus.FAILED,
            error_message=error_message,
        )
