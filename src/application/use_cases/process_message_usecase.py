"""
Application layer: the single use case shared by both consumers.

Both the RabbitMQ and the Kafka adapters call into this same use case,
which knows nothing about brokers - only about the domain and the
repository port. This is the core of the hexagonal architecture: swap
RabbitMQ for SQS, or Postgres for Mongo, and this file never changes.
"""
from __future__ import annotations

from typing import Any

from src.domain.entities import EventSource, ProcessedEvent
from src.domain.ports import EventRepositoryPort
from src.infrastructure.config.logging_config import get_logger

logger = get_logger(__name__)


class ProcessMessageUseCase:
    """Processes an inbound message and records the outcome."""

    def __init__(self, event_repository: EventRepositoryPort) -> None:
        self._event_repository = event_repository

    async def execute(self, source: EventSource, payload: dict[str, Any]) -> ProcessedEvent:
        """
        Process a single message.

        Business processing itself is intentionally minimal here (this is
        a generic ingestion worker) - the important behavior is that
        *every* message, successful or not, results in an auditable
        `ProcessedEvent` row. Replace `_apply_business_rules` with real
        domain logic as requirements grow.
        """
        try:
            self._apply_business_rules(source, payload)
            event = ProcessedEvent.success(source=source, payload=payload)
            await self._event_repository.save(event)
            logger.info(
                "message_processed",
                source=source.value,
                event_id=str(event.id),
                status=event.status.value,
            )
            return event

        except Exception as exc:  # noqa: BLE001 - broad catch is intentional here
            event = ProcessedEvent.failure(
                source=source, payload=payload, error_message=str(exc)
            )
            await self._event_repository.save(event)
            logger.error(
                "message_processing_failed",
                source=source.value,
                event_id=str(event.id),
                status=event.status.value,
                error=str(exc),
                exc_info=True,
            )
            return event

    @staticmethod
    def _apply_business_rules(source: EventSource, payload: dict[str, Any]) -> None:
        """
        Placeholder for real domain/business validation.

        Raises a `ValueError` if the payload is structurally invalid, which
        the `execute` method above converts into a FAILED `ProcessedEvent`.
        """
        if not isinstance(payload, dict) or not payload:
            raise ValueError(f"Empty or invalid payload received from {source.value}")
