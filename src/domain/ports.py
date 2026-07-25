"""
Ports (interfaces) of the hexagonal architecture.

The application/domain layers depend only on these abstractions.
Concrete implementations live in `src/infrastructure` and are injected
at composition time (see `main.py`).
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from src.domain.entities import ProcessedEvent


class EventRepositoryPort(ABC):
    """Output port: persistence of processed events."""

    @abstractmethod
    async def save(self, event: ProcessedEvent) -> None:
        """Persist a processed event. Must never raise for business
        reasons; infrastructure errors may propagate to the caller."""
        raise NotImplementedError


class MessageConsumerPort(ABC):
    """Input port: a long-running consumer that feeds messages into the
    application layer. Implemented by RabbitMQ / Kafka adapters."""

    @abstractmethod
    async def start(self) -> None:
        """Connect to the broker and start consuming until cancelled."""
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        """Gracefully release broker resources (connections, channels)."""
        raise NotImplementedError
