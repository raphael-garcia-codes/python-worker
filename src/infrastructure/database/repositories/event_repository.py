"""
Concrete adapter for `EventRepositoryPort`, backed by PostgreSQL via
SQLAlchemy 2.0 (async) / asyncpg.
"""
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.domain.entities import ProcessedEvent
from src.domain.ports import EventRepositoryPort
from src.infrastructure.database.models import ProcessedEventModel


class PostgresEventRepository(EventRepositoryPort):
    """Persists `ProcessedEvent` domain entities into `processed_events`."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save(self, event: ProcessedEvent) -> None:
        model = ProcessedEventModel(
            id=event.id,
            source=event.source.value,
            payload=event.payload,
            status=event.status.value,
            error_message=event.error_message,
            processed_at=event.processed_at,
        )
        async with self._session_factory() as session:
            async with session.begin():
                session.add(model)
