"""
SQLAlchemy 2.0 ORM models (infrastructure layer).

Note: this is deliberately separate from `src.domain.entities.ProcessedEvent`.
The domain entity has no knowledge of SQLAlchemy; the repository is
responsible for mapping between the two.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ProcessedEventModel(Base):
    __tablename__ = "processed_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    source: Mapped[str] = mapped_column(nullable=False, index=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(nullable=False, index=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return (
            f"<ProcessedEventModel id={self.id} source={self.source} "
            f"status={self.status}>"
        )
