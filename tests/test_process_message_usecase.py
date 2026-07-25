"""
Because the use case only depends on the `EventRepositoryPort` abstraction
(injected via constructor), it can be unit-tested with a simple in-memory
fake, with zero database, zero broker, and zero network calls.
"""
import asyncio

import pytest

from src.application.use_cases.process_message_usecase import ProcessMessageUseCase
from src.domain.entities import EventSource, EventStatus, ProcessedEvent
from src.domain.ports import EventRepositoryPort


class FakeEventRepository(EventRepositoryPort):
    def __init__(self) -> None:
        self.saved_events: list[ProcessedEvent] = []

    async def save(self, event: ProcessedEvent) -> None:
        self.saved_events.append(event)


@pytest.mark.asyncio
async def test_execute_saves_processed_event_on_success() -> None:
    repo = FakeEventRepository()
    use_case = ProcessMessageUseCase(event_repository=repo)

    event = await use_case.execute(
        source=EventSource.KAFKA_API_MESSAGES, payload={"user_id": 123}
    )

    assert event.status == EventStatus.PROCESSED
    assert repo.saved_events == [event]


@pytest.mark.asyncio
async def test_execute_saves_failed_event_on_empty_payload() -> None:
    repo = FakeEventRepository()
    use_case = ProcessMessageUseCase(event_repository=repo)

    event = await use_case.execute(source=EventSource.RABBITMQ_USERS_CREATION, payload={})

    assert event.status == EventStatus.FAILED
    assert event.error_message is not None
    assert repo.saved_events == [event]


if __name__ == "__main__":
    asyncio.run(test_execute_saves_processed_event_on_success())
