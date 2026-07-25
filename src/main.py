"""
Application entrypoint.

Wires up all infrastructure adapters (composition root), then runs both
consumers concurrently with `asyncio.gather`, handling graceful shutdown
on SIGINT/SIGTERM.
"""
from __future__ import annotations

import asyncio
import signal
from typing import Sequence

from src.application.use_cases.process_message_usecase import ProcessMessageUseCase
from src.domain.ports import MessageConsumerPort
from src.infrastructure.config.logging_config import configure_logging, get_logger
from src.infrastructure.config.settings import get_settings
from src.infrastructure.database.repositories.event_repository import (
    PostgresEventRepository,
)
from src.infrastructure.database.session import create_engine, create_session_factory
from src.infrastructure.messaging.kafka_consumer import KafkaApiMessagesConsumer
from src.infrastructure.messaging.rabbitmq_consumer import RabbitMQUsersCreationConsumer

logger = get_logger(__name__)


async def _run_consumer(consumer: MessageConsumerPort) -> None:
    """Wraps a consumer's `start()` so that one consumer crashing does not
    silently vanish - it is logged and the exception is re-raised so
    `asyncio.gather` can surface it and trigger shutdown of the others."""
    try:
        await consumer.start()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.error("consumer_crashed", consumer=type(consumer).__name__, exc_info=True)
        raise


async def _shutdown(consumers: Sequence[MessageConsumerPort]) -> None:
    logger.info("shutdown_initiated")
    await asyncio.gather(*(c.stop() for c in consumers), return_exceptions=True)
    logger.info("shutdown_complete")


async def main() -> None:
    settings = get_settings()
    configure_logging(log_level=settings.log_level, app_name=settings.app_name)

    logger.info("application_starting", app=settings.app_name)

    # --- Composition root: wire infrastructure into the application layer ---
    engine = create_engine(settings.postgres.async_dsn)
    session_factory = create_session_factory(engine)
    event_repository = PostgresEventRepository(session_factory=session_factory)
    use_case = ProcessMessageUseCase(event_repository=event_repository)

    rabbitmq_consumer = RabbitMQUsersCreationConsumer(
        amqp_url=settings.rabbitmq.url,
        queue_name=settings.rabbitmq.users_queue_name,
        use_case=use_case,
    )
    kafka_consumer = KafkaApiMessagesConsumer(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        topic_name=settings.kafka.topic_name,
        client_id=settings.kafka.client_id,
        group_id=settings.kafka.group_id,
        use_case=use_case,
    )

    consumers: list[MessageConsumerPort] = [rabbitmq_consumer, kafka_consumer]

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("shutdown_signal_received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    tasks = [asyncio.create_task(_run_consumer(c)) for c in consumers]
    stopper = asyncio.create_task(stop_event.wait())

    try:
        done, pending = await asyncio.wait(
            [*tasks, stopper], return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        for task in done:
            if task is not stopper and task.exception() is not None:
                logger.error("worker_task_failed", error=str(task.exception()))

    finally:
        await _shutdown(consumers)
        await engine.dispose()
        logger.info("application_stopped")


if __name__ == "__main__":
    asyncio.run(main())
