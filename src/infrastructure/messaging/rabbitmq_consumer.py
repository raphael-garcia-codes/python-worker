"""
RabbitMQ consumer adapter (`users.creation` queue), implemented with
aio-pika. Implements the `MessageConsumerPort`.
"""
from __future__ import annotations

import json

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection

from src.application.use_cases.process_message_usecase import ProcessMessageUseCase
from src.domain.entities import EventSource
from src.domain.ports import MessageConsumerPort
from src.infrastructure.config.logging_config import bind_correlation_id, get_logger

logger = get_logger(__name__)


class RabbitMQUsersCreationConsumer(MessageConsumerPort):
    """Consumes messages from the `users.creation` queue."""

    def __init__(
        self,
        amqp_url: str,
        queue_name: str,
        use_case: ProcessMessageUseCase,
        prefetch_count: int = 10,
    ) -> None:
        self._amqp_url = amqp_url
        self._queue_name = queue_name
        self._use_case = use_case
        self._prefetch_count = prefetch_count
        self._connection: AbstractRobustConnection | None = None

    async def start(self) -> None:
        logger.info("rabbitmq_connecting", queue=self._queue_name)

        self._connection = await aio_pika.connect_robust(self._amqp_url)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=self._prefetch_count)

        queue = await channel.declare_queue(self._queue_name, durable=True)

        logger.info("rabbitmq_consuming_started", queue=self._queue_name)
        await queue.consume(self._on_message)

    async def stop(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
            logger.info("rabbitmq_connection_closed", queue=self._queue_name)

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """
        `queue.consume` runs this callback per message. We ack/nack
        explicitly so a persistence failure (rather than a processing
        error, which the use case already handles) still results in a
        requeue instead of silent message loss.
        """
        bind_correlation_id()

        async with message.process(ignore_processed=True):
            try:
                payload = json.loads(message.body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.error(
                    "rabbitmq_invalid_payload",
                    error=str(exc),
                    raw_body=message.body[:500],
                )
                # Malformed payload cannot be retried meaningfully; ack and
                # move on so the queue is not blocked. In production this
                # would typically go to a dead-letter exchange instead.
                return

            logger.info("rabbitmq_message_received", queue=self._queue_name)
            await self._use_case.execute(
                source=EventSource.RABBITMQ_USERS_CREATION, payload=payload
            )
