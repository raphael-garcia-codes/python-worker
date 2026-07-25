"""
Kafka consumer adapter (`api.messages` topic), implemented with aiokafka.
Implements the `MessageConsumerPort`.
"""
from __future__ import annotations

import asyncio
import json

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from src.application.use_cases.process_message_usecase import ProcessMessageUseCase
from src.domain.entities import EventSource
from src.domain.ports import MessageConsumerPort
from src.infrastructure.config.logging_config import bind_correlation_id, get_logger

logger = get_logger(__name__)


class KafkaApiMessagesConsumer(MessageConsumerPort):
    """Consumes messages from the `api.messages` topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic_name: str,
        client_id: str,
        group_id: str,
        use_case: ProcessMessageUseCase,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic_name = topic_name
        self._client_id = client_id
        self._group_id = group_id
        self._use_case = use_case
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self) -> None:
        logger.info("kafka_connecting", topic=self._topic_name)

        self._consumer = AIOKafkaConsumer(
            self._topic_name,
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            group_id=self._group_id,
            value_deserializer=lambda v: v,  # raw bytes, decoded manually below
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

        await self._consumer.start()
        self._running = True
        logger.info("kafka_consuming_started", topic=self._topic_name)

        try:
            async for message in self._consumer:
                if not self._running:
                    break
                await self._handle_message(message)
        except asyncio.CancelledError:
            logger.info("kafka_consume_loop_cancelled", topic=self._topic_name)
            raise
        except KafkaError as exc:
            logger.error("kafka_consume_error", error=str(exc), exc_info=True)
            raise

    async def stop(self) -> None:
        self._running = False
        if self._consumer is not None:
            await self._consumer.stop()
            logger.info("kafka_connection_closed", topic=self._topic_name)

    async def _handle_message(self, message) -> None:  # type: ignore[no-untyped-def]
        bind_correlation_id()

        try:
            payload = json.loads(message.value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error(
                "kafka_invalid_payload",
                error=str(exc),
                partition=message.partition,
                offset=message.offset,
            )
            await self._consumer.commit()  # skip malformed message
            return

        logger.info(
            "kafka_message_received",
            topic=self._topic_name,
            partition=message.partition,
            offset=message.offset,
        )
        await self._use_case.execute(source=EventSource.KAFKA_API_MESSAGES, payload=payload)

        # Manual commit only after the use case has durably recorded the
        # outcome (success or failure) - this gives at-least-once delivery.
        await self._consumer.commit()
