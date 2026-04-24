from __future__ import annotations

import asyncio

import aio_pika

from src.config.models import PublisherSettings
from src.domain.entities.models import ParameterEvent


class RabbitMqPublisher:
    def __init__(self, settings: PublisherSettings) -> None:
        self.settings = settings
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None
        self._lock = asyncio.Lock()

    async def publish(self, event: ParameterEvent) -> None:
        await self._ensure_connected()
        assert self._exchange is not None
        message = aio_pika.Message(
            body=event.model_dump_json().encode("utf-8"),
            content_type="application/json",
            delivery_mode=(
                aio_pika.DeliveryMode.PERSISTENT
                if self.settings.durable
                else aio_pika.DeliveryMode.NOT_PERSISTENT
            ),
            message_id=event.event_id,
            timestamp=event.ingested_at,
            type="opcua.parameter_event",
            headers={
                "endpoint_id": event.endpoint_id,
                "node_id": event.node_id,
                "parameter_code": event.parameter_code,
            },
        )
        await self._exchange.publish(message, routing_key=self.settings.routing_key)

    async def close(self) -> None:
        if self._channel is not None and not self._channel.is_closed:
            await self._channel.close()
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
        self._channel = None
        self._connection = None
        self._exchange = None

    async def _ensure_connected(self) -> None:
        if (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
            and self._exchange is not None
        ):
            return
        async with self._lock:
            if (
                self._connection is not None
                and not self._connection.is_closed
                and self._channel is not None
                and not self._channel.is_closed
                and self._exchange is not None
            ):
                return
            self._connection = await aio_pika.connect_robust(self.settings.url, timeout=self.settings.timeout_seconds)
            self._channel = await self._connection.channel()
            assert self._channel is not None
            if self.settings.exchange:
                if self.settings.declare_exchange:
                    self._exchange = await self._channel.declare_exchange(
                        self.settings.exchange,
                        type=self.settings.exchange_type,
                        durable=self.settings.durable,
                    )
                else:
                    self._exchange = await self._channel.get_exchange(self.settings.exchange, ensure=False)
            else:
                self._exchange = self._channel.default_exchange

            if self.settings.queue_name and self.settings.declare_queue:
                queue = await self._channel.declare_queue(
                    self.settings.queue_name,
                    durable=self.settings.durable,
                )
                if self.settings.exchange:
                    await queue.bind(self._exchange, routing_key=self.settings.routing_key)
