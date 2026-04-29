from __future__ import annotations

import asyncio
from datetime import UTC
from typing import Any

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
        body, message_type = self._build_message_body(event)
        message = aio_pika.Message(
            body=body,
            content_type="application/json",
            delivery_mode=(
                aio_pika.DeliveryMode.PERSISTENT
                if self.settings.durable
                else aio_pika.DeliveryMode.NOT_PERSISTENT
            ),
            message_id=event.event_id,
            timestamp=event.ingested_at,
            type=message_type,
            headers={
                "endpoint_id": event.endpoint_id,
                "node_id": event.node_id,
                "parameter_code": event.parameter_code,
                "message_format": self.settings.message_format,
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
                await self._declare_queue_dead_letter_exchange(self.settings.queue_name)
                queue = await self._channel.declare_queue(
                    self.settings.queue_name,
                    durable=self.settings.durable,
                    arguments=self._queue_arguments(self.settings.queue_name),
                )
                if self.settings.exchange:
                    await queue.bind(self._exchange, routing_key=self.settings.routing_key)

    def _build_message_body(self, event: ParameterEvent) -> tuple[bytes, str]:
        if self.settings.message_format == "params_validator_envelope":
            return self._build_params_validator_envelope(event), "params.validator.envelope"
        return event.model_dump_json().encode("utf-8"), "opcua.parameter_event"

    def _build_params_validator_envelope(self, event: ParameterEvent) -> bytes:
        source_time = event.source_timestamp or event.ingested_at
        server_time = event.server_timestamp or event.ingested_at
        if source_time.tzinfo is None:
            source_time = source_time.replace(tzinfo=UTC)
        if server_time.tzinfo is None:
            server_time = server_time.replace(tzinfo=UTC)

        envelope = {
            "schema_version": self.settings.schema_version,
            "message_id": event.event_id,
            "trace_id": event.event_id,
            "payload": {
                "name": event.parameter_code,
                "id_by_dict": event.id_by_dict,
                "value": event.value_normalized,
                "type": self._map_validator_type(event.value_type),
                "type_by_dict": event.type_by_dict,
                "status": self._status_to_uint32(event),
                "sourcetime": source_time.isoformat(),
                "servertime": server_time.isoformat(),
                "uom": event.unit,
                "uom_by_dict": event.uom_by_dict,
                "owner": event.owner_id,
                "source": event.source_id,
            },
            "validation": {},
            "metadata": {
                "opcua": {
                    "endpoint_id": event.endpoint_id,
                    "node_id": event.node_id,
                    "quality": event.quality,
                    "quality_code": event.quality_code,
                    "status_text": event.status_text,
                    "acquisition_mode": event.acquisition_mode,
                    "parameter_name": event.parameter_name,
                },
                "client_event": {
                    "sequence_id": event.sequence_id,
                    "validation_state": event.validation_state,
                    "validation_errors": event.validation_errors,
                    "tags": event.tags,
                },
            },
        }
        return self._dump_json(envelope)

    @staticmethod
    def _map_validator_type(value_type: str) -> str:
        normalized = value_type.strip().lower()
        mapping = {
            "bool": "boolean",
            "boolean": "boolean",
            "int": "integer",
            "integer": "integer",
            "float": "float",
            "double": "float",
            "real": "float",
            "str": "text",
            "string": "text",
            "text": "text",
            "char": "text",
            "bool[]": "array_boolean",
            "boolean[]": "array_boolean",
            "int[]": "array_integer",
            "integer[]": "array_integer",
            "float[]": "array_float",
            "double[]": "array_float",
            "str[]": "array_text",
            "string[]": "array_text",
            "text[]": "array_text",
        }
        return mapping.get(normalized, normalized)

    @staticmethod
    def _status_to_uint32(event: ParameterEvent) -> int:
        raw_status = event.metadata.get("opcua", {}).get("status_code_raw")
        try:
            if raw_status is not None:
                return max(0, min(int(raw_status), 0xFFFFFFFF))
        except (TypeError, ValueError):
            pass
        return 0 if event.quality_code.lower() == "good" else 1

    @staticmethod
    def _dump_json(payload: dict[str, Any]) -> bytes:
        from json import dumps

        return dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    def _queue_arguments(self, queue_name: str) -> dict[str, Any] | None:
        if self.settings.message_format != "params_validator_envelope":
            return None
        return {
            "x-dead-letter-exchange": f"{queue_name}.dlx",
            "x-dead-letter-routing-key": f"{queue_name}.dlq",
        }

    async def _declare_queue_dead_letter_exchange(self, queue_name: str) -> None:
        if self.settings.message_format != "params_validator_envelope":
            return
        assert self._channel is not None
        await self._channel.declare_exchange(
            f"{queue_name}.dlx",
            type="direct",
            durable=self.settings.durable,
        )
