from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from src.adapters.metrics.registry import MetricsRegistry
from src.config.models import BufferSettings
from src.domain.entities.enums import AcquisitionMode, QualityCategory, ValidationState
from src.domain.entities.models import BufferedEvent, ParameterEvent
from src.domain.services.buffer_worker import BufferedDeliveryWorker


class BufferWithInvalidSourceEvent:
    def __init__(self, event: BufferedEvent) -> None:
        self.event = event
        self.dead_letter_error = None

    async def get_due_events(self, limit: int):
        return [] if self.dead_letter_error else [self.event]

    async def move_to_dead_letter(self, buffer_id: str, error: str) -> None:
        self.dead_letter_error = error

    async def mark_published(self, buffer_id: str) -> None:
        raise AssertionError("invalid source event must not be marked published")

    async def mark_failure(self, buffer_id: str, error: str) -> None:
        raise AssertionError("invalid source event must not be retried")


class PublisherMustNotBeCalled:
    async def publish(self, event: ParameterEvent) -> None:
        raise AssertionError("invalid source event must not reach the publisher")


@pytest.mark.asyncio
async def test_buffer_worker_dead_letters_legacy_event_without_source_uuid() -> None:
    parameter_event = ParameterEvent(
        source_id="remote-opc-lab",
        id_source=None,
        endpoint_id="remote-opc-server",
        owner_type="rig",
        owner_id="rig-01",
        parameter_code="PRESSURE",
        parameter_name="Pressure",
        node_id="ns=2;s=Pressure",
        value_normalized=12.3,
        value_type="float",
        quality=QualityCategory.GOOD,
        quality_code="Good",
        source_timestamp=datetime.now(UTC),
        validation_state=ValidationState.VALID,
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
    )
    buffered_event = BufferedEvent(
        id="buffer-1",
        event_id=parameter_event.event_id,
        payload=parameter_event,
        attempts=0,
        next_attempt_at=datetime.now(UTC),
        created_at=datetime.now(UTC),
    )
    buffer = BufferWithInvalidSourceEvent(buffered_event)
    worker = BufferedDeliveryWorker(
        buffer=buffer,
        publisher=PublisherMustNotBeCalled(),
        settings=BufferSettings(flush_interval_seconds=0.01),
        metrics=MetricsRegistry(),
    )

    await worker.start()
    for _ in range(20):
        if buffer.dead_letter_error:
            break
        await asyncio.sleep(0.01)
    await worker.stop()

    assert buffer.dead_letter_error == "id_source is required before publishing endpoint data."
