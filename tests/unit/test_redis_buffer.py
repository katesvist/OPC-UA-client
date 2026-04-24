from __future__ import annotations

from datetime import UTC, datetime

import fakeredis.aioredis
import pytest

from src.adapters.buffer.redis_buffer import RedisEventBuffer
from src.config.models import BufferSettings
from src.domain.entities.enums import AcquisitionMode, QualityCategory, ValidationState
from src.domain.entities.models import ParameterEvent


def build_event() -> ParameterEvent:
    return ParameterEvent(
        source_id="source-1",
        endpoint_id="endpoint-1",
        owner_type="rig",
        owner_id="rig-1",
        parameter_code="PUMP_PRESSURE",
        parameter_name="Давление насоса",
        node_id="ns=2;s=Pump01.Pressure",
        value_raw=123.4,
        value_normalized=123.4,
        value_type="float",
        unit="bar",
        quality=QualityCategory.GOOD,
        quality_code="good",
        source_timestamp=datetime.now(UTC),
        server_timestamp=datetime.now(UTC),
        validation_state=ValidationState.VALID,
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
    )


@pytest.mark.asyncio
async def test_redis_buffer_enqueue_and_dead_letter() -> None:
    settings = BufferSettings(
        key_prefix="test-redis-buffer",
        retry_base_delay_seconds=0.01,
        retry_max_delay_seconds=0.05,
    )
    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    buffer = RedisEventBuffer(settings, client=redis_client)
    await buffer.start()

    event = build_event()
    await buffer.enqueue(event, "downstream unavailable")

    due_events = await buffer.get_due_events(limit=10)
    assert len(due_events) == 1
    assert due_events[0].event_id == event.event_id

    await buffer.mark_failure(due_events[0].id, "retry later")
    due_events_after_failure = await buffer.get_due_events(limit=10)
    assert due_events_after_failure == []

    await buffer.move_to_dead_letter(due_events[0].id, "too many retries")
    stats = await buffer.stats()
    assert stats == {"buffered_events": 0, "dead_letter_events": 1}

    dead_letters = await buffer.dead_letters()
    assert dead_letters[0].event_id == event.event_id
    assert dead_letters[0].error == "too many retries"

    await buffer.close()
