from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.adapters.metrics.registry import MetricsRegistry
from src.domain.entities.enums import AcquisitionMode, ValidationState
from src.domain.entities.errors import DownstreamPublishError
from src.domain.entities.models import Observation
from src.domain.services.pipeline import EventPipeline


class InMemoryBuffer:
    def __init__(self) -> None:
        self.items = []

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def enqueue(self, event, error: str) -> None:
        self.items.append((event, error))

    async def get_due_events(self, limit: int):
        return []

    async def mark_published(self, buffer_id: str) -> None:
        return None

    async def mark_failure(self, buffer_id: str, error: str) -> None:
        return None

    async def move_to_dead_letter(self, buffer_id: str, error: str) -> None:
        return None

    async def stats(self) -> dict[str, int]:
        return {"buffered_events": len(self.items), "dead_letter_events": 0}

    async def dead_letters(self, limit: int = 100):
        return []


class PublisherOk:
    def __init__(self) -> None:
        self.events = []

    async def publish(self, event) -> None:
        self.events.append(event)

    async def close(self) -> None:
        return None


class PublisherFailOnce:
    def __init__(self) -> None:
        self.calls = 0

    async def publish(self, event) -> None:
        self.calls += 1
        raise RuntimeError("downstream unavailable")

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_pipeline_builds_valid_event(endpoint_config, node_config) -> None:
    publisher = PublisherOk()
    buffer = InMemoryBuffer()
    pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=MetricsRegistry())
    observation = Observation(
        endpoint_id=endpoint_config.id,
        source_id=endpoint_config.metadata.source_id,
        owner_type=endpoint_config.metadata.owner_type,
        owner_id=endpoint_config.metadata.owner_id,
        node_id=node_config.node_id,
        raw_value=12.3,
        status_code="Good",
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
        source_timestamp=datetime.now(UTC),
    )

    event = await pipeline.process(observation, endpoint_config, node_config)

    assert event is not None
    assert event.validation_state == ValidationState.VALID
    assert publisher.events[0].parameter_code == "PUMP_PRESSURE"
    assert event.metadata["source_binding"]["source_id"] == "source-1"
    assert event.metadata["node_registry"]["id"] == "node-1"


@pytest.mark.asyncio
async def test_pipeline_buffers_on_publish_failure(endpoint_config, node_config) -> None:
    publisher = PublisherFailOnce()
    buffer = InMemoryBuffer()
    pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=MetricsRegistry())
    observation = Observation(
        endpoint_id=endpoint_config.id,
        source_id=endpoint_config.metadata.source_id,
        owner_type=endpoint_config.metadata.owner_type,
        owner_id=endpoint_config.metadata.owner_id,
        node_id=node_config.node_id,
        raw_value=12.3,
        status_code="Good",
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
        source_timestamp=datetime.now(UTC),
    )

    with pytest.raises(DownstreamPublishError):
        await pipeline.process(observation, endpoint_config, node_config)

    assert len(buffer.items) == 1


@pytest.mark.asyncio
async def test_pipeline_normalizes_char_value_to_symbol(endpoint_config, node_config) -> None:
    publisher = PublisherOk()
    buffer = InMemoryBuffer()
    pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=MetricsRegistry())
    char_node = node_config.model_copy(
        update={
            "id": "node-char",
            "node_id": "ns=3;s=DB_For_Test.cTest",
            "parameter_code": "CHAR_TEST",
            "parameter_name": "CHAR test",
            "expected_type": "char",
            "unit": "char",
        }
    )
    observation = Observation(
        endpoint_id=endpoint_config.id,
        source_id=endpoint_config.metadata.source_id,
        owner_type=endpoint_config.metadata.owner_type,
        owner_id=endpoint_config.metadata.owner_id,
        node_id=char_node.node_id,
        raw_value=90,
        status_code="Good",
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
        source_timestamp=datetime.now(UTC),
    )

    event = await pipeline.process(observation, endpoint_config, char_node)

    assert event is not None
    assert event.value_raw == 90
    assert event.value_normalized == "Z"
    assert event.value_type == "char"


@pytest.mark.asyncio
async def test_pipeline_normalizes_integer_array(endpoint_config, node_config) -> None:
    publisher = PublisherOk()
    buffer = InMemoryBuffer()
    pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=MetricsRegistry())
    array_node = node_config.model_copy(
        update={
            "id": "node-array",
            "node_id": 'ns=3;s="DB_For_Test"."testLoad"',
            "parameter_code": "ARRAY_TEST",
            "parameter_name": "ARRAY test",
            "expected_type": "int",
            "value_shape": "array",
            "unit": "unit",
        }
    )
    observation = Observation(
        endpoint_id=endpoint_config.id,
        source_id=endpoint_config.metadata.source_id,
        owner_type=endpoint_config.metadata.owner_type,
        owner_id=endpoint_config.metadata.owner_id,
        node_id=array_node.node_id,
        raw_value=[1, 2, 3],
        data_type="Int16",
        value_rank=1,
        array_dimensions=[3],
        status_code="Good",
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
        source_timestamp=datetime.now(UTC),
    )

    event = await pipeline.process(observation, endpoint_config, array_node)

    assert event is not None
    assert event.value_raw == [1, 2, 3]
    assert event.value_normalized == [1, 2, 3]
    assert event.value_type == "int[]"
    assert event.metadata["node_registry"]["value_shape"] == "array"
    assert event.metadata["value_rank"] == 1
    assert event.metadata["array_dimensions"] == [3]
