from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.adapters.metrics.registry import MetricsRegistry
from src.domain.entities.enums import AcquisitionMode, ValidationState
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


class DiagnosticsRecorder:
    def __init__(self) -> None:
        self.records = []

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def record_publish_decision(self, record) -> None:
        self.records.append(record)

    async def publish_audit(self, *, limit=500, endpoint_id=None, node_id=None, decision=None, status_code=None):
        return self.records[:limit]

    async def publish_stats(self):
        return {}

    async def status_alarms(self):
        return []

    async def status_alarm_history(self, *, limit=200):
        return []


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
    assert publisher.events[0].id_source == "22222222-2222-2222-2222-222222222222"
    assert event.metadata["source_binding"]["source_id"] == "source-1"
    assert event.metadata["source_binding"]["id_source"] == "22222222-2222-2222-2222-222222222222"
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

    event = await pipeline.process(observation, endpoint_config, node_config)

    assert event is not None
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


@pytest.mark.asyncio
async def test_pipeline_converts_unexpected_normalization_error_to_invalid_event(endpoint_config, node_config) -> None:
    publisher = PublisherOk()
    buffer = InMemoryBuffer()
    diagnostics = DiagnosticsRecorder()
    pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=MetricsRegistry(), diagnostics=diagnostics)
    observation = Observation(
        endpoint_id=endpoint_config.id,
        source_id=endpoint_config.metadata.source_id,
        owner_type=endpoint_config.metadata.owner_type,
        owner_id=endpoint_config.metadata.owner_id,
        node_id=node_config.node_id,
        raw_value=datetime.now(UTC),
        status_code="Good",
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
        source_timestamp=datetime.now(UTC),
    )

    event = await pipeline.process(observation, endpoint_config, node_config)

    assert event is not None
    assert event.validation_state == ValidationState.INVALID
    assert len(publisher.events) == 1
    assert diagnostics.records[0]["decision"] == "published"


@pytest.mark.asyncio
async def test_pipeline_suppresses_same_value_and_status(endpoint_config, node_config) -> None:
    publisher = PublisherOk()
    buffer = InMemoryBuffer()
    diagnostics = DiagnosticsRecorder()
    pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=MetricsRegistry(), diagnostics=diagnostics)
    first_timestamp = datetime.now(UTC)
    second_timestamp = first_timestamp + timedelta(seconds=1)

    first = Observation(
        endpoint_id=endpoint_config.id,
        source_id=endpoint_config.metadata.source_id,
        owner_type=endpoint_config.metadata.owner_type,
        owner_id=endpoint_config.metadata.owner_id,
        node_id=node_config.node_id,
        raw_value=12.3,
        status_code="Good",
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
        source_timestamp=first_timestamp,
        metadata={"opcua": {"status_code_raw": 0}},
    )
    duplicate = first.model_copy(update={"source_timestamp": second_timestamp})

    first_event = await pipeline.process(first, endpoint_config, node_config)
    duplicate_event = await pipeline.process(duplicate, endpoint_config, node_config)

    assert first_event is not None
    assert duplicate_event is None
    assert len(publisher.events) == 1
    assert [record["decision"] for record in diagnostics.records] == ["published", "suppressed"]
    assert diagnostics.records[1]["reason"] == "unchanged_value_and_status"


@pytest.mark.asyncio
async def test_pipeline_publishes_same_value_when_status_changes(endpoint_config, node_config) -> None:
    publisher = PublisherOk()
    buffer = InMemoryBuffer()
    diagnostics = DiagnosticsRecorder()
    pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=MetricsRegistry(), diagnostics=diagnostics)
    timestamp = datetime.now(UTC)

    first = Observation(
        endpoint_id=endpoint_config.id,
        source_id=endpoint_config.metadata.source_id,
        owner_type=endpoint_config.metadata.owner_type,
        owner_id=endpoint_config.metadata.owner_id,
        node_id=node_config.node_id,
        raw_value=12.3,
        status_code="Good",
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
        source_timestamp=timestamp,
        metadata={"opcua": {"status_code_raw": 3080192}},
    )
    status_recovery = first.model_copy(update={"metadata": {"opcua": {"status_code_raw": 0}}})

    first_event = await pipeline.process(first, endpoint_config, node_config)
    recovery_event = await pipeline.process(status_recovery, endpoint_config, node_config)

    assert first_event is not None
    assert recovery_event is not None
    assert len(publisher.events) == 2
    assert [record["published_status"] for record in diagnostics.records] == [3080192, 0]


@pytest.mark.asyncio
async def test_pipeline_suppresses_same_published_status_when_raw_status_is_missing(endpoint_config, node_config) -> None:
    publisher = PublisherOk()
    buffer = InMemoryBuffer()
    pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=MetricsRegistry())
    timestamp = datetime.now(UTC)

    first = Observation(
        endpoint_id=endpoint_config.id,
        source_id=endpoint_config.metadata.source_id,
        owner_type=endpoint_config.metadata.owner_type,
        owner_id=endpoint_config.metadata.owner_id,
        node_id=node_config.node_id,
        raw_value=12.3,
        status_code="Good",
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
        source_timestamp=timestamp,
        metadata={"opcua": {"status_code_raw": 0}},
    )
    duplicate_without_raw_status = first.model_copy(
        update={
            "source_timestamp": timestamp + timedelta(seconds=1),
            "metadata": {"opcua": {}},
        }
    )

    first_event = await pipeline.process(first, endpoint_config, node_config)
    duplicate_event = await pipeline.process(duplicate_without_raw_status, endpoint_config, node_config)

    assert first_event is not None
    assert duplicate_event is None
    assert len(publisher.events) == 1
