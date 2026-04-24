from __future__ import annotations

from datetime import UTC, datetime

import fakeredis.aioredis
import pytest

from src.adapters.buffer.redis_buffer import RedisEventBuffer
from src.adapters.metrics.registry import MetricsRegistry
from src.config.models import BufferSettings
from src.domain.entities.enums import AcquisitionMode
from src.domain.entities.errors import DownstreamPublishError
from src.domain.entities.models import Observation
from src.domain.services.buffer_worker import BufferedDeliveryWorker
from src.domain.services.pipeline import EventPipeline
from tests.integration.helpers import wait_until


class FlakyPublisher:
    def __init__(self) -> None:
        self.calls = 0
        self.events = []

    async def publish(self, event) -> None:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("publisher offline")
        self.events.append(event)

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_buffer_flushes_after_retry(tmp_path, endpoint_config, node_config) -> None:
    settings = BufferSettings(
        key_prefix="test-buffer-retry",
        flush_interval_seconds=0.2,
        retry_base_delay_seconds=0.1,
        retry_max_delay_seconds=0.2,
        max_attempts=3,
    )
    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    buffer = RedisEventBuffer(settings, client=redis_client)
    await buffer.start()
    publisher = FlakyPublisher()
    metrics = MetricsRegistry()
    pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=metrics)
    worker = BufferedDeliveryWorker(buffer=buffer, publisher=publisher, settings=settings, metrics=metrics)
    await worker.start()

    observation = Observation(
        endpoint_id=endpoint_config.id,
        source_id=endpoint_config.metadata.source_id,
        owner_type=endpoint_config.metadata.owner_type,
        owner_id=endpoint_config.metadata.owner_id,
        node_id=node_config.node_id,
        raw_value=18.2,
        status_code="Good",
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
        source_timestamp=datetime.now(UTC),
    )

    with pytest.raises(DownstreamPublishError):
        await pipeline.process(observation, endpoint_config, node_config)

    await wait_until(lambda: len(publisher.events) == 1, timeout_seconds=5)
    stats = await buffer.stats()
    assert stats["buffered_events"] == 0

    await worker.stop()
    await buffer.close()
