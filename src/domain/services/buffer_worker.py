from __future__ import annotations

import asyncio
from contextlib import suppress

from src.adapters.logging.setup import get_logger
from src.adapters.metrics.registry import MetricsRegistry
from src.config.models import BufferSettings
from src.domain.ports.buffer import EventBuffer
from src.domain.ports.publisher import DownstreamPublisher


class BufferedDeliveryWorker:
    def __init__(
        self,
        buffer: EventBuffer,
        publisher: DownstreamPublisher,
        settings: BufferSettings,
        metrics: MetricsRegistry,
    ) -> None:
        self.buffer = buffer
        self.publisher = publisher
        self.settings = settings
        self.metrics = metrics
        self.logger = get_logger(__name__)
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="buffered-delivery-worker")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            due_events = await self.buffer.get_due_events(self.settings.flush_batch_size)
            for event in due_events:
                try:
                    await self.publisher.publish(event.payload)
                    await self.buffer.mark_published(event.id)
                except Exception as exc:
                    self.logger.warning(
                        "buffered_publish_failed",
                        buffer_id=event.id,
                        event_id=event.event_id,
                        attempts=event.attempts,
                        error=str(exc),
                    )
                    if event.attempts + 1 >= self.settings.max_attempts:
                        await self.buffer.move_to_dead_letter(event.id, str(exc))
                        self.metrics.inc_dead_letter_events(event.payload.endpoint_id)
                    else:
                        await self.buffer.mark_failure(event.id, str(exc))
                        self.metrics.inc_downstream_publish_failures(event.payload.endpoint_id)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.settings.flush_interval_seconds)
            except TimeoutError:
                continue
