from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


class RawCaptureAlreadyActiveError(RuntimeError):
    pass


class RawCaptureFileExistsError(RuntimeError):
    pass


class RawNotificationCapture:
    def __init__(self, *, queue_size: int = 10000) -> None:
        self.queue_size = max(1, queue_size)
        self._queue: asyncio.Queue[dict[str, Any]] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._timer_task: asyncio.Task[None] | None = None
        self._accepting = False
        self._capture_id: str | None = None
        self._output_path: Path | None = None
        self._endpoint_id: str | None = None
        self._max_records = 0
        self._enqueued = 0
        self._dropped = 0

    async def start_capture(
        self,
        *,
        duration_seconds: float,
        output_path: str | None = None,
        endpoint_id: str | None = None,
        max_records: int = 100000,
    ) -> dict[str, Any]:
        if self._writer_task is not None and not self._writer_task.done():
            raise RawCaptureAlreadyActiveError("Raw OPC UA notification capture is already active.")

        duration_seconds = max(0.1, min(float(duration_seconds), 60.0))
        self._max_records = max(1, min(int(max_records), 1_000_000))
        self._enqueued = 0
        self._dropped = 0
        self._endpoint_id = endpoint_id
        self._capture_id = uuid4().hex
        self._output_path = Path(output_path) if output_path else self._default_output_path()
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._output_path.open("x", encoding="utf-8"):
                pass
        except FileExistsError as exc:
            raise RawCaptureFileExistsError(f"Capture file already exists: {self._output_path}") from exc
        except OSError:
            self._capture_id = None
            self._output_path = None
            self._endpoint_id = None
            raise

        self._queue = asyncio.Queue(maxsize=self.queue_size)
        self._accepting = True
        self._writer_task = asyncio.create_task(
            self._write_loop(self._output_path),
            name=f"opcua-raw-capture-writer-{self._capture_id}",
        )
        self._timer_task = asyncio.create_task(
            self._stop_accepting_after(duration_seconds),
            name=f"opcua-raw-capture-timer-{self._capture_id}",
        )
        return {
            "capture_id": self._capture_id,
            "started": True,
            "duration_seconds": duration_seconds,
            "output": str(self._output_path),
            "endpoint_id": endpoint_id,
            "max_records": self._max_records,
        }

    def record(self, record: dict[str, Any]) -> None:
        if not self._accepting or self._queue is None:
            return
        if self._endpoint_id is not None and record.get("endpoint_id") != self._endpoint_id:
            return
        if self._enqueued >= self._max_records:
            self._dropped += 1
            self._accepting = False
            return
        try:
            self._queue.put_nowait(record)
            self._enqueued += 1
        except asyncio.QueueFull:
            self._dropped += 1

    async def stop(self) -> None:
        self._accepting = False
        if self._timer_task is not None and not self._timer_task.done():
            self._timer_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._timer_task
        if self._writer_task is not None:
            try:
                await self._writer_task
            finally:
                self._writer_task = None
        self._timer_task = None

    async def _stop_accepting_after(self, duration_seconds: float) -> None:
        await asyncio.sleep(duration_seconds)
        self._accepting = False

    async def _write_loop(self, output_path: Path) -> None:
        assert self._queue is not None
        with output_path.open("a", encoding="utf-8") as handle:
            while self._accepting or not self._queue.empty():
                try:
                    record = await asyncio.wait_for(self._queue.get(), timeout=0.1)
                except TimeoutError:
                    continue
                handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str))
                handle.write("\n")
                self._queue.task_done()
            handle.write(
                json.dumps(
                    {
                        "capture_meta": {
                            "capture_id": self._capture_id,
                            "completed_at": datetime.now(UTC).isoformat(),
                            "records": self._enqueued,
                            "dropped": self._dropped,
                            "max_records": self._max_records,
                        }
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            handle.write("\n")

    def _default_output_path(self) -> Path:
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        return Path(f"/tmp/opcua-raw-capture-{timestamp}-{self._capture_id}.jsonl")
