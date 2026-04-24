from __future__ import annotations

import inspect
import math
from datetime import UTC, datetime, timedelta
from typing import Any

from redis.asyncio import Redis

from src.config.models import BufferSettings
from src.domain.entities.errors import BufferPersistenceError
from src.domain.entities.models import BufferedEvent, DeadLetterEvent, ParameterEvent
from src.domain.ports.buffer import EventBuffer


class RedisEventBuffer(EventBuffer):
    def __init__(self, settings: BufferSettings, client: Redis | None = None) -> None:
        self.settings = settings
        self.redis = client

    async def start(self) -> None:
        if self.redis is None:
            self.redis = Redis.from_url(self.settings.redis_url, decode_responses=True)
        await self.redis.ping()
        await self._cleanup_expired()

    async def close(self) -> None:
        if self.redis is not None:
            await self.redis.aclose()

    async def enqueue(self, event: ParameterEvent, error: str) -> None:
        assert self.redis is not None
        now = datetime.now(UTC)
        key = self._event_key(event.event_id)
        payload = {
            "event_id": event.event_id,
            "payload_json": event.model_dump_json(),
            "attempts": "0",
            "next_attempt_at": self._dt_to_score(now),
            "last_error": error,
            "created_at": now.isoformat(),
        }
        try:
            async with self.redis.pipeline(transaction=True) as pipe:
                pipe.hset(key, mapping=payload)
                pipe.zadd(self._pending_key, {event.event_id: self._dt_to_score(now)})
                await pipe.execute()
        except Exception as exc:
            raise BufferPersistenceError(str(exc)) from exc

    async def get_due_events(self, limit: int) -> list[BufferedEvent]:
        assert self.redis is not None
        due_ids = await self.redis.zrangebyscore(
            self._pending_key,
            min="-inf",
            max=self._dt_to_score(datetime.now(UTC)),
            start=0,
            num=limit,
        )
        if not due_ids:
            return []
        async with self.redis.pipeline(transaction=False) as pipe:
            for event_id in due_ids:
                pipe.hgetall(self._event_key(event_id))
            rows = await pipe.execute()
        events: list[BufferedEvent] = []
        for event_id, row in zip(due_ids, rows, strict=False):
            if not row:
                await self.redis.zrem(self._pending_key, event_id)
                continue
            events.append(self._to_buffered_event(event_id, row))
        return events

    async def mark_published(self, buffer_id: str) -> None:
        assert self.redis is not None
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self._pending_key, buffer_id)
            pipe.delete(self._event_key(buffer_id))
            await pipe.execute()

    async def mark_failure(self, buffer_id: str, error: str) -> None:
        assert self.redis is not None
        row = await self._read_hash(self._event_key(buffer_id))
        if not row:
            await self.redis.zrem(self._pending_key, buffer_id)
            return
        attempts = int(row.get("attempts", "0")) + 1
        delay = min(
            self.settings.retry_max_delay_seconds,
            self.settings.retry_base_delay_seconds * math.pow(2, max(attempts - 1, 0)),
        )
        next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                self._event_key(buffer_id),
                mapping={
                    "attempts": str(attempts),
                    "next_attempt_at": self._dt_to_score(next_attempt_at),
                    "last_error": error,
                },
            )
            pipe.zadd(self._pending_key, {buffer_id: self._dt_to_score(next_attempt_at)})
            await pipe.execute()

    async def move_to_dead_letter(self, buffer_id: str, error: str) -> None:
        assert self.redis is not None
        row = await self._read_hash(self._event_key(buffer_id))
        if not row:
            await self.redis.zrem(self._pending_key, buffer_id)
            return
        attempts = int(row.get("attempts", "0")) + 1
        dead_key = self._dead_letter_event_key(buffer_id)
        dead_time = datetime.now(UTC)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                dead_key,
                mapping={
                    "event_id": row["event_id"],
                    "payload_json": row["payload_json"],
                    "error": error,
                    "attempts": str(attempts),
                    "created_at": dead_time.isoformat(),
                },
            )
            pipe.zadd(self._dead_letter_index_key, {buffer_id: self._dt_to_score(dead_time)})
            pipe.zrem(self._pending_key, buffer_id)
            pipe.delete(self._event_key(buffer_id))
            await pipe.execute()
        await self._cleanup_expired()

    async def stats(self) -> dict[str, int]:
        assert self.redis is not None
        buffered = int(await self.redis.zcard(self._pending_key))
        dead = int(await self.redis.zcard(self._dead_letter_index_key))
        return {"buffered_events": buffered, "dead_letter_events": dead}

    async def dead_letters(self, limit: int = 100) -> list[DeadLetterEvent]:
        assert self.redis is not None
        event_ids = await self.redis.zrevrange(self._dead_letter_index_key, 0, max(limit - 1, 0))
        if not event_ids:
            return []
        async with self.redis.pipeline(transaction=False) as pipe:
            for event_id in event_ids:
                pipe.hgetall(self._dead_letter_event_key(event_id))
            rows = await pipe.execute()
        dead_letters: list[DeadLetterEvent] = []
        for event_id, row in zip(event_ids, rows, strict=False):
            if not row:
                await self.redis.zrem(self._dead_letter_index_key, event_id)
                continue
            dead_letters.append(
                DeadLetterEvent(
                    id=event_id,
                    event_id=row["event_id"],
                    payload=ParameterEvent.model_validate_json(row["payload_json"]),
                    error=row["error"],
                    attempts=int(row["attempts"]),
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
            )
        return dead_letters

    async def _cleanup_expired(self) -> None:
        assert self.redis is not None
        cutoff = datetime.now(UTC) - timedelta(hours=self.settings.retention_hours)
        expired = await self.redis.zrangebyscore(
            self._dead_letter_index_key,
            min="-inf",
            max=self._dt_to_score(cutoff),
        )
        if not expired:
            return
        async with self.redis.pipeline(transaction=True) as pipe:
            for event_id in expired:
                pipe.zrem(self._dead_letter_index_key, event_id)
                pipe.delete(self._dead_letter_event_key(event_id))
            await pipe.execute()

    def _to_buffered_event(self, event_id: str, row: dict[str, Any]) -> BufferedEvent:
        return BufferedEvent(
            id=event_id,
            event_id=row["event_id"],
            payload=ParameterEvent.model_validate_json(row["payload_json"]),
            attempts=int(row["attempts"]),
            next_attempt_at=self._score_to_dt(row["next_attempt_at"]),
            last_error=row.get("last_error"),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _dt_to_score(value: datetime) -> float:
        return value.astimezone(UTC).timestamp()

    @staticmethod
    def _score_to_dt(value: str | float) -> datetime:
        return datetime.fromtimestamp(float(value), tz=UTC)

    def _event_key(self, event_id: str) -> str:
        return f"{self.settings.key_prefix}:buffer:event:{event_id}"

    def _dead_letter_event_key(self, event_id: str) -> str:
        return f"{self.settings.key_prefix}:buffer:dead:{event_id}"

    async def _read_hash(self, key: str) -> dict[str, Any]:
        assert self.redis is not None
        result = self.redis.hgetall(key)
        if inspect.isawaitable(result):
            return await result
        return result

    @property
    def _pending_key(self) -> str:
        return f"{self.settings.key_prefix}:buffer:pending"

    @property
    def _dead_letter_index_key(self) -> str:
        return f"{self.settings.key_prefix}:buffer:dead:index"
