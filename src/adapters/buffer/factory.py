from __future__ import annotations

from src.adapters.buffer.redis_buffer import RedisEventBuffer
from src.config.models import BufferSettings
from src.domain.ports.buffer import EventBuffer


def create_buffer(settings: BufferSettings) -> EventBuffer:
    return RedisEventBuffer(settings)
