from __future__ import annotations

from typing import Protocol

from src.domain.entities.models import ParameterEvent


class DownstreamPublisher(Protocol):
    async def publish(self, event: ParameterEvent) -> None: ...

    async def close(self) -> None: ...
