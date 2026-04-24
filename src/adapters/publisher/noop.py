from __future__ import annotations

from src.domain.entities.models import ParameterEvent
from src.domain.ports.publisher import DownstreamPublisher


class NoopPublisher(DownstreamPublisher):
    def __init__(self) -> None:
        self.events: list[ParameterEvent] = []

    async def publish(self, event: ParameterEvent) -> None:
        self.events.append(event)

    async def close(self) -> None:
        return None
