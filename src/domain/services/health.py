from __future__ import annotations

from src.domain.entities.enums import ConnectionState
from src.domain.entities.models import EndpointStatus


class HealthService:
    def readiness(self, endpoints: list[EndpointStatus], buffer_ready: bool) -> tuple[bool, dict[str, object]]:
        endpoint_ready = all(
            status.state in {ConnectionState.CONNECTED, ConnectionState.DEGRADED}
            for status in endpoints
        ) if endpoints else False
        ready = buffer_ready and endpoint_ready
        payload = {
            "ready": ready,
            "buffer_ready": buffer_ready,
            "endpoints": [status.model_dump(mode="json") for status in endpoints],
        }
        return ready, payload
