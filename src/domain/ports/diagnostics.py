from __future__ import annotations

from typing import Any, Protocol


class DiagnosticsStore(Protocol):
    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def record_publish_decision(self, record: dict[str, Any]) -> None: ...

    async def publish_audit(
        self,
        *,
        limit: int = 500,
        endpoint_id: str | None = None,
        node_id: str | None = None,
        decision: str | None = None,
        status_code: int | None = None,
    ) -> list[dict[str, Any]]: ...

    async def publish_stats(self) -> dict[str, Any]: ...

    async def status_alarms(self) -> list[dict[str, Any]]: ...

    async def status_alarm_history(self, *, limit: int = 200) -> list[dict[str, Any]]: ...
