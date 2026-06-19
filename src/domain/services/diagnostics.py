from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis

from src.config.models import BufferSettings, DiagnosticsSettings
from src.domain.entities.models import ParameterEvent
from src.domain.ports.diagnostics import DiagnosticsStore

NORMAL_STATUS_CODE = 0
GOOD_OVERLOAD_STATUS_CODE = 3_080_192
GOOD_OVERLOAD_STATUS_NAME = "GoodOverload"


class NoopDiagnosticsStore:
    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def record_publish_decision(self, record: dict[str, Any]) -> None:
        return None

    async def record_connection_event(self, record: dict[str, Any]) -> None:
        return None

    async def publish_audit(
        self,
        *,
        limit: int = 500,
        endpoint_id: str | None = None,
        node_id: str | None = None,
        decision: str | None = None,
        status_code: int | None = None,
    ) -> list[dict[str, Any]]:
        return []

    async def publish_stats(self) -> dict[str, Any]:
        return {
            "enabled": False,
            "window_records": 0,
            "decisions": {},
            "statuses": {},
            "top_nodes": [],
            "latest_at": None,
        }

    async def status_overload_counter(self) -> dict[str, Any]:
        return {
            "enabled": False,
            "status_code": GOOD_OVERLOAD_STATUS_CODE,
            "status_name": GOOD_OVERLOAD_STATUS_NAME,
            "count": 0,
            "started_at": None,
            "elapsed_seconds": 0,
            "last_seen_at": None,
            "last_node_id": None,
            "last_parameter_code": None,
        }

    async def set_status_overload_counter_enabled(self, enabled: bool) -> dict[str, Any]:
        return await self.status_overload_counter()

    async def status_alarms(self) -> list[dict[str, Any]]:
        return []

    async def status_alarm_history(self, *, limit: int = 200) -> list[dict[str, Any]]:
        return []

    async def connection_events(
        self,
        *,
        limit: int = 200,
        endpoint_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return []


class RedisDiagnosticsStore:
    def __init__(
        self,
        settings: DiagnosticsSettings,
        buffer_settings: BufferSettings,
        client: Redis | None = None,
    ) -> None:
        self.settings = settings
        self.redis = client
        self._redis_url = settings.redis_url or buffer_settings.redis_url

    async def start(self) -> None:
        if self.redis is None:
            self.redis = Redis.from_url(self._redis_url, decode_responses=True)
        await self.redis.ping()

    async def close(self) -> None:
        if self.redis is not None:
            await self.redis.aclose()

    async def record_publish_decision(self, record: dict[str, Any]) -> None:
        if not self.settings.enabled:
            return
        assert self.redis is not None
        serialized = json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.lpush(self._audit_key, serialized)
            pipe.ltrim(self._audit_key, 0, max(self.settings.max_records - 1, 0))
            pipe.expire(self._audit_key, self.settings.ttl_seconds)
            await pipe.execute()
        await self._update_status_overload_counter(record)

    async def record_connection_event(self, record: dict[str, Any]) -> None:
        if not self.settings.enabled:
            return
        assert self.redis is not None
        payload = {
            "recorded_at": datetime.now(UTC).isoformat(),
            **record,
        }
        serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.lpush(self._connection_events_key, serialized)
            pipe.ltrim(self._connection_events_key, 0, max(self.settings.max_records - 1, 0))
            pipe.expire(self._connection_events_key, self.settings.ttl_seconds)
            await pipe.execute()

    async def publish_audit(
        self,
        *,
        limit: int = 500,
        endpoint_id: str | None = None,
        node_id: str | None = None,
        decision: str | None = None,
        status_code: int | None = None,
    ) -> list[dict[str, Any]]:
        assert self.redis is not None
        max_limit = max(1, min(limit, self.settings.max_records))
        rows = await self.redis.lrange(self._audit_key, 0, max_limit - 1)
        records = [self._loads(row) for row in rows]
        return [
            record
            for record in records
            if (endpoint_id is None or record.get("endpoint_id") == endpoint_id)
            and (node_id is None or record.get("node_id") == node_id)
            and (decision is None or record.get("decision") == decision)
            and (status_code is None or record.get("published_status") == status_code)
        ]

    async def publish_stats(self) -> dict[str, Any]:
        assert self.redis is not None
        rows = await self.redis.lrange(self._audit_key, 0, max(self.settings.max_records - 1, 0))
        records = [self._loads(row) for row in rows]
        decisions: Counter[str] = Counter()
        statuses: Counter[str] = Counter()
        nodes: Counter[str] = Counter()
        latest_at: str | None = None
        for record in records:
            decisions[str(record.get("decision") or "unknown")] += 1
            status_name = str(record.get("status_name") or record.get("quality_code") or record.get("published_status"))
            statuses[status_name] += 1
            node_label = str(record.get("parameter_code") or record.get("node_id") or "-")
            nodes[node_label] += 1
            latest_at = latest_at or record.get("recorded_at")
        return {
            "enabled": True,
            "window_records": len(records),
            "max_records": self.settings.max_records,
            "ttl_seconds": self.settings.ttl_seconds,
            "decisions": dict(decisions),
            "statuses": dict(statuses),
            "top_nodes": [{"node": node, "count": count} for node, count in nodes.most_common(20)],
            "latest_at": latest_at,
        }

    async def status_alarms(self) -> list[dict[str, Any]]:
        return []

    async def status_overload_counter(self) -> dict[str, Any]:
        assert self.redis is not None
        raw = await self.redis.hgetall(self._status_overload_counter_key)
        enabled = self._counter_enabled(raw)
        started_at = raw.get("started_at") if enabled else None
        if enabled and not started_at:
            started_at = datetime.now(UTC).isoformat()
            await self.redis.hset(
                self._status_overload_counter_key,
                mapping={
                    "enabled": "true",
                    "status_code": str(GOOD_OVERLOAD_STATUS_CODE),
                    "status_name": GOOD_OVERLOAD_STATUS_NAME,
                    "count": raw.get("count") or "0",
                    "started_at": started_at,
                    "last_seen_at": raw.get("last_seen_at") or "",
                    "last_node_id": raw.get("last_node_id") or "",
                    "last_parameter_code": raw.get("last_parameter_code") or "",
                },
            )
        started_dt = self._parse_dt(started_at)
        elapsed_seconds = int((datetime.now(UTC) - started_dt).total_seconds()) if started_dt else 0
        return {
            "enabled": enabled,
            "status_code": GOOD_OVERLOAD_STATUS_CODE,
            "status_name": GOOD_OVERLOAD_STATUS_NAME,
            "count": int(raw.get("count") or 0) if enabled else 0,
            "started_at": started_at,
            "elapsed_seconds": max(0, elapsed_seconds),
            "last_seen_at": raw.get("last_seen_at") if enabled else None,
            "last_node_id": raw.get("last_node_id") if enabled else None,
            "last_parameter_code": raw.get("last_parameter_code") if enabled else None,
        }

    async def set_status_overload_counter_enabled(self, enabled: bool) -> dict[str, Any]:
        assert self.redis is not None
        now = datetime.now(UTC).isoformat()
        mapping = {
            "enabled": "true" if enabled else "false",
            "status_code": str(GOOD_OVERLOAD_STATUS_CODE),
            "status_name": GOOD_OVERLOAD_STATUS_NAME,
            "count": "0",
            "started_at": now if enabled else "",
            "last_seen_at": "",
            "last_node_id": "",
            "last_parameter_code": "",
        }
        await self.redis.hset(self._status_overload_counter_key, mapping=mapping)
        return await self.status_overload_counter()

    async def status_alarm_history(self, *, limit: int = 200) -> list[dict[str, Any]]:
        assert self.redis is not None
        rows = await self.redis.lrange(self._status_history_key, 0, max(limit - 1, 0))
        return [self._loads(row) for row in rows]

    async def connection_events(
        self,
        *,
        limit: int = 200,
        endpoint_id: str | None = None,
    ) -> list[dict[str, Any]]:
        assert self.redis is not None
        max_limit = max(1, min(limit, self.settings.max_records))
        rows = await self.redis.lrange(self._connection_events_key, 0, max_limit - 1)
        records = [self._loads(row) for row in rows]
        return [
            record
            for record in records
            if endpoint_id is None or record.get("endpoint_id") == endpoint_id
        ]

    async def _update_status_overload_counter(self, record: dict[str, Any]) -> None:
        assert self.redis is not None
        if not self._is_good_overload(record):
            return

        existing = await self.redis.hgetall(self._status_overload_counter_key)
        if not self._counter_enabled(existing):
            return
        now = datetime.now(UTC).isoformat()
        count = int(existing.get("count", "0")) + 1
        mapping = {
            "enabled": "true",
            "status_code": str(GOOD_OVERLOAD_STATUS_CODE),
            "status_name": GOOD_OVERLOAD_STATUS_NAME,
            "count": str(count),
            "started_at": existing.get("started_at") or now,
            "last_seen_at": now,
            "last_node_id": str(record.get("node_id") or ""),
            "last_parameter_code": str(record.get("parameter_code") or ""),
        }
        await self.redis.hset(self._status_overload_counter_key, mapping=mapping)

    def _is_good_overload(self, record: dict[str, Any]) -> bool:
        status_code = int(record.get("published_status") or 0)
        status_name = str(record.get("status_name") or record.get("quality_code") or "")
        return status_code == GOOD_OVERLOAD_STATUS_CODE or status_name.lower() == "goodoverload"

    def _counter_enabled(self, raw: dict[str, str]) -> bool:
        if not raw:
            return True
        return raw.get("enabled", "true").lower() == "true"

    async def _remove_node_from_status(self, node_key: str, raw_status: str, record: dict[str, Any]) -> None:
        assert self.redis is not None
        await self.redis.srem(self._status_nodes_key(raw_status), node_key)
        remaining = int(await self.redis.scard(self._status_nodes_key(raw_status)))
        if remaining > 0:
            return
        active_key = self._active_status_key(raw_status)
        alarm = await self.redis.hgetall(active_key)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.delete(active_key)
            pipe.srem(self._active_status_index_key, raw_status)
            if alarm:
                pipe.lpush(self._status_history_key, self._dump_history("cleared", alarm, record))
                pipe.ltrim(self._status_history_key, 0, max(self.settings.max_records - 1, 0))
                pipe.expire(self._status_history_key, self.settings.ttl_seconds)
            await pipe.execute()

    async def _sample_nodes(self, raw_status: str) -> list[str]:
        assert self.redis is not None
        return sorted(await self.redis.srandmember(self._status_nodes_key(raw_status), 20) or [])

    def _dump_history(self, action: str, alarm: dict[str, Any], record: dict[str, Any]) -> str:
        payload = {
            "action": action,
            "at": datetime.now(UTC).isoformat(),
            "status_code": int(alarm.get("status_code", "0")),
            "status_name": alarm.get("status_name"),
            "message": alarm.get("message"),
            "node_id": record.get("node_id"),
            "parameter_code": record.get("parameter_code"),
            "endpoint_id": record.get("endpoint_id"),
        }
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    def _severity(self, *, status_code: int, status_name: str, affected_nodes: int) -> str:
        normalized = status_name.lower()
        if normalized.startswith("bad"):
            return "critical"
        if "overload" in normalized and affected_nodes >= self.settings.overload_major_node_threshold:
            return "major"
        if "overload" in normalized:
            return "warning"
        if normalized.startswith("uncertain"):
            return "warning"
        if status_code != NORMAL_STATUS_CODE:
            return "warning"
        return "info"

    def _alarm_message(self, status_code: int, status_name: str) -> str:
        if "overload" in status_name.lower():
            return "OPC UA server reports sampling overload for one or more nodes."
        if status_name.lower().startswith("bad"):
            return "OPC UA server reports bad quality/status for one or more nodes."
        return f"OPC UA server reports non-normal status {status_name or status_code}."

    def _loads(self, raw: str) -> dict[str, Any]:
        try:
            loaded = json.loads(raw)
            return loaded if isinstance(loaded, dict) else {"raw": loaded}
        except json.JSONDecodeError:
            return {"raw": raw}

    def _parse_dt(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @property
    def _audit_key(self) -> str:
        return f"{self.settings.key_prefix}:diagnostics:publish:audit"

    @property
    def _node_status_key(self) -> str:
        return f"{self.settings.key_prefix}:diagnostics:status:node-current"

    @property
    def _active_status_index_key(self) -> str:
        return f"{self.settings.key_prefix}:diagnostics:status:active:index"

    @property
    def _status_history_key(self) -> str:
        return f"{self.settings.key_prefix}:diagnostics:status:history"

    @property
    def _connection_events_key(self) -> str:
        return f"{self.settings.key_prefix}:diagnostics:connection:events"

    @property
    def _status_overload_counter_key(self) -> str:
        return f"{self.settings.key_prefix}:diagnostics:status-overload:counter"

    def _active_status_key(self, raw_status: str) -> str:
        return f"{self.settings.key_prefix}:diagnostics:status:active:{raw_status}"

    def _status_nodes_key(self, raw_status: str) -> str:
        return f"{self.settings.key_prefix}:diagnostics:status:nodes:{raw_status}"


def create_diagnostics_store(
    settings: DiagnosticsSettings,
    buffer_settings: BufferSettings,
) -> DiagnosticsStore:
    if not settings.enabled:
        return NoopDiagnosticsStore()
    return RedisDiagnosticsStore(settings, buffer_settings)


def build_publish_audit_record(
    *,
    event: ParameterEvent,
    decision: str,
    published_status: int,
    reason: str | None = None,
    error: str | None = None,
    value_preview_max_length: int = 500,
) -> dict[str, Any]:
    value_json = json.dumps(event.value_normalized, sort_keys=True, default=str, ensure_ascii=False, separators=(",", ":"))
    if len(value_json) > value_preview_max_length:
        value_preview = f"{value_json[:value_preview_max_length]}..."
    else:
        value_preview = value_json
    return {
        "recorded_at": datetime.now(UTC).isoformat(),
        "decision": decision,
        "reason": reason,
        "error": error,
        "event_id": event.event_id,
        "endpoint_id": event.endpoint_id,
        "source_id": event.source_id,
        "owner_type": event.owner_type,
        "owner_id": event.owner_id,
        "node_id": event.node_id,
        "parameter_code": event.parameter_code,
        "parameter_name": event.parameter_name,
        "value_preview": value_preview,
        "value_hash": hashlib.sha256(value_json.encode("utf-8")).hexdigest(),
        "value_type": event.value_type,
        "quality": _enum_value(event.quality),
        "quality_code": event.quality_code,
        "status_name": event.quality_code,
        "status_text": event.status_text,
        "published_status": published_status,
        "source_timestamp": event.source_timestamp.isoformat() if event.source_timestamp else None,
        "server_timestamp": event.server_timestamp.isoformat() if event.server_timestamp else None,
        "ingested_at": event.ingested_at.isoformat(),
        "acquisition_mode": _enum_value(event.acquisition_mode),
        "sequence_id": event.sequence_id,
        "validation_state": _enum_value(event.validation_state),
        "validation_errors": event.validation_errors,
    }


def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)
