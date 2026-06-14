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


class NoopDiagnosticsStore:
    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def record_publish_decision(self, record: dict[str, Any]) -> None:
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

    async def status_alarms(self) -> list[dict[str, Any]]:
        return []

    async def status_alarm_history(self, *, limit: int = 200) -> list[dict[str, Any]]:
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
        await self._update_status_state(record)

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
        assert self.redis is not None
        status_codes = await self.redis.smembers(self._active_status_index_key)
        alarms: list[dict[str, Any]] = []
        now = datetime.now(UTC)
        for raw_status in status_codes:
            alarm = await self.redis.hgetall(self._active_status_key(raw_status))
            if not alarm:
                await self.redis.srem(self._active_status_index_key, raw_status)
                continue
            node_count = int(await self.redis.scard(self._status_nodes_key(raw_status)))
            last_seen_at = self._parse_dt(alarm.get("last_seen_at"))
            inactive_seconds = (now - last_seen_at).total_seconds() if last_seen_at else None
            severity = self._severity(
                status_code=int(alarm.get("status_code", "0")),
                status_name=alarm.get("status_name") or "",
                affected_nodes=node_count,
            )
            alarms.append(
                {
                    **alarm,
                    "status_code": int(alarm.get("status_code", "0")),
                    "count": int(alarm.get("count", "0")),
                    "affected_nodes": node_count,
                    "severity": severity,
                    "stale": (
                        inactive_seconds is not None
                        and inactive_seconds > self.settings.status_alarm_clear_after_seconds
                    ),
                    "inactive_seconds": inactive_seconds,
                    "sample_nodes": await self._sample_nodes(raw_status),
                }
            )
        severity_order = {"critical": 4, "major": 3, "warning": 2, "info": 1}
        return sorted(
            alarms,
            key=lambda item: (severity_order.get(str(item["severity"]), 0), item["last_seen_at"]),
            reverse=True,
        )

    async def status_alarm_history(self, *, limit: int = 200) -> list[dict[str, Any]]:
        assert self.redis is not None
        rows = await self.redis.lrange(self._status_history_key, 0, max(limit - 1, 0))
        return [self._loads(row) for row in rows]

    async def _update_status_state(self, record: dict[str, Any]) -> None:
        assert self.redis is not None
        node_key = f"{record.get('endpoint_id')}:{record.get('node_id')}"
        current_status = int(record.get("published_status") or 0)
        previous_status = await self.redis.hget(self._node_status_key, node_key)
        if current_status == NORMAL_STATUS_CODE:
            if previous_status and previous_status != str(NORMAL_STATUS_CODE):
                await self._remove_node_from_status(node_key, previous_status, record)
            await self.redis.hset(self._node_status_key, node_key, str(NORMAL_STATUS_CODE))
            return

        raw_status = str(current_status)
        if previous_status and previous_status not in {str(NORMAL_STATUS_CODE), raw_status}:
            await self._remove_node_from_status(node_key, previous_status, record)
        await self.redis.hset(self._node_status_key, node_key, raw_status)
        await self.redis.sadd(self._status_nodes_key(raw_status), node_key)
        await self.redis.expire(self._status_nodes_key(raw_status), self.settings.ttl_seconds)

        active_key = self._active_status_key(raw_status)
        existing = await self.redis.hgetall(active_key)
        now = datetime.now(UTC).isoformat()
        first_seen = existing.get("first_seen_at") or now
        count = int(existing.get("count", "0")) + 1
        status_name = str(record.get("status_name") or record.get("quality_code") or current_status)
        mapping = {
            "status_code": str(current_status),
            "status_name": status_name,
            "quality": str(record.get("quality") or ""),
            "status_text": str(record.get("status_text") or ""),
            "first_seen_at": first_seen,
            "last_seen_at": now,
            "count": str(count),
            "last_endpoint_id": str(record.get("endpoint_id") or ""),
            "last_node_id": str(record.get("node_id") or ""),
            "last_parameter_code": str(record.get("parameter_code") or ""),
            "message": self._alarm_message(current_status, status_name),
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(active_key, mapping=mapping)
            pipe.sadd(self._active_status_index_key, raw_status)
            pipe.expire(active_key, self.settings.ttl_seconds)
            pipe.expire(self._active_status_index_key, self.settings.ttl_seconds)
            if not existing:
                pipe.lpush(self._status_history_key, self._dump_history("raised", mapping, record))
                pipe.ltrim(self._status_history_key, 0, max(self.settings.max_records - 1, 0))
                pipe.expire(self._status_history_key, self.settings.ttl_seconds)
            await pipe.execute()

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
