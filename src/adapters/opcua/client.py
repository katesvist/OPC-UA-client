from __future__ import annotations

import asyncio
import inspect
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

from asyncua import Client, ua

from src.adapters.logging.setup import get_logger
from src.adapters.metrics.registry import MetricsRegistry
from src.config.models import EndpointConfig, NodeRegistryEntry
from src.domain.entities.enums import AcquisitionMode, ConnectionState
from src.domain.entities.errors import (
    BrowseError,
    ConnectionError,
    NodeNotFoundError,
    NodeReadError,
    NodeWriteError,
    SubscriptionError,
    WriteNotAllowedError,
)
from src.domain.entities.models import BrowseNodeResult, EndpointStatus, Observation, ReadResult, WriteResult
from src.domain.services.pipeline import EventPipeline
from src.modules.subscriptions.registry import NodeRegistry


class _SubscriptionHandler:
    def __init__(self, manager: OpcUaConnectionManager) -> None:
        self.manager = manager

    def datachange_notification(self, node: Any, val: Any, data: Any) -> None:
        self.manager.track_background_task(self.manager.handle_datachange(node, val, data))

    def status_change_notification(self, status: Any) -> None:
        self.manager.logger.warning("opcua_subscription_status_changed", status=str(status))


class OpcUaConnectionManager:
    def __init__(
        self,
        endpoint: EndpointConfig,
        registry: NodeRegistry,
        pipeline: EventPipeline,
        metrics: MetricsRegistry,
    ) -> None:
        self.endpoint = endpoint
        self.registry = registry
        self.pipeline = pipeline
        self.metrics = metrics
        self.logger = get_logger(__name__).bind(endpoint_id=endpoint.id)
        self._stop_event = asyncio.Event()
        self._reconnect_event = asyncio.Event()
        self._supervisor_task: asyncio.Task[None] | None = None
        self._polling_tasks: list[asyncio.Task[None]] = []
        self._client: Client | None = None
        self._subscription: Any = None
        self._connected_since: datetime | None = None
        self._last_data_at: datetime | None = None
        self._last_error: str | None = None
        self._reconnect_attempts = 0
        self._state = ConnectionState.DISCONNECTED
        self._node_metadata: dict[str, dict[str, Any]] = {}
        self._disabled_node_ids: set[str] = set()
        self._background_tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        self._supervisor_task = asyncio.create_task(self._run(), name=f"opcua-connection-{self.endpoint.id}")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._supervisor_task is not None:
            self._supervisor_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._supervisor_task
        await self._cleanup_connection()

    async def reconnect(self) -> None:
        self._reconnect_event.set()
        await self._cleanup_connection()

    def status(self) -> EndpointStatus:
        return EndpointStatus(
            endpoint_id=self.endpoint.id,
            state=self._state,
            connected=self._state in {ConnectionState.CONNECTED, ConnectionState.DEGRADED},
            last_error=self._last_error,
            connected_since=self._connected_since,
            last_data_at=self._last_data_at,
            reconnect_attempts=self._reconnect_attempts,
        )

    async def browse(
        self,
        node_id: str | None = None,
        max_depth: int = 1,
        include_variables: bool = True,
        include_objects: bool = True,
    ) -> list[BrowseNodeResult]:
        client = self._require_client()
        start_node = client.get_node(node_id) if node_id else client.nodes.objects
        results: list[BrowseNodeResult] = []
        try:
            await self._browse_recursive(
                node=start_node,
                results=results,
                parent_node_id=None,
                depth=0,
                max_depth=max(0, max_depth),
                include_variables=include_variables,
                include_objects=include_objects,
                is_root=True,
            )
        except NodeNotFoundError:
            raise
        except Exception as exc:
            raise BrowseError(f"Не удалось выполнить browse для узла {node_id or 'Objects'}: {exc}") from exc
        return results

    async def read_node(self, node_id: str) -> ReadResult:
        client = self._require_client()
        node = client.get_node(node_id)
        try:
            data_value = await node.read_data_value()
            browse_name = await node.read_browse_name()
            display_name = await node.read_display_name()
            data_type = await self._read_data_type_name(node)
            return ReadResult(
                endpoint_id=self.endpoint.id,
                node_id=node_id,
                namespace_index=getattr(node.nodeid, "NamespaceIndex", None),
                browse_name=getattr(browse_name, "to_string", lambda: str(browse_name))(),
                display_name=getattr(display_name, "Text", str(display_name)),
                data_type=data_type,
                value=self._extract_variant_value(data_value),
                source_timestamp=self._extract_timestamp(data_value, "SourceTimestamp"),
                server_timestamp=self._extract_timestamp(data_value, "ServerTimestamp"),
                status_code=self._extract_status_code(data_value),
                status_text=self._extract_status_text(data_value),
                metadata={
                    "opcua": {
                        "status_code_raw": self._extract_status_raw(data_value),
                        "source_picoseconds": getattr(data_value, "SourcePicoseconds", None),
                        "server_picoseconds": getattr(data_value, "ServerPicoseconds", None),
                    }
                },
            )
        except ua.UaStatusCodeError as exc:
            raise NodeNotFoundError(f"Узел {node_id} не найден или недоступен: {exc}") from exc
        except Exception as exc:
            raise NodeReadError(f"Не удалось прочитать узел {node_id}: {exc}") from exc

    async def write_node(self, node_id: str, value: Any) -> WriteResult:
        client = self._require_client()
        node_cfg = self.registry.get_by_opc_node(self.endpoint.id, node_id)
        if node_cfg is not None and not node_cfg.write_enabled:
            raise WriteNotAllowedError(f"Запись в узел {node_id} запрещена конфигурацией.")
        node = client.get_node(node_id)
        try:
            data_value = await self._build_write_data_value(node, value, node_cfg)
            await node.write_attribute(ua.AttributeIds.Value, data_value)
            return WriteResult(
                endpoint_id=self.endpoint.id,
                node_id=node_id,
                success=True,
                status_code="Good",
                message="Значение записано.",
            )
        except WriteNotAllowedError:
            raise
        except ua.UaStatusCodeError as exc:
            raise NodeWriteError(f"Сервер OPC UA отклонил запись в узел {node_id}: {exc}") from exc
        except Exception as exc:
            raise NodeWriteError(f"Не удалось записать значение в узел {node_id}: {exc}") from exc

    async def handle_datachange(self, node: Any, value: Any, data: Any) -> None:
        try:
            node_id = node.nodeid.to_string()
            node_config = self.registry.get_by_opc_node(self.endpoint.id, node_id)
            if node_config is None:
                self.logger.warning("unregistered_node_update", node_id=node_id)
                return
            metadata = self._node_metadata.get(node_id, {})
            data_value = getattr(getattr(data, "monitored_item", None), "Value", None)
            observation = Observation(
                endpoint_id=self.endpoint.id,
                source_id=self.endpoint.metadata.source_id,
                owner_type=self.endpoint.metadata.owner_type,
                owner_id=self.endpoint.metadata.owner_id,
                node_id=node_id,
                namespace_index=getattr(node.nodeid, "NamespaceIndex", None),
                namespace_uri=node_config.namespace_uri,
                browse_name=metadata.get("browse_name", node_config.browse_name),
                display_name=metadata.get("display_name", node_config.display_name),
                data_type=metadata.get("data_type"),
                raw_value=value,
                source_timestamp=self._extract_timestamp(data_value, "SourceTimestamp"),
                server_timestamp=self._extract_timestamp(data_value, "ServerTimestamp"),
                status_code=self._extract_status_code(data_value),
                status_text=self._extract_status_text(data_value),
                acquisition_mode=AcquisitionMode.SUBSCRIPTION,
                metadata={
                    "opcua": {
                        "status_code_raw": self._extract_status_raw(data_value),
                        "source_picoseconds": getattr(data_value, "SourcePicoseconds", None),
                        "server_picoseconds": getattr(data_value, "ServerPicoseconds", None),
                        "node_id": node_id,
                        "namespace_index": getattr(node.nodeid, "NamespaceIndex", None),
                        "browse_name": metadata.get("browse_name", node_config.browse_name),
                        "display_name": metadata.get("display_name", node_config.display_name),
                        "data_type": metadata.get("data_type"),
                    },
                    **node_config.metadata,
                },
                tags=node_config.tags,
            )
            await self.pipeline.process(observation, self.endpoint, node_config)
            self._last_data_at = datetime.now(UTC)
            self.registry.touch(self.endpoint.id, node_id, self._last_data_at)
            if observation.source_timestamp is not None:
                lag = max(0.0, (observation.ingested_at - observation.source_timestamp).total_seconds())
                self.metrics.set_subscription_lag(self.endpoint.id, lag)
        except Exception as exc:
            self.logger.exception("opcua_datachange_processing_failed", error=str(exc))

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._connect_once()
                await self._monitor_connection()
                self._reconnect_attempts = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = str(exc)
                self.logger.warning("opcua_connection_cycle_failed", error=str(exc))
                self.metrics.set_active_connection(self.endpoint.id, False)
                self._state = ConnectionState.RECONNECTING if self._connected_since else ConnectionState.FAILED
                await self._cleanup_connection(preserve_state=True)
                self._reconnect_attempts += 1
                self.metrics.inc_reconnect_attempts(self.endpoint.id)
                await asyncio.sleep(self._backoff_seconds(self._reconnect_attempts))

    async def _connect_once(self) -> None:
        self._state = ConnectionState.CONNECTING if self._reconnect_attempts == 0 else ConnectionState.RECONNECTING
        self.logger.info("opcua_connecting", url=self.endpoint.url, state=self._state.value)
        self._client = Client(url=self.endpoint.url, timeout=self.endpoint.request_timeout_seconds)
        self._client.session_timeout = self.endpoint.session_timeout_ms
        await self._apply_security(self._client)
        await self._client.connect()
        self._connected_since = datetime.now(UTC)
        self._last_error = None
        self._state = ConnectionState.CONNECTED
        self.metrics.set_active_connection(self.endpoint.id, True)
        await self._setup_monitored_items()

    async def _monitor_connection(self) -> None:
        while not self._stop_event.is_set():
            if self._reconnect_event.is_set():
                self._reconnect_event.clear()
                raise ConnectionError("Инициирован принудительный reconnect.")
            if self._client is None:
                raise ConnectionError("Клиент OPC UA отсутствует.")
            await self._client.check_connection()
            await asyncio.sleep(max(1.0, self.endpoint.request_timeout_seconds))

    async def _setup_monitored_items(self) -> None:
        if self._client is None:
            raise ConnectionError("Клиент OPC UA не инициализирован.")
        nodes = self.registry.by_endpoint(self.endpoint.id)
        subscription_nodes = [node for node in nodes if node.acquisition_mode == "subscription"]
        polling_nodes = [node for node in nodes if node.acquisition_mode == "polling"]

        for node in nodes:
            await self._load_node_metadata(node)

        if subscription_nodes:
            handler = _SubscriptionHandler(self)
            self._subscription = await self._client.create_subscription(
                self.endpoint.subscription_defaults.publish_interval_ms,
                handler,
            )
            for node_cfg in subscription_nodes:
                if node_cfg.node_id in self._disabled_node_ids:
                    self.registry.mark_error(node_cfg, "Узел отключен: не найден в адресном пространстве OPC UA.")
                    continue
                try:
                    node = self._client.get_node(node_cfg.node_id)
                    await self._subscription.subscribe_data_change(node)
                    self.registry.mark_active(node_cfg, True)
                except Exception as exc:
                    if self._is_missing_node_error(exc):
                        self._disable_node(node_cfg, str(exc))
                        continue
                    self.registry.mark_error(node_cfg, str(exc))
                    raise SubscriptionError(f"Не удалось подписаться на {node_cfg.node_id}: {exc}") from exc

        active_subscription_nodes = [
            node
            for node in subscription_nodes
            if node.node_id not in self._disabled_node_ids
        ]
        self.metrics.set_subscribed_nodes(self.endpoint.id, len(active_subscription_nodes))

        for node_cfg in polling_nodes:
            if node_cfg.node_id in self._disabled_node_ids:
                self.registry.mark_error(node_cfg, "Узел отключен: не найден в адресном пространстве OPC UA.")
                continue
            task = asyncio.create_task(self._poll_node(node_cfg), name=f"poll-{self.endpoint.id}-{node_cfg.id}")
            self._polling_tasks.append(task)
            self.registry.mark_active(node_cfg, True)

    async def _poll_node(self, node_cfg: NodeRegistryEntry) -> None:
        while not self._stop_event.is_set():
            if self._client is None:
                return
            try:
                node = self._client.get_node(node_cfg.node_id)
                data_value = await node.read_data_value()
                observation = Observation(
                    endpoint_id=self.endpoint.id,
                    source_id=self.endpoint.metadata.source_id,
                    owner_type=self.endpoint.metadata.owner_type,
                    owner_id=self.endpoint.metadata.owner_id,
                    node_id=node_cfg.node_id,
                    namespace_index=getattr(node.nodeid, "NamespaceIndex", None),
                    namespace_uri=node_cfg.namespace_uri,
                    browse_name=self._metadata_value(node_cfg.node_id, "browse_name", node_cfg.browse_name),
                    display_name=self._metadata_value(node_cfg.node_id, "display_name", node_cfg.display_name),
                    data_type=self._node_metadata.get(node_cfg.node_id, {}).get("data_type"),
                    raw_value=self._extract_variant_value(data_value),
                    source_timestamp=self._extract_timestamp(data_value, "SourceTimestamp"),
                    server_timestamp=self._extract_timestamp(data_value, "ServerTimestamp"),
                    status_code=self._extract_status_code(data_value),
                    status_text=self._extract_status_text(data_value),
                    acquisition_mode=AcquisitionMode.POLLING,
                    metadata={
                        "opcua": {
                            "status_code_raw": self._extract_status_raw(data_value),
                            "source_picoseconds": getattr(data_value, "SourcePicoseconds", None),
                            "server_picoseconds": getattr(data_value, "ServerPicoseconds", None),
                            "node_id": node_cfg.node_id,
                            "namespace_index": getattr(node.nodeid, "NamespaceIndex", None),
                            "browse_name": self._metadata_value(node_cfg.node_id, "browse_name", node_cfg.browse_name),
                            "display_name": self._metadata_value(node_cfg.node_id, "display_name", node_cfg.display_name),
                            "data_type": self._node_metadata.get(node_cfg.node_id, {}).get("data_type"),
                        },
                        **node_cfg.metadata,
                    },
                    tags=node_cfg.tags,
                )
                await self.pipeline.process(observation, self.endpoint, node_cfg)
                self._last_data_at = datetime.now(UTC)
                self._state = ConnectionState.CONNECTED
                self.registry.touch(self.endpoint.id, node_cfg.node_id, self._last_data_at)
            except Exception as exc:
                if self._is_missing_node_error(exc):
                    self._disable_node(node_cfg, str(exc))
                    return
                self.logger.warning("opcua_polling_failed", node_id=node_cfg.node_id, error=str(exc))
                self.registry.mark_error(node_cfg, str(exc))
                self._state = ConnectionState.DEGRADED
            await asyncio.sleep(max(0.2, node_cfg.polling_interval_seconds))

    async def _load_node_metadata(self, node_cfg: NodeRegistryEntry) -> None:
        if self._client is None:
            return
        try:
            node = self._client.get_node(node_cfg.node_id)
            browse_name = await node.read_browse_name()
            display_name = await node.read_display_name()
            variant_type = await node.read_data_type_as_variant_type()
            self._node_metadata[node_cfg.node_id] = {
                "browse_name": getattr(browse_name, "to_string", lambda: str(browse_name))(),
                "display_name": getattr(display_name, "Text", str(display_name)),
                "data_type": getattr(variant_type, "name", str(variant_type)),
            }
        except Exception as exc:
            if self._is_missing_node_error(exc):
                self._disable_node(node_cfg, str(exc))
                return
            self.logger.info("opcua_metadata_read_failed", node_id=node_cfg.node_id, error=str(exc))

    async def _cleanup_connection(self, preserve_state: bool = False) -> None:
        for task in self._polling_tasks:
            task.cancel()
        self._polling_tasks.clear()
        for task in list(self._background_tasks):
            task.cancel()
        self._background_tasks.clear()
        if self._subscription is not None:
            with suppress(Exception):
                await self._subscription.delete()
            self._subscription = None
        if self._client is not None:
            with suppress(Exception):
                await self._client.disconnect()
            self._client = None
        self._disabled_node_ids.clear()
        for node in self.registry.by_endpoint(self.endpoint.id):
            self.registry.mark_active(node, False)
        self.metrics.set_subscribed_nodes(self.endpoint.id, 0)
        self.metrics.clear_subscription_lag(self.endpoint.id)
        if not preserve_state and self._state != ConnectionState.FAILED:
            self._state = ConnectionState.DISCONNECTED
        self.metrics.set_active_connection(self.endpoint.id, False)

    async def _apply_security(self, client: Client) -> None:
        auth = self.endpoint.auth
        if auth.mode == "username_password":
            if auth.username:
                client.set_user(auth.username)
            if auth.password:
                client.set_password(auth.password.get_secret_value())
        if self.endpoint.security_policy != "None" or self.endpoint.security_mode != "None":
            if auth.mode == "certificate" and auth.certificate_path and auth.private_key_path:
                result = client.set_security_string(
                    f"{self.endpoint.security_policy},{self.endpoint.security_mode},{auth.certificate_path},{auth.private_key_path}"
                )
            else:
                result = client.set_security_string(f"{self.endpoint.security_policy},{self.endpoint.security_mode}")
            if inspect.isawaitable(result):
                await result

    def _backoff_seconds(self, attempt: int) -> float:
        policy = self.endpoint.reconnect_policy
        delay = policy.initial_delay_seconds * (policy.backoff_multiplier ** max(attempt - 1, 0))
        return min(delay, policy.max_delay_seconds)

    def _extract_status_code(self, data_value: Any) -> str:
        status = getattr(data_value, "StatusCode", None) or getattr(data_value, "StatusCode_", None)
        if status is None:
            return "unknown"
        return getattr(status, "name", str(status))

    def _extract_status_text(self, data_value: Any) -> str | None:
        status = getattr(data_value, "StatusCode", None) or getattr(data_value, "StatusCode_", None)
        if status is None:
            return None
        return getattr(status, "doc", None) or getattr(status, "name", None) or str(status)

    def _extract_status_raw(self, data_value: Any) -> int | None:
        status = getattr(data_value, "StatusCode", None) or getattr(data_value, "StatusCode_", None)
        if status is None:
            return None
        raw_value = getattr(status, "value", None)
        if raw_value is None:
            return None
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return None

    def _extract_timestamp(self, data_value: Any, attribute: str) -> datetime | None:
        if data_value is None:
            return None
        timestamp = getattr(data_value, attribute, None)
        if isinstance(timestamp, datetime) and timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=UTC)
        if isinstance(timestamp, datetime):
            return timestamp
        return None

    def _extract_variant_value(self, data_value: Any) -> Any:
        variant = getattr(data_value, "Value", None)
        return getattr(variant, "Value", None)

    def _metadata_value(self, node_id: str, field: str, default: str | None) -> str | None:
        return self._node_metadata.get(node_id, {}).get(field, default)

    def track_background_task(self, awaitable: Any) -> None:
        task = asyncio.create_task(awaitable)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _require_client(self) -> Client:
        if self._client is None:
            raise ConnectionError(f"Endpoint {self.endpoint.id} не подключён.")
        return self._client

    async def _browse_recursive(
        self,
        node: Any,
        results: list[BrowseNodeResult],
        parent_node_id: str | None,
        depth: int,
        max_depth: int,
        include_variables: bool,
        include_objects: bool,
        is_root: bool = False,
    ) -> None:
        current_node_id = None
        try:
            current_node_id = node.nodeid.to_string()
            node_class = await node.read_node_class()
            node_class_name = getattr(node_class, "name", str(node_class))
            if self._should_include_node(node_class_name, include_variables, include_objects):
                browse_name = await node.read_browse_name()
                display_name = await node.read_display_name()
                children = await node.get_children()
                access_level = await self._read_access_level(node)
                results.append(
                    BrowseNodeResult(
                        node_id=current_node_id,
                        parent_node_id=parent_node_id,
                        browse_name=getattr(browse_name, "to_string", lambda: str(browse_name))(),
                        display_name=getattr(display_name, "Text", str(display_name)),
                        node_class=node_class_name,
                        data_type=await self._read_data_type_name(node),
                        access_level=access_level,
                        has_children=bool(children),
                        depth=depth,
                    )
                )
            else:
                children = await node.get_children()
        except ua.UaStatusCodeError as exc:
            if is_root:
                raise NodeNotFoundError(f"Узел {current_node_id or parent_node_id or 'Objects'} не найден или недоступен: {exc}") from exc
            self.logger.warning(
                "opcua_browse_node_skipped",
                node_id=current_node_id,
                parent_node_id=parent_node_id,
                depth=depth,
                error=str(exc),
            )
            return
        except Exception as exc:
            if is_root:
                raise BrowseError(f"Ошибка обхода узла: {exc}") from exc
            self.logger.warning(
                "opcua_browse_node_skipped",
                node_id=current_node_id,
                parent_node_id=parent_node_id,
                depth=depth,
                error=str(exc),
            )
            return

        if depth >= max_depth:
            return
        for child in children:
            await self._browse_recursive(
                node=child,
                results=results,
                parent_node_id=current_node_id,
                depth=depth + 1,
                max_depth=max_depth,
                include_variables=include_variables,
                include_objects=include_objects,
                is_root=False,
            )

    def _should_include_node(self, node_class_name: str, include_variables: bool, include_objects: bool) -> bool:
        if node_class_name == "Variable":
            return include_variables
        if node_class_name == "Object":
            return include_objects
        return include_variables or include_objects

    async def _read_data_type_name(self, node: Any) -> str | None:
        try:
            variant_type = await node.read_data_type_as_variant_type()
            return getattr(variant_type, "name", str(variant_type))
        except Exception:
            return None

    async def _read_access_level(self, node: Any) -> list[str]:
        try:
            data_value = await node.read_attribute(ua.AttributeIds.AccessLevel)
            raw_value = self._extract_variant_value(data_value)
            if raw_value is None:
                return []
            names: list[str] = []
            for access_level in ua.AccessLevel:
                if int(raw_value) & int(access_level):
                    names.append(access_level.name)
            return names
        except Exception:
            return []

    async def _build_write_data_value(self, node: Any, value: Any, node_cfg: NodeRegistryEntry | None) -> ua.DataValue:
        coerced_value = self._coerce_write_value(value, node_cfg)
        variant_type = await self._resolve_write_variant_type(node, node_cfg)
        if variant_type is None:
            return ua.DataValue(ua.Variant(coerced_value))
        return ua.DataValue(ua.Variant(coerced_value, variant_type))

    async def _resolve_write_variant_type(self, node: Any, node_cfg: NodeRegistryEntry | None) -> ua.VariantType | None:
        try:
            return await node.read_data_type_as_variant_type()
        except Exception:
            return self._variant_type_from_config(node_cfg)

    def _variant_type_from_config(self, node_cfg: NodeRegistryEntry | None) -> ua.VariantType | None:
        expected_type = node_cfg.expected_type if node_cfg is not None else None
        mapping: dict[str, ua.VariantType] = {
            "bool": ua.VariantType.Boolean,
            "int": ua.VariantType.Int64,
            "float": ua.VariantType.Double,
            "str": ua.VariantType.String,
            "char": ua.VariantType.Byte,
            "datetime": ua.VariantType.DateTime,
        }
        return mapping.get(expected_type) if expected_type is not None else None

    def _coerce_write_value(self, value: Any, node_cfg: NodeRegistryEntry | None) -> Any:
        expected_type = node_cfg.expected_type if node_cfg is not None else None
        if expected_type == "bool":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"true", "1", "yes", "on"}:
                    return True
                if lowered in {"false", "0", "no", "off"}:
                    return False
            raise NodeWriteError(f"Не удалось привести значение {value!r} к bool.")
        if expected_type == "int":
            return int(value)
        if expected_type == "float":
            return float(value)
        if expected_type == "str":
            return str(value)
        if expected_type == "char":
            if isinstance(value, str):
                normalized = value.strip()
                if len(normalized) == 1:
                    return ord(normalized)
                if normalized.isdigit():
                    return int(normalized)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return int(value)
            raise NodeWriteError(f"Не удалось привести значение {value!r} к char.")
        if expected_type == "datetime":
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                normalized = value.strip()
                if normalized.endswith("Z"):
                    normalized = f"{normalized[:-1]}+00:00"
                try:
                    return datetime.fromisoformat(normalized)
                except ValueError as exc:
                    raise NodeWriteError(f"Не удалось привести значение {value!r} к datetime.") from exc
            raise NodeWriteError(f"Не удалось привести значение {value!r} к datetime.")
        return value

    def _disable_node(self, node_cfg: NodeRegistryEntry, error: str) -> None:
        self._disabled_node_ids.add(node_cfg.node_id)
        self.registry.mark_error(node_cfg, error)
        self.logger.warning("opcua_node_disabled", node_id=node_cfg.node_id, reason=error)

    def _is_missing_node_error(self, exc: Exception) -> bool:
        if isinstance(exc, ua.UaStatusCodeError) and getattr(exc, "code", None) == ua.StatusCodes.BadNodeIdUnknown:
            return True
        message = str(exc)
        return "BadNodeIdUnknown" in message or "does not exist in the server address space" in message
