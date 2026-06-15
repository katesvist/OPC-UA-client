from __future__ import annotations

import asyncio
import inspect
from collections import deque
from contextlib import suppress
from datetime import UTC, datetime, timedelta
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
from src.domain.entities.models import (
    BrowseNodeResult,
    EndpointStatus,
    MethodCallResult,
    Observation,
    ReadResult,
    WriteResult,
)
from src.domain.ports.diagnostics import DiagnosticsStore
from src.domain.services.diagnostics import NoopDiagnosticsStore
from src.domain.services.pipeline import EventPipeline
from src.modules.subscriptions.registry import NodeRegistry


class _SubscriptionHandler:
    def __init__(self, manager: OpcUaConnectionManager) -> None:
        self.manager = manager

    def datachange_notification(self, node: Any, val: Any, data: Any) -> None:
        self.manager.enqueue_datachange(node, val, data)

    def event_notification(self, event: Any) -> None:
        self.manager.record_event_notification(event)

    def status_change_notification(self, status: Any) -> None:
        self.manager.logger.warning("opcua_subscription_status_changed", status=str(status))


class OpcUaConnectionManager:
    def __init__(
        self,
        endpoint: EndpointConfig,
        registry: NodeRegistry,
        pipeline: EventPipeline,
        metrics: MetricsRegistry,
        diagnostics: DiagnosticsStore | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.registry = registry
        self.pipeline = pipeline
        self.metrics = metrics
        self.diagnostics = diagnostics or NoopDiagnosticsStore()
        self.logger = get_logger(__name__).bind(endpoint_id=endpoint.id)
        self._stop_event = asyncio.Event()
        self._reconnect_event = asyncio.Event()
        self._supervisor_task: asyncio.Task[None] | None = None
        self._polling_tasks: dict[str, asyncio.Task[None]] = {}
        self._client: Client | None = None
        self._subscription: Any = None
        self._subscription_handles: dict[str, Any] = {}
        self._connected_since: datetime | None = None
        self._last_data_at: datetime | None = None
        self._last_error: str | None = None
        self._last_error_type: str | None = None
        self._last_error_stage: str | None = None
        self._last_attempt_at: datetime | None = None
        self._next_retry_at: datetime | None = None
        self._retry_delay_duration_seconds: float | None = None
        self._cooldown_until: datetime | None = None
        self._last_connected_at: datetime | None = None
        self._connection_phase: str | None = "disconnected"
        self._reconnect_attempts = 0
        self._state = ConnectionState.DISCONNECTED
        self._node_metadata: dict[str, dict[str, Any]] = {}
        self._disabled_node_ids: set[str] = set()
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._notification_queue: asyncio.Queue[str] = asyncio.Queue(
            maxsize=max(1, endpoint.subscription_defaults.notification_queue_size)
        )
        self._notification_workers: set[asyncio.Task[None]] = set()
        self._pending_notifications: dict[str, tuple[Any, Any, Any]] = {}
        self._queued_notification_node_ids: set[str] = set()
        self._polling_failure_counts: dict[str, int] = {}
        self._event_subscription_handles: list[Any] = []
        self._opcua_events: deque[dict[str, Any]] = deque(maxlen=max(1, endpoint.events.max_cached_events))
        self._opcua_alarms: deque[dict[str, Any]] = deque(maxlen=max(1, endpoint.alarms_conditions.max_cached_events))
        self._server_capabilities: dict[str, Any] = {
            "discovery_enabled": endpoint.discovery.enabled,
            "events_enabled": endpoint.events.enabled,
            "alarms_conditions_enabled": endpoint.alarms_conditions.enabled,
            "methods_api_enabled": True,
            "redundancy": "not_configured",
            "gds": "not_configured",
            "backpressure": self._backpressure_capabilities(),
            "endpoints": [],
        }
        self._config_lock = asyncio.Lock()

    async def start(self) -> None:
        self._start_notification_workers()
        self._supervisor_task = asyncio.create_task(self._run(), name=f"opcua-connection-{self.endpoint.id}")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._supervisor_task is not None:
            self._supervisor_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._supervisor_task
        await self._cleanup_connection()
        notification_workers = list(self._notification_workers)
        for task in notification_workers:
            task.cancel()
        for task in notification_workers:
            with suppress(asyncio.CancelledError):
                await task
        self._notification_workers.clear()

    async def reconnect(self) -> None:
        self._connection_phase = "manual_reconnect"
        self._next_retry_at = None
        self._retry_delay_duration_seconds = None
        self._cooldown_until = None
        await self._record_connection_event("opcua_manual_reconnect_requested", stage="manual_reconnect")
        self._reconnect_event.set()
        await self._cleanup_connection()

    async def activate_node(self, node_cfg: NodeRegistryEntry) -> tuple[bool, str]:
        async with self._config_lock:
            if self._client is None or self._state not in {ConnectionState.CONNECTED, ConnectionState.DEGRADED}:
                self.registry.mark_active(node_cfg, False)
                return False, "Endpoint is not connected; node will be applied on reconnect."
            try:
                self._disabled_node_ids.discard(node_cfg.node_id)
                await self._load_node_metadata(node_cfg)
                if node_cfg.node_id in self._disabled_node_ids:
                    return False, "Node is not available in OPC UA address space."
                if node_cfg.acquisition_mode == "polling":
                    await self._start_polling_node(node_cfg)
                else:
                    await self._subscribe_node(node_cfg)
                self._refresh_subscribed_metrics()
                return True, "Node applied."
            except Exception as exc:
                self.registry.mark_error(node_cfg, str(exc))
                return False, str(exc)

    async def deactivate_node(self, node_cfg: NodeRegistryEntry) -> tuple[bool, str]:
        async with self._config_lock:
            polling_task = self._polling_tasks.pop(node_cfg.id, None)
            if polling_task is not None:
                polling_task.cancel()
                with suppress(asyncio.CancelledError):
                    await polling_task

            handle = self._subscription_handles.pop(node_cfg.id, None)
            if handle is not None and self._subscription is not None:
                with suppress(Exception):
                    await self._subscription.unsubscribe(handle)

            self._node_metadata.pop(node_cfg.node_id, None)
            self._disabled_node_ids.discard(node_cfg.node_id)
            if self.registry.get(node_cfg.id) is not None:
                self.registry.mark_active(node_cfg, False)
            self._refresh_subscribed_metrics()
            return True, "Node deactivated."

    def status(self) -> EndpointStatus:
        return EndpointStatus(
            endpoint_id=self.endpoint.id,
            state=self._state,
            connected=self._state in {ConnectionState.CONNECTED, ConnectionState.DEGRADED},
            connection_phase=self._connection_phase,
            last_error=self._last_error,
            last_error_type=self._last_error_type,
            last_error_stage=self._last_error_stage,
            connected_since=self._connected_since,
            last_connected_at=self._last_connected_at,
            last_data_at=self._last_data_at,
            reconnect_attempts=self._reconnect_attempts,
            last_attempt_at=self._last_attempt_at,
            next_retry_at=self._next_retry_at,
            retry_delay_seconds=self._retry_delay_duration_seconds,
            cooldown=self._cooldown_until is not None and self._cooldown_until > datetime.now(UTC),
            cooldown_until=self._cooldown_until,
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
        semaphore = asyncio.Semaphore(10)
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
                _semaphore=semaphore,
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
            value_rank = await self._read_value_rank(node)
            array_dimensions = await self._read_array_dimensions(node)
            return ReadResult(
                endpoint_id=self.endpoint.id,
                node_id=node_id,
                namespace_index=getattr(node.nodeid, "NamespaceIndex", None),
                browse_name=getattr(browse_name, "to_string", lambda: str(browse_name))(),
                display_name=getattr(display_name, "Text", str(display_name)),
                data_type=data_type,
                value_rank=value_rank,
                array_dimensions=array_dimensions,
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
                        "value_rank": value_rank,
                        "array_dimensions": array_dimensions,
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

    async def call_method(self, object_node_id: str, method_node_id: str, input_arguments: list[Any]) -> MethodCallResult:
        client = self._require_client()
        object_node = client.get_node(object_node_id)
        try:
            raw_result = await object_node.call_method(method_node_id, *input_arguments)
        except ua.UaStatusCodeError as exc:
            raise NodeWriteError(f"Сервер OPC UA отклонил вызов метода {method_node_id}: {exc}") from exc
        except Exception as exc:
            raise NodeWriteError(f"Не удалось вызвать метод {method_node_id}: {exc}") from exc

        if isinstance(raw_result, list):
            output_arguments = raw_result
        elif raw_result is None:
            output_arguments = []
        else:
            output_arguments = [raw_result]
        return MethodCallResult(
            endpoint_id=self.endpoint.id,
            object_node_id=object_node_id,
            method_node_id=method_node_id,
            output_arguments=[self._json_safe(value) for value in output_arguments],
        )

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
                value_rank=metadata.get("value_rank"),
                array_dimensions=list(metadata.get("array_dimensions", [])),
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
                        "value_rank": metadata.get("value_rank"),
                        "array_dimensions": list(metadata.get("array_dimensions", [])),
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
                stage = self._last_error_stage or self._connection_phase or "connection"
                error = self._format_exception(exc, stage=stage)
                self._last_error = error
                self._last_error_type = type(exc).__name__ or "Error"
                self._last_error_stage = stage
                self.logger.warning("opcua_connection_cycle_failed", error=error, stage=stage, error_type=self._last_error_type)
                await self._record_connection_event(
                    "opcua_connection_cycle_failed",
                    stage=stage,
                    error=error,
                    error_type=self._last_error_type,
                )
                self.metrics.set_active_connection(self.endpoint.id, False)
                self._state = ConnectionState.RECONNECTING if self._connected_since else ConnectionState.FAILED
                await self._cleanup_connection(preserve_state=True)
                self._reconnect_attempts += 1
                self.metrics.inc_reconnect_attempts(self.endpoint.id)
                delay = self._retry_delay_seconds(self._reconnect_attempts)
                self._retry_delay_duration_seconds = delay
                self._next_retry_at = datetime.now(UTC) + timedelta(seconds=delay)
                if self._is_cooldown_attempt(self._reconnect_attempts):
                    self._connection_phase = "cooldown"
                    self._cooldown_until = self._next_retry_at
                    self.logger.warning(
                        "opcua_reconnect_cooldown_started",
                        attempts=self._reconnect_attempts,
                        cooldown_seconds=delay,
                    )
                    await self._record_connection_event(
                        "opcua_reconnect_cooldown_started",
                        stage="cooldown",
                        attempts=self._reconnect_attempts,
                        cooldown_seconds=delay,
                        next_retry_at=self._next_retry_at.isoformat(),
                    )
                else:
                    self._connection_phase = "retry_wait"
                    self._cooldown_until = None
                await self._wait_before_retry(delay)

    async def _connect_once(self) -> None:
        self._state = ConnectionState.CONNECTING if self._reconnect_attempts == 0 else ConnectionState.RECONNECTING
        self._connection_phase = "connecting"
        self._last_attempt_at = datetime.now(UTC)
        self._next_retry_at = None
        self._retry_delay_duration_seconds = None
        self._cooldown_until = None
        self.logger.info("opcua_connecting", url=self.endpoint.url, state=self._state.value)
        await self._record_connection_event(
            "opcua_connecting",
            stage="connecting",
            url=self.endpoint.url,
            state=self._state.value,
            attempts=self._reconnect_attempts,
        )
        selected_url = await self._select_endpoint_url()
        self._connection_phase = "session"
        self._last_error_stage = "session"
        self._client = Client(url=selected_url, timeout=self.endpoint.request_timeout_seconds)
        self._client.session_timeout = self.endpoint.session_timeout_ms
        await self._apply_security(self._client)
        await self._client.connect()
        self._connection_phase = "session_check"
        self._last_error_stage = "session_check"
        await self._verify_connection_ready()
        self._connected_since = datetime.now(UTC)
        self._last_connected_at = self._connected_since
        self._last_error = None
        self._last_error_type = None
        self._last_error_stage = None
        self._connection_phase = "connected"
        self._state = ConnectionState.CONNECTED
        self.metrics.set_active_connection(self.endpoint.id, True)
        self.logger.info("opcua_connected", url=selected_url)
        await self._record_connection_event("opcua_connected", stage="connected", url=selected_url)
        self._connection_phase = "subscriptions"
        self._last_error_stage = "subscriptions"
        await self._setup_monitored_items()
        self._connection_phase = "monitoring"
        self._last_error_stage = None
        self._reconnect_attempts = 0

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

        if subscription_nodes or self.endpoint.events.enabled or self.endpoint.alarms_conditions.enabled:
            await self._ensure_subscription()

        if subscription_nodes:
            await self._subscribe_nodes_in_batches(subscription_nodes)

        await self._setup_event_subscriptions()

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
            await self._start_polling_node(node_cfg)

    async def _ensure_subscription(self) -> None:
        if self._client is None:
            raise ConnectionError("Клиент OPC UA не инициализирован.")
        if self._subscription is not None:
            return
        handler = _SubscriptionHandler(self)
        self._subscription = await self._client.create_subscription(self._subscription_parameters(), handler)

    async def _subscribe_node(self, node_cfg: NodeRegistryEntry) -> None:
        if self._client is None:
            raise ConnectionError("Клиент OPC UA не инициализирован.")
        await self._ensure_subscription()
        if node_cfg.id in self._subscription_handles:
            self.registry.mark_active(node_cfg, True)
            return
        node = self._client.get_node(node_cfg.node_id)
        handle = await self._subscription.subscribe_data_change(
            node,
            queuesize=max(1, self.endpoint.subscription_defaults.queue_size),
            sampling_interval=float(node_cfg.sampling_interval_ms or self.endpoint.subscription_defaults.publish_interval_ms),
        )
        self._subscription_handles[node_cfg.id] = handle
        self.registry.mark_active(node_cfg, True)

    async def _subscribe_nodes_in_batches(self, nodes: list[NodeRegistryEntry]) -> None:
        defaults = self.endpoint.subscription_defaults
        batch_size = max(1, defaults.subscribe_batch_size)
        pause_seconds = max(0.0, defaults.subscribe_batch_pause_seconds)

        for batch_start in range(0, len(nodes), batch_size):
            batch = nodes[batch_start : batch_start + batch_size]
            for node_cfg in batch:
                if node_cfg.node_id in self._disabled_node_ids:
                    self.registry.mark_error(node_cfg, "Узел отключен: не найден в адресном пространстве OPC UA.")
                    continue
                try:
                    await self._subscribe_node(node_cfg)
                except Exception as exc:
                    if self._is_missing_node_error(exc):
                        self._disable_node(node_cfg, str(exc))
                        continue
                    if self._is_connection_lost_error(exc):
                        self.registry.mark_error(node_cfg, str(exc))
                        raise SubscriptionError(f"Потеряна OPC UA сессия при подписке на {node_cfg.node_id}: {exc}") from exc
                    self.registry.mark_error(node_cfg, str(exc))
                    self.logger.warning("opcua_node_subscribe_failed", node_id=node_cfg.node_id, error=str(exc))

            subscribed_count = min(batch_start + len(batch), len(nodes))
            self.logger.info(
                "opcua_subscription_batch_completed",
                subscribed_count=subscribed_count,
                total_count=len(nodes),
                batch_size=len(batch),
            )
            if subscribed_count < len(nodes) and pause_seconds > 0:
                await asyncio.sleep(pause_seconds)

    async def _start_polling_node(self, node_cfg: NodeRegistryEntry) -> None:
        if node_cfg.id in self._polling_tasks and not self._polling_tasks[node_cfg.id].done():
            self.registry.mark_active(node_cfg, True)
            return
        task = asyncio.create_task(self._poll_node(node_cfg), name=f"poll-{self.endpoint.id}-{node_cfg.id}")
        self._polling_tasks[node_cfg.id] = task
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
                    value_rank=self._node_metadata.get(node_cfg.node_id, {}).get("value_rank"),
                    array_dimensions=list(self._node_metadata.get(node_cfg.node_id, {}).get("array_dimensions", [])),
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
                            "value_rank": self._node_metadata.get(node_cfg.node_id, {}).get("value_rank"),
                            "array_dimensions": list(self._node_metadata.get(node_cfg.node_id, {}).get("array_dimensions", [])),
                        },
                        **node_cfg.metadata,
                    },
                    tags=node_cfg.tags,
                )
                await self.pipeline.process(observation, self.endpoint, node_cfg)
                self._last_data_at = datetime.now(UTC)
                self._state = ConnectionState.CONNECTED
                self._polling_failure_counts.pop(node_cfg.id, None)
                self.registry.touch(self.endpoint.id, node_cfg.node_id, self._last_data_at)
            except Exception as exc:
                if self._is_missing_node_error(exc):
                    self._disable_node(node_cfg, str(exc))
                    return
                self.logger.warning("opcua_polling_failed", node_id=node_cfg.node_id, error=str(exc))
                self.registry.mark_error(node_cfg, str(exc))
                self._polling_failure_counts[node_cfg.id] = self._polling_failure_counts.get(node_cfg.id, 0) + 1
                if self._is_connection_lost_error(exc):
                    raise
                await asyncio.sleep(self._polling_error_backoff_seconds(node_cfg))
                continue
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
                "value_rank": await self._read_value_rank(node),
                "array_dimensions": await self._read_array_dimensions(node),
            }
        except Exception as exc:
            if self._is_missing_node_error(exc):
                self._disable_node(node_cfg, str(exc))
                return
            if self._is_connection_lost_error(exc):
                raise
            self.logger.info("opcua_metadata_read_failed", node_id=node_cfg.node_id, error=str(exc))

    async def _cleanup_connection(self, preserve_state: bool = False) -> None:
        for task in self._polling_tasks.values():
            task.cancel()
        self._polling_tasks.clear()
        for task in list(self._background_tasks):
            task.cancel()
        self._background_tasks.clear()
        self._pending_notifications.clear()
        self._queued_notification_node_ids.clear()
        self._polling_failure_counts.clear()
        while not self._notification_queue.empty():
            with suppress(asyncio.QueueEmpty):
                self._notification_queue.get_nowait()
                self._notification_queue.task_done()
        self._event_subscription_handles.clear()
        if self._subscription is not None:
            with suppress(Exception):
                await self._subscription.delete()
            self._subscription = None
        self._subscription_handles.clear()
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

    def _subscription_parameters(self) -> ua.CreateSubscriptionParameters:
        defaults = self.endpoint.subscription_defaults
        params = ua.CreateSubscriptionParameters()
        params.RequestedPublishingInterval = float(defaults.publish_interval_ms)
        params.RequestedMaxKeepAliveCount = max(1, int(defaults.keepalive_count))
        params.RequestedLifetimeCount = max(
            int(defaults.lifetime_count),
            int(defaults.keepalive_count) * 3,
        )
        params.MaxNotificationsPerPublish = max(0, int(defaults.queue_size))
        params.PublishingEnabled = True
        params.Priority = 0
        return params

    async def _select_endpoint_url(self) -> str:
        discovery = self.endpoint.discovery
        if not discovery.enabled:
            return self.endpoint.url

        discovery_url = discovery.discovery_url or self.endpoint.url
        self._connection_phase = "discovery"
        self._last_error_stage = "discovery"
        discovery_client = Client(url=discovery_url, timeout=self.endpoint.request_timeout_seconds)
        try:
            endpoints = await discovery_client.connect_and_get_server_endpoints()
        except Exception as exc:
            error = self._format_exception(exc, stage="discovery")
            self.logger.warning("opcua_discovery_failed", url=discovery_url, error=error, error_type=type(exc).__name__)
            await self._record_connection_event(
                "opcua_discovery_failed",
                stage="discovery",
                url=discovery_url,
                error=error,
                error_type=type(exc).__name__,
                required=discovery.required,
            )
            if discovery.required:
                raise
            return self.endpoint.url

        self.logger.info(
            "opcua_discovery_completed",
            endpoint_count=len(endpoints),
            configured_url=self.endpoint.url,
        )
        await self._record_connection_event(
            "opcua_discovery_completed",
            stage="discovery",
            endpoint_count=len(endpoints),
            configured_url=self.endpoint.url,
        )
        self._server_capabilities["endpoints"] = [
            self._endpoint_description_to_dict(endpoint)
            for endpoint in endpoints
        ]
        selected = self._select_discovered_endpoint(endpoints)
        if selected is None:
            message = "OPC UA server did not advertise an endpoint matching configured security."
            self.logger.warning("opcua_endpoint_selection_failed", error=message)
            await self._record_connection_event(
                "opcua_endpoint_selection_failed",
                stage="endpoint_selection",
                error=message,
            )
            if discovery.required:
                raise ConnectionError(message)
            return self.endpoint.url

        selected_url = str(getattr(selected, "EndpointUrl", self.endpoint.url) or self.endpoint.url)
        self.logger.info(
            "opcua_endpoint_selected",
            url=selected_url,
            security_mode=getattr(getattr(selected, "SecurityMode", None), "name", None),
            security_policy=getattr(selected, "SecurityPolicyUri", None),
            policy=discovery.endpoint_selection_policy,
        )
        await self._record_connection_event(
            "opcua_endpoint_selected",
            stage="endpoint_selection",
            url=selected_url,
            security_mode=getattr(getattr(selected, "SecurityMode", None), "name", None),
            security_policy=getattr(selected, "SecurityPolicyUri", None),
            policy=discovery.endpoint_selection_policy,
        )
        return selected_url if discovery.endpoint_selection_policy == "best_available" else self.endpoint.url

    def _endpoint_description_to_dict(self, endpoint: Any) -> dict[str, Any]:
        user_tokens = []
        for token in getattr(endpoint, "UserIdentityTokens", []) or []:
            token_type = getattr(token, "TokenType", None)
            user_tokens.append(
                {
                    "policy_id": getattr(token, "PolicyId", None),
                    "token_type": getattr(token_type, "name", str(token_type)),
                    "security_policy_uri": getattr(token, "SecurityPolicyUri", None),
                }
            )
        security_mode = getattr(endpoint, "SecurityMode", None)
        return {
            "url": getattr(endpoint, "EndpointUrl", None),
            "security_mode": getattr(security_mode, "name", str(security_mode)),
            "security_policy_uri": getattr(endpoint, "SecurityPolicyUri", None),
            "transport_profile_uri": getattr(endpoint, "TransportProfileUri", None),
            "security_level": getattr(endpoint, "SecurityLevel", None),
            "user_identity_tokens": user_tokens,
        }

    def _select_discovered_endpoint(self, endpoints: list[Any]) -> Any | None:
        if self.endpoint.discovery.endpoint_selection_policy == "best_available":
            return self._best_available_endpoint(endpoints)
        desired_mode = self._message_security_mode(self.endpoint.security_mode)
        desired_policy = self._security_policy_uri(self.endpoint.security_policy)
        for endpoint in endpoints:
            if getattr(endpoint, "SecurityMode", None) == desired_mode and getattr(endpoint, "SecurityPolicyUri", None) == desired_policy:
                return endpoint
        return None

    def _best_available_endpoint(self, endpoints: list[Any]) -> Any | None:
        if not endpoints:
            return None
        scored = sorted(
            endpoints,
            key=lambda endpoint: (
                self._security_score(getattr(endpoint, "SecurityPolicyUri", "")),
                self._security_mode_score(getattr(endpoint, "SecurityMode", None)),
            ),
            reverse=True,
        )
        return scored[0]

    def _security_score(self, policy_uri: str) -> int:
        policy = policy_uri.rsplit("#", 1)[-1].lower()
        scores = {
            "aes256_sha256_rsassa_pss": 50,
            "aes128_sha256_rsaoaep": 40,
            "basic256sha256": 30,
            "basic256": 20,
            "basic128rsa15": 10,
            "none": 0,
        }
        return scores.get(policy, 0)

    def _security_mode_score(self, mode: Any) -> int:
        name = getattr(mode, "name", str(mode)).lower()
        if "signandencrypt" in name:
            return 2
        if "sign" in name:
            return 1
        return 0

    def _message_security_mode(self, security_mode: str) -> ua.MessageSecurityMode:
        normalized = security_mode.replace("_", "").replace(" ", "").lower()
        if normalized in {"none", "none_"}:
            return ua.MessageSecurityMode.None_
        if normalized == "sign":
            return ua.MessageSecurityMode.Sign
        if normalized == "signandencrypt":
            return ua.MessageSecurityMode.SignAndEncrypt
        return ua.MessageSecurityMode.None_

    def _security_policy_uri(self, security_policy: str) -> str:
        normalized = security_policy.rsplit("#", 1)[-1]
        if normalized.lower() in {"none", "nosecurity"}:
            return "http://opcfoundation.org/UA/SecurityPolicy#None"
        return f"http://opcfoundation.org/UA/SecurityPolicy#{normalized}"

    async def _setup_event_subscriptions(self) -> None:
        if self._subscription is None:
            return
        if self.endpoint.events.enabled:
            try:
                handle = await self._subscription.subscribe_events(
                    sourcenode=self.endpoint.events.source_node_id,
                    evtypes=self.endpoint.events.event_type_id,
                    queuesize=max(1, self.endpoint.events.queue_size),
                )
                self._event_subscription_handles.append(handle)
                self.logger.info("opcua_events_subscription_started", source_node_id=self.endpoint.events.source_node_id)
            except Exception as exc:
                self.logger.warning("opcua_events_subscription_failed", error=str(exc))
        if self.endpoint.alarms_conditions.enabled:
            try:
                handle = await self._subscription.subscribe_alarms_and_conditions(
                    sourcenode=self.endpoint.alarms_conditions.source_node_id,
                    evtypes=self.endpoint.alarms_conditions.event_type_id,
                    queuesize=max(1, self.endpoint.alarms_conditions.queue_size),
                )
                self._event_subscription_handles.append(handle)
                self.logger.info("opcua_alarms_conditions_subscription_started", source_node_id=self.endpoint.alarms_conditions.source_node_id)
            except Exception as exc:
                self.logger.warning("opcua_alarms_conditions_subscription_failed", error=str(exc))

    def enqueue_datachange(self, node: Any, value: Any, data: Any) -> None:
        try:
            node_id = node.nodeid.to_string()
        except Exception:
            self.track_background_task(self.handle_datachange(node, value, data))
            return

        defaults = self.endpoint.subscription_defaults
        if not defaults.coalesce_notifications:
            try:
                self._notification_queue.put_nowait(node_id)
                self._pending_notifications[node_id] = (node, value, data)
                self._queued_notification_node_ids.add(node_id)
            except asyncio.QueueFull:
                self.logger.warning("opcua_datachange_queue_full", node_id=node_id)
            return

        self._pending_notifications[node_id] = (node, value, data)
        if node_id in self._queued_notification_node_ids:
            return
        try:
            self._notification_queue.put_nowait(node_id)
            self._queued_notification_node_ids.add(node_id)
        except asyncio.QueueFull:
            self.logger.warning("opcua_datachange_queue_full", node_id=node_id)

    def _start_notification_workers(self) -> None:
        if self._notification_workers:
            return
        worker_count = max(1, self.endpoint.subscription_defaults.notification_workers)
        for index in range(worker_count):
            task = asyncio.create_task(
                self._notification_worker(),
                name=f"opcua-notification-worker-{self.endpoint.id}-{index}",
            )
            self._notification_workers.add(task)
            task.add_done_callback(self._notification_workers.discard)

    async def _notification_worker(self) -> None:
        while not self._stop_event.is_set():
            node_id = await self._notification_queue.get()
            try:
                item = self._pending_notifications.pop(node_id, None)
                self._queued_notification_node_ids.discard(node_id)
                if item is None:
                    continue
                node, value, data = item
                await self.handle_datachange(node, value, data)
            finally:
                self._notification_queue.task_done()

    def record_event_notification(self, event: Any) -> None:
        payload = self._event_to_dict(event)
        event_type = str(payload.get("event_type") or payload.get("EventType") or "")
        if self._looks_like_alarm_or_condition(payload, event_type):
            self._opcua_alarms.append(payload)
        else:
            self._opcua_events.append(payload)
        self.logger.info("opcua_event_received", event_type=event_type or None)

    def event_notifications(self) -> list[dict[str, Any]]:
        return list(self._opcua_events)

    def alarm_notifications(self) -> list[dict[str, Any]]:
        return list(self._opcua_alarms)

    def capabilities(self) -> dict[str, Any]:
        return dict(self._server_capabilities)

    def _event_to_dict(self, event: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {"received_at": datetime.now(UTC).isoformat()}
        for name in dir(event):
            if name.startswith("_"):
                continue
            try:
                value = getattr(event, name)
            except Exception:
                continue
            if callable(value):
                continue
            payload[name] = self._json_safe(value)
        return payload

    def _looks_like_alarm_or_condition(self, payload: dict[str, Any], event_type: str) -> bool:
        event_type_lower = event_type.lower()
        if "condition" in event_type_lower or "alarm" in event_type_lower:
            return True
        condition_fields = {
            "AckedState",
            "ActiveState",
            "ConfirmedState",
            "ConditionClassId",
            "ConditionName",
            "EnabledState",
            "Retain",
            "Severity",
        }
        if any(field in payload for field in condition_fields):
            return True
        return self.endpoint.alarms_conditions.enabled and not self.endpoint.events.enabled

    def _backpressure_capabilities(self) -> dict[str, Any]:
        defaults = self.endpoint.subscription_defaults
        return {
            "notification_queue_size": defaults.notification_queue_size,
            "notification_workers": defaults.notification_workers,
            "coalesce_notifications": defaults.coalesce_notifications,
            "suppress_unchanged_values": defaults.suppress_unchanged_values,
            "monitored_item_queue_size": defaults.queue_size,
        }

    def _json_safe(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, (list, tuple)):
            return [self._json_safe(item) for item in value]
        if isinstance(value, dict):
            return {str(key): self._json_safe(item) for key, item in value.items()}
        return str(value)

    def _polling_error_backoff_seconds(self, node_cfg: NodeRegistryEntry) -> float:
        defaults = self.endpoint.subscription_defaults
        failures = max(1, self._polling_failure_counts.get(node_cfg.id, 1))
        delay = defaults.polling_error_backoff_seconds * (2 ** (failures - 1))
        return min(max(0.2, delay), defaults.polling_error_backoff_max_seconds)

    def _refresh_subscribed_metrics(self) -> None:
        active_subscription_nodes = [
            node
            for node in self.registry.by_endpoint(self.endpoint.id)
            if node.acquisition_mode == "subscription"
            and node.id in self._subscription_handles
            and node.node_id not in self._disabled_node_ids
        ]
        self.metrics.set_subscribed_nodes(self.endpoint.id, len(active_subscription_nodes))

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

    async def _verify_connection_ready(self) -> None:
        if self._client is None:
            raise ConnectionError("Клиент OPC UA отсутствует после подключения.")
        await self._client.check_connection()

    def _retry_delay_seconds(self, attempt: int) -> float:
        if self._is_cooldown_attempt(attempt):
            return max(0.0, self.endpoint.reconnect_policy.cooldown_after_failures_seconds)
        return self._backoff_seconds(attempt)

    def _is_cooldown_attempt(self, attempt: int) -> bool:
        policy = self.endpoint.reconnect_policy
        return policy.failure_threshold > 0 and attempt >= policy.failure_threshold

    async def _wait_before_retry(self, delay: float) -> None:
        if delay <= 0:
            return
        try:
            await asyncio.wait_for(self._reconnect_event.wait(), timeout=delay)
        except TimeoutError:
            return
        self._reconnect_event.clear()
        self._next_retry_at = None
        self._retry_delay_duration_seconds = None
        self._cooldown_until = None
        self._connection_phase = "reconnecting"
        await self._record_connection_event("opcua_retry_wait_interrupted", stage="manual_reconnect")

    def _backoff_seconds(self, attempt: int) -> float:
        policy = self.endpoint.reconnect_policy
        delay = policy.initial_delay_seconds * (policy.backoff_multiplier ** max(attempt - 1, 0))
        return min(delay, policy.max_delay_seconds)

    def _format_exception(self, exc: Exception, *, stage: str) -> str:
        message = str(exc).strip()
        error_type = type(exc).__name__ or "Error"
        if message:
            return f"{error_type}: {message}"
        if isinstance(exc, TimeoutError):
            return f"TimeoutError during {stage} after {self.endpoint.request_timeout_seconds:g}s"
        return f"{error_type} during {stage}"

    async def _record_connection_event(self, event: str, *, stage: str, **extra: Any) -> None:
        try:
            await self.diagnostics.record_connection_event(
                {
                    "endpoint_id": self.endpoint.id,
                    "event": event,
                    "stage": stage,
                    "state": self._state.value,
                    "phase": self._connection_phase,
                    **extra,
                }
            )
        except Exception:
            self.logger.exception("connection_diagnostics_record_failed")

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
        _semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        if _semaphore is None:
            _semaphore = asyncio.Semaphore(10)

        current_node_id = None
        children: list[Any] = []
        try:
            current_node_id = node.nodeid.to_string()
            should_fetch_children = depth < max_depth
            attributes_task = self._read_browse_attributes(node)
            if should_fetch_children:
                attributes, children = await asyncio.gather(attributes_task, node.get_children())
            else:
                attributes = await attributes_task

            node_class_name = attributes["node_class"]

            if self._should_include_node(node_class_name, include_variables, include_objects):
                results.append(
                    BrowseNodeResult(
                        node_id=current_node_id,
                        parent_node_id=parent_node_id,
                        browse_name=attributes["browse_name"],
                        display_name=attributes["display_name"],
                        node_class=node_class_name,
                        data_type=attributes["data_type"],
                        value_rank=attributes["value_rank"],
                        array_dimensions=attributes["array_dimensions"],
                        access_level=attributes["access_level"],
                        has_children=bool(children) if should_fetch_children else self._may_have_children(node_class_name),
                        depth=depth,
                    )
                )
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

        # Process siblings concurrently — semaphore limits outstanding OPC UA requests.
        async def _browse_child(child: Any) -> None:
            async with _semaphore:
                await self._browse_recursive(
                    node=child,
                    results=results,
                    parent_node_id=current_node_id,
                    depth=depth + 1,
                    max_depth=max_depth,
                    include_variables=include_variables,
                    include_objects=include_objects,
                    _semaphore=_semaphore,
                )

        await asyncio.gather(*[_browse_child(child) for child in children], return_exceptions=True)

    async def _read_browse_attributes(self, node: Any) -> dict[str, Any]:
        if hasattr(node, "read_attributes"):
            attrs = await node.read_attributes([
                ua.AttributeIds.NodeClass,
                ua.AttributeIds.BrowseName,
                ua.AttributeIds.DisplayName,
                ua.AttributeIds.AccessLevel,
                ua.AttributeIds.ValueRank,
                ua.AttributeIds.ArrayDimensions,
                ua.AttributeIds.DataType,
            ])
            values = [self._extract_variant_value(attr) for attr in attrs]
            return {
                "node_class": self._format_node_class(values[0]),
                "browse_name": self._format_browse_name(values[1]),
                "display_name": self._format_display_name(values[2]),
                "access_level": self._format_access_level(values[3]),
                "value_rank": self._format_int(values[4]),
                "array_dimensions": self._format_array_dimensions(values[5]),
                "data_type": self._format_data_type(values[6]),
            }

        node_class, browse_name, display_name, access_level, value_rank, array_dimensions, data_type = await asyncio.gather(
            node.read_node_class(),
            node.read_browse_name(),
            node.read_display_name(),
            self._read_access_level(node),
            self._read_value_rank(node),
            self._read_array_dimensions(node),
            self._read_data_type_name(node),
        )
        return {
            "node_class": getattr(node_class, "name", str(node_class)),
            "browse_name": self._format_browse_name(browse_name),
            "display_name": self._format_display_name(display_name),
            "access_level": access_level,
            "value_rank": value_rank,
            "array_dimensions": array_dimensions,
            "data_type": data_type,
        }

    def _format_node_class(self, value: Any) -> str:
        if value is None:
            return "Unknown"
        name = getattr(value, "name", None)
        if isinstance(name, str):
            return name
        try:
            return ua.NodeClass(int(value)).name
        except (TypeError, ValueError):
            return str(value)

    def _format_browse_name(self, value: Any) -> str | None:
        if value is None:
            return None
        return getattr(value, "to_string", lambda: str(value))()

    def _format_display_name(self, value: Any) -> str | None:
        if value is None:
            return None
        return getattr(value, "Text", str(value))

    def _format_access_level(self, value: Any) -> list[str]:
        if value is None:
            return []
        names: list[str] = []
        try:
            raw_value = int(value)
        except (TypeError, ValueError):
            return names
        for access_level in ua.AccessLevel:
            if raw_value & int(access_level):
                names.append(access_level.name)
        return names

    def _format_int(self, value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _format_array_dimensions(self, value: Any) -> list[int]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return [int(item) for item in value]
        try:
            return [int(value)]
        except (TypeError, ValueError):
            return []

    def _format_data_type(self, value: Any) -> str | None:
        if value is None:
            return None
        namespace_index = getattr(value, "NamespaceIndex", None)
        identifier = getattr(value, "Identifier", None)
        if namespace_index in {None, 0} and isinstance(identifier, int):
            builtin_names = {
                1: "Boolean",
                2: "SByte",
                3: "Byte",
                4: "Int16",
                5: "UInt16",
                6: "Int32",
                7: "UInt32",
                8: "Int64",
                9: "UInt64",
                10: "Float",
                11: "Double",
                12: "String",
                13: "DateTime",
                14: "Guid",
                15: "ByteString",
                16: "XmlElement",
                17: "NodeId",
                18: "ExpandedNodeId",
                19: "StatusCode",
                20: "QualifiedName",
                21: "LocalizedText",
                22: "ExtensionObject",
            }
            if identifier in builtin_names:
                return builtin_names[identifier]
        return getattr(value, "to_string", lambda: str(value))()

    def _may_have_children(self, node_class_name: str) -> bool:
        return node_class_name == "Object"

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

    async def _read_value_rank(self, node: Any) -> int | None:
        try:
            data_value = await node.read_attribute(ua.AttributeIds.ValueRank)
            raw_value = self._extract_variant_value(data_value)
            if raw_value is None:
                return None
            return int(raw_value)
        except Exception:
            return None

    async def _read_array_dimensions(self, node: Any) -> list[int]:
        try:
            data_value = await node.read_attribute(ua.AttributeIds.ArrayDimensions)
            raw_value = self._extract_variant_value(data_value)
            if raw_value is None:
                return []
            if isinstance(raw_value, (list, tuple)):
                return [int(item) for item in raw_value]
            return [int(raw_value)]
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
        if node_cfg is not None and node_cfg.value_shape == "array":
            if not isinstance(value, (list, tuple)):
                raise NodeWriteError(f"Не удалось привести значение {value!r} к массиву {node_cfg.expected_type}.")
            return [self._coerce_write_scalar(item, node_cfg.expected_type) for item in value]
        if node_cfg is not None and node_cfg.value_shape == "object":
            return value
        expected_type = node_cfg.expected_type if node_cfg is not None else None
        return self._coerce_write_scalar(value, expected_type)

    def _coerce_write_scalar(self, value: Any, expected_type: str | None) -> Any:
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

    def _is_connection_lost_error(self, exc: Exception) -> bool:
        if isinstance(exc, (TimeoutError, OSError, ConnectionError)):
            return True
        message = str(exc).lower()
        connection_markers = (
            "failed to send request to opc ua server",
            "badsessionidinvalid",
            "badsessionnotactivated",
            "session id is not valid",
            "session cannot be used",
            "connection is closed",
            "connection lost",
            "connection reset",
            "timeout",
            "timed out",
            "socket",
        )
        return any(marker in message for marker in connection_markers)
