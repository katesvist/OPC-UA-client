from __future__ import annotations

from src.adapters.metrics.registry import MetricsRegistry
from src.adapters.opcua.client import OpcUaConnectionManager
from src.config.models import EndpointConfig
from src.domain.entities.errors import EndpointNotFoundError
from src.domain.entities.models import BrowseNodeResult, EndpointStatus, ReadResult, WriteResult
from src.domain.services.pipeline import EventPipeline
from src.modules.subscriptions.registry import NodeRegistry


class ConnectionsCoordinator:
    def __init__(
        self,
        endpoints: list[EndpointConfig],
        registry: NodeRegistry,
        pipeline: EventPipeline,
        metrics: MetricsRegistry,
    ) -> None:
        self._managers = {
            endpoint.id: OpcUaConnectionManager(endpoint, registry, pipeline, metrics)
            for endpoint in endpoints
            if endpoint.enabled
        }

    async def start(self) -> None:
        for manager in self._managers.values():
            await manager.start()

    async def stop(self) -> None:
        for manager in self._managers.values():
            await manager.stop()

    def statuses(self) -> list[EndpointStatus]:
        return [manager.status() for manager in self._managers.values()]

    async def reconnect(self, endpoint_id: str) -> None:
        manager = self._get_manager(endpoint_id)
        await manager.reconnect()

    async def browse(
        self,
        endpoint_id: str,
        node_id: str | None = None,
        max_depth: int = 1,
        include_variables: bool = True,
        include_objects: bool = True,
    ) -> list[BrowseNodeResult]:
        manager = self._get_manager(endpoint_id)
        return await manager.browse(
            node_id=node_id,
            max_depth=max_depth,
            include_variables=include_variables,
            include_objects=include_objects,
        )

    async def read(self, endpoint_id: str, node_id: str) -> ReadResult:
        manager = self._get_manager(endpoint_id)
        return await manager.read_node(node_id)

    async def write(self, endpoint_id: str, node_id: str, value: object) -> WriteResult:
        manager = self._get_manager(endpoint_id)
        return await manager.write_node(node_id, value)

    def _get_manager(self, endpoint_id: str) -> OpcUaConnectionManager:
        manager = self._managers.get(endpoint_id)
        if manager is None:
            raise EndpointNotFoundError(f"Endpoint {endpoint_id} не зарегистрирован.")
        return manager
