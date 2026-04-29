from __future__ import annotations

from src.adapters.metrics.registry import MetricsRegistry
from src.adapters.opcua.client import OpcUaConnectionManager
from src.config.models import EndpointConfig, NodeRegistryEntry
from src.domain.entities.errors import EndpointNotFoundError
from src.domain.entities.models import BrowseNodeResult, EndpointStatus, NodeConfigApplyResult, ReadResult, WriteResult
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
        self.registry = registry

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

    async def replace_nodes(self, nodes: list[NodeRegistryEntry]) -> list[NodeConfigApplyResult]:
        old_by_id = {node.id: node for node in self.registry.all()}
        new_by_id = {node.id: node for node in nodes}
        results: list[NodeConfigApplyResult] = []

        for removed_id in old_by_id.keys() - new_by_id.keys():
            old_node = old_by_id[removed_id]
            manager = self._managers.get(old_node.endpoint_id)
            if manager is not None:
                applied, message = await manager.deactivate_node(old_node)
            else:
                applied, message = True, "Endpoint is not active."
            self.registry.remove(removed_id)
            results.append(
                NodeConfigApplyResult(
                    node_id=old_node.node_id,
                    config_id=old_node.id,
                    endpoint_id=old_node.endpoint_id,
                    action="removed",
                    applied=applied,
                    message=message,
                )
            )

        for node in nodes:
            manager = self._managers.get(node.endpoint_id)
            if manager is None:
                results.append(
                    NodeConfigApplyResult(
                        node_id=node.node_id,
                        config_id=node.id,
                        endpoint_id=node.endpoint_id,
                        action="failed",
                        applied=False,
                        requires_endpoint_reconnect=True,
                        message=f"Endpoint {node.endpoint_id} is not active.",
                    )
                )
                continue

            previous_node = old_by_id.get(node.id)
            if previous_node is None:
                self.registry.upsert(node)
                applied, message = await manager.activate_node(node)
                action = "added"
            elif previous_node.endpoint_id != node.endpoint_id:
                old_manager = self._managers.get(previous_node.endpoint_id)
                if old_manager is not None:
                    await old_manager.deactivate_node(previous_node)
                self.registry.remove(previous_node.id)
                self.registry.upsert(node)
                applied, message = await manager.activate_node(node)
                action = "moved"
            elif self._requires_resubscribe(previous_node, node):
                await manager.deactivate_node(previous_node)
                self.registry.upsert(node)
                applied, message = await manager.activate_node(node)
                action = "resubscribed"
            else:
                self.registry.upsert(node)
                applied, message = True, "Node mapping updated."
                action = "updated"

            results.append(
                NodeConfigApplyResult(
                    node_id=node.node_id,
                    config_id=node.id,
                    endpoint_id=node.endpoint_id,
                    action=action,
                    applied=applied,
                    message=message,
                )
            )

        return results

    def _get_manager(self, endpoint_id: str) -> OpcUaConnectionManager:
        manager = self._managers.get(endpoint_id)
        if manager is None:
            raise EndpointNotFoundError(f"Endpoint {endpoint_id} не зарегистрирован.")
        return manager

    def _requires_resubscribe(self, old_node: NodeRegistryEntry, new_node: NodeRegistryEntry) -> bool:
        return (
            old_node.node_id != new_node.node_id
            or old_node.acquisition_mode != new_node.acquisition_mode
            or old_node.sampling_interval_ms != new_node.sampling_interval_ms
            or old_node.polling_interval_seconds != new_node.polling_interval_seconds
        )
