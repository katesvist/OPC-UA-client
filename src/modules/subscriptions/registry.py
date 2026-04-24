from __future__ import annotations

from datetime import datetime

from src.config.models import NodeRegistryEntry
from src.domain.entities.enums import AcquisitionMode
from src.domain.entities.models import SubscriptionStatus


class NodeRegistry:
    def __init__(self, nodes: list[NodeRegistryEntry]) -> None:
        self._nodes_by_id = {node.id: node for node in nodes}
        self._nodes_by_endpoint: dict[str, list[NodeRegistryEntry]] = {}
        self._status: dict[str, SubscriptionStatus] = {}
        for node in nodes:
            self._nodes_by_endpoint.setdefault(node.endpoint_id, []).append(node)
            self._status[node.id] = SubscriptionStatus(
                endpoint_id=node.endpoint_id,
                node_id=node.node_id,
                parameter_code=node.parameter_code,
                acquisition_mode=AcquisitionMode(node.acquisition_mode),
                active=False,
                sampling_interval_ms=node.sampling_interval_ms,
            )

    def by_endpoint(self, endpoint_id: str) -> list[NodeRegistryEntry]:
        return list(self._nodes_by_endpoint.get(endpoint_id, []))

    def get(self, node_id: str) -> NodeRegistryEntry | None:
        return self._nodes_by_id.get(node_id)

    def get_by_opc_node(self, endpoint_id: str, opc_node_id: str) -> NodeRegistryEntry | None:
        for node in self._nodes_by_endpoint.get(endpoint_id, []):
            if node.node_id == opc_node_id:
                return node
        return None

    def mark_active(self, node: NodeRegistryEntry, active: bool, last_value_at: datetime | None = None) -> None:
        status = self._status[node.id]
        status.active = active
        status.last_value_at = last_value_at
        if active:
            status.last_error = None

    def mark_error(self, node: NodeRegistryEntry, error: str, last_value_at: datetime | None = None) -> None:
        status = self._status[node.id]
        status.active = False
        status.last_value_at = last_value_at
        status.last_error = error

    def touch(self, endpoint_id: str, opc_node_id: str, timestamp: datetime) -> None:
        node = self.get_by_opc_node(endpoint_id, opc_node_id)
        if node is None:
            return
        status = self._status[node.id]
        status.last_value_at = timestamp

    def statuses(self) -> list[SubscriptionStatus]:
        return list(self._status.values())
