from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.adapters.metrics.registry import MetricsRegistry
from src.adapters.opcua.client import OpcUaConnectionManager
from src.config.models import EndpointConfig, NodeRegistryEntry
from src.modules.subscriptions.registry import NodeRegistry


class _FakeBrowseName:
    def __init__(self, value: str) -> None:
        self._value = value

    def to_string(self) -> str:
        return self._value


class _FakeNode:
    def __init__(
        self,
        node_id: str,
        node_class: str,
        *,
        browse_name: str,
        display_name: str,
        children: list[_FakeNode] | None = None,
        data_type: str = "Float",
        fail_on: str | None = None,
    ) -> None:
        self.nodeid = SimpleNamespace(to_string=lambda: node_id)
        self._node_class = node_class
        self._browse_name = browse_name
        self._display_name = display_name
        self._children = children or []
        self._data_type = data_type
        self._fail_on = fail_on

    async def read_node_class(self) -> SimpleNamespace:
        if self._fail_on == "read_node_class":
            raise RuntimeError("broken node class")
        return SimpleNamespace(name=self._node_class)

    async def read_browse_name(self) -> _FakeBrowseName:
        if self._fail_on == "read_browse_name":
            raise RuntimeError("broken browse name")
        return _FakeBrowseName(self._browse_name)

    async def read_display_name(self) -> SimpleNamespace:
        if self._fail_on == "read_display_name":
            raise RuntimeError("broken display name")
        return SimpleNamespace(Text=self._display_name)

    async def get_children(self) -> list[_FakeNode]:
        if self._fail_on == "get_children":
            raise RuntimeError("broken children")
        return self._children

    async def read_data_type_as_variant_type(self) -> SimpleNamespace:
        if self._fail_on == "read_data_type":
            raise RuntimeError("broken data type")
        return SimpleNamespace(name=self._data_type)

    async def read_attribute(self, _attribute_id: object) -> SimpleNamespace:
        return SimpleNamespace(Value=SimpleNamespace(Value=0))


def _build_manager(
    endpoint_config: EndpointConfig,
    node_config: NodeRegistryEntry,
    metrics: MetricsRegistry,
) -> OpcUaConnectionManager:
    manager = OpcUaConnectionManager(
        endpoint=endpoint_config,
        registry=NodeRegistry([node_config]),
        pipeline=MagicMock(),
        metrics=metrics,
    )
    manager._client = MagicMock()
    return manager


@pytest.mark.asyncio
async def test_browse_skips_unreadable_child_nodes(endpoint_config, node_config, metrics) -> None:
    manager = _build_manager(endpoint_config, node_config, metrics)

    good_leaf = _FakeNode(
        "ns=2;s=Pump01.Pressure",
        "Variable",
        browse_name="2:Pressure",
        display_name="Pressure",
    )
    broken_leaf = _FakeNode(
        "ns=2;s=Pump01.Broken",
        "Variable",
        browse_name="2:Broken",
        display_name="Broken",
        fail_on="read_display_name",
    )
    plc_node = _FakeNode(
        "ns=2;s=Pump01",
        "Object",
        browse_name="2:Pump01",
        display_name="Pump01",
        children=[good_leaf, broken_leaf],
    )
    root = _FakeNode(
        "i=85",
        "Object",
        browse_name="0:Objects",
        display_name="Objects",
        children=[plc_node],
    )

    manager._client.nodes.objects = root

    results = await manager.browse(max_depth=2)
    result_ids = {item.node_id for item in results}

    assert "i=85" in result_ids
    assert "ns=2;s=Pump01" in result_ids
    assert "ns=2;s=Pump01.Pressure" in result_ids
    assert "ns=2;s=Pump01.Broken" not in result_ids

