from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from asyncua import ua

from src.adapters.metrics.registry import MetricsRegistry
from src.adapters.opcua.client import OpcUaConnectionManager
from src.config.models import EndpointConfig, NodeRegistryEntry
from src.modules.subscriptions.registry import NodeRegistry


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
async def test_write_node_uses_node_variant_type_without_timestamps(endpoint_config, node_config, metrics) -> None:
    writeable_node = node_config.model_copy(update={"write_enabled": True})
    manager = _build_manager(endpoint_config, writeable_node, metrics)

    fake_node = MagicMock()
    fake_node.read_data_type_as_variant_type = AsyncMock(return_value=ua.VariantType.Float)
    fake_node.write_attribute = AsyncMock()
    fake_node.write_value = AsyncMock()
    manager._client.get_node.return_value = fake_node

    result = await manager.write_node(writeable_node.node_id, "123.5")

    assert result.success is True
    fake_node.write_value.assert_not_awaited()
    fake_node.write_attribute.assert_awaited_once()

    attribute_id, data_value = fake_node.write_attribute.await_args.args
    assert attribute_id == ua.AttributeIds.Value
    assert data_value.SourceTimestamp is None
    assert data_value.ServerTimestamp is None
    assert data_value.Value is not None
    assert data_value.Value.VariantType == ua.VariantType.Float
    assert float(data_value.Value.Value) == pytest.approx(123.5)


@pytest.mark.asyncio
async def test_write_node_uses_server_variant_type_for_integer_writes(endpoint_config, node_config, metrics) -> None:
    writeable_node = node_config.model_copy(update={"write_enabled": True, "expected_type": "int"})
    manager = _build_manager(endpoint_config, writeable_node, metrics)

    fake_node = MagicMock()
    fake_node.read_data_type_as_variant_type = AsyncMock(return_value=ua.VariantType.Int16)
    fake_node.write_attribute = AsyncMock()
    manager._client.get_node.return_value = fake_node

    await manager.write_node(writeable_node.node_id, "7")

    _, data_value = fake_node.write_attribute.await_args.args
    assert data_value.Value is not None
    assert data_value.Value.VariantType == ua.VariantType.Int16
    assert data_value.Value.Value == 7


@pytest.mark.asyncio
async def test_write_node_falls_back_to_config_variant_type_when_lookup_fails(endpoint_config, node_config, metrics) -> None:
    writeable_node = node_config.model_copy(update={"write_enabled": True, "expected_type": "bool"})
    manager = _build_manager(endpoint_config, writeable_node, metrics)

    fake_node = MagicMock()
    fake_node.read_data_type_as_variant_type = AsyncMock(side_effect=RuntimeError("lookup failed"))
    fake_node.write_attribute = AsyncMock()
    manager._client.get_node.return_value = fake_node

    await manager.write_node(writeable_node.node_id, "true")

    _, data_value = fake_node.write_attribute.await_args.args
    assert data_value.Value is not None
    assert data_value.Value.VariantType == ua.VariantType.Boolean
    assert data_value.Value.Value is True


@pytest.mark.asyncio
async def test_write_node_converts_char_to_byte(endpoint_config, node_config, metrics) -> None:
    writeable_node = node_config.model_copy(update={"write_enabled": True, "expected_type": "char"})
    manager = _build_manager(endpoint_config, writeable_node, metrics)

    fake_node = MagicMock()
    fake_node.read_data_type_as_variant_type = AsyncMock(return_value=ua.VariantType.Byte)
    fake_node.write_attribute = AsyncMock()
    manager._client.get_node.return_value = fake_node

    await manager.write_node(writeable_node.node_id, "Z")

    _, data_value = fake_node.write_attribute.await_args.args
    assert data_value.Value is not None
    assert data_value.Value.VariantType == ua.VariantType.Byte
    assert data_value.Value.Value == ord("Z")
