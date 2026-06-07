from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.adapters.metrics.registry import MetricsRegistry
from src.adapters.opcua.client import OpcUaConnectionManager
from src.config.models import EndpointConfig, NodeRegistryEntry, SubscriptionDefaults
from src.domain.entities.enums import ConnectionState
from src.domain.entities.errors import SubscriptionError
from src.modules.subscriptions.registry import NodeRegistry


def _node(base: NodeRegistryEntry, index: int) -> NodeRegistryEntry:
    return base.model_copy(
        update={
            "id": f"node-{index}",
            "node_id": f"ns=2;s=Pump01.Value{index}",
            "parameter_code": f"VALUE_{index}",
        }
    )


def _manager(endpoint: EndpointConfig, nodes: list[NodeRegistryEntry]) -> OpcUaConnectionManager:
    return OpcUaConnectionManager(
        endpoint=endpoint,
        registry=NodeRegistry(nodes),
        pipeline=MagicMock(),
        metrics=MetricsRegistry(),
    )


@pytest.mark.asyncio
async def test_subscribe_nodes_in_batches_pauses_between_batches(
    endpoint_config: EndpointConfig,
    node_config: NodeRegistryEntry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = endpoint_config.model_copy(
        update={
            "subscription_defaults": SubscriptionDefaults(
                subscribe_batch_size=2,
                subscribe_batch_pause_seconds=0.5,
            )
        }
    )
    nodes = [_node(node_config, index) for index in range(5)]
    manager = _manager(endpoint, nodes)
    manager._subscribe_node = AsyncMock()
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("src.adapters.opcua.client.asyncio.sleep", fake_sleep)

    await manager._subscribe_nodes_in_batches(nodes)

    assert manager._subscribe_node.await_count == 5
    assert sleeps == [0.5, 0.5]


@pytest.mark.asyncio
async def test_subscribe_nodes_in_batches_keeps_endpoint_for_node_level_error(
    endpoint_config: EndpointConfig,
    node_config: NodeRegistryEntry,
) -> None:
    endpoint = endpoint_config.model_copy(
        update={
            "subscription_defaults": SubscriptionDefaults(
                subscribe_batch_size=10,
                subscribe_batch_pause_seconds=0,
            )
        }
    )
    nodes = [_node(node_config, index) for index in range(3)]
    manager = _manager(endpoint, nodes)

    async def subscribe(node: NodeRegistryEntry) -> None:
        if node.id == "node-1":
            raise RuntimeError("sampling interval rejected")
        manager.registry.mark_active(node, True)

    manager._subscribe_node = AsyncMock(side_effect=subscribe)

    await manager._subscribe_nodes_in_batches(nodes)

    statuses = {status.node_id: status for status in manager.registry.statuses()}
    assert statuses[nodes[0].node_id].active is True
    assert statuses[nodes[1].node_id].active is False
    assert statuses[nodes[1].node_id].last_error == "sampling interval rejected"
    assert statuses[nodes[2].node_id].active is True


@pytest.mark.asyncio
async def test_subscribe_nodes_in_batches_aborts_on_connection_level_error(
    endpoint_config: EndpointConfig,
    node_config: NodeRegistryEntry,
) -> None:
    endpoint = endpoint_config.model_copy(
        update={
            "subscription_defaults": SubscriptionDefaults(
                subscribe_batch_size=10,
                subscribe_batch_pause_seconds=0,
            )
        }
    )
    nodes = [_node(node_config, index) for index in range(3)]
    manager = _manager(endpoint, nodes)

    async def subscribe(node: NodeRegistryEntry) -> None:
        if node.id == "node-1":
            raise RuntimeError("The session id is not valid.(BadSessionIdInvalid)")
        manager.registry.mark_active(node, True)

    manager._subscribe_node = AsyncMock(side_effect=subscribe)

    with pytest.raises(SubscriptionError):
        await manager._subscribe_nodes_in_batches(nodes)

    assert manager._subscribe_node.await_count == 2


def test_subscription_parameters_include_keepalive_lifetime_and_queue(
    endpoint_config: EndpointConfig,
    node_config: NodeRegistryEntry,
) -> None:
    endpoint = endpoint_config.model_copy(
        update={
            "subscription_defaults": SubscriptionDefaults(
                publish_interval_ms=1000,
                keepalive_count=30,
                lifetime_count=180,
                queue_size=250,
            )
        }
    )
    manager = _manager(endpoint, [node_config])

    params = manager._subscription_parameters()

    assert params.RequestedPublishingInterval == 1000.0
    assert params.RequestedMaxKeepAliveCount == 30
    assert params.RequestedLifetimeCount == 180
    assert params.MaxNotificationsPerPublish == 250


@pytest.mark.asyncio
async def test_polling_node_error_does_not_degrade_endpoint(
    endpoint_config: EndpointConfig,
    node_config: NodeRegistryEntry,
) -> None:
    node = node_config.model_copy(update={"acquisition_mode": "polling"})
    manager = _manager(endpoint_config, [node])
    manager._state = ConnectionState.CONNECTED

    client = MagicMock()
    opc_node = MagicMock()
    opc_node.read_data_value = AsyncMock(side_effect=RuntimeError("BadUserAccessDenied"))
    client.get_node.return_value = opc_node
    manager._client = client

    sleep_count = 0

    async def sleep_once(_: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        manager._stop_event.set()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("src.adapters.opcua.client.asyncio.sleep", sleep_once)
        await manager._poll_node(node)

    assert manager._state == ConnectionState.CONNECTED
    assert sleep_count == 1
