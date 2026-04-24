from __future__ import annotations

import fakeredis.aioredis
import pytest

from src.adapters.buffer.redis_buffer import RedisEventBuffer
from src.adapters.metrics.registry import MetricsRegistry
from src.adapters.publisher.noop import NoopPublisher
from src.config.models import BufferSettings, EndpointConfig, EndpointMetadata, NodeConfig
from src.domain.entities.enums import ConnectionState
from src.domain.services.pipeline import EventPipeline
from src.modules.connections.manager import ConnectionsCoordinator
from src.modules.subscriptions.registry import NodeRegistry
from tests.integration.helpers import MockOpcUaServer, get_free_port, wait_until


@pytest.mark.asyncio
async def test_opcua_subscription_receives_event() -> None:
    port = get_free_port()
    endpoint_url = f"opc.tcp://127.0.0.1:{port}/freeopcua/server/"
    server = MockOpcUaServer(endpoint_url)
    await server.start()
    try:
        endpoint = EndpointConfig(
            id="endpoint-1",
            url=endpoint_url,
            metadata=EndpointMetadata(source_id="source-1", owner_type="rig", owner_id="rig-1"),
            request_timeout_seconds=0.5,
        )
        node = NodeConfig(
            id="node-1",
            endpoint_id="endpoint-1",
            node_id=f"ns={server.namespace_idx};s=Pump01.Pressure",
            parameter_code="PUMP_PRESSURE",
            parameter_name="Давление насоса",
            expected_type="float",
            unit="bar",
        )
        settings = BufferSettings(key_prefix="test-opcua-integration")
        redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
        buffer = RedisEventBuffer(settings, client=redis_client)
        await buffer.start()
        publisher = NoopPublisher()
        metrics = MetricsRegistry()
        pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=metrics)
        registry = NodeRegistry([node])
        coordinator = ConnectionsCoordinator([endpoint], registry, pipeline, metrics)

        await coordinator.start()
        await server.write_pressure(123.4)
        await wait_until(lambda: len(publisher.events) >= 1, timeout_seconds=10)

        assert publisher.events[-1].parameter_code == "PUMP_PRESSURE"
        assert float(publisher.events[-1].value_normalized) == pytest.approx(123.4)
        statuses = coordinator.statuses()
        assert statuses[0].state in {ConnectionState.CONNECTED, ConnectionState.DEGRADED}

        await coordinator.reconnect("endpoint-1")
        await wait_until(lambda: coordinator.statuses()[0].state in {ConnectionState.CONNECTED, ConnectionState.RECONNECTING}, timeout_seconds=10)

        await coordinator.stop()
        await buffer.close()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_opcua_browse_read_write_operations() -> None:
    port = get_free_port()
    endpoint_url = f"opc.tcp://127.0.0.1:{port}/freeopcua/server/"
    server = MockOpcUaServer(endpoint_url)
    await server.start()
    try:
        endpoint = EndpointConfig(
            id="endpoint-1",
            url=endpoint_url,
            metadata=EndpointMetadata(source_id="source-1", owner_type="rig", owner_id="rig-1"),
            request_timeout_seconds=0.5,
        )
        pressure_node_id = f"ns={server.namespace_idx};s=Pump01.Pressure"
        temperature_node_id = f"ns={server.namespace_idx};s=Pump01.Temperature"
        nodes = [
            NodeConfig(
                id="node-1",
                endpoint_id="endpoint-1",
                node_id=pressure_node_id,
                parameter_code="PUMP_PRESSURE",
                parameter_name="Давление насоса",
                expected_type="float",
                unit="bar",
                write_enabled=True,
            ),
            NodeConfig(
                id="node-2",
                endpoint_id="endpoint-1",
                node_id=temperature_node_id,
                parameter_code="PUMP_TEMPERATURE",
                parameter_name="Температура насоса",
                expected_type="float",
                unit="celsius",
                write_enabled=True,
            ),
        ]
        settings = BufferSettings(key_prefix="test-opcua-browse-read-write")
        redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
        buffer = RedisEventBuffer(settings, client=redis_client)
        await buffer.start()
        publisher = NoopPublisher()
        metrics = MetricsRegistry()
        pipeline = EventPipeline(publisher=publisher, buffer=buffer, metrics=metrics)
        registry = NodeRegistry(nodes)
        coordinator = ConnectionsCoordinator([endpoint], registry, pipeline, metrics)

        await coordinator.start()
        await wait_until(lambda: coordinator.statuses()[0].state in {ConnectionState.CONNECTED, ConnectionState.DEGRADED}, timeout_seconds=10)

        browse_results = await coordinator.browse("endpoint-1", max_depth=2)
        assert any(item.node_id == pressure_node_id for item in browse_results)
        assert any(item.node_id == temperature_node_id for item in browse_results)

        read_result = await coordinator.read("endpoint-1", pressure_node_id)
        assert read_result.node_id == pressure_node_id
        assert float(read_result.value) == pytest.approx(100.0)

        write_result = await coordinator.write("endpoint-1", pressure_node_id, 222.5)
        assert write_result.success is True

        await wait_until(
            lambda: publisher.events and float(publisher.events[-1].value_normalized) == pytest.approx(222.5),
            timeout_seconds=10,
        )

        read_after_write = await coordinator.read("endpoint-1", pressure_node_id)
        assert float(read_after_write.value) == pytest.approx(222.5)

        await coordinator.stop()
        await buffer.close()
    finally:
        await server.stop()
