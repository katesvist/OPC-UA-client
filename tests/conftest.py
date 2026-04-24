from __future__ import annotations

import pytest

from src.adapters.metrics.registry import MetricsRegistry
from src.config.models import (
    EndpointConfig,
    EndpointMetadata,
    NodeRegistryEntry,
)


@pytest.fixture
def endpoint_config() -> EndpointConfig:
    return EndpointConfig(
        id="endpoint-1",
        url="opc.tcp://127.0.0.1:4840/freeopcua/server/",
        metadata=EndpointMetadata(
            source_id="source-1",
            owner_type="rig",
            owner_id="rig-1",
        ),
    )


@pytest.fixture
def node_config() -> NodeRegistryEntry:
    return NodeRegistryEntry(
        id="node-1",
        endpoint_id="endpoint-1",
        node_id="ns=2;s=Pump01.Pressure",
        local_name="pump01_pressure",
        parameter_code="PUMP_PRESSURE",
        parameter_name="Давление насоса",
        expected_type="float",
        unit="bar",
    )


@pytest.fixture
def metrics() -> MetricsRegistry:
    return MetricsRegistry()
