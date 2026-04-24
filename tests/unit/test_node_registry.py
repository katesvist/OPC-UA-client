from __future__ import annotations

from src.config.models import NodeRegistryEntry
from src.modules.subscriptions.registry import NodeRegistry


def test_registry_can_mark_node_error() -> None:
    node = NodeRegistryEntry(
        id="node-1",
        endpoint_id="endpoint-1",
        node_id='ns=3;s="DB_For_Test"."missingNode"',
        parameter_code="MISSING_NODE",
        parameter_name="Missing node",
        expected_type="float",
    )
    registry = NodeRegistry([node])

    registry.mark_error(node, "BadNodeIdUnknown")

    statuses = registry.statuses()
    assert len(statuses) == 1
    assert statuses[0].active is False
    assert statuses[0].last_error == "BadNodeIdUnknown"
