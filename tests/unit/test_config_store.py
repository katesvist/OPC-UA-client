from __future__ import annotations

import yaml

from src.config.models import NodeRegistryEntry
from src.config.store import YamlConfigStore


def test_yaml_config_store_replaces_only_nodes_section(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "service": {"name": "opc-ua-client-service"},
                "endpoints": [{"id": "endpoint-1", "url": "opc.tcp://127.0.0.1:4840"}],
                "nodes": [{"id": "old-node"}],
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    node = NodeRegistryEntry(
        id="node-1",
        endpoint_id="endpoint-1",
        node_id="ns=2;s=Pump01.Pressure",
        parameter_code="DICT.PRESSURE",
        parameter_name="Pressure",
        dict_param_id="dict-1",
        type_by_dict="Float",
        unit_by_dict="bar",
        expected_type="float",
        unit="bar",
    )

    YamlConfigStore(config_path).save_nodes([node])

    saved = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert saved["service"]["name"] == "opc-ua-client-service"
    assert saved["endpoints"][0]["id"] == "endpoint-1"
    assert saved["nodes"][0]["id"] == "node-1"
    assert saved["nodes"][0]["dict_param_id"] == "dict-1"
