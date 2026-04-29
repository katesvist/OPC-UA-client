from __future__ import annotations

import json
from datetime import UTC, datetime

from src.adapters.publisher.rabbitmq import RabbitMqPublisher
from src.config.models import PublisherSettings
from src.domain.entities.enums import AcquisitionMode, QualityCategory, ValidationState
from src.domain.entities.models import ParameterEvent


def test_rabbitmq_publisher_builds_params_validator_envelope() -> None:
    publisher = RabbitMqPublisher(
        PublisherSettings(
            message_format="params_validator_envelope",
            exchange="validator.in.q",
            routing_key="validator.in.q",
            queue_name=None,
        )
    )
    event = ParameterEvent(
        event_id="00000000-0000-0000-0000-000000000001",
        source_id="remote-opc-lab",
        endpoint_id="remote-opc-server",
        owner_type="rig",
        owner_id="rig-01",
        parameter_code="ASPD.APD1.Diff_Pressure.Drill.Limit",
        parameter_name="Diff pressure limit",
        id_by_dict="dict-1",
        type_by_dict="Float",
        uom_by_dict="bar",
        node_id='ns=3;s="OPC_Data_exchange"."OPC_Dif_Pressure_Reg_Limit"',
        value_raw=12.3,
        value_normalized=12.3,
        value_type="float",
        unit="bar",
        quality=QualityCategory.GOOD,
        quality_code="Good",
        source_timestamp=datetime(2026, 4, 29, 10, 0, tzinfo=UTC),
        server_timestamp=datetime(2026, 4, 29, 10, 0, 1, tzinfo=UTC),
        validation_state=ValidationState.VALID,
        acquisition_mode=AcquisitionMode.SUBSCRIPTION,
        metadata={"opcua": {"status_code_raw": 0}},
    )

    body, message_type = publisher._build_message_body(event)
    payload = json.loads(body)

    assert message_type == "params.validator.envelope"
    assert payload["schema_version"] == "1.0"
    assert payload["message_id"] == event.event_id
    assert payload["payload"]["name"] == event.parameter_code
    assert payload["payload"]["value"] == 12.3
    assert payload["payload"]["type"] == "float"
    assert payload["payload"]["status"] == 0
    assert payload["payload"]["owner"] == "rig-01"
    assert payload["payload"]["source"] == "remote-opc-lab"
    assert payload["payload"]["id_by_dict"] == "dict-1"
    assert payload["metadata"]["opcua"]["node_id"] == event.node_id
