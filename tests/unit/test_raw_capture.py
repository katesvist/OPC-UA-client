from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.adapters.metrics.registry import MetricsRegistry
from src.adapters.opcua.client import OpcUaConnectionManager
from src.domain.services.raw_capture import (
    RawCaptureFileExistsError,
    RawNotificationCapture,
)
from src.modules.subscriptions.registry import NodeRegistry


@pytest.mark.asyncio
async def test_raw_capture_writes_jsonl_and_footer(tmp_path: Path) -> None:
    output = tmp_path / "capture.jsonl"
    capture = RawNotificationCapture()

    await capture.start_capture(duration_seconds=2, output_path=str(output), max_records=10)
    capture.record({"endpoint_id": "endpoint-1", "node_id": "ns=2;s=Pump01.Value", "value": 42})
    await capture.stop()

    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert rows[0]["node_id"] == "ns=2;s=Pump01.Value"
    assert rows[0]["value"] == 42
    assert rows[-1]["capture_meta"]["records"] == 1
    assert rows[-1]["capture_meta"]["dropped"] == 0


@pytest.mark.asyncio
async def test_raw_capture_does_not_overwrite_existing_file(tmp_path: Path) -> None:
    output = tmp_path / "capture.jsonl"
    output.write_text("old\n")
    capture = RawNotificationCapture()

    with pytest.raises(RawCaptureFileExistsError):
        await capture.start_capture(duration_seconds=2, output_path=str(output))


def test_datachange_is_recorded_before_notification_coalescing(endpoint_config, node_config) -> None:
    raw_capture = MagicMock()
    manager = OpcUaConnectionManager(
        endpoint=endpoint_config,
        registry=NodeRegistry([node_config]),
        pipeline=MagicMock(),
        metrics=MetricsRegistry(),
        raw_capture=raw_capture,
    )
    node = MagicMock()
    node.nodeid.to_string.return_value = node_config.node_id
    node.nodeid.NamespaceIndex = 2
    status = MagicMock()
    status.value = 0
    status.name = "Good"
    status.doc = "Good"
    data_value = MagicMock()
    data_value.SourceTimestamp = datetime(2026, 6, 25, 12, 0, tzinfo=UTC)
    data_value.ServerTimestamp = datetime(2026, 6, 25, 12, 0, 1, tzinfo=UTC)
    data_value.StatusCode = status
    data = MagicMock()
    data.monitored_item.Value = data_value

    manager.enqueue_datachange(node, 123, data)

    raw_capture.record.assert_called_once()
    record = raw_capture.record.call_args.args[0]
    assert record["endpoint_id"] == endpoint_config.id
    assert record["node_id"] == node_config.node_id
    assert record["value"] == 123
    assert record["status_raw"] == 0
    assert record["status_code"] == "Good"
    assert record["source_timestamp"] == "2026-06-25T12:00:00+00:00"
