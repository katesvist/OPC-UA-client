from __future__ import annotations

from src.adapters.metrics.registry import MetricsRegistry


def test_metrics_registry_tracks_endpoint_and_service_values() -> None:
    metrics = MetricsRegistry()

    metrics.set_active_connection("endpoint-a", True)
    metrics.set_active_connection("endpoint-b", False)
    metrics.set_subscribed_nodes("endpoint-a", 3)
    metrics.set_subscribed_nodes("endpoint-b", 2)
    metrics.set_subscription_lag("endpoint-a", 1.5)
    metrics.set_subscription_lag("endpoint-b", 2.5)
    metrics.inc_incoming_events("endpoint-a")
    metrics.inc_valid_events("endpoint-a")
    metrics.inc_downstream_publish_failures("endpoint-b")

    rendered = metrics.render().decode("utf-8")

    assert 'opc_active_connections{endpoint_id="endpoint-a"} 1.0' in rendered
    assert 'opc_active_connections{endpoint_id="endpoint-b"} 0.0' in rendered
    assert "opc_service_active_connections 1.0" in rendered
    assert 'opc_subscribed_nodes{endpoint_id="endpoint-a"} 3.0' in rendered
    assert 'opc_subscribed_nodes{endpoint_id="endpoint-b"} 2.0' in rendered
    assert "opc_service_subscribed_nodes 5.0" in rendered
    assert 'opc_subscription_lag_seconds{endpoint_id="endpoint-b"} 2.5' in rendered
    assert "opc_service_max_subscription_lag_seconds 2.5" in rendered
    assert 'opc_incoming_events_total{endpoint_id="endpoint-a"} 1.0' in rendered
    assert "opc_service_incoming_events_total 1.0" in rendered
    assert 'opc_valid_events_total{endpoint_id="endpoint-a"} 1.0' in rendered
    assert 'opc_downstream_publish_failures_total{endpoint_id="endpoint-b"} 1.0' in rendered
