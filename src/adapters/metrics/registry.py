from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest


class MetricsRegistry:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self._endpoint_label = ("endpoint_id",)
        self._active_connections_by_endpoint: dict[str, int] = {}
        self._subscribed_nodes_by_endpoint: dict[str, int] = {}
        self._subscription_lag_by_endpoint: dict[str, float] = {}
        self.active_connections = Gauge(
            "opc_active_connections",
            "Количество активных OPC UA соединений",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_active_connections = Gauge(
            "opc_service_active_connections",
            "Количество активных OPC UA соединений по сервису",
            registry=self.registry,
        )
        self.reconnect_attempts = Counter(
            "opc_reconnect_attempts_total",
            "Количество попыток переподключения",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_reconnect_attempts = Counter(
            "opc_service_reconnect_attempts_total",
            "Количество попыток переподключения по сервису",
            registry=self.registry,
        )
        self.subscribed_nodes = Gauge(
            "opc_subscribed_nodes",
            "Количество активных подписанных узлов",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_subscribed_nodes = Gauge(
            "opc_service_subscribed_nodes",
            "Количество активных подписанных узлов по сервису",
            registry=self.registry,
        )
        self.incoming_events_total = Counter(
            "opc_incoming_events_total",
            "Всего входящих событий",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_incoming_events_total = Counter(
            "opc_service_incoming_events_total",
            "Всего входящих событий по сервису",
            registry=self.registry,
        )
        self.valid_events_total = Counter(
            "opc_valid_events_total",
            "Всего валидных событий",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_valid_events_total = Counter(
            "opc_service_valid_events_total",
            "Всего валидных событий по сервису",
            registry=self.registry,
        )
        self.invalid_events_total = Counter(
            "opc_invalid_events_total",
            "Всего невалидных событий",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_invalid_events_total = Counter(
            "opc_service_invalid_events_total",
            "Всего невалидных событий по сервису",
            registry=self.registry,
        )
        self.stale_events_total = Counter(
            "opc_stale_events_total",
            "Всего устаревших событий",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_stale_events_total = Counter(
            "opc_service_stale_events_total",
            "Всего устаревших событий по сервису",
            registry=self.registry,
        )
        self.bad_quality_events_total = Counter(
            "opc_bad_quality_events_total",
            "Всего событий с плохим качеством",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_bad_quality_events_total = Counter(
            "opc_service_bad_quality_events_total",
            "Всего событий с плохим качеством по сервису",
            registry=self.registry,
        )
        self.buffered_events_total = Counter(
            "opc_buffered_events_total",
            "Всего событий, отправленных в буфер",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_buffered_events_total = Counter(
            "opc_service_buffered_events_total",
            "Всего событий, отправленных в буфер по сервису",
            registry=self.registry,
        )
        self.dead_letter_events_total = Counter(
            "opc_dead_letter_events_total",
            "Всего событий в dead-letter",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_dead_letter_events_total = Counter(
            "opc_service_dead_letter_events_total",
            "Всего событий в dead-letter по сервису",
            registry=self.registry,
        )
        self.downstream_publish_failures_total = Counter(
            "opc_downstream_publish_failures_total",
            "Ошибки публикации во внешний downstream",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_downstream_publish_failures_total = Counter(
            "opc_service_downstream_publish_failures_total",
            "Ошибки публикации во внешний downstream по сервису",
            registry=self.registry,
        )
        self.subscription_lag = Gauge(
            "opc_subscription_lag_seconds",
            "Лаг между timestamp источника и ingestion",
            self._endpoint_label,
            registry=self.registry,
        )
        self.service_max_subscription_lag = Gauge(
            "opc_service_max_subscription_lag_seconds",
            "Максимальный лаг подписки по всем endpoint сервиса",
            registry=self.registry,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)

    def set_active_connection(self, endpoint_id: str, active: bool) -> None:
        value = 1 if active else 0
        self._active_connections_by_endpoint[endpoint_id] = value
        self.active_connections.labels(endpoint_id=endpoint_id).set(value)
        self.service_active_connections.set(sum(self._active_connections_by_endpoint.values()))

    def inc_reconnect_attempts(self, endpoint_id: str) -> None:
        self.reconnect_attempts.labels(endpoint_id=endpoint_id).inc()
        self.service_reconnect_attempts.inc()

    def set_subscribed_nodes(self, endpoint_id: str, count: int) -> None:
        self._subscribed_nodes_by_endpoint[endpoint_id] = count
        self.subscribed_nodes.labels(endpoint_id=endpoint_id).set(count)
        self.service_subscribed_nodes.set(sum(self._subscribed_nodes_by_endpoint.values()))

    def set_subscription_lag(self, endpoint_id: str, lag_seconds: float) -> None:
        self._subscription_lag_by_endpoint[endpoint_id] = lag_seconds
        self.subscription_lag.labels(endpoint_id=endpoint_id).set(lag_seconds)
        self.service_max_subscription_lag.set(max(self._subscription_lag_by_endpoint.values(), default=0.0))

    def clear_subscription_lag(self, endpoint_id: str) -> None:
        self._subscription_lag_by_endpoint.pop(endpoint_id, None)
        self.subscription_lag.labels(endpoint_id=endpoint_id).set(0.0)
        self.service_max_subscription_lag.set(max(self._subscription_lag_by_endpoint.values(), default=0.0))

    def inc_incoming_events(self, endpoint_id: str) -> None:
        self.incoming_events_total.labels(endpoint_id=endpoint_id).inc()
        self.service_incoming_events_total.inc()

    def inc_valid_events(self, endpoint_id: str) -> None:
        self.valid_events_total.labels(endpoint_id=endpoint_id).inc()
        self.service_valid_events_total.inc()

    def inc_invalid_events(self, endpoint_id: str) -> None:
        self.invalid_events_total.labels(endpoint_id=endpoint_id).inc()
        self.service_invalid_events_total.inc()

    def inc_stale_events(self, endpoint_id: str) -> None:
        self.stale_events_total.labels(endpoint_id=endpoint_id).inc()
        self.service_stale_events_total.inc()

    def inc_bad_quality_events(self, endpoint_id: str) -> None:
        self.bad_quality_events_total.labels(endpoint_id=endpoint_id).inc()
        self.service_bad_quality_events_total.inc()

    def inc_buffered_events(self, endpoint_id: str) -> None:
        self.buffered_events_total.labels(endpoint_id=endpoint_id).inc()
        self.service_buffered_events_total.inc()

    def inc_dead_letter_events(self, endpoint_id: str) -> None:
        self.dead_letter_events_total.labels(endpoint_id=endpoint_id).inc()
        self.service_dead_letter_events_total.inc()

    def inc_downstream_publish_failures(self, endpoint_id: str) -> None:
        self.downstream_publish_failures_total.labels(endpoint_id=endpoint_id).inc()
        self.service_downstream_publish_failures_total.inc()
