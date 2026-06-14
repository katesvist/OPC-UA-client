from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, Field, SecretStr


def _mask_endpoint(endpoint: EndpointConfig) -> dict[str, Any]:
    data = endpoint.model_dump(mode="json")
    if data.get("auth", {}).get("password"):
        data["auth"]["password"] = "***"
    return data


class ApiSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080
    management_token: SecretStr | None = None


class RetryPolicyConfig(BaseModel):
    initial_delay_seconds: float = 2.0
    max_delay_seconds: float = 30.0
    backoff_multiplier: float = 2.0
    failure_threshold: int = 10
    cooldown_after_failures_seconds: float = 14400.0


class AuthConfig(BaseModel):
    mode: Literal["anonymous", "username_password", "certificate"] = "anonymous"
    username: str | None = None
    password: SecretStr | None = None
    certificate_path: Path | None = None
    private_key_path: Path | None = None


class DiscoveryConfig(BaseModel):
    enabled: bool = True
    required: bool = False
    endpoint_selection_policy: Literal["configured", "best_available"] = "configured"
    discovery_url: str | None = None


class SubscriptionDefaults(BaseModel):
    publish_interval_ms: int = 1000
    keepalive_count: int = 10
    lifetime_count: int = 30
    queue_size: int = 100
    subscribe_batch_size: int = 100
    subscribe_batch_pause_seconds: float = 0.25
    notification_queue_size: int = 10000
    notification_workers: int = 4
    coalesce_notifications: bool = True
    suppress_unchanged_values: bool = True
    polling_error_backoff_seconds: float = 60.0
    polling_error_backoff_max_seconds: float = 900.0


class EventSubscriptionConfig(BaseModel):
    enabled: bool = False
    source_node_id: str = "i=2253"
    event_type_id: str = "i=2041"
    queue_size: int = 100
    max_cached_events: int = 200


class AlarmsConditionsConfig(BaseModel):
    enabled: bool = False
    source_node_id: str = "i=2253"
    event_type_id: str = "i=2782"
    queue_size: int = 100
    max_cached_events: int = 200


class EndpointMetadata(BaseModel):
    source_id: str
    source_system_id: str | None = None
    owner_type: str
    owner_id: str
    site_id: str | None = None
    asset_id: str | None = None
    well_id: str | None = None
    tags: list[str] = Field(default_factory=list)


class EndpointConfig(BaseModel):
    id: str
    enabled: bool = True
    url: str
    security_policy: str = "None"
    security_mode: str = "None"
    auth: AuthConfig = Field(default_factory=AuthConfig)
    session_timeout_ms: int = 60000
    request_timeout_seconds: float = 10.0
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    reconnect_policy: RetryPolicyConfig = Field(default_factory=RetryPolicyConfig)
    subscription_defaults: SubscriptionDefaults = Field(default_factory=SubscriptionDefaults)
    events: EventSubscriptionConfig = Field(default_factory=EventSubscriptionConfig)
    alarms_conditions: AlarmsConditionsConfig = Field(default_factory=AlarmsConditionsConfig)
    metadata: EndpointMetadata


class ThresholdRule(BaseModel):
    min_value: float | None = None
    max_value: float | None = None


class ValueTransformConfig(BaseModel):
    scale_factor: float = 1.0
    offset: float = 0.0
    target_unit: str | None = None


class InputControlConfig(BaseModel):
    stale_after_seconds: int = 30
    suppress_duplicates: bool = False
    suppress_timestamp_only_changes: bool = False
    deadband: float | None = None
    thresholds: ThresholdRule | None = None


class NodeRegistryEntry(BaseModel):
    id: str
    endpoint_id: str
    node_id: str
    local_name: str | None = None
    namespace_uri: str | None = None
    browse_name: str | None = None
    display_name: str | None = None
    acquisition_mode: Literal["subscription", "polling"] = "subscription"
    read_enabled: bool = True
    write_enabled: bool = False
    sampling_interval_ms: int | None = None
    polling_interval_seconds: float = 5.0
    parameter_code: str
    parameter_name: str
    dict_param_id: str | None = None
    type_by_dict: str | None = None
    unit_by_dict: str | None = None
    expected_type: Literal["bool", "int", "float", "str", "char", "datetime"] = "float"
    value_shape: Literal["scalar", "array", "object"] = "scalar"
    unit: str | None = None
    group_id: str | None = None
    group_path: list[str] = Field(default_factory=list)
    group_display_name: str | None = None
    input_control: InputControlConfig = Field(
        default_factory=InputControlConfig,
        validation_alias=AliasChoices("input_control", "validation"),
    )
    value_transform: ValueTransformConfig = Field(
        default_factory=ValueTransformConfig,
        validation_alias=AliasChoices("value_transform", "transform"),
    )
    metadata: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


NodeConfig = NodeRegistryEntry


class BufferSettings(BaseModel):
    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "opcua-client"
    max_attempts: int = 10
    flush_batch_size: int = 100
    flush_interval_seconds: float = 5.0
    retry_base_delay_seconds: float = 5.0
    retry_max_delay_seconds: float = 300.0
    retention_hours: int = 72


class PublisherSettings(BaseModel):
    mode: Literal["rabbitmq", "noop"] = "rabbitmq"
    url: str = "amqp://guest:guest@localhost:5672/"
    exchange: str = "opcua.events"
    exchange_type: Literal["direct", "topic", "fanout"] = "direct"
    routing_key: str = "opcua.parameter.events"
    queue_name: str | None = "opcua.parameter.events"
    message_format: Literal["parameter_event", "params_validator_envelope"] = "parameter_event"
    schema_version: str = "1.0"
    declare_exchange: bool = True
    declare_queue: bool = True
    durable: bool = True
    timeout_seconds: float = 5.0


class LoggingSettings(BaseModel):
    level: str = "INFO"
    json_logs: bool = True


class DiagnosticsSettings(BaseModel):
    enabled: bool = True
    redis_url: str | None = None
    key_prefix: str = "opcua-client"
    max_records: int = 20000
    ttl_seconds: int = 86400
    value_preview_max_length: int = 500
    status_alarm_clear_after_seconds: int = 600
    overload_major_node_threshold: int = 100


class ServiceSettings(BaseModel):
    name: str = "opc-ua-client-service"
    environment: str = "dev"
    mode: Literal["edge", "cloud"] = "edge"


class AppConfigModel(BaseModel):
    service: ServiceSettings = Field(default_factory=ServiceSettings)
    api: ApiSettings = Field(default_factory=ApiSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    buffer: BufferSettings = Field(default_factory=BufferSettings)
    publisher: PublisherSettings = Field(default_factory=PublisherSettings)
    diagnostics: DiagnosticsSettings = Field(default_factory=DiagnosticsSettings)
    endpoints: list[EndpointConfig] = Field(default_factory=list)
    nodes: list[NodeRegistryEntry] = Field(default_factory=list)
