from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from src.domain.entities.enums import AcquisitionMode, ConnectionState, QualityCategory, ValidationState


class Observation(BaseModel):
    endpoint_id: str
    source_id: str
    owner_type: str
    owner_id: str
    node_id: str
    namespace_index: int | None = None
    namespace_uri: str | None = None
    browse_name: str | None = None
    display_name: str | None = None
    data_type: str | None = None
    raw_value: Any = None
    source_timestamp: datetime | None = None
    server_timestamp: datetime | None = None
    status_code: str
    status_text: str | None = None
    acquisition_mode: AcquisitionMode
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class ParameterEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    source_id: str
    endpoint_id: str
    owner_type: str
    owner_id: str
    parameter_code: str
    parameter_name: str
    node_id: str
    value_raw: Any = None
    value_normalized: Any = None
    value_type: str
    unit: str | None = None
    quality: QualityCategory
    quality_code: str
    status_text: str | None = None
    source_timestamp: datetime | None = None
    server_timestamp: datetime | None = None
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    validation_state: ValidationState
    validation_errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    acquisition_mode: AcquisitionMode
    sequence_id: int | None = None


class ValidationResult(BaseModel):
    state: ValidationState
    errors: list[str] = Field(default_factory=list)
    is_duplicate: bool = False


class QualityAssessment(BaseModel):
    category: QualityCategory
    quality_code: str
    status_text: str | None = None
    warnings: list[str] = Field(default_factory=list)


class EndpointStatus(BaseModel):
    endpoint_id: str
    state: ConnectionState
    connected: bool
    last_error: str | None = None
    connected_since: datetime | None = None
    last_data_at: datetime | None = None
    reconnect_attempts: int = 0


class SubscriptionStatus(BaseModel):
    endpoint_id: str
    node_id: str
    parameter_code: str
    acquisition_mode: AcquisitionMode
    active: bool
    sampling_interval_ms: int | None = None
    last_value_at: datetime | None = None
    last_error: str | None = None


class BufferedEvent(BaseModel):
    id: str
    event_id: str
    payload: ParameterEvent
    attempts: int
    next_attempt_at: datetime
    last_error: str | None = None
    created_at: datetime


class DeadLetterEvent(BaseModel):
    id: str
    event_id: str
    payload: ParameterEvent
    error: str
    attempts: int
    created_at: datetime


class BrowseRequest(BaseModel):
    endpoint_id: str
    node_id: str | None = None
    max_depth: int = 1
    include_variables: bool = True
    include_objects: bool = True


class BrowseNodeResult(BaseModel):
    node_id: str
    parent_node_id: str | None = None
    browse_name: str | None = None
    display_name: str | None = None
    node_class: str
    data_type: str | None = None
    access_level: list[str] = Field(default_factory=list)
    has_children: bool = False
    depth: int = 0


class ReadRequest(BaseModel):
    endpoint_id: str
    node_id: str


class ReadResult(BaseModel):
    endpoint_id: str
    node_id: str
    namespace_index: int | None = None
    browse_name: str | None = None
    display_name: str | None = None
    data_type: str | None = None
    value: Any = None
    source_timestamp: datetime | None = None
    server_timestamp: datetime | None = None
    status_code: str
    status_text: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class WriteRequest(BaseModel):
    endpoint_id: str
    node_id: str
    value: Any


class WriteResult(BaseModel):
    endpoint_id: str
    node_id: str
    success: bool
    status_code: str
    message: str | None = None
