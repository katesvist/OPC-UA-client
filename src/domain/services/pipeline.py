from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.adapters.metrics.registry import MetricsRegistry
from src.config.models import EndpointConfig, NodeRegistryEntry
from src.domain.entities.enums import QualityCategory, ValidationState
from src.domain.entities.errors import DatatypeMappingError, DownstreamPublishError
from src.domain.entities.models import Observation, ParameterEvent
from src.domain.ports.buffer import EventBuffer
from src.domain.ports.publisher import DownstreamPublisher
from src.domain.quality.interpreter import QualityInterpreter
from src.domain.transform.normalizer import ValueNormalizer
from src.domain.validation.engine import ValidationEngine


class EventPipeline:
    def __init__(
        self,
        publisher: DownstreamPublisher,
        buffer: EventBuffer,
        metrics: MetricsRegistry,
        quality_interpreter: QualityInterpreter | None = None,
        normalizer: ValueNormalizer | None = None,
        validator: ValidationEngine | None = None,
    ) -> None:
        self.publisher = publisher
        self.buffer = buffer
        self.metrics = metrics
        self.quality_interpreter = quality_interpreter or QualityInterpreter()
        self.normalizer = normalizer or ValueNormalizer()
        self.validator = validator or ValidationEngine()
        self._sequence = 0
        self._last_values: dict[str, tuple[Any, datetime | None]] = {}

    async def process(self, observation: Observation, endpoint: EndpointConfig, node: NodeRegistryEntry) -> ParameterEvent | None:
        self.metrics.inc_incoming_events(observation.endpoint_id)
        self._sequence += 1
        quality = self.quality_interpreter.interpret(
            status_code=observation.status_code,
            status_text=observation.status_text,
            source_timestamp=observation.source_timestamp,
            stale_after_seconds=node.input_control.stale_after_seconds,
        )

        previous_value, previous_timestamp = self._last_values.get(self._key(observation), (None, None))

        try:
            normalized_value, value_type, unit = self.normalizer.normalize(observation.raw_value, node)
        except DatatypeMappingError as exc:
            event = self._build_error_event(
                observation=observation,
                endpoint=endpoint,
                node=node,
                quality=quality.category,
                quality_code=quality.quality_code,
                status_text=str(exc),
                errors=[str(exc)],
            )
            await self._publish_or_buffer(event, str(exc))
            self.metrics.inc_invalid_events(observation.endpoint_id)
            return event

        validation = self.validator.validate(
            node=node,
            value=normalized_value,
            quality=quality,
            source_timestamp=observation.source_timestamp,
            previous_value=previous_value,
            previous_timestamp=previous_timestamp,
        )

        if validation.is_duplicate:
            return None

        event = ParameterEvent(
            source_id=observation.source_id,
            endpoint_id=observation.endpoint_id,
            owner_type=observation.owner_type,
            owner_id=observation.owner_id,
            parameter_code=node.parameter_code or node.local_name or observation.node_id,
            parameter_name=node.parameter_name or node.local_name or observation.display_name or observation.browse_name or observation.node_id,
            node_id=observation.node_id,
            value_raw=observation.raw_value,
            value_normalized=normalized_value,
            value_type=value_type,
            unit=unit,
            quality=quality.category,
            quality_code=quality.quality_code,
            status_text=quality.status_text,
            source_timestamp=observation.source_timestamp,
            server_timestamp=observation.server_timestamp,
            ingested_at=observation.ingested_at.astimezone(UTC),
            validation_state=validation.state,
            validation_errors=validation.errors,
            metadata={
                "source_binding": endpoint.metadata.model_dump(mode="json"),
                "node_registry": {
                    "id": node.id,
                    "local_name": node.local_name,
                    "node_id": node.node_id,
                    "endpoint_id": node.endpoint_id,
                    "acquisition_mode": node.acquisition_mode,
                    "read_enabled": node.read_enabled,
                    "write_enabled": node.write_enabled,
                    "expected_type": node.expected_type,
                    "value_shape": node.value_shape,
                    "unit": node.unit,
                },
                "namespace_index": observation.namespace_index,
                "namespace_uri": observation.namespace_uri,
                "browse_name": observation.browse_name,
                "display_name": observation.display_name,
                "data_type": observation.data_type,
                "value_rank": observation.value_rank,
                "array_dimensions": observation.array_dimensions,
                "endpoint_tags": endpoint.metadata.tags,
                **node.metadata,
                **observation.metadata,
            },
            tags=list(dict.fromkeys([*endpoint.metadata.tags, *node.tags, *observation.tags])),
            acquisition_mode=observation.acquisition_mode,
            sequence_id=self._sequence,
        )
        self._last_values[self._key(observation)] = (normalized_value, observation.source_timestamp)

        self._count_validation_metrics(observation.endpoint_id, validation.state, quality.category)
        await self._publish_or_buffer(event, "Первичная публикация не удалась.")
        return event

    async def _publish_or_buffer(self, event: ParameterEvent, error_message: str) -> None:
        try:
            await self.publisher.publish(event)
        except Exception as exc:
            self.metrics.inc_downstream_publish_failures(event.endpoint_id)
            await self.buffer.enqueue(event, str(exc) or error_message)
            self.metrics.inc_buffered_events(event.endpoint_id)
            raise DownstreamPublishError(str(exc)) from exc

    def _build_error_event(
        self,
        observation: Observation,
        endpoint: EndpointConfig,
        node: NodeRegistryEntry,
        quality: QualityCategory,
        quality_code: str,
        status_text: str,
        errors: list[str],
    ) -> ParameterEvent:
        return ParameterEvent(
            source_id=observation.source_id,
            endpoint_id=observation.endpoint_id,
            owner_type=observation.owner_type,
            owner_id=observation.owner_id,
            parameter_code=node.parameter_code or node.local_name or observation.node_id,
            parameter_name=node.parameter_name or node.local_name or observation.display_name or observation.browse_name or observation.node_id,
            node_id=observation.node_id,
            value_raw=observation.raw_value,
            value_normalized=None,
            value_type=self._event_value_type(node),
            unit=node.unit,
            quality=quality,
            quality_code=quality_code,
            status_text=status_text,
            source_timestamp=observation.source_timestamp,
            server_timestamp=observation.server_timestamp,
            ingested_at=datetime.now(UTC),
            validation_state=ValidationState.INVALID,
            validation_errors=errors,
            metadata={
                "source_binding": endpoint.metadata.model_dump(mode="json"),
                "node_registry": {
                    "id": node.id,
                    "local_name": node.local_name,
                    "node_id": node.node_id,
                    "endpoint_id": node.endpoint_id,
                    "acquisition_mode": node.acquisition_mode,
                    "read_enabled": node.read_enabled,
                    "write_enabled": node.write_enabled,
                    "expected_type": node.expected_type,
                    "value_shape": node.value_shape,
                    "unit": node.unit,
                },
                "namespace_index": observation.namespace_index,
                "namespace_uri": observation.namespace_uri,
                "browse_name": observation.browse_name,
                "display_name": observation.display_name,
                "data_type": observation.data_type,
                "value_rank": observation.value_rank,
                "array_dimensions": observation.array_dimensions,
                "endpoint_tags": endpoint.metadata.tags,
                **node.metadata,
                **observation.metadata,
            },
            tags=list(dict.fromkeys([*endpoint.metadata.tags, *node.tags, *observation.tags])),
            acquisition_mode=observation.acquisition_mode,
            sequence_id=self._sequence,
        )

    def _count_validation_metrics(self, endpoint_id: str, state: ValidationState, quality: QualityCategory) -> None:
        if state in {ValidationState.VALID, ValidationState.VALID_WITH_WARNING}:
            self.metrics.inc_valid_events(endpoint_id)
        if state == ValidationState.INVALID:
            self.metrics.inc_invalid_events(endpoint_id)
        if state == ValidationState.STALE:
            self.metrics.inc_stale_events(endpoint_id)
        if state == ValidationState.LOW_QUALITY or quality in {
            QualityCategory.BAD,
            QualityCategory.COMMUNICATION_ERROR,
            QualityCategory.SENSOR_ERROR,
        }:
            self.metrics.inc_bad_quality_events(endpoint_id)

    def _key(self, observation: Observation) -> str:
        return f"{observation.endpoint_id}:{observation.node_id}"

    def _event_value_type(self, node: NodeRegistryEntry) -> str:
        if node.value_shape == "array":
            return f"{node.expected_type}[]"
        if node.value_shape == "object":
            return "object"
        return node.expected_type
