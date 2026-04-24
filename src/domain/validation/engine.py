from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any

from src.config.models import NodeRegistryEntry
from src.domain.entities.enums import QualityCategory, ValidationState
from src.domain.entities.models import QualityAssessment, ValidationResult


class ValidationEngine:
    def validate(
        self,
        node: NodeRegistryEntry,
        value: Any,
        quality: QualityAssessment,
        source_timestamp: datetime | None,
        previous_value: Any = None,
        previous_timestamp: datetime | None = None,
    ) -> ValidationResult:
        errors: list[str] = []
        warnings = list(quality.warnings)
        state = ValidationState.VALID
        is_duplicate = False

        if value is None:
            errors.append("Получено пустое значение.")

        if source_timestamp is not None and source_timestamp.tzinfo is None:
            errors.append("source_timestamp должен содержать timezone.")

        if source_timestamp is not None:
            source_ts = source_timestamp.astimezone(UTC) if source_timestamp.tzinfo else source_timestamp.replace(tzinfo=UTC)
            if datetime.now(UTC) - source_ts > timedelta(seconds=node.input_control.stale_after_seconds):
                state = ValidationState.STALE
                warnings.append("Значение признано устаревшим.")

        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            errors.append("Числовое значение содержит NaN или Infinity.")

        thresholds = node.input_control.thresholds
        if thresholds and isinstance(value, (int, float)) and not isinstance(value, bool):
            if thresholds.min_value is not None and value < thresholds.min_value:
                warnings.append(f"Значение ниже минимального порога {thresholds.min_value}.")
            if thresholds.max_value is not None and value > thresholds.max_value:
                warnings.append(f"Значение выше максимального порога {thresholds.max_value}.")

        if quality.category in {QualityCategory.BAD, QualityCategory.COMMUNICATION_ERROR, QualityCategory.SENSOR_ERROR}:
            state = ValidationState.LOW_QUALITY
            warnings.append("Качество источника ниже допустимого.")
        elif quality.category == QualityCategory.STALE:
            state = ValidationState.STALE
        elif quality.category in {QualityCategory.UNCERTAIN, QualityCategory.UNKNOWN} and state == ValidationState.VALID:
            state = ValidationState.VALID_WITH_WARNING

        if node.input_control.suppress_duplicates and previous_timestamp == source_timestamp and previous_value == value:
            is_duplicate = True
            warnings.append("Дубликат подавлен.")

        deadband = node.input_control.deadband
        if (
            deadband is not None
            and previous_value is not None
            and isinstance(value, (int, float))
            and isinstance(previous_value, (int, float))
            and abs(float(value) - float(previous_value)) < deadband
        ):
            is_duplicate = True
            warnings.append("Изменение отфильтровано deadband.")

        if errors:
            state = ValidationState.INVALID

        return ValidationResult(state=state, errors=errors + warnings, is_duplicate=is_duplicate)
