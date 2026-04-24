from __future__ import annotations

from datetime import UTC, datetime

from src.domain.entities.enums import QualityCategory, ValidationState
from src.domain.entities.models import QualityAssessment
from src.domain.validation.engine import ValidationEngine


def test_validation_marks_low_quality(node_config) -> None:
    engine = ValidationEngine()
    result = engine.validate(
        node=node_config,
        value=12.5,
        quality=QualityAssessment(category=QualityCategory.BAD, quality_code="Bad"),
        source_timestamp=datetime.now(UTC),
    )
    assert result.state == ValidationState.LOW_QUALITY


def test_validation_marks_invalid_none(node_config) -> None:
    engine = ValidationEngine()
    result = engine.validate(
        node=node_config,
        value=None,
        quality=QualityAssessment(category=QualityCategory.GOOD, quality_code="Good"),
        source_timestamp=datetime.now(UTC),
    )
    assert result.state == ValidationState.INVALID
    assert "Получено пустое значение." in result.errors


def test_validation_suppresses_duplicate(node_config) -> None:
    engine = ValidationEngine()
    now = datetime.now(UTC)
    node_config.input_control.suppress_duplicates = True
    result = engine.validate(
        node=node_config,
        value=10.0,
        quality=QualityAssessment(category=QualityCategory.GOOD, quality_code="Good"),
        source_timestamp=now,
        previous_value=10.0,
        previous_timestamp=now,
    )
    assert result.is_duplicate is True


def test_validation_does_not_suppress_duplicate_by_default(node_config) -> None:
    engine = ValidationEngine()
    now = datetime.now(UTC)
    result = engine.validate(
        node=node_config,
        value=10.0,
        quality=QualityAssessment(category=QualityCategory.GOOD, quality_code="Good"),
        source_timestamp=now,
        previous_value=10.0,
        previous_timestamp=now,
    )
    assert result.is_duplicate is False
