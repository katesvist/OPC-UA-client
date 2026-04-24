from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.domain.entities.enums import QualityCategory
from src.domain.quality.interpreter import QualityInterpreter


def test_quality_good() -> None:
    interpreter = QualityInterpreter()
    result = interpreter.interpret("Good", "Good", datetime.now(UTC), stale_after_seconds=60)
    assert result.category == QualityCategory.GOOD


def test_quality_bad_communication() -> None:
    interpreter = QualityInterpreter()
    result = interpreter.interpret("BadCommunicationError", None, datetime.now(UTC), stale_after_seconds=60)
    assert result.category == QualityCategory.COMMUNICATION_ERROR


def test_quality_stale_by_timestamp() -> None:
    interpreter = QualityInterpreter()
    result = interpreter.interpret(
        "Good",
        None,
        datetime.now(UTC) - timedelta(seconds=120),
        stale_after_seconds=30,
    )
    assert result.category == QualityCategory.STALE
