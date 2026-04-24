from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.domain.entities.enums import QualityCategory
from src.domain.entities.models import QualityAssessment


class QualityInterpreter:
    def interpret(
        self,
        status_code: str,
        status_text: str | None,
        source_timestamp: datetime | None,
        stale_after_seconds: int,
    ) -> QualityAssessment:
        normalized_code = (status_code or "unknown").strip()
        status_text_normalized = status_text or normalized_code
        lowered = normalized_code.lower()

        category = QualityCategory.UNKNOWN
        warnings: list[str] = []

        if "good" in lowered:
            category = QualityCategory.GOOD
        elif "uncertain" in lowered:
            category = QualityCategory.UNCERTAIN
            warnings.append("Статус OPC UA помечен как uncertain.")
        elif "communication" in lowered or "connection" in lowered:
            category = QualityCategory.COMMUNICATION_ERROR
        elif "sensor" in lowered or "device" in lowered:
            category = QualityCategory.SENSOR_ERROR
        elif "bad" in lowered:
            category = QualityCategory.BAD

        if source_timestamp is not None:
            now = datetime.now(UTC)
            if source_timestamp.tzinfo is None:
                source_timestamp = source_timestamp.replace(tzinfo=UTC)
            if now - source_timestamp > timedelta(seconds=stale_after_seconds):
                category = QualityCategory.STALE
                warnings.append("Источник отдал устаревшее значение.")

        return QualityAssessment(
            category=category,
            quality_code=normalized_code,
            status_text=status_text_normalized,
            warnings=warnings,
        )
