from __future__ import annotations

from uuid import UUID


def source_identity_error(id_source: str | None) -> str | None:
    if id_source is None or not str(id_source).strip():
        return "id_source is required before publishing endpoint data."
    try:
        UUID(str(id_source))
    except (TypeError, ValueError, AttributeError):
        return "id_source must be a valid UUID before publishing endpoint data."
    return None
