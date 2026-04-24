from enum import StrEnum


class AcquisitionMode(StrEnum):
    SUBSCRIPTION = "subscription"
    POLLING = "polling"


class ValidationState(StrEnum):
    VALID = "valid"
    VALID_WITH_WARNING = "valid_with_warning"
    INVALID = "invalid"
    STALE = "stale"
    LOW_QUALITY = "low_quality"
    DISCONNECTED_SOURCE = "disconnected_source"


class QualityCategory(StrEnum):
    GOOD = "good"
    UNCERTAIN = "uncertain"
    BAD = "bad"
    COMMUNICATION_ERROR = "communication_error"
    SENSOR_ERROR = "sensor_error"
    STALE = "stale"
    UNKNOWN = "unknown"


class ConnectionState(StrEnum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DEGRADED = "degraded"
    FAILED = "failed"
