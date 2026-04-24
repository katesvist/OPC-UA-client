class OpcUaServiceError(Exception):
    """Базовая ошибка сервиса OPC UA."""


class ConnectionError(OpcUaServiceError):
    """Ошибка соединения с OPC UA endpoint."""


class SessionError(OpcUaServiceError):
    """Ошибка сессии OPC UA."""


class BrowseError(OpcUaServiceError):
    """Ошибка browse-операции."""


class NodeReadError(OpcUaServiceError):
    """Ошибка чтения значения OPC UA узла."""


class NodeWriteError(OpcUaServiceError):
    """Ошибка записи значения в OPC UA узел."""


class EndpointNotFoundError(OpcUaServiceError):
    """Указанный endpoint не найден в runtime."""


class NodeNotFoundError(OpcUaServiceError):
    """Указанный OPC UA узел не найден."""


class WriteNotAllowedError(OpcUaServiceError):
    """Запись в узел запрещена конфигурацией клиента."""


class SubscriptionError(OpcUaServiceError):
    """Ошибка подписки на узлы."""


class DatatypeMappingError(OpcUaServiceError):
    """Ошибка преобразования типа данных."""


class EventValidationError(OpcUaServiceError):
    """Ошибка первичной валидации."""


class DownstreamPublishError(OpcUaServiceError):
    """Ошибка публикации вниз по конвейеру."""


class BufferPersistenceError(OpcUaServiceError):
    """Ошибка записи или чтения буфера."""
