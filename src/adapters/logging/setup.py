from __future__ import annotations

import logging
import sys
from contextvars import ContextVar
from typing import Any, cast

import structlog
from structlog.stdlib import BoundLogger

from src.config.models import LoggingSettings

request_id_context: ContextVar[str | None] = ContextVar("request_id", default=None)


def configure_logging(settings: LoggingSettings) -> None:
    timestamper = structlog.processors.TimeStamper(fmt="iso")
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        timestamper,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer = structlog.processors.JSONRenderer() if settings.json_logs else structlog.dev.ConsoleRenderer()

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.level.upper(), logging.INFO),
    )
    logging.getLogger("asyncua").setLevel(logging.ERROR)
    logging.getLogger("asyncua.server").setLevel(logging.ERROR)
    logging.getLogger("asyncua.client").setLevel(logging.ERROR)
    logging.getLogger("uamqp").setLevel(logging.ERROR)
    logging.getLogger("aio_pika").setLevel(logging.WARNING)
    logging.getLogger("aiormq").setLevel(logging.WARNING)

    structlog.configure(
        processors=[*processors, renderer],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> BoundLogger:
    return cast(BoundLogger, structlog.get_logger(name))
