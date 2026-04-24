from __future__ import annotations

from src.adapters.publisher.noop import NoopPublisher
from src.adapters.publisher.rabbitmq import RabbitMqPublisher
from src.config.models import PublisherSettings
from src.domain.ports.publisher import DownstreamPublisher


def create_publisher(settings: PublisherSettings) -> DownstreamPublisher:
    if settings.mode == "noop":
        return NoopPublisher()
    return RabbitMqPublisher(settings)
