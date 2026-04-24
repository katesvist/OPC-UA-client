from __future__ import annotations

from dataclasses import dataclass

from src.adapters.buffer.factory import create_buffer
from src.adapters.logging.setup import configure_logging, get_logger
from src.adapters.metrics.registry import MetricsRegistry
from src.adapters.publisher.factory import create_publisher
from src.config.models import AppConfigModel
from src.config.settings import load_settings
from src.domain.ports.buffer import EventBuffer
from src.domain.ports.publisher import DownstreamPublisher
from src.domain.services.buffer_worker import BufferedDeliveryWorker
from src.domain.services.health import HealthService
from src.domain.services.pipeline import EventPipeline
from src.modules.connections.manager import ConnectionsCoordinator
from src.modules.subscriptions.registry import NodeRegistry


@dataclass(slots=True)
class AppRuntime:
    config: AppConfigModel
    metrics: MetricsRegistry
    registry: NodeRegistry
    buffer: EventBuffer
    publisher: DownstreamPublisher
    pipeline: EventPipeline
    connections: ConnectionsCoordinator
    health: HealthService
    buffer_worker: BufferedDeliveryWorker
    buffer_ready: bool = False

    @classmethod
    async def build(cls) -> AppRuntime:
        config = load_settings()
        configure_logging(config.logging)
        logger = get_logger(__name__)
        metrics = MetricsRegistry()
        registry = NodeRegistry(config.nodes)
        buffer = create_buffer(config.buffer)
        publisher = create_publisher(config.publisher)
        pipeline = EventPipeline(
            publisher=publisher,
            buffer=buffer,
            metrics=metrics,
        )
        connections = ConnectionsCoordinator(
            endpoints=config.endpoints,
            registry=registry,
            pipeline=pipeline,
            metrics=metrics,
        )
        buffer_worker = BufferedDeliveryWorker(
            buffer=buffer,
            publisher=publisher,
            settings=config.buffer,
            metrics=metrics,
        )
        runtime = cls(
            config=config,
            metrics=metrics,
            registry=registry,
            buffer=buffer,
            publisher=publisher,
            pipeline=pipeline,
            connections=connections,
            health=HealthService(),
            buffer_worker=buffer_worker,
        )
        logger.info(
            "runtime_built",
            endpoints=len(config.endpoints),
            nodes=len(config.nodes),
            mode=config.service.mode,
            publisher_mode=config.publisher.mode,
        )
        return runtime

    async def start(self) -> None:
        await self.buffer.start()
        self.buffer_ready = True
        await self.buffer_worker.start()
        await self.connections.start()

    async def stop(self) -> None:
        await self.connections.stop()
        await self.buffer_worker.stop()
        await self.publisher.close()
        await self.buffer.close()
