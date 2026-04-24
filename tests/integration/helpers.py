from __future__ import annotations

import asyncio
import socket
from collections.abc import Callable
from contextlib import suppress

from asyncua import Server


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(sock.getsockname()[1])


class MockOpcUaServer:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self.server = Server()
        self.namespace_idx: int | None = None
        self.pressure = None
        self.temperature = None
        self._update_task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        await self.server.init()
        self.server.set_endpoint(self.endpoint)
        self.server.set_server_name("Mock OPC UA Test Server")
        self.namespace_idx = await self.server.register_namespace("urn:opcua:mvp")
        pump = await self.server.nodes.objects.add_object(self.namespace_idx, "Pump01")
        self.pressure = await pump.add_variable(f"ns={self.namespace_idx};s=Pump01.Pressure", "Pressure", 100.0)
        self.temperature = await pump.add_variable(f"ns={self.namespace_idx};s=Pump01.Temperature", "Temperature", 45.0)
        await self.pressure.set_writable()
        await self.temperature.set_writable()
        await self.server.start()
        self._running = True

    async def stop(self) -> None:
        if self._update_task is not None:
            self._update_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._update_task
        if self._running:
            await self.server.stop()
            self._running = False

    async def write_pressure(self, value: float) -> None:
        await self.pressure.write_value(value)

    async def start_updates(self, values: list[float], interval_seconds: float = 0.5) -> None:
        async def _runner() -> None:
            for value in values:
                await self.write_pressure(value)
                await asyncio.sleep(interval_seconds)

        self._update_task = asyncio.create_task(_runner())


async def wait_until(assertion: Callable[[], bool], timeout_seconds: float = 10.0, interval_seconds: float = 0.1) -> None:
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while asyncio.get_event_loop().time() < deadline:
        if assertion():
            return
        await asyncio.sleep(interval_seconds)
    raise AssertionError("Условие не выполнено за отведенное время.")
