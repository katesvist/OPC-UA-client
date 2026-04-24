from __future__ import annotations

import asyncio
import math

from asyncua import Server


async def main() -> None:
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")
    server.set_server_name("OPC UA MVP Mock Server")

    namespace = await server.register_namespace("urn:opcua:mvp")
    pump = await server.nodes.objects.add_object(namespace, "Pump01")
    pressure = await pump.add_variable(f"ns={namespace};s=Pump01.Pressure", "Pressure", 100.0)
    temperature = await pump.add_variable(f"ns={namespace};s=Pump01.Temperature", "Temperature", 45.0)
    await pressure.set_writable()
    await temperature.set_writable()

    await server.start()
    print("Mock OPC UA server started at opc.tcp://0.0.0.0:4840/freeopcua/server/")
    try:
        tick = 0
        while True:
            tick += 1
            await pressure.write_value(100.0 + math.sin(tick / 5) * 10)
            await temperature.write_value(45.0 + math.cos(tick / 10) * 3)
            await asyncio.sleep(1)
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
