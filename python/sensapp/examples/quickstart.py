from __future__ import annotations

import asyncio

from sensapp import SensAppClient


async def main() -> None:
    async with SensAppClient.from_env() as client:
        await client.publish("temperature", 21.5)
        series = await client.query_one("temperature[1h]")
        print(series.frame)


if __name__ == "__main__":
    asyncio.run(main())
