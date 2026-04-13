from __future__ import annotations

import asyncio

from sensapp import SensAppClient


async def main() -> None:
    async with SensAppClient.from_env() as client:
        metrics = await client.list_metrics()
        print("metrics:")
        for metric in metrics.metrics:
            print(f"- {metric.name} ({metric.sensor_type}) x{metric.series_count}")

        series_catalog = await client.list_series(limit=5)
        print("\nseries:")
        for series in series_catalog.series:
            print(f"- {series.uuid}: {series.name} {series.labels}")


if __name__ == "__main__":
    asyncio.run(main())
