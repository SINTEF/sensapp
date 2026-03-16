from datetime import datetime, timezone

from sensapp_sdk import SamplePoint, SensAppClient

with SensAppClient("http://127.0.0.1:3000") as client:
    client.publish_samples(
        sensor_name="temperature",
        samples=[
            SamplePoint(datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc), 21.5),
            SamplePoint(datetime(2026, 3, 16, 12, 1, tzinfo=timezone.utc), 21.7),
            SamplePoint(datetime(2026, 3, 16, 12, 2, tzinfo=timezone.utc), 21.9),
        ],
    )
