from __future__ import annotations

import asyncio

from sensapp import SensAppClient


async def main() -> None:
    # Needs a token with the `delete` scope: SENSAPP_TOKEN, and SENSAPP_URL if not local
    async with SensAppClient.from_env() as client:
        result = await client.admin.vacuum()
        if result.duplicates_removed is None:
            print("vacuum done, this backend cannot remove duplicates")
        else:
            print(f"vacuum done, {result.duplicates_removed} duplicate samples removed")


if __name__ == "__main__":
    asyncio.run(main())
