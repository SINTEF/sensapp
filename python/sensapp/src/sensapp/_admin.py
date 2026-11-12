from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from ._models import (
    CreatedToken,
    DeletedSamples,
    VacuumResult,
    format_time,
    parse_created_token,
    parse_deleted_samples,
    parse_vacuum_result,
)

if TYPE_CHECKING:
    from .client import SensAppClient

# A little above the maintenance timeout of the server (one hour by default,
# `SENSAPP_HTTP_MAINTENANCE_TIMEOUT_SECONDS`), so that its answer reaches the client.
DEFAULT_VACUUM_TIMEOUT = 3630.0


class AdminClient:
    """Maintenance, deletes and tokens, as ``client.admin``.

    These calls change or remove data, so they need more than the ``read write``
    scope of a normal token: ``delete`` for the deletes and the vacuum, ``admin`` for
    tokens. They use the URL and the token of the client that owns them. The deletes
    follow its retry policy, but not after a ``504`` or a timeout: the server may
    still be deleting. The vacuum and the tokens are never retried.
    """

    def __init__(self, client: SensAppClient) -> None:
        self._client = client

    async def vacuum(self, *, timeout: float = DEFAULT_VACUUM_TIMEOUT) -> VacuumResult:
        """Remove the duplicate samples, then clean up and compact the database.

        Only exact duplicates go (same series, timestamp and value, the first one
        written is kept). ``duplicates_removed`` is ``None`` on a backend that cannot
        remove them. On a large database this scans every value table and is slow:
        ``timeout`` is the time to wait for it, in seconds.

        It is **never retried**, whatever the retry policy of the client: after a
        ``504`` or a lost connection the server carries on, and two vacuums running
        together compete for the same rows. Check the server before running it again.

        Needs the ``delete`` scope, on a token without a sensor allow list (``403``
        otherwise).
        """
        response = await self._client._post(
            "/api/v1/admin/vacuum", timeout=timeout, retry=False
        )
        return parse_vacuum_result(response.json())

    async def delete_series(self, series_uuid: str | UUID) -> None:
        """Delete a series: its samples, its labels and the sensor itself.

        Publishing the same sensor again recreates it with the same UUID. Raises
        `SensAppHTTPError` with status 404 when the series does not exist, and 501 on
        a backend that cannot delete (RRDCached). It is retried when the server is
        overloaded or the connection fails, not after a ``504`` or a timeout. If a lost
        answer made the client retry a delete that had worked, the retry gets a 404.
        Needs the ``delete`` scope.
        """
        await self._client._delete(f"/series/{series_uuid}")

    async def delete_samples(
        self,
        series_uuid: str | UUID,
        *,
        start: datetime | str,
        end: datetime | str,
    ) -> DeletedSamples:
        """Delete the samples of a series between ``start`` and ``end``, inclusive.

        Both bounds are required, so that a call cannot wipe a series by accident (use
        `delete_series` for that). The sensor and its labels are kept. To correct a
        value, delete its timestamp and publish it again. It is retried like
        `delete_series`: if a lost answer made the client retry a delete that had
        worked, the retry reports 0 deleted samples.
        Raises `SensAppHTTPError` with status 404 when the series does not exist, and
        501 on a backend that cannot delete. Needs the ``delete`` scope.
        """
        response = await self._client._delete(
            f"/series/{series_uuid}/samples",
            params={"start": format_time(start), "end": format_time(end)},
        )
        return parse_deleted_samples(response.json())

    async def create_token(
        self,
        subject: str,
        scope: list[str],
        *,
        duration_seconds: int,
        sensors: list[str] | None = None,
    ) -> CreatedToken:
        """Make a token for a client: who it is for, what it may do, for how long.

        ``scope`` is a list among ``"read"``, ``"write"`` and ``"delete"``. The server
        never makes an ``admin`` token here (``403``): those come from
        ``sensapp generate-token``. ``duration_seconds`` is how long the token is
        valid, at most what the server allows (a year by default). ``sensors`` limits
        the token to those sensor names.

        The token is only shown in this answer: the server keeps nothing, so it cannot
        be listed or looked up later. It is **never retried**: after a lost answer a
        retry would make a second token, valid until it expires, while the first one
        may have been made too and cannot be revoked. Raises `SensAppHTTPError` with
        status 404 when authentication is disabled. Needs the ``admin`` scope.
        """
        body: dict[str, object] = {
            "subject": subject,
            "scope": scope,
            "duration_seconds": duration_seconds,
        }
        if sensors is not None:
            body["sensors"] = sensors
        response = await self._client._post(
            "/api/v1/admin/tokens", json_body=body, retry=False
        )
        return parse_created_token(response.json())
