from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


class SensAppError(Exception):
    """Base class for SDK errors."""


class SensAppValidationError(SensAppError):
    """Raised when client-side input validation fails."""


@dataclass(slots=True)
class SensAppHTTPError(SensAppError):
    status_code: int
    message: str
    details: dict[str, Any] | None = None
    response_text: str | None = None

    @classmethod
    def from_response(cls, response: httpx.Response) -> SensAppHTTPError:
        details: dict[str, Any] | None = None
        message = response.text or f"HTTP {response.status_code}"

        try:
            payload = response.json()
        except ValueError:
            payload = None

        if isinstance(payload, dict):
            details = payload
            if len(payload) == 1:
                key, value = next(iter(payload.items()))
                message = f"{key}: {value}"
            else:
                message = str(payload)

        return cls(
            status_code=response.status_code,
            message=message,
            details=details,
            response_text=response.text,
        )

    def __str__(self) -> str:
        return f"SensApp request failed with status {self.status_code}: {self.message}"
