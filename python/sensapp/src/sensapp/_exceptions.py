from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class SensAppError(Exception):
    """Base class for sensapp errors."""


class SensAppValidationError(SensAppError):
    """Raised when client-side input validation fails."""


@dataclass(slots=True)
class SensAppHTTPError(SensAppError):
    status_code: int
    message: str
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        return f"SensApp HTTP {self.status_code}: {self.message}"
