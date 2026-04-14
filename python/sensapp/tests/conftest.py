from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import niquests
import pytest


@dataclass
class MockResponse:
    """Minimal stand-in for ``niquests.Response`` in unit tests."""

    status_code: int = 200
    _content: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def text(self) -> str:
        return self._content.decode("utf-8")

    def json(self) -> Any:
        return json.loads(self._content)

    # ── convenience constructors ────────────────────────────────

    @classmethod
    def json_ok(cls, data: Any) -> MockResponse:
        return cls(_content=json.dumps(data).encode())

    @classmethod
    def text_ok(cls, text: str) -> MockResponse:
        return cls(_content=text.encode())

    @classmethod
    def bytes_ok(cls, data: bytes) -> MockResponse:
        return cls(_content=data)

    @classmethod
    def error(cls, status_code: int, body: str = "") -> MockResponse:
        return cls(status_code=status_code, _content=body.encode())


@pytest.fixture(scope="session")
def live_server_url() -> str:
    base_url = os.environ.get("SENSAPP_LIVE_URL", "http://127.0.0.1:3000")

    try:
        response = niquests.get(f"{base_url}/health/ready", timeout=1)
    except Exception as exc:
        pytest.skip(f"live SensApp server not available at {base_url}: {exc}")

    if response.status_code not in {200, 503}:
        pytest.skip(
            f"live SensApp server at {base_url} returned unexpected status "
            f"{response.status_code}"
        )

    return base_url
