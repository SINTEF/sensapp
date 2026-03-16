from __future__ import annotations

import os
import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _wait_for_server(
    base_url: str,
    process: subprocess.Popen[str],
    log_path: Path,
    timeout_seconds: float = 90.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    with httpx.Client(timeout=1.0) as client:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                log_output = log_path.read_text(encoding="utf-8", errors="replace")
                raise RuntimeError(
                    "SensApp server exited before becoming ready. "
                    f"Exit code: {process.returncode}. Log output:\n{log_output}"
                )
            try:
                response = client.get(f"{base_url}/health/ready")
                if response.status_code in {200, 503}:
                    return
            except httpx.HTTPError as exc:
                last_error = exc
            time.sleep(0.25)

    log_output = log_path.read_text(encoding="utf-8", errors="replace")
    raise RuntimeError(
        f"SensApp server did not become ready: {last_error!r}\n{log_output}"
    )


@pytest.fixture(scope="session")
def live_server_url() -> Iterator[str]:
    repo_root = _repo_root()
    port = _find_free_port()

    with TemporaryDirectory(prefix="sensapp-sdk-it-") as temp_dir:
        database_path = Path(temp_dir) / "sensapp-sdk-integration.db"
        log_path = Path(temp_dir) / "sensapp.log"
        env = {
            "SENSAPP_ENDPOINT": "127.0.0.1",
            "SENSAPP_PORT": str(port),
            "SENSAPP_STORAGE_CONNECTION_STRING": f"sqlite://{database_path}",
        }

        command = [
            "cargo",
            "run",
            "--quiet",
            "--no-default-features",
            "--features",
            "sqlite",
        ]

        with log_path.open("w", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                command,
                cwd=repo_root,
                env={**os.environ, **env},
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
            )

            base_url = f"http://127.0.0.1:{port}"
            try:
                _wait_for_server(base_url, process, log_path)
                yield base_url
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
