"""Shared fixtures: a worker client and the strict OpenMetadata replay server."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _start(env: dict[str, str]) -> tuple[subprocess.Popen[str], str]:
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "test" / "mock_openmetadata" / "server.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env={**os.environ, **env},
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT:"), line
    return proc, f"http://127.0.0.1:{line.removeprefix('PORT:')}"


@pytest.fixture(scope="session")
def mock_om() -> Iterator[str]:
    """The flaky replay server (429 on every third new request); returns its root URL."""
    proc, url = _start({"MOCK_FLAKY": "1"})
    yield url
    proc.terminate()
    proc.wait(timeout=10)
