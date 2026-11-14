"""Helpers shared by the integration tests."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import psycopg


def query(dsn: str, sql: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(sql.encode()).fetchall()


def wait_until(condition: Callable[[], bool], timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.25)
    raise AssertionError("condition not met in time")
