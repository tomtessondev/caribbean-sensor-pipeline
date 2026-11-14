from __future__ import annotations

from collections.abc import Iterator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer


# Module scope: each test module gets its own database, so migrations start from scratch.
@pytest.fixture(scope="module")
def dsn() -> Iterator[str]:
    with PostgresContainer(
        "timescale/timescaledb:2.22.1-pg17",
        username="sensors",
        password="sensors",
        dbname="sensors",
    ) as db:
        yield db.get_connection_url(driver=None)


@pytest.fixture(scope="module")
def broker() -> Iterator[tuple[str, int]]:
    container = (
        DockerContainer("eclipse-mosquitto:2.0.22")
        .with_command("mosquitto -c /mosquitto-no-auth.conf")
        .with_exposed_ports(1883)
    )
    with container:
        wait_for_logs(container, "running")
        yield container.get_container_host_ip(), int(container.get_exposed_port(1883))
