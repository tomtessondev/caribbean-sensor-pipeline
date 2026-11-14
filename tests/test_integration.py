"""End to end on real containers: Mosquitto, TimescaleDB, the simulator and the ingestor."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import paho.mqtt.client as mqtt
import psycopg
import pytest
from paho.mqtt.enums import CallbackAPIVersion
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from sensor_pipeline.config import Settings
from sensor_pipeline.db import Store, migrate
from sensor_pipeline.ingest import ingest
from sensor_pipeline.messages import Telemetry, telemetry_topic
from sensor_pipeline.publisher import simulate
from sensor_pipeline.simulator import FaultRates
from sensor_pipeline.stations import STATIONS

pytestmark = pytest.mark.integration


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


def publish_raw(host: str, port: int, topic: str, payload: str) -> None:
    client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="test-raw")
    client.connect(host, port)
    client.loop_start()
    client.publish(topic, payload, qos=1).wait_for_publish(timeout=10)
    client.disconnect()
    client.loop_stop()


def test_migrations_are_idempotent(dsn: str) -> None:
    assert migrate(dsn) == ["001_schema.sql"]
    assert migrate(dsn) == []
    assert query(dsn, "select count(*) from stations") == [(6,)]


def test_simulated_stations_land_in_timescaledb(dsn: str, broker: tuple[str, int]) -> None:
    host, port = broker
    settings = Settings(mqtt_host=host, mqtt_port=port, db_dsn=dsn)
    migrate(dsn)
    store = Store.connect(dsn)
    stop_ingest = threading.Event()
    ingestor = threading.Thread(
        target=ingest, args=(settings, store, stop_ingest), kwargs={"max_wait_s": 0.5}
    )
    ingestor.start()
    time.sleep(2)  # let the ingestor subscribe before anything is published

    try:
        already_stopped = threading.Event()
        already_stopped.set()  # backfill only, no live loop
        published = simulate(
            settings,
            STATIONS,
            seed=1,
            interval_s=10,
            backfill_hours=1,
            backfill_step_s=60,
            stop=already_stopped,
            faults=FaultRates(drop=0, stuck_start=0, spike=0),
        )
        assert published == 6 * 60
        wait_until(lambda: query(dsn, "select count(*) from telemetry") == [(published,)])

        # QoS 1 may deliver twice: a redelivered reading must not create a second row.
        first = query(dsn, "select station_id, ts, seq from telemetry order by ts limit 1")[0]
        duplicate = Telemetry(station_id=first[0], ts=first[1], seq=first[2])
        publish_raw(host, port, telemetry_topic(first[0]), duplicate.model_dump_json())
        publish_raw(host, port, telemetry_topic(first[0]), '{"temperature_c": "hot"}')
        wait_until(lambda: query(dsn, "select count(*) from ingest_errors") == [(1,)])
        assert query(dsn, "select count(*) from telemetry") == [(published,)]
    finally:
        stop_ingest.set()
        ingestor.join(timeout=10)
        store.close()

    # simulate() closed every station cleanly: each one announced "offline".
    assert query(dsn, "select distinct status from station_status") == [("offline",)]
    assert query(dsn, "select count(*) from station_status") == [(6,)]
    # The hourly rollup answers from raw data that is not materialised yet.
    hourly = query(dsn, "select sum(readings) from telemetry_hourly")
    assert hourly == [(published,)]
