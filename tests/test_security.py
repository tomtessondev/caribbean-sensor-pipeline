"""The secured broker of docker-compose.yml, attacked on a real Mosquitto: TLS required, a
valid login required, stations confined to their own topics, the ingestor read-only. And
the ingestor's database role: insert, nothing more."""

from __future__ import annotations

import queue
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt
import psycopg
import pytest
from paho.mqtt.enums import CallbackAPIVersion
from psycopg.conninfo import make_conninfo
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from sensor_pipeline.config import Settings
from sensor_pipeline.db import Store, migrate
from sensor_pipeline.ingest import ingest
from sensor_pipeline.messages import Telemetry, telemetry_topic
from sensor_pipeline.provision import INGESTOR_USER, load_credentials, provision_broker
from sensor_pipeline.publisher import simulate
from sensor_pipeline.simulator import FaultRates
from sensor_pipeline.stations import STATIONS
from tests.support import query, wait_until

pytestmark = pytest.mark.integration

DOCKER = Path(__file__).resolve().parents[1] / "docker"


@pytest.fixture(scope="module")
def secrets_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("broker")
    provision_broker(out, [s.station_id for s in STATIONS])
    return out


@pytest.fixture(scope="module")
def secure_broker(secrets_dir: Path) -> Iterator[tuple[str, int]]:
    """Mosquitto exactly as docker-compose.yml runs it."""
    container = (
        DockerContainer("eclipse-mosquitto:2.0.22")
        .with_volume_mapping(str(secrets_dir), "/broker", "ro")
        .with_volume_mapping(
            str(DOCKER / "mosquitto.conf"), "/mosquitto/config/mosquitto.conf", "ro"
        )
        .with_volume_mapping(
            str(DOCKER / "broker-entrypoint.sh"), "/docker/broker-entrypoint.sh", "ro"
        )
        .with_kwargs(entrypoint=["/bin/sh", "/docker/broker-entrypoint.sh"])
        .with_exposed_ports(8883)
    )
    with container:
        wait_for_logs(container, "running")
        yield container.get_container_host_ip(), int(container.get_exposed_port(8883))


def client(
    secrets_dir: Path, client_id: str, username: str | None, password: str | None, tls: bool = True
) -> tuple[mqtt.Client, queue.Queue[Any]]:
    """A connected (or refused) client; the queue receives the CONNACK reason, then messages."""
    events: queue.Queue[Any] = queue.Queue()
    c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id)
    if tls:
        c.tls_set(ca_certs=str(secrets_dir / "ca.crt"))
    if username is not None:
        c.username_pw_set(username, password)
    c.on_connect = lambda _c, _u, _f, reason, _p: events.put(reason)
    c.on_message = lambda _c, _u, msg: events.put(msg.topic)
    return c, events


def connack(
    secrets_dir: Path, broker: tuple[str, int], username: str | None, password: str | None
) -> Any:
    c, events = client(secrets_dir, f"probe-{username}", username, password)
    c.connect(*broker)
    c.loop_start()
    try:
        return events.get(timeout=10)
    finally:
        c.disconnect()
        c.loop_stop()


def test_plain_mqtt_is_refused(secrets_dir: Path, secure_broker: tuple[str, int]) -> None:
    """Valid credentials, but no TLS: the broker never accepts the session."""
    password = load_credentials(secrets_dir / "credentials.json")["gp-pap-01"]
    c, events = client(secrets_dir, "plain", "gp-pap-01", password, tls=False)
    try:
        c.connect(*secure_broker)
    except OSError:
        return  # dropped while connecting: refused
    c.loop_start()
    try:
        reason = events.get(timeout=3)
        assert reason.is_failure, "a plaintext session was accepted"
    except queue.Empty:
        pass  # no CONNACK at all: the TLS listener dropped the plaintext bytes
    finally:
        c.loop_stop()
    assert not c.is_connected()


def test_logins_are_checked(secrets_dir: Path, secure_broker: tuple[str, int]) -> None:
    password = load_credentials(secrets_dir / "credentials.json")["gp-pap-01"]

    assert not connack(secrets_dir, secure_broker, "gp-pap-01", password).is_failure
    assert connack(secrets_dir, secure_broker, "gp-pap-01", "not-the-password").is_failure
    assert connack(secrets_dir, secure_broker, "mq-fdf-01", password).is_failure  # another's
    assert connack(secrets_dir, secure_broker, None, None).is_failure  # anonymous


def test_stations_cannot_impersonate_each_other(
    secrets_dir: Path, secure_broker: tuple[str, int]
) -> None:
    credentials = load_credentials(secrets_dir / "credentials.json")
    reader, received = client(secrets_dir, "reader", INGESTOR_USER, credentials[INGESTOR_USER])
    reader.connect(*secure_broker)
    reader.loop_start()
    assert not received.get(timeout=10).is_failure
    reader.subscribe("antilles/+/telemetry", qos=1)
    time.sleep(1)

    station, connected = client(secrets_dir, "pap", "gp-pap-01", credentials["gp-pap-01"])
    station.connect(*secure_broker)
    station.loop_start()
    connected.get(timeout=10)
    station.publish(telemetry_topic("mq-fdf-01"), "spoofed", qos=1).wait_for_publish(5)
    station.publish(telemetry_topic("gp-pap-01"), "genuine", qos=1).wait_for_publish(5)
    # The ingestor itself may read, not write.
    reader.publish(telemetry_topic("gp-bte-01"), "from-the-ingestor", qos=1).wait_for_publish(5)
    time.sleep(2)

    topics = []
    while not received.empty():
        topics.append(received.get())
    assert topics == [telemetry_topic("gp-pap-01")]
    for c in (station, reader):
        c.disconnect()
        c.loop_stop()


def test_secured_pipeline_end_to_end(
    dsn: str, secrets_dir: Path, secure_broker: tuple[str, int]
) -> None:
    """The simulator logs each station in with its own password, the ingestor with its own
    MQTT login and its insert-only database role."""
    migrate(dsn, ingestor_password="ingestor-test")
    host, port = secure_broker
    common = {
        "mqtt_host": host,
        "mqtt_port": port,
        "mqtt_tls_ca": secrets_dir / "ca.crt",
        "mqtt_credentials_file": secrets_dir / "credentials.json",
    }
    ingestor_dsn = make_conninfo(dsn, user="sensor_ingestor", password="ingestor-test")
    store = Store.connect(ingestor_dsn)
    stop = threading.Event()
    worker = threading.Thread(
        target=ingest,
        args=(Settings(**common, mqtt_username=INGESTOR_USER, db_dsn=ingestor_dsn), store, stop),
        kwargs={"max_wait_s": 0.5},
    )
    worker.start()
    time.sleep(2)
    try:
        backfill_only = threading.Event()
        backfill_only.set()
        published = simulate(
            Settings(**common, db_dsn=dsn),
            STATIONS,
            seed=3,
            interval_s=60,
            backfill_hours=1,
            stop=backfill_only,
            faults=FaultRates(drop=0, stuck_start=0, spike=0),
        )
        wait_until(lambda: query(dsn, "select count(*) from telemetry") == [(published,)])
    finally:
        stop.set()
        worker.join(timeout=10)
        store.close()
    assert query(dsn, "select count(*), min(status) from station_status") == [(6, "offline")]


def test_ingestor_role_can_only_insert(dsn: str) -> None:
    migrate(dsn, ingestor_password="ingestor-test")
    ingestor_dsn = make_conninfo(dsn, user="sensor_ingestor", password="ingestor-test")
    reading = Telemetry(station_id="gp-pap-01", ts="2026-11-01T00:00:00+00:00", seq=1)

    store = Store.connect(ingestor_dsn)
    assert store.insert_readings([reading]) == 1
    assert store.insert_readings([reading]) == 0  # duplicate skipped, no SELECT needed
    store.close()

    for statement in (
        "select pm10_ugm3 from telemetry",
        "delete from telemetry",
        "update telemetry set pm10_ugm3 = 0",
        "select * from reading_flags",
        "drop table telemetry",
    ):
        with (
            psycopg.connect(ingestor_dsn) as conn,
            pytest.raises((psycopg.errors.InsufficientPrivilege, psycopg.errors.UndefinedTable)),
        ):
            conn.execute(statement.encode())
