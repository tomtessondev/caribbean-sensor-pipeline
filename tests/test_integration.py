"""End to end on real containers: Mosquitto, TimescaleDB, the simulator and the ingestor."""

from __future__ import annotations

import threading
import time

import paho.mqtt.client as mqtt
import pytest
from paho.mqtt.enums import CallbackAPIVersion

from sensor_pipeline.config import Settings
from sensor_pipeline.db import Store, migrate
from sensor_pipeline.ingest import ingest
from sensor_pipeline.messages import Telemetry, telemetry_topic
from sensor_pipeline.publisher import simulate
from sensor_pipeline.simulator import FaultRates
from sensor_pipeline.stations import STATIONS
from tests.support import query, wait_until

pytestmark = pytest.mark.integration


def publish_raw(host: str, port: int, topic: str, payload: str) -> None:
    client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="test-raw")
    client.connect(host, port)
    client.loop_start()
    client.publish(topic, payload, qos=1).wait_for_publish(timeout=10)
    client.disconnect()
    client.loop_stop()


def test_migrations_are_idempotent(dsn: str) -> None:
    assert migrate(dsn) == [
        "001_schema.sql",
        "002_station_health.sql",
        "003_data_quality.sql",
        "004_ingestor_role.sql",
        "005_station_cadence.sql",
        "006_site_reader.sql",
        "007_active_stations.sql",
        "008_mountain_temperatures.sql",
    ]
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
            interval_s=60,
            backfill_hours=1,
            stop=already_stopped,
            faults=FaultRates(drop=0, stuck_start=0, spike=0),
        )
        assert published == 6 * 61  # one reading per minute, both ends included
        wait_until(lambda: query(dsn, "select count(*) from telemetry") == [(published,)])

        # QoS 1 may deliver twice: a redelivered reading must not create a second row.
        first = query(dsn, "select station_id, ts, seq from telemetry order by ts limit 1")[0]
        duplicate = Telemetry(station_id=first[0], ts=first[1], seq=first[2])
        publish_raw(host, port, telemetry_topic(first[0]), duplicate.model_dump_json())
        publish_raw(host, port, telemetry_topic(first[0]), '{"temperature_c": "hot"}')
        wait_until(lambda: query(dsn, "select count(*) from ingest_errors") == [(1,)])
        assert query(dsn, "select count(*) from telemetry") == [(published,)]

        # A restarted simulator republishes its history: same slots, same sequence numbers,
        # so nothing is duplicated and no loss appears (at most the newest minute is new).
        again = simulate(
            settings,
            STATIONS,
            seed=1,
            interval_s=60,
            backfill_hours=1,
            stop=already_stopped,
            faults=FaultRates(drop=0, stuck_start=0, spike=0),
        )
        wait_until(lambda: query(dsn, "select count(*) from telemetry")[0][0] >= published)
        time.sleep(2)  # let the ingestor write the republished batch
        stored = query(dsn, "select count(*) from telemetry")[0][0]
        assert stored in (published, published + 6), (stored, again)
        assert query(dsn, "select max(loss_pct_24h) from station_health")[0][0] == 0
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
