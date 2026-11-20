"""Real stations on a real TimescaleDB: the registry is seeded instead of the simulated
stations, and each station goes offline after its own silence threshold."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from sensor_pipeline.db import Store, migrate
from sensor_pipeline.messages import Telemetry
from sensor_pipeline.registry import OFFLINE_AFTER_S, REGISTRY_PATH, load_registry
from sensor_pipeline.stations import STATIONS
from tests.support import query

pytestmark = pytest.mark.integration


def test_migrate_seeds_the_registry(dsn: str) -> None:
    registry = load_registry(REGISTRY_PATH)
    migrate(dsn, source="meteofrance")

    rows = query(dsn, "select station_id, town, territory, offline_after_s from stations")
    assert sorted(rows) == sorted(
        (s.station_id, s.name, s.territory, s.offline_after_s) for s in registry
    )
    assert not {s.town for s in STATIONS} & {r[1] for r in rows}


def test_offline_follows_each_stations_threshold(dsn: str) -> None:
    migrate(dsn, source="meteofrance")
    registry = load_registry(REGISTRY_PATH)
    on_time, late, hourly = registry[:3]
    # The registry holds 6-minute stations only today: make one hourly, as
    # scripts/classify_cadence.py would for a station seen only on the hour.
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "update stations set offline_after_s = %s where station_id = %s",
            (OFFLINE_AFTER_S["hourly"], hourly.station_id),
        )
    now = datetime.now(UTC).replace(microsecond=0)
    store = Store.connect(dsn)
    store.insert_readings(
        [
            # 88 minutes of silence: normal for a 6-minute station delivering hourly
            # batches, read 80 minutes late...
            Telemetry(station_id=on_time.station_id, ts=now - timedelta(minutes=88), seq=1),
            # ...110 minutes is an outage...
            Telemetry(station_id=late.station_id, ts=now - timedelta(minutes=110), seq=1),
            # ...but normal for an hourly station read on the second pass only.
            Telemetry(station_id=hourly.station_id, ts=now - timedelta(minutes=140), seq=1),
        ]
    )
    store.close()

    rows = dict(query(dsn, "select station_id, offline_ratio from station_health"))
    assert rows[on_time.station_id] < 1
    assert rows[late.station_id] > 1
    assert rows[hourly.station_id] < 1


def test_simulated_stations_keep_five_minutes(dsn: str) -> None:
    migrate(dsn)  # STATION_SOURCE unset: the simulated network
    now = datetime.now(UTC).replace(microsecond=0)
    store = Store.connect(dsn)
    store.insert_readings([Telemetry(station_id="gp-pap-01", ts=now - timedelta(minutes=6), seq=1)])
    store.close()

    assert query(
        dsn,
        "select offline_after_s, offline_ratio > 1 from station_health"
        " where station_id = 'gp-pap-01'",
    ) == [(300, True)]
