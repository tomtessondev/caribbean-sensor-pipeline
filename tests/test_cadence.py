"""Real stations on a real TimescaleDB: the registry is seeded instead of the simulated
stations, and each station goes offline after its own silence threshold."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

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
    prompt = next(s for s in registry if s.offline_after_s == OFFLINE_AFTER_S["prompt"])
    batched = next(s for s in registry if s.offline_after_s == OFFLINE_AFTER_S["hourly_batch"])
    late = next(
        s for s in registry if s.offline_after_s == OFFLINE_AFTER_S["prompt"] and s != prompt
    )
    now = datetime.now(UTC).replace(microsecond=0)
    store = Store.connect(dsn)
    store.insert_readings(
        [
            # 19 minutes of silence: normal for a station read about 10 minutes late.
            Telemetry(station_id=prompt.station_id, ts=now - timedelta(minutes=19), seq=1),
            # 80 minutes: normal for a station that sends its readings once an hour...
            Telemetry(station_id=batched.station_id, ts=now - timedelta(minutes=80), seq=1),
            # ...but an outage for a prompt one.
            Telemetry(station_id=late.station_id, ts=now - timedelta(minutes=80), seq=1),
        ]
    )
    store.close()

    rows = dict(query(dsn, "select station_id, offline_ratio from station_health"))
    assert rows[prompt.station_id] < 1
    assert rows[batched.station_id] < 1
    assert rows[late.station_id] > 1


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
