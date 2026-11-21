"""The spike rule on hourly readings (the official air-quality stations), on a real
TimescaleDB: a reading arrives about 1.5 h after its hour, and its following neighbours
one per hour after it, so it must not be judged before they exist."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from sensor_pipeline.db import migrate
from tests.support import query

pytestmark = pytest.mark.integration

HOUR = timedelta(hours=1)
LATENCY = timedelta(minutes=90)


@pytest.fixture(scope="module")
def db(dsn: str) -> str:
    migrate(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "insert into stations (station_id, town, territory, latitude, longitude,"
            " offline_after_s, source)"
            " values ('aq-plume', 'Plume', 'Guadeloupe', 16.2, -61.5, 10800, 'airquality'),"
            "        ('aq-spike', 'Spike', 'Guadeloupe', 16.3, -61.6, 10800, 'airquality')"
        )
    return dsn


def arrive(dsn: str, station: str, ts: datetime, pm10: float, received_at: datetime) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "insert into telemetry (station_id, ts, seq, pm10_ugm3, received_at)"
            " values (%s, %s, %s, %s, %s)",
            (station, ts, int(ts.timestamp()) // 3600, pm10, received_at),
        )


def run_scheduled_rules(dsn: str) -> None:
    """flag_readings with the configuration of its scheduled job."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "select config from timescaledb_information.jobs where proc_name = 'flag_readings'"
        ).fetchone()
        assert row is not None
        conn.execute("call flag_readings(null, %s)", (psycopg.types.json.Jsonb(row[0]),))


def spikes(dsn: str, station: str) -> list[datetime]:
    rows = query(
        dsn,
        f"select ts from reading_flags where station_id = '{station}' and flag = 'pm_spike'"
        " order by ts",
    )
    return [r[0] for r in rows]


def test_an_isolated_spike_is_flagged_once_its_neighbours_arrived(db: str) -> None:
    """The spike arrived 3.5 hours ago, its third follower half an hour ago: the rule
    still looks back far enough to judge it."""
    now = datetime.now(UTC)
    values = [15.0] * 8 + [80.0] + [15.0, 16.0, 14.0]
    first = now.replace(minute=0, second=0, microsecond=0) - len(values) * HOUR - LATENCY
    for i, pm10 in enumerate(values):
        ts = first + i * HOUR
        arrive(db, "aq-spike", ts, pm10, received_at=ts + LATENCY)

    run_scheduled_rules(db)

    assert spikes(db, "aq-spike") == [first + 8 * HOUR]


def test_a_rising_value_is_not_flagged_before_its_neighbours_arrive(db: str) -> None:
    """15, 15, 15, then 50: alone, the 50 looks like a spike. Its next hours, near 50,
    show the start of a plume: once they arrived, the 50 must not have been flagged."""
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = now - 8 * HOUR
    for i, pm10 in enumerate([15.0, 15.0, 15.0, 50.0, 52.0, 49.0, 51.0]):
        ts = start + i * HOUR
        arrive(db, "aq-plume", ts, pm10, received_at=datetime.now(UTC))
        run_scheduled_rules(db)

    assert spikes(db, "aq-plume") == []


def test_rules_look_back_twelve_hours(db: str) -> None:
    jobs = query(
        db,
        "select config ->> 'lookback' from timescaledb_information.jobs"
        " where proc_name = 'flag_readings'",
    )
    assert jobs == [("12 hours",)]
