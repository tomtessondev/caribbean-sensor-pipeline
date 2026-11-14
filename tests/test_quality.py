"""The data-quality rules on crafted data, on a real TimescaleDB."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from sensor_pipeline.db import Store, migrate
from sensor_pipeline.messages import Telemetry
from tests.support import query

pytestmark = pytest.mark.integration

STEP = timedelta(seconds=10)


def run_rules(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("""call flag_readings(null, '{"lookback": "2 hours"}')""")


def flags(dsn: str) -> list[tuple[str, str, int]]:
    rows = query(
        dsn,
        "select station_id, flag, count(*) from reading_flags group by 1, 2 order by 1, 2",
    )
    return [(r[0], r[1], r[2]) for r in rows]


@pytest.fixture(scope="module")
def seeded(dsn: str) -> tuple[str, datetime]:
    migrate(dsn)
    start = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=1)
    readings: list[Telemetry] = []

    def add(station: str, i: int, pm10: float, temperature: float = 27.0) -> None:
        ts = start + i * STEP
        readings.append(
            Telemetry(
                station_id=station,
                ts=ts,
                seq=int(ts.timestamp()) // 10,
                pm10_ugm3=pm10,
                temperature_c=temperature,
                humidity_pct=75,
                pressure_hpa=1013,
            )
        )

    # Basse-Terre replays two-day-old readings after an outage: they arrive now, so they
    # are checked now. One of them is a spike.
    for i in range(60):
        ts = start - timedelta(days=2) + i * STEP
        readings.append(
            Telemetry(
                station_id="gp-bte-01",
                ts=ts,
                seq=int(ts.timestamp()) // 10,
                pm10_ugm3=350.0 if i == 30 else 14.0 + (i % 6) * 0.5,
            )
        )
    for i in range(120):
        # Pointe-à-Pitre: noisy background, one electrical spike at i=60, slot 90 lost.
        if i != 90:
            add("gp-pap-01", i, 400.0 if i == 60 else 18.0 + (i % 7) * 0.4)
        # Fort-de-France: the particle counter hangs at i=40 for 40 readings.
        add("mq-fdf-01", i, 25.3 if 40 <= i < 80 else 20.0 + (i % 5) * 0.3)
        # Le Lamentin: a Saharan plume ramping from 20 to 220: high, but smooth.
        add("mq-lam-01", i, 20.0 + i * 200 / 119 + (i % 3) * 0.2)
        # Sainte-Anne: one impossible temperature for a sea-level Caribbean town.
        add("mq-san-01", i, 15.0 + (i % 4) * 0.5, temperature=55.0 if i == 30 else 27.5)

    store = Store.connect(dsn)
    store.insert_readings(readings)
    store.close()
    run_rules(dsn)
    return dsn, start


def test_each_rule_flags_exactly_its_readings(seeded: tuple[str, datetime]) -> None:
    dsn, _ = seeded
    assert flags(dsn) == [
        ("gp-bte-01", "pm_spike", 1),  # two days old, but received in the lookback
        ("gp-pap-01", "pm_spike", 1),
        ("mq-fdf-01", "pm_stuck", 40),
        ("mq-san-01", "implausible_weather", 1),
    ]  # and nothing on the smooth plume at Le Lamentin


def test_rules_are_idempotent(seeded: tuple[str, datetime]) -> None:
    dsn, _ = seeded
    before = flags(dsn)
    run_rules(dsn)
    assert flags(dsn) == before


def test_clean_view_drops_only_the_suspect_values(seeded: tuple[str, datetime]) -> None:
    dsn, start = seeded
    spike_ts = start + 60 * STEP
    raw = query(
        dsn, f"select pm10_ugm3 from telemetry where station_id = 'gp-pap-01' and ts = '{spike_ts}'"
    )
    clean = query(
        dsn,
        "select pm10_ugm3, temperature_c from telemetry_clean"
        f" where station_id = 'gp-pap-01' and ts = '{spike_ts}'",
    )
    assert raw == [(400.0,)]  # the raw table is never modified
    assert clean == [(None, 27.0)]  # the spike is hidden, the temperature kept


def test_quality_score_combines_losses_and_flags(seeded: tuple[str, datetime]) -> None:
    dsn, _ = seeded
    rows = query(
        dsn,
        "select station_id, sum(received), sum(lost), sum(flagged) from station_quality_daily"
        " where station_id in ('gp-pap-01', 'mq-fdf-01', 'mq-lam-01')"
        " group by 1 order by 1",
    )
    assert rows == [
        ("gp-pap-01", 119, 1, 1),  # slot 90 lost, one spike
        ("mq-fdf-01", 120, 0, 40),
        ("mq-lam-01", 120, 0, 0),
    ]
    # 118 valid readings out of 120 expected.
    score = query(
        dsn,
        "select round(100.0 * sum(received - flagged) / sum(received + lost), 1)"
        " from station_quality_daily where station_id = 'gp-pap-01'",
    )
    assert float(score[0][0]) == pytest.approx(98.3)


def test_rules_run_on_a_schedule(seeded: tuple[str, datetime]) -> None:
    dsn, _ = seeded
    jobs = query(
        dsn,
        "select schedule_interval from timescaledb_information.jobs"
        " where proc_name = 'flag_readings'",
    )
    assert jobs == [(timedelta(minutes=5),)]
