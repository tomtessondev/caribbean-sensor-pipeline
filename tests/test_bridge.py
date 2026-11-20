"""The bridge from Météo-France packages to one MQTT session per station, with a fake API
and fake sessions."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from sensor_pipeline.bridge import Bridge, mqtt_sessions, run
from sensor_pipeline.config import Settings
from sensor_pipeline.messages import Telemetry
from sensor_pipeline.meteofrance import Observation
from sensor_pipeline.provision import provision_broker
from sensor_pipeline.registry import REGISTRY_PATH, RegistryStation, load_registry, seeded_stations

REGISTRY = load_registry(REGISTRY_PATH)
RAIZET, DESAIX = (next(s for s in REGISTRY if s.mf_id == i) for i in ("97101015", "97209004"))
NOW = datetime(2026, 11, 20, 4, 10, 30, tzinfo=UTC)
FIRST_PASS = datetime(2026, 11, 20, 4, 0, tzinfo=UTC)  # slot(NOW - 10 min)
SECOND_PASS = datetime(2026, 11, 20, 2, 48, tzinfo=UTC)  # slot(NOW - 80 min)


def obs(mf_id: str, at: datetime, **measures: Any) -> Observation:
    values = {"t": 300.15, "u": 80, "pmer": 101300, **measures}
    return Observation.model_validate(
        {"geo_id_insee": mf_id, "validity_time": at.isoformat(), **values}
    )


class FakeApi:
    """Answers each slot with what `packages` holds for it."""

    def __init__(self) -> None:
        self.packages: dict[datetime, list[Observation]] = {}
        self.requested: list[datetime] = []
        self.fail: Exception | None = None

    def package(self, at: datetime) -> list[Observation]:
        self.requested.append(at)
        if self.fail is not None:
            raise self.fail
        return self.packages.get(at, [])


class FakeSession:
    def __init__(self, station: RegistryStation) -> None:
        self.station = station
        self.sent: list[Telemetry] = []
        self.closed = False

    def publish(self, reading: Telemetry) -> None:
        self.sent.append(reading)

    def close(self) -> None:
        self.closed = True


class Sessions:
    def __init__(self) -> None:
        self.opened: list[FakeSession] = []

    def __call__(self, station: RegistryStation) -> FakeSession:
        session = FakeSession(station)
        self.opened.append(session)
        return session

    def sent(self) -> list[tuple[str, datetime]]:
        return [(r.station_id, r.ts) for s in self.opened for r in s.sent]


class Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> datetime:
        return self.now


def bridge(api: FakeApi, sessions: Sessions, clock: Clock | None = None) -> Bridge:
    return Bridge(api, REGISTRY, sessions, clock or Clock())


def test_slots_are_read_ten_and_eighty_minutes_late() -> None:
    api, sessions, clock = FakeApi(), Sessions(), Clock()
    b = bridge(api, sessions, clock)

    b.tick()
    b.tick()  # same slots: nothing new to ask
    clock.now += timedelta(minutes=6)
    b.tick()

    assert api.requested == [
        FIRST_PASS,
        SECOND_PASS,
        FIRST_PASS + timedelta(minutes=6),
        SECOND_PASS + timedelta(minutes=6),
    ]


def test_each_station_publishes_with_its_own_session() -> None:
    api, sessions, clock = FakeApi(), Sessions(), Clock()
    api.packages[FIRST_PASS] = [obs(RAIZET.mf_id, FIRST_PASS), obs(DESAIX.mf_id, FIRST_PASS)]
    later = FIRST_PASS + timedelta(minutes=6)
    api.packages[later] = [obs(RAIZET.mf_id, later)]
    b = bridge(api, sessions, clock)

    assert b.tick() == 2
    clock.now += timedelta(minutes=6)
    assert b.tick() == 1

    assert [s.station.station_id for s in sessions.opened] == [
        RAIZET.station_id,
        DESAIX.station_id,
    ]
    for session in sessions.opened:
        assert {r.station_id for r in session.sent} == {session.station.station_id}
    b.close()
    assert all(s.closed for s in sessions.opened)


def test_all_null_reading_is_not_published() -> None:
    api, sessions = FakeApi(), Sessions()
    api.packages[FIRST_PASS] = [obs(RAIZET.mf_id, FIRST_PASS, t=None, u=None, pmer=None)]

    assert bridge(api, sessions).tick() == 0
    assert sessions.sent() == []


def test_same_reading_is_not_republished() -> None:
    api, sessions, clock = FakeApi(), Sessions(), Clock()
    api.packages[FIRST_PASS] = [obs(RAIZET.mf_id, FIRST_PASS)]
    # 80 minutes later the same slot is read again for the late stations.
    api.packages[FIRST_PASS + timedelta(minutes=6)] = [obs(RAIZET.mf_id, FIRST_PASS)]
    b = bridge(api, sessions, clock)

    b.tick()
    clock.now += timedelta(minutes=6)
    b.tick()
    clock.now += timedelta(minutes=64)
    b.tick()  # second pass of FIRST_PASS

    assert sessions.sent() == [(RAIZET.station_id, FIRST_PASS)]


def test_late_reading_is_published_once_on_the_second_pass() -> None:
    """Stations that send their readings in hourly batches are missing from the first read
    of a slot and appear on the second one, after newer readings of other slots."""
    api, sessions, clock = FakeApi(), Sessions(), Clock()
    b = bridge(api, sessions, clock)
    newer = FIRST_PASS + timedelta(minutes=6)
    api.packages[newer] = [obs(DESAIX.mf_id, newer)]
    clock.now += timedelta(minutes=6)
    b.tick()
    api.packages[FIRST_PASS] = [obs(DESAIX.mf_id, FIRST_PASS)]  # arrived with the batch
    clock.now += timedelta(minutes=64)
    b.tick()
    b.tick()

    assert sessions.sent() == [(DESAIX.station_id, newer), (DESAIX.station_id, FIRST_PASS)]


def test_reading_older_than_the_window_is_dropped() -> None:
    api, sessions = FakeApi(), Sessions()
    stale = FIRST_PASS - timedelta(hours=3)
    api.packages[FIRST_PASS] = [obs(RAIZET.mf_id, stale)]

    assert bridge(api, sessions).tick() == 0


def test_unknown_station_is_skipped_and_logged(caplog: pytest.LogCaptureFixture) -> None:
    api, sessions, clock = FakeApi(), Sessions(), Clock()
    # In the package, but no temperature sensor, hence not in the registry.
    api.packages[FIRST_PASS] = [obs("97108001", FIRST_PASS), obs(RAIZET.mf_id, FIRST_PASS)]
    later = FIRST_PASS + timedelta(minutes=6)
    api.packages[later] = [obs("97108001", later)]
    b = bridge(api, sessions, clock)

    with caplog.at_level(logging.WARNING, logger="sensor_pipeline.bridge"):
        b.tick()
        clock.now += timedelta(minutes=6)
        b.tick()

    assert sessions.sent() == [(RAIZET.station_id, FIRST_PASS)]
    assert len(sessions.opened) == 1  # no session, hence no credentials, for it
    warnings = [r.getMessage() for r in caplog.records if "97108001" in r.getMessage()]
    assert len(warnings) == 1  # once, not every 6 minutes


def test_run_survives_api_errors_and_closes_sessions() -> None:
    api, sessions = FakeApi(), Sessions()
    api.packages[FIRST_PASS] = [obs(RAIZET.mf_id, FIRST_PASS)]
    b = bridge(api, sessions)
    b.tick()
    api.fail = httpx.ConnectError("unreachable")
    stop = threading.Event()
    calls = 0

    def wait(_seconds: float) -> bool:
        nonlocal calls
        calls += 1
        return calls >= 2  # two ticks, then stop

    stop.wait = wait  # type: ignore[method-assign]  # a fake wait instead of real sleeping
    b.fetched.clear()  # make the next tick ask the API again

    run(b, stop, poll_s=120)

    assert calls == 2
    assert sessions.opened[0].closed


def test_bridge_refuses_a_broker_provisioned_for_other_stations(tmp_path: Path) -> None:
    provision_broker(tmp_path, [s.station_id for s in seeded_stations("simulated")])
    settings = Settings(mqtt_credentials_file=tmp_path / "credentials.json")

    with pytest.raises(ValueError, match="STATION_SOURCE=meteofrance"):
        mqtt_sessions(settings, REGISTRY)
