"""The air-quality poller of the bridge: day files to one MQTT session per particle
station, with fake files and fake sessions."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from sensor_pipeline.airquality import DayFile, Version
from sensor_pipeline.bridge import AIR_QUALITY_WINDOW, AirQualityBridge, run, run_all
from sensor_pipeline.registry import load_air_quality_registry
from tests.test_airquality import csv_file, row
from tests.test_bridge import Sessions

REGISTRY = load_air_quality_registry()
CONTENT = (Path(__file__).parent / "fixtures" / "airquality" / "FR_E2_2026-11-20.csv").read_bytes()
STAMP = "Sat, 21 Nov 2026 03:30:27 GMT"
# 22:00 in the Antilles on the 20th: the files of the 19th and the 20th are read.
NOW = datetime(2026, 11, 21, 2, 0, tzinfo=UTC)
NOV_19, NOV_20, NOV_21 = date(2026, 11, 19), date(2026, 11, 20), date(2026, 11, 21)
# The hours of the fixture: 19:00, 20:00 and 21:00 local time.
HOURS = [
    datetime(2026, 11, 20, 23, tzinfo=UTC),
    *(datetime(2026, 11, 21, h, tzinfo=UTC) for h in (0, 1)),
]
VICTOIRE = next(s for s in REGISTRY if s.site_code == "FR37006")


class FakeFiles:
    """Serves `files[day]`, answering None (not modified) when the caller already has
    that version."""

    def __init__(self) -> None:
        self.files: dict[date, DayFile] = {}
        self.requested: list[tuple[date, Version | None]] = []
        self.fail: dict[date, Exception] = {}

    def day_file(self, day: date, known: Version | None) -> DayFile | None:
        self.requested.append((day, known))
        if day in self.fail:
            raise self.fail[day]
        file = self.files.get(day)
        if file is None or file.version == known:
            return None
        return file


class Clock:
    def __init__(self, now: datetime = NOW) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def poller(
    files: FakeFiles, sessions: Sessions, clock: Clock | None = None, registry=REGISTRY
) -> AirQualityBridge:
    return AirQualityBridge(files, registry, sessions, clock or Clock())


def test_reads_the_previous_and_the_current_local_day() -> None:
    files, clock = FakeFiles(), Clock()
    b = poller(files, Sessions(), clock)

    b.tick()
    clock.now = datetime(2026, 11, 21, 4, 30, tzinfo=UTC)  # 00:30 local on the 21st
    b.tick()

    assert [day for day, _ in files.requested] == [NOV_19, NOV_20, NOV_20, NOV_21]


def test_one_reading_per_station_and_hour_on_its_own_session() -> None:
    files, sessions = FakeFiles(), Sessions()
    files.files[NOV_20] = DayFile(CONTENT, Version(STAMP))

    assert poller(files, sessions).tick() == 36

    assert len(sessions.opened) == 12
    for session in sessions.opened:
        assert {r.station_id for r in session.sent} == {session.station.station_id}
        assert [r.ts for r in session.sent] == HOURS  # oldest first
    victoire = next(s for s in sessions.opened if s.station.station_id == VICTOIRE.station_id)
    assert [(r.pm10_ugm3, r.pm2_5_ugm3) for r in victoire.sent][-1] == (14.3, 6.8)


def test_unchanged_file_is_not_downloaded_again() -> None:
    files, sessions = FakeFiles(), Sessions()
    files.files[NOV_20] = DayFile(CONTENT, Version(STAMP))
    b = poller(files, sessions)

    b.tick()
    files.requested.clear()

    assert b.tick() == 0
    assert files.requested == [(NOV_19, None), (NOV_20, Version(STAMP))]


def test_rewritten_file_publishes_only_the_new_hours() -> None:
    files, sessions = FakeFiles(), Sessions()
    files.files[NOV_20] = DayFile(CONTENT, Version(STAMP))
    b = poller(files, sessions)
    b.tick()
    # Belcourt measures PM10 only: its new hour is not held for a PM2.5.
    new_hour = row(start="2026/11/20 22:00:00", site="FR37001", value="18") + "\n"
    files.files[NOV_20] = DayFile(
        CONTENT + new_hour.encode(), Version("Sat, 21 Nov 2026 04:30:27 GMT")
    )

    assert b.tick() == 1
    belcourt = next(s for s in REGISTRY if s.site_code == "FR37001")
    session = next(s for s in sessions.opened if s.station.station_id == belcourt.station_id)
    assert session.sent[-1].ts == datetime(2026, 11, 21, 2, tzinfo=UTC)
    assert len(sessions.sent()) == 37


def victoire_and_belcourt(*, pm25_at_21: bool) -> bytes:
    """Victoire (PM10 and PM2.5) and Belcourt (PM10 only) at 19:00 and 21:00 local; the
    PM2.5 of Victoire at 21:00 only when `pm25_at_21`."""
    rows = [
        row(start=f"2026/11/20 {hour}:00:00", site=site, pollutant=pollutant, value=value)
        for hour in ("19", "21")
        for site, pollutant, value in (
            ("FR37006", "PM10", "14"),
            ("FR37006", "PM2.5", "7"),
            ("FR37001", "PM10", "12"),
        )
        if pm25_at_21 or (hour, site, pollutant) != ("21", "FR37006", "PM2.5")
    ]
    return csv_file(*rows)


def readings_of(sessions: Sessions, station_id: str) -> list[tuple[datetime, float | None]]:
    return [
        (r.ts, r.pm2_5_ugm3)
        for s in sessions.opened
        if s.station.station_id == station_id
        for r in s.sent
    ]


def test_pm10_waits_for_the_pm25_of_its_hour() -> None:
    """The PM2.5 of an hour can reach the file after its PM10: a station that reports
    PM2.5 is not sent a PM10-only hour while that hour is less than 3 hours old, or the
    PM2.5 would be lost (the hour counts as sent). A PM10-only station is sent at once."""
    files, sessions = FakeFiles(), Sessions()
    files.files[NOV_20] = DayFile(victoire_and_belcourt(pm25_at_21=False), Version(STAMP))
    b = poller(files, sessions)
    belcourt = next(s.station_id for s in REGISTRY if s.site_code == "FR37001")

    assert b.tick() == 3
    assert readings_of(sessions, VICTOIRE.station_id) == [(HOURS[0], 7.0)]
    assert [ts for ts, _ in readings_of(sessions, belcourt)] == [HOURS[0], HOURS[2]]

    later = "Sat, 21 Nov 2026 04:30:27 GMT"
    files.files[NOV_20] = DayFile(victoire_and_belcourt(pm25_at_21=True), Version(later))
    assert b.tick() == 1
    assert readings_of(sessions, VICTOIRE.station_id) == [(HOURS[0], 7.0), (HOURS[2], 7.0)]


def test_pm10_is_sent_alone_once_its_pm25_is_3_hours_late() -> None:
    files, sessions, clock = FakeFiles(), Sessions(), Clock()
    files.files[NOV_20] = DayFile(victoire_and_belcourt(pm25_at_21=False), Version(STAMP))
    b = poller(files, sessions, clock)
    b.tick()

    clock.now = HOURS[2] + timedelta(hours=3)
    assert b.tick() == 0  # still within the wait: the hour is exactly 3 hours old
    clock.now += timedelta(minutes=15)  # the file did not change: sent from memory

    assert b.tick() == 1
    assert readings_of(sessions, VICTOIRE.station_id) == [(HOURS[0], 7.0), (HOURS[2], None)]


def test_readings_older_than_72_hours_are_dropped() -> None:
    """The files are rewritten for three days: the window that remembers what was sent
    must cover them, and nothing older is published."""
    assert timedelta(hours=72) == AIR_QUALITY_WINDOW
    files, sessions = FakeFiles(), Sessions()
    # 20:30 local on the 23rd: of the fixture's hours, only 01:00 UTC on the 21st is
    # less than 72 hours old.
    clock = Clock(datetime(2026, 11, 24, 0, 30, tzinfo=UTC))
    files.files[date(2026, 11, 22)] = DayFile(CONTENT, Version(STAMP))

    assert poller(files, sessions, clock).tick() == 12
    assert {ts for _, ts in sessions.sent()} == {HOURS[-1]}


def test_sent_hours_are_remembered_for_the_whole_window() -> None:
    files, sessions, clock = FakeFiles(), Sessions(), Clock()
    files.files[NOV_20] = DayFile(CONTENT, Version(STAMP))
    b = poller(files, sessions, clock)
    b.tick()
    # Two days later the 20th is still rewritten, with a new date: nothing is resent.
    clock.now += timedelta(days=2)
    files.files[date(2026, 11, 21)] = DayFile(CONTENT, Version("Mon, 23 Nov 2026 01:30:00 GMT"))

    assert b.tick() == 0


def test_unknown_station_is_skipped_and_logged_once(caplog: pytest.LogCaptureFixture) -> None:
    files, sessions = FakeFiles(), Sessions()
    files.files[NOV_20] = DayFile(CONTENT, Version(STAMP))
    b = poller(files, sessions, registry=[s for s in REGISTRY if s is not VICTOIRE])

    with caplog.at_level(logging.WARNING, logger="sensor_pipeline.bridge"):
        assert b.tick() == 33
        files.files[NOV_20] = DayFile(CONTENT, Version("Sat, 21 Nov 2026 04:30:27 GMT"))
        b.tick()

    assert VICTOIRE.station_id not in {station for station, _ in sessions.sent()}
    assert len([r for r in caplog.records if "FR37006" in r.getMessage()]) == 1


@pytest.mark.parametrize(
    "error",
    [httpx.ConnectError("unreachable"), ValueError("garbage"), OSError("broker down")],
)
def test_a_failing_day_does_not_stop_the_other_and_is_read_again(error: Exception) -> None:
    files, sessions = FakeFiles(), Sessions()
    files.files[NOV_20] = DayFile(CONTENT, Version(STAMP))
    files.fail[NOV_19] = error
    b = poller(files, sessions)

    assert b.tick() == 36
    del files.fail[NOV_19]
    files.requested.clear()
    b.tick()

    assert files.requested == [(NOV_19, None), (NOV_20, Version(STAMP))]


def status_error(code: int, day: date) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", f"https://storage.example/FR_E2_{day.isoformat()}.csv")
    return httpx.HTTPStatusError(
        str(code), request=request, response=httpx.Response(code, request=request)
    )


def test_forbidden_file_of_the_current_day_is_not_published_yet(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Just after local midnight the storage may answer 403 instead of 404 for a file it
    does not hold yet: for the current day that is "not published yet", not a failure."""
    files, sessions = FakeFiles(), Sessions()
    files.fail[NOV_20] = status_error(403, NOV_20)
    files.fail[NOV_19] = status_error(403, NOV_19)

    with caplog.at_level(logging.WARNING, logger="sensor_pipeline.bridge"):
        assert poller(files, sessions).tick() == 0

    warnings = [r.getMessage() for r in caplog.records]
    assert len(warnings) == 1 and "2026-11-19" in warnings[0]  # the previous day's 403


def test_file_is_read_again_when_publishing_failed() -> None:
    """Its version is kept only once its readings went out: otherwise the next tick
    would get "not modified" and the readings would wait for the next rewrite."""
    files = FakeFiles()
    files.files[NOV_20] = DayFile(CONTENT, Version(STAMP))
    failing = True

    class BrokenSessions(Sessions):
        def __call__(self, station):  # type: ignore[no-untyped-def]  # test double
            if failing:
                raise OSError("broker down")
            return super().__call__(station)

    sessions = BrokenSessions()
    b = poller(files, sessions)
    assert b.tick() == 0
    failing = False

    assert b.tick() == 36


def test_acks_are_awaited_once_per_tick_with_one_deadline() -> None:
    files, sessions = FakeFiles(), Sessions()
    files.files[NOV_20] = DayFile(CONTENT, Version(STAMP))

    poller(files, sessions).tick()

    waits = [s.waits for s in sessions.opened]
    assert all(len(w) == 1 for w in waits)
    assert len({w[0] for w in waits}) == 1


def test_run_survives_errors_and_closes_sessions() -> None:
    files, sessions = FakeFiles(), Sessions()
    files.files[NOV_20] = DayFile(CONTENT, Version(STAMP))
    b = poller(files, sessions)
    b.tick()
    files.fail[NOV_19] = httpx.ConnectError("unreachable")
    stop = threading.Event()
    calls = 0

    def wait(_seconds: float) -> bool:
        nonlocal calls
        calls += 1
        return calls >= 2

    stop.wait = wait  # type: ignore[method-assign]  # a fake wait instead of real sleeping

    run(b, stop, poll_s=900)

    assert calls == 2
    assert all(s.closed for s in sessions.opened)


class FakePoller:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.ticks = 0
        self.closed = False

    def tick(self) -> int:
        self.ticks += 1
        if self.error is not None:
            raise self.error
        return 0

    def close(self) -> None:
        self.closed = True


def test_pollers_run_side_by_side_until_stopped() -> None:
    weather, particles = FakePoller(), FakePoller()
    stop = threading.Event()
    timer = threading.Timer(0.3, stop.set)
    timer.start()

    run_all([(weather, 0.01), (particles, 0.01)], stop)

    assert weather.ticks > 1 and particles.ticks > 1
    assert weather.closed and particles.closed


def test_a_crashed_poller_stops_the_others() -> None:
    """An unexpected error (a bug, not a failing source) ends the process, so that its
    restart policy brings the whole bridge back instead of half of it running on."""
    weather, particles = FakePoller(), FakePoller(KeyError("bug"))
    stop = threading.Event()

    with pytest.raises(RuntimeError, match="crashed"):
        run_all([(weather, 0.01), (particles, 0.01)], stop)

    assert stop.is_set()
    assert weather.closed and particles.closed
