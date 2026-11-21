"""Real stations into MQTT, each one as if it were a device of ours: Météo-France's
automatic weather stations and the official particle stations of Gwad'Air and Madininair.

Météo-France: every station measures every 6 minutes, but the readings reach the API in
two ways: most within about 8 minutes, the others in one batch per hour, up to an hour
late. The bridge therefore reads each 6-minute slot twice: 10 minutes after it (prompt
stations) and 80 minutes after it (the batches), and after a pause (laptop asleep, API
down) catches up on every slot it missed, up to 3 hours back.

Air quality: hourly means, published about 1.5 h after the hour (2.5 h at worst) in one
file per local day that is rewritten every hour, the previous days' files too. A second
poller looks at the files of the current and previous local day every 15 minutes, asking
the server to send them only when they changed (If-Modified-Since).

Each reading not sent yet is published on the station's own MQTT session, logged in with
the station's own credentials and last will, so the broker's ACL and everything
downstream are the same as for a physical station.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Protocol

import httpx
import paho.mqtt.client as mqtt
from pydantic import ValidationError

from sensor_pipeline.airquality import (
    LOCAL_TIME,
    DayFile,
    hourly_readings,
    parse_day_file,
)
from sensor_pipeline.config import Settings
from sensor_pipeline.messages import Telemetry, telemetry_topic
from sensor_pipeline.meteofrance import SLOT_S, Observation, to_telemetry
from sensor_pipeline.provision import load_credentials
from sensor_pipeline.publisher import StationClient
from sensor_pipeline.registry import AirQualityStation, RegistryStation

logger = logging.getLogger(__name__)

# When each slot is read, after its time: once for the prompt stations, once for the
# hourly batches.
PASSES = (timedelta(minutes=10), timedelta(minutes=80))
# Météo-France readings older than this are neither published nor remembered.
WINDOW = timedelta(hours=3)
POLL_S = 120.0
# The air-quality files are rewritten for three days while late values arrive: what was
# sent is remembered that long, and nothing older is published.
AIR_QUALITY_WINDOW = timedelta(hours=72)
AIR_QUALITY_POLL_S = 900.0
# Total wait for the broker's acknowledgements at the end of a tick, all sessions together.
ACK_TIMEOUT_S = 30.0
# What a tick may raise without stopping the bridge: the API unreachable or answering
# garbage (JSONDecodeError is a ValueError), the broker unreachable, paho refusing a message.
TICK_ERRORS = (httpx.HTTPError, ValueError, OSError, RuntimeError)


class Api(Protocol):
    def package(self, at: datetime) -> list[Observation]: ...


class DayFiles(Protocol):
    def day_file(self, day: date, if_modified_since: str | None) -> DayFile | None: ...


class Station(Protocol):
    """What a session needs to know of a station, whatever its source."""

    @property
    def station_id(self) -> str: ...


class Session(Protocol):
    def publish(self, reading: Telemetry) -> None: ...
    def wait_for_acks(self, deadline: float) -> None: ...
    def close(self) -> None: ...


SessionFactory = Callable[[Station], Session]


class _Sessions:
    """The open MQTT sessions of one poller, one per station, opened on its first reading.
    Acknowledgements are awaited once per tick, with one deadline for all sessions."""

    def __init__(self, factory: SessionFactory) -> None:
        self._factory = factory
        self._open: dict[str, Session] = {}
        self._touched: set[str] = set()

    def publish(self, station: Station, reading: Telemetry) -> None:
        session = self._open.get(station.station_id)
        if session is None:
            session = self._open[station.station_id] = self._factory(station)
        session.publish(reading)
        self._touched.add(station.station_id)

    def wait_for_acks(self) -> None:
        deadline = time.monotonic() + ACK_TIMEOUT_S
        for station_id in sorted(self._touched):
            self._open[station_id].wait_for_acks(deadline)
        self._touched.clear()

    def close(self) -> None:
        for session in self._open.values():
            session.close()
        self._open.clear()


def slot(t: datetime) -> datetime:
    return datetime.fromtimestamp(t.timestamp() // SLOT_S * SLOT_S, UTC)


class Bridge:
    """The Météo-France poller."""

    def __init__(
        self,
        api: Api,
        registry: Sequence[RegistryStation],
        sessions: SessionFactory,
        clock: Callable[[], datetime],
    ) -> None:
        self._api = api
        self._stations = {s.mf_id: s for s in registry}
        self._sessions = _Sessions(sessions)
        self._clock = clock
        self._sent: dict[str, set[datetime]] = {}
        self._unknown: set[str] = set()
        # The last slot each pass read.
        self.last_read: dict[int, datetime] = {}

    def tick(self) -> int:
        """Read the slots that are due and publish what is new. Returns readings published.
        Each pass reads every slot due since the last one it read, back to WINDOW at most:
        after a sleep or an outage the gap is filled. A failing pass is logged and resumes
        from its last slot on the next tick; it does not stop the other pass."""
        now = self._clock()
        published = 0
        try:
            for index, delay in enumerate(PASSES):
                try:
                    for at in self._due(index, slot(now - delay), now):
                        for obs in self._api.package(at):
                            published += self._handle(obs, now)
                        self.last_read[index] = at
                except TICK_ERRORS as exc:
                    logger.warning("Pass %d failed, resuming next tick: %r", index, exc)
        finally:
            self._sessions.wait_for_acks()
        self._forget(now)
        return published

    def _due(self, index: int, latest: datetime, now: datetime) -> list[datetime]:
        last = self.last_read.get(index)
        if last is None:
            return [latest]
        step = timedelta(seconds=SLOT_S)
        oldest = slot(now - WINDOW)
        if oldest < now - WINDOW:
            oldest += step
        at = max(last + step, oldest)
        due = []
        while at <= latest:
            due.append(at)
            at += step
        return due

    def close(self) -> None:
        self._sessions.close()

    def _handle(self, obs: Observation, now: datetime) -> int:
        station = self._stations.get(obs.station)
        if station is None:
            if obs.station not in self._unknown:
                self._unknown.add(obs.station)
                logger.warning("Station %s is not in the registry: skipped", obs.station)
            return 0
        sent = self._sent.setdefault(station.station_id, set())
        if obs.validity_time in sent or obs.validity_time < now - WINDOW:
            return 0
        try:
            reading = to_telemetry(obs, station)
        except ValidationError as exc:
            logger.warning("Reading of %s outside the contract: %s", station.station_id, exc)
            return 0
        if reading is None:
            return 0
        self._sessions.publish(station, reading)
        sent.add(obs.validity_time)
        return 1

    def _forget(self, now: datetime) -> None:
        for times in self._sent.values():
            times.difference_update({t for t in times if t < now - WINDOW})


class AirQualityBridge:
    """The air-quality poller: the files of the previous and the current local day."""

    def __init__(
        self,
        files: DayFiles,
        registry: Sequence[AirQualityStation],
        sessions: SessionFactory,
        clock: Callable[[], datetime],
    ) -> None:
        self._files = files
        self._stations = {s.site_code: s for s in registry}
        self._sessions = _Sessions(sessions)
        self._clock = clock
        self._sent: dict[str, set[datetime]] = {}
        self._unknown: set[str] = set()
        # Last-Modified of each day file whose readings all went out.
        self._modified: dict[date, str] = {}

    def tick(self) -> int:
        """Read the day files that changed and publish the readings not sent yet. Returns
        readings published. A day that fails is logged and read again in full on the next
        tick; it does not stop the other day."""
        now = self._clock()
        today = now.astimezone(LOCAL_TIME).date()
        days = (today - timedelta(days=1), today)
        published = 0
        try:
            for day in days:
                try:
                    file = self._files.day_file(day, self._modified.get(day))
                    if file is None:
                        continue
                    published += self._handle(file, now)
                    if file.last_modified is not None:
                        self._modified[day] = file.last_modified
                except TICK_ERRORS as exc:
                    logger.warning("Air-quality file of %s failed, retrying: %r", day, exc)
        finally:
            self._sessions.wait_for_acks()
        self._modified = {d: m for d, m in self._modified.items() if d in days}
        for times in self._sent.values():
            times.difference_update({t for t in times if t < now - AIR_QUALITY_WINDOW})
        return published

    def close(self) -> None:
        self._sessions.close()

    def _handle(self, file: DayFile, now: datetime) -> int:
        means = parse_day_file(file.content)
        for site_code in sorted({m.site_code for m in means} - set(self._stations)):
            if site_code not in self._unknown:
                self._unknown.add(site_code)
                logger.warning("Air-quality site %s is not in the registry: skipped", site_code)
        by_id = {s.station_id: s for s in self._stations.values()}
        published = 0
        for reading in sorted(
            hourly_readings(means, self._stations), key=lambda r: (r.ts, r.station_id)
        ):
            sent = self._sent.setdefault(reading.station_id, set())
            if reading.ts in sent or reading.ts < now - AIR_QUALITY_WINDOW:
                continue
            self._sessions.publish(by_id[reading.station_id], reading)
            sent.add(reading.ts)
            published += 1
        return published


class Poller(Protocol):
    def tick(self) -> int: ...
    def close(self) -> None: ...


class Publisher(Protocol):
    """What a session needs from the MQTT client (StationClient)."""

    def publish(self, topic: str, payload: str) -> mqtt.MQTTMessageInfo: ...
    def close(self) -> None: ...


class MqttSession:
    """A station's MQTT session. Readings are queued at once; the broker's
    acknowledgements are awaited together at the end of the tick. While the connection is
    down paho keeps the messages (MQTT_ERR_NO_CONN) and sends them on reconnection."""

    def __init__(self, client: Publisher, station: Station) -> None:
        self._client = client
        self._station_id = station.station_id
        self._topic = telemetry_topic(station.station_id)
        self._pending: list[mqtt.MQTTMessageInfo] = []

    @classmethod
    def open(cls, station: Station, settings: Settings, password: str | None) -> MqttSession:
        return cls(
            StationClient(station.station_id, settings, password, client_prefix="mf"), station
        )

    def publish(self, reading: Telemetry) -> None:
        self._pending.append(self._client.publish(self._topic, reading.model_dump_json()))

    def wait_for_acks(self, deadline: float) -> None:
        unacknowledged = 0
        for info in self._pending:
            try:
                info.wait_for_publish(timeout=max(0.0, deadline - time.monotonic()))
                acknowledged = info.is_published()
            except (ValueError, RuntimeError):  # not connected: queued, not refused
                acknowledged = False
            unacknowledged += not acknowledged
        self._pending.clear()
        if unacknowledged:
            logger.warning(
                "%s: %d readings not acknowledged yet, kept for the next connection",
                self._station_id,
                unacknowledged,
            )

    def close(self) -> None:
        self._client.close()


def mqtt_sessions(settings: Settings, registry: Sequence[Station] = ()) -> SessionFactory:
    """Sessions logged in with each station's own password from credentials.json (none
    on a broker without authentication, as in the tests). Refuses to start when a station
    of `registry` has no password: the broker was provisioned for another source."""
    credentials = (
        load_credentials(settings.mqtt_credentials_file)
        if settings.mqtt_credentials_file is not None
        else {}
    )
    missing = [s.station_id for s in registry if s.station_id not in credentials]
    if settings.mqtt_credentials_file is not None and missing:
        raise ValueError(
            f"{len(missing)} stations have no broker login (e.g. {missing[0]}): "
            "run provision-broker with the bridge's STATION_SOURCE (meteofrance or real)"
        )

    def open_session(station: Station) -> Session:
        return MqttSession.open(station, settings, credentials.get(station.station_id))

    return open_session


def run(bridge: Poller, stop: threading.Event, poll_s: float = POLL_S) -> None:
    """Tick every `poll_s` seconds until `stop` is set. A failing API or broker is logged
    and retried on the next tick; it never stops the bridge."""
    try:
        while True:
            try:
                published = bridge.tick()
                if published:
                    logger.info("Published %d readings", published)
            except TICK_ERRORS as exc:
                logger.warning("Tick failed, retrying next tick: %r", exc)
            if stop.wait(poll_s):
                break
    finally:
        bridge.close()


def run_all(pollers: Sequence[tuple[Poller, float]], stop: threading.Event) -> None:
    """Run each poller (with its poll interval) in its own thread until `stop` is set.
    A poller that dies of an unexpected error (a bug, not a failing source, which `run`
    absorbs) stops the others and is raised again as RuntimeError: the process exits and
    its restart policy brings the whole bridge back, never half of it."""
    crashed: list[BaseException] = []

    def guarded(poller: Poller, poll_s: float) -> None:
        try:
            run(poller, stop, poll_s)
        except Exception as exc:
            logger.exception("Poller crashed, stopping the bridge")
            crashed.append(exc)
        finally:
            stop.set()

    threads = [
        threading.Thread(target=guarded, args=(poller, poll_s), daemon=True)
        for poller, poll_s in pollers
    ]
    for thread in threads:
        thread.start()
    # Short joins: the main thread must stay free to run the signal handlers.
    for thread in threads:
        while thread.is_alive():
            thread.join(timeout=1)
    if crashed:
        raise RuntimeError("a poller crashed") from crashed[0]
