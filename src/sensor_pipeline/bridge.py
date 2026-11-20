"""Météo-France's automatic stations into MQTT, each one as if it were a device of ours.

Every station measures every 6 minutes, but the readings reach the API in two ways: most
within about 8 minutes, the others in one batch per hour, up to an hour late. The bridge
therefore reads each 6-minute slot twice: 10 minutes after it (prompt stations) and 80
minutes after it (the batches). Each reading not sent yet is published on the station's
own MQTT session, logged in with the station's own credentials and last will, so the
broker's ACL and everything downstream are the same as for a physical station.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Protocol

import httpx
from pydantic import ValidationError

from sensor_pipeline.config import Settings
from sensor_pipeline.messages import Telemetry, telemetry_topic
from sensor_pipeline.meteofrance import SLOT_S, Observation, to_telemetry
from sensor_pipeline.provision import load_credentials
from sensor_pipeline.publisher import StationClient
from sensor_pipeline.registry import RegistryStation

logger = logging.getLogger(__name__)

# When each slot is read, after its time: once for the prompt stations, once for the
# hourly batches.
PASSES = (timedelta(minutes=10), timedelta(minutes=80))
# Readings older than this are neither published nor remembered.
WINDOW = timedelta(hours=3)
POLL_S = 120.0


class Api(Protocol):
    def package(self, at: datetime) -> list[Observation]: ...


class Session(Protocol):
    def publish(self, reading: Telemetry) -> None: ...
    def close(self) -> None: ...


SessionFactory = Callable[[RegistryStation], Session]


def slot(t: datetime) -> datetime:
    return datetime.fromtimestamp(t.timestamp() // SLOT_S * SLOT_S, UTC)


class Bridge:
    def __init__(
        self,
        api: Api,
        registry: Sequence[RegistryStation],
        sessions: SessionFactory,
        clock: Callable[[], datetime],
    ) -> None:
        self._api = api
        self._stations = {s.mf_id: s for s in registry}
        self._open_session = sessions
        self._clock = clock
        self._sessions: dict[str, Session] = {}
        self._sent: dict[str, set[datetime]] = {}
        self._unknown: set[str] = set()
        # (slot, pass) already read.
        self.fetched: set[tuple[datetime, int]] = set()

    def tick(self) -> int:
        """Read the slots that are due and publish what is new. Returns readings published."""
        now = self._clock()
        published = 0
        for index, delay in enumerate(PASSES):
            at = slot(now - delay)
            if (at, index) in self.fetched:
                continue
            for obs in self._api.package(at):
                published += self._handle(obs, now)
            self.fetched.add((at, index))
        self._forget(now)
        return published

    def close(self) -> None:
        for session in self._sessions.values():
            session.close()
        self._sessions.clear()

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
        session = self._sessions.get(station.station_id)
        if session is None:
            session = self._sessions[station.station_id] = self._open_session(station)
        session.publish(reading)
        sent.add(obs.validity_time)
        return 1

    def _forget(self, now: datetime) -> None:
        for times in self._sent.values():
            times.difference_update({t for t in times if t < now - WINDOW})
        self.fetched = {(at, i) for at, i in self.fetched if at >= now - WINDOW}


class MqttSession:
    """A station's MQTT session; waits for the broker's acknowledgement of each reading."""

    def __init__(self, station: RegistryStation, settings: Settings, password: str | None):
        self._client = StationClient(station.station_id, settings, password, client_prefix="mf")
        self._topic = telemetry_topic(station.station_id)

    def publish(self, reading: Telemetry) -> None:
        self._client.publish(self._topic, reading.model_dump_json()).wait_for_publish(timeout=30)

    def close(self) -> None:
        self._client.close()


def mqtt_sessions(settings: Settings, registry: Sequence[RegistryStation] = ()) -> SessionFactory:
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
            "run provision-broker with STATION_SOURCE=meteofrance"
        )

    def open_session(station: RegistryStation) -> Session:
        return MqttSession(station, settings, credentials.get(station.station_id))

    return open_session


def run(bridge: Bridge, stop: threading.Event, poll_s: float = POLL_S) -> None:
    """Tick every `poll_s` seconds until `stop` is set. An unreachable or failing API is
    logged and retried on the next tick; it never stops the bridge."""
    try:
        while True:
            try:
                published = bridge.tick()
                if published:
                    logger.info("Published %d readings", published)
            except httpx.HTTPError as exc:
                logger.warning("Météo-France API failed, retrying next tick: %s", exc)
            if stop.wait(poll_s):
                break
    finally:
        bridge.close()
