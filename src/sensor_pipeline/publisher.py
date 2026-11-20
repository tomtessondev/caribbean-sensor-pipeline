"""Publish simulated stations to MQTT, one connection per station like real devices."""

from __future__ import annotations

import logging
import threading
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

from sensor_pipeline.config import Settings
from sensor_pipeline.messages import status_topic, telemetry_topic
from sensor_pipeline.mqtt import new_client
from sensor_pipeline.provision import load_credentials
from sensor_pipeline.simulator import DUST_EPOCH, FaultRates, StationSimulator, dust_episodes
from sensor_pipeline.stations import Station

logger = logging.getLogger(__name__)


class StationClient:
    """An MQTT session for one station, logged in as the station itself (its id is its
    username, so the broker's ACL confines it to its own topics). The broker publishes the
    last will ("offline", retained) if the connection drops without a clean disconnect."""

    def __init__(
        self,
        station_id: str,
        settings: Settings,
        password: str | None = None,
        client_prefix: str = "sim",
    ) -> None:
        self.station_id = station_id
        self._client = new_client(
            settings,
            f"{client_prefix}-{station_id}",
            username=station_id if password is not None else None,
            password=password,
            clean_session=True,
        )
        self._client.will_set(status_topic(station_id), "offline", qos=1, retain=True)
        self._client.on_connect = self._announce_online
        self._client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=30)
        self._client.loop_start()

    def _announce_online(
        self,
        client: mqtt.Client,
        _userdata: object,
        _flags: mqtt.ConnectFlags,
        reason_code: ReasonCode,
        _properties: Properties | None,
    ) -> None:
        """On every connection, not only the first: after a drop the broker has published
        the last will ("offline", retained), which only a new "online" replaces."""
        if not reason_code.is_failure:
            client.publish(status_topic(self.station_id), "online", qos=1, retain=True)

    def publish(self, topic: str, payload: str) -> mqtt.MQTTMessageInfo:
        return self._client.publish(topic, payload, qos=1)

    def close(self) -> None:
        self._client.publish(status_topic(self.station_id), "offline", qos=1, retain=True)
        self._client.disconnect()
        self._client.loop_stop()


def simulate(
    settings: Settings,
    stations: Sequence[Station],
    *,
    seed: int,
    interval_s: float,
    backfill_hours: float,
    stop: threading.Event,
    faults: FaultRates | None = None,
) -> int:
    """Publish `backfill_hours` of history at once, then one reading per station every
    `interval_s` seconds until `stop` is set. Readings sit on a fixed grid of `interval_s`,
    so a restart republishes identical history. Returns the number of messages published."""
    step = timedelta(seconds=interval_s)

    def slot(t: datetime) -> datetime:
        return datetime.fromtimestamp(t.timestamp() // interval_s * interval_s, UTC)

    now = slot(datetime.now(UTC))
    episodes = dust_episodes(seed, DUST_EPOCH, days=(now - DUST_EPOCH).days + 60)
    sims = [
        StationSimulator(station, episodes, seed, faults or FaultRates(), interval_s)
        for station in stations
    ]
    credentials = (
        load_credentials(settings.mqtt_credentials_file)
        if settings.mqtt_credentials_file is not None
        else {}
    )
    clients = {
        s.station_id: StationClient(s.station_id, settings, credentials.get(s.station_id))
        for s in stations
    }
    published = 0
    pending: list[mqtt.MQTTMessageInfo] = []

    def emit(ts: datetime) -> None:
        nonlocal published
        for sim in sims:
            reading = sim.reading(ts)
            if reading is None:
                continue
            pending.append(
                clients[sim.station.station_id].publish(
                    telemetry_topic(sim.station.station_id), reading.model_dump_json()
                )
            )
            published += 1

    def wait_for_acks() -> None:
        for info in pending:
            info.wait_for_publish(timeout=30)
        pending.clear()

    try:
        ts = slot(now - timedelta(hours=backfill_hours))
        while ts <= now:
            emit(ts)
            ts += step
        wait_for_acks()
        logger.info(
            "Backfilled %d messages since %s",
            published,
            (now - timedelta(hours=backfill_hours)).isoformat(),
        )
        # Live: publish each slot when its time comes.
        while not stop.wait(max(0.0, (ts - datetime.now(UTC)).total_seconds())):
            emit(ts)
            wait_for_acks()
            ts += step
    finally:
        for client in clients.values():
            client.close()
    return published
