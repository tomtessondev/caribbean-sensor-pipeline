"""Publish simulated stations to MQTT, one connection per station like real devices."""

from __future__ import annotations

import logging
import threading
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from sensor_pipeline.config import Settings
from sensor_pipeline.messages import status_topic, telemetry_topic
from sensor_pipeline.simulator import FaultRates, StationSimulator, dust_episodes
from sensor_pipeline.stations import Station

logger = logging.getLogger(__name__)


class StationClient:
    """An MQTT session for one station. The broker publishes the last will ("offline",
    retained) if the connection drops without a clean disconnect."""

    def __init__(self, station_id: str, settings: Settings) -> None:
        self.station_id = station_id
        self._client = mqtt.Client(
            CallbackAPIVersion.VERSION2, client_id=f"sim-{station_id}", clean_session=True
        )
        self._client.will_set(status_topic(station_id), "offline", qos=1, retain=True)
        self._client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=30)
        self._client.loop_start()
        self._client.publish(status_topic(station_id), "online", qos=1, retain=True)

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
    backfill_step_s: float = 60,
    faults: FaultRates | None = None,
) -> int:
    """Publish `backfill_hours` of history at once (one reading per `backfill_step_s`), then
    one reading per station every `interval_s` seconds until `stop` is set. Returns the
    number of messages published."""
    now = datetime.now(UTC).replace(microsecond=0)
    start = now - timedelta(hours=backfill_hours)
    episodes = dust_episodes(seed, start - timedelta(days=4), days=int(backfill_hours / 24) + 60)
    sims = [
        StationSimulator(station, episodes, seed, faults or FaultRates()) for station in stations
    ]
    clients = {s.station_id: StationClient(s.station_id, settings) for s in stations}
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
        ts = start
        while ts < now:
            emit(ts)
            ts += timedelta(seconds=backfill_step_s)
        wait_for_acks()
        logger.info("Backfilled %d messages since %s", published, start.isoformat())
        while not stop.is_set():
            emit(datetime.now(UTC).replace(microsecond=0))
            wait_for_acks()
            stop.wait(interval_s)
    finally:
        for client in clients.values():
            client.close()
    return published
