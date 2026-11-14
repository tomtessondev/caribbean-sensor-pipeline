"""MQTT to TimescaleDB: validate each message, write in batches, keep the rejects.

The paho network thread only enqueues raw messages. The main thread validates them and
writes a batch when it reaches `max_batch` readings or has waited `max_wait_s`, whichever
comes first: one transaction per batch instead of one per message, with a bounded delay.
"""

from __future__ import annotations

import contextlib
import json
import logging
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode
from pydantic import ValidationError

from sensor_pipeline.config import Settings
from sensor_pipeline.db import Rejected
from sensor_pipeline.messages import (
    STATUS_SUBSCRIPTION,
    TELEMETRY_SUBSCRIPTION,
    StationStatus,
    Telemetry,
    parse_status,
    station_from_topic,
)
from sensor_pipeline.mqtt import new_client
from sensor_pipeline.provision import load_credentials

logger = logging.getLogger(__name__)


class Sink(Protocol):
    def insert_readings(self, readings: list[Telemetry]) -> int: ...
    def insert_rejected(self, rejected: list[Rejected]) -> None: ...
    def set_status(self, station_id: str, status: StationStatus, at: datetime) -> None: ...


@dataclass(frozen=True)
class Incoming:
    topic: str
    payload: bytes
    received_at: datetime


@dataclass(frozen=True)
class StatusChange:
    station_id: str
    status: StationStatus
    at: datetime


def classify(message: Incoming) -> Telemetry | StatusChange | Rejected:
    """Turn a raw MQTT message into a reading, a status change or a reject."""
    try:
        if message.topic.endswith("/status"):
            return StatusChange(
                station_from_topic(message.topic),
                parse_status(message.payload),
                message.received_at,
            )
        reading = Telemetry.model_validate_json(message.payload)
        if reading.station_id != station_from_topic(message.topic):
            raise ValueError("station_id does not match the topic")
        return reading
    except (ValidationError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return Rejected(
            received_at=message.received_at,
            topic=message.topic,
            payload=message.payload.decode("utf-8", errors="replace"),
            error=str(exc)[:1000],
        )


@dataclass
class Batch:
    max_size: int
    max_wait_s: float
    clock: Callable[[], float] = time.monotonic
    readings: list[Telemetry] = field(default_factory=list)
    rejected: list[Rejected] = field(default_factory=list)
    _opened_at: float | None = None

    def add(self, item: Telemetry | Rejected) -> None:
        if self._opened_at is None:
            self._opened_at = self.clock()
        if isinstance(item, Telemetry):
            self.readings.append(item)
        else:
            self.rejected.append(item)

    def seconds_left(self) -> float:
        if self._opened_at is None:
            return self.max_wait_s
        return max(0.0, self._opened_at + self.max_wait_s - self.clock())

    def is_due(self) -> bool:
        return len(self.readings) >= self.max_size or (
            self._opened_at is not None and self.seconds_left() == 0
        )

    def flush(self, sink: Sink) -> int:
        inserted = sink.insert_readings(self.readings)
        sink.insert_rejected(self.rejected)
        if self.readings or self.rejected:
            logger.info(
                "Wrote %d new readings (%d duplicates), %d rejects",
                inserted,
                len(self.readings) - inserted,
                len(self.rejected),
            )
        self.readings, self.rejected, self._opened_at = [], [], None
        return inserted


def _handle(message: Incoming, sink: Sink, batch: Batch) -> None:
    item = classify(message)
    if isinstance(item, StatusChange):
        sink.set_status(item.station_id, item.status, item.at)
    else:
        batch.add(item)


def process(inbox: queue.Queue[Incoming], sink: Sink, batch: Batch, stop: threading.Event) -> None:
    """The ingestion loop, separate from MQTT so it can be tested with a plain queue."""
    while not stop.is_set():
        with contextlib.suppress(queue.Empty):
            _handle(inbox.get(timeout=batch.seconds_left() or 0.05), sink, batch)
        if batch.is_due():
            batch.flush(sink)
    # paho acknowledged these messages already: write them before exiting, or they are lost.
    while True:
        try:
            _handle(inbox.get_nowait(), sink, batch)
        except queue.Empty:
            break
    batch.flush(sink)


def ingest(
    settings: Settings,
    sink: Sink,
    stop: threading.Event,
    *,
    max_batch: int = 500,
    max_wait_s: float = 2.0,
    ready_file: Path | None = None,
) -> None:
    """Consume until `stop` is set. `ready_file` is created once the broker has confirmed
    the subscriptions: a container health check, so stations start publishing only when
    nothing they send can be missed."""
    inbox: queue.Queue[Incoming] = queue.Queue()

    def on_connect(
        client: mqtt.Client,
        _userdata: object,
        _flags: mqtt.ConnectFlags,
        reason_code: ReasonCode,
        _properties: Properties | None,
    ) -> None:
        if reason_code.is_failure:
            # Wrong credentials or not authorised: paho keeps retrying, the log says why.
            logger.error("Broker refused the connection: %s", reason_code)
            return
        logger.info("Connected to the broker: %s", reason_code)
        # Subscribing here re-subscribes after every reconnection.
        client.subscribe([(TELEMETRY_SUBSCRIPTION, 1), (STATUS_SUBSCRIPTION, 1)])

    def on_subscribe(
        _client: mqtt.Client,
        _userdata: object,
        _mid: int,
        _reason_codes: list[ReasonCode],
        _properties: Properties | None,
    ) -> None:
        logger.info("Subscribed to %s and %s", TELEMETRY_SUBSCRIPTION, STATUS_SUBSCRIPTION)
        if ready_file is not None:
            ready_file.touch()

    def on_message(_client: mqtt.Client, _userdata: object, msg: mqtt.MQTTMessage) -> None:
        inbox.put(Incoming(msg.topic, bytes(msg.payload), datetime.now(UTC)))

    password = settings.mqtt_password
    if password is None and settings.mqtt_username and settings.mqtt_credentials_file:
        password = load_credentials(settings.mqtt_credentials_file)[settings.mqtt_username]
    # A persistent session (clean_session=False) makes the broker queue QoS 1 messages
    # while the ingestor restarts, so a deployment loses nothing.
    client = new_client(
        settings,
        "sensor-ingestor",
        username=settings.mqtt_username,
        password=password,
        clean_session=False,
    )
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=30)
    client.loop_start()
    try:
        process(inbox, sink, Batch(max_batch, max_wait_s), stop)
    finally:
        client.disconnect()
        client.loop_stop()
