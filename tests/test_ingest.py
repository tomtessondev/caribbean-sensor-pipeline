from __future__ import annotations

import queue
import threading
from collections.abc import Sequence
from datetime import UTC, datetime

from sensor_pipeline.db import Rejected
from sensor_pipeline.ingest import Batch, Incoming, StatusChange, classify, process
from sensor_pipeline.messages import StationStatus, Telemetry

NOW = datetime(2026, 11, 14, 10, tzinfo=UTC)


def telemetry(seq: int, station: str = "gp-pap-01") -> Incoming:
    reading = Telemetry(station_id=station, ts=NOW.replace(minute=seq % 60), seq=seq)
    return Incoming(f"antilles/{station}/telemetry", reading.model_dump_json().encode(), NOW)


class FakeSink:
    def __init__(self) -> None:
        self.batches: list[list[Telemetry]] = []
        self.rejected: list[Rejected] = []
        self.statuses: list[tuple[str, StationStatus]] = []

    def insert_readings(self, readings: Sequence[Telemetry]) -> int:
        if readings:
            self.batches.append(list(readings))
        return len(readings)

    def insert_rejected(self, rejected: Sequence[Rejected]) -> None:
        self.rejected.extend(rejected)

    def set_status(self, station_id: str, status: StationStatus, at: datetime) -> None:
        self.statuses.append((station_id, status))


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_classify_routes_each_kind_of_message() -> None:
    assert isinstance(classify(telemetry(1)), Telemetry)
    assert classify(Incoming("antilles/gp-pap-01/status", b"offline", NOW)) == StatusChange(
        "gp-pap-01", "offline", NOW
    )
    for topic, payload in [
        ("antilles/gp-pap-01/telemetry", b"not json"),
        ("antilles/gp-pap-01/telemetry", b'{"v": 1}'),
        ("antilles/gp-pap-01/telemetry", b"\xff\xfe"),
        ("antilles/mq-fdf-01/telemetry", telemetry(1).payload),  # wrong station for topic
    ]:
        rejected = classify(Incoming(topic, payload, NOW))
        assert isinstance(rejected, Rejected), payload
        assert rejected.error


def test_batch_flushes_when_full_or_when_the_deadline_passes() -> None:
    clock = FakeClock()
    batch = Batch(max_size=3, max_wait_s=2.0, clock=clock)
    sink = FakeSink()

    assert not batch.is_due()
    for seq in range(3):
        item = classify(telemetry(seq))
        assert isinstance(item, Telemetry)
        batch.add(item)
    assert batch.is_due()
    batch.flush(sink)

    item = classify(telemetry(10))
    assert isinstance(item, Telemetry)
    batch.add(item)
    clock.now = 1.0
    assert not batch.is_due()
    clock.now = 2.5
    assert batch.is_due()
    batch.flush(sink)

    assert [len(b) for b in sink.batches] == [3, 1]


def test_process_writes_everything_received_before_stopping() -> None:
    inbox: queue.Queue[Incoming] = queue.Queue()
    for seq in range(5):
        inbox.put(telemetry(seq))
    inbox.put(Incoming("antilles/gp-pap-01/telemetry", b"garbage", NOW))
    inbox.put(Incoming("antilles/gp-pap-01/status", b"online", NOW))
    sink = FakeSink()
    stop = threading.Event()
    stop.set()  # already stopping: everything queued must still be written

    process(inbox, sink, Batch(max_size=100, max_wait_s=60), stop)

    assert sum(len(b) for b in sink.batches) == 5
    assert len(sink.rejected) == 1
    assert sink.statuses == [("gp-pap-01", "online")]
