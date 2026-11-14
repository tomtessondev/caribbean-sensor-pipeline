"""The firmware and the ingestor speak the same contract.

The firmware's host tests assert that its serialiser produces exactly this golden message
(firmware/test/test_logic); here the ingestor's model must accept it, field for field.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from sensor_pipeline.ingest import Incoming, classify
from sensor_pipeline.messages import Telemetry, telemetry_topic

GOLDEN = Path(__file__).resolve().parents[1] / "firmware" / "test" / "golden" / "telemetry_v1.json"


def test_golden_firmware_message_is_valid_telemetry() -> None:
    payload = GOLDEN.read_bytes().strip()
    reading = Telemetry.model_validate_json(payload)

    assert reading.ts == datetime(2026, 11, 14, 10, tzinfo=UTC)
    # Numbered by 10-second slot, like the simulator.
    assert reading.seq == int(reading.ts.timestamp()) // 10


def test_firmware_sends_every_field_of_the_contract() -> None:
    sent = set(json.loads(GOLDEN.read_bytes()))
    assert sent == set(Telemetry.model_fields)


def test_ingestor_accepts_the_firmware_message() -> None:
    payload = GOLDEN.read_bytes().strip()
    item = classify(Incoming(telemetry_topic("gp-pap-01"), payload, datetime.now(UTC)))
    assert isinstance(item, Telemetry)
