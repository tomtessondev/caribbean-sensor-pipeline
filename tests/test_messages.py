from __future__ import annotations

import pytest
from pydantic import ValidationError

from sensor_pipeline.messages import (
    Telemetry,
    parse_status,
    station_from_topic,
    status_topic,
    telemetry_topic,
)

VALID = (
    b'{"v": 1, "station_id": "gp-pap-01", "ts": "2026-11-14T10:00:00+00:00", "seq": 7,'
    b' "temperature_c": 28.4, "pm10_ugm3": 22.0}'
)


def test_valid_reading_parses_with_optional_fields_missing() -> None:
    reading = Telemetry.model_validate_json(VALID)

    assert reading.station_id == "gp-pap-01"
    assert reading.seq == 7
    assert reading.humidity_pct is None


@pytest.mark.parametrize(
    "change",
    [
        {"v": 2},  # unknown contract version
        {"ts": "2026-11-14T10:00:00"},  # no timezone: ambiguous
        {"station_id": "PAP"},  # not a station id
        {"seq": -1},
        {"temperature_c": 120},  # beyond the BME280 range
        {"pm10_ugm3": -3},
        {"unexpected": 1},  # typo or newer firmware: refuse rather than drop silently
    ],
)
def test_broken_readings_are_rejected(change: dict[str, object]) -> None:
    payload = Telemetry.model_validate_json(VALID).model_dump(mode="json") | change
    with pytest.raises(ValidationError):
        Telemetry.model_validate(payload)


def test_topics_round_trip() -> None:
    assert telemetry_topic("mq-fdf-01") == "antilles/mq-fdf-01/telemetry"
    assert station_from_topic(status_topic("mq-fdf-01")) == "mq-fdf-01"
    with pytest.raises(ValueError):
        station_from_topic("other/mq-fdf-01/telemetry")


def test_status_payload() -> None:
    assert parse_status(b"offline") == "offline"
    with pytest.raises(ValueError):
        parse_status(b"sleeping")
