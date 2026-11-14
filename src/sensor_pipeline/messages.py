"""The MQTT contract shared by the simulator, the future ESP32 firmware and the ingestor.

Topics:
    antilles/<station_id>/telemetry   one JSON reading per interval, QoS 1
    antilles/<station_id>/status      "online" / "offline", retained; "offline" is the
                                      MQTT last will, sent by the broker if the station
                                      disappears without saying goodbye

Telemetry is versioned (`v`): a firmware in the field cannot be updated in lockstep with
the server, so the ingestor must recognise which shape it receives.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

TOPIC_ROOT = "antilles"

StationId = Annotated[str, StringConstraints(pattern=r"^[a-z]{2}-[a-z]{3}-\d{2}$")]


def telemetry_topic(station_id: str) -> str:
    return f"{TOPIC_ROOT}/{station_id}/telemetry"


def status_topic(station_id: str) -> str:
    return f"{TOPIC_ROOT}/{station_id}/status"


TELEMETRY_SUBSCRIPTION = f"{TOPIC_ROOT}/+/telemetry"
STATUS_SUBSCRIPTION = f"{TOPIC_ROOT}/+/status"


class Telemetry(BaseModel):
    """One reading. Ranges are the sensors' physical limits, not expected values: a value
    outside them is a broken message, while an odd but possible value is data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    v: Literal[1] = 1
    station_id: StationId
    ts: AwareDatetime
    # Increases by one per reading, including readings that never arrived: gaps in the
    # sequence measure message loss.
    seq: int = Field(ge=0)
    temperature_c: float | None = Field(default=None, ge=-40, le=85)  # BME280 range
    humidity_pct: float | None = Field(default=None, ge=0, le=100)
    pressure_hpa: float | None = Field(default=None, ge=300, le=1100)
    pm2_5_ugm3: float | None = Field(default=None, ge=0, le=1000)  # PMS5003 range
    pm10_ugm3: float | None = Field(default=None, ge=0, le=1000)
    battery_v: float | None = Field(default=None, ge=0, le=5)
    rssi_dbm: int | None = Field(default=None, ge=-120, le=0)


StationStatus = Literal["online", "offline"]


def parse_status(payload: bytes) -> StationStatus:
    text = payload.decode("utf-8").strip()
    if text not in ("online", "offline"):
        raise ValueError(f"unknown status {text!r}")
    return "online" if text == "online" else "offline"


def station_from_topic(topic: str) -> str:
    parts = topic.split("/")
    if len(parts) != 3 or parts[0] != TOPIC_ROOT:
        raise ValueError(f"unexpected topic {topic!r}")
    return parts[1]
