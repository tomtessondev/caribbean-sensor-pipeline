"""The Météo-France client and the conversion to the telemetry contract, on a package
recorded from the real API. HTTP is mocked: tests never call Météo-France."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from sensor_pipeline.meteofrance import (
    PACKAGE_URL,
    TOKEN_URL,
    MeteoFranceClient,
    Observation,
    to_telemetry,
)
from sensor_pipeline.registry import REGISTRY_PATH, load_registry

FIXTURES = Path(__file__).parent / "fixtures" / "meteofrance"
PACKAGE = json.loads((FIXTURES / "package_infrahoraire_6m.json").read_text(encoding="utf-8"))
SLOT = datetime.fromisoformat(PACKAGE[0]["validity_time"])
# A made-up application id: base64 of "client:secret", the shape the portal hands out.
APPLICATION_ID = base64.b64encode(b"client:secret").decode()
STATION = next(s for s in load_registry(REGISTRY_PATH) if s.mf_id == "97101015")


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def token(value: str, expires_in: int = 3600) -> httpx.Response:
    return httpx.Response(
        200, json={"access_token": value, "token_type": "Bearer", "expires_in": expires_in}
    )


def test_conversion_units_and_seq() -> None:
    obs = Observation.model_validate(
        {
            "geo_id_insee": "97101015",
            "validity_time": "2026-11-20T03:48:00Z",
            "t": 297.75,
            "u": 93,
            "pres": 101430,
            "pmer": 101520,
            "ff": 0.8,
        }
    )

    reading = to_telemetry(obs, STATION)

    assert reading is not None
    assert reading.station_id == STATION.station_id
    assert reading.ts == datetime(2026, 11, 20, 3, 48, tzinfo=UTC)
    assert reading.temperature_c == 24.6  # kelvin to °C
    assert reading.humidity_pct == 93
    assert reading.pressure_hpa == 1015.2  # sea-level pressure, Pa to hPa
    assert reading.pm2_5_ugm3 is None and reading.pm10_ugm3 is None
    assert reading.battery_v is None and reading.rssi_dbm is None
    # 6-minute slots since the epoch: consecutive readings differ by one.
    assert reading.seq == int(reading.ts.timestamp()) // 360
    assert reading.seq * 360 == int(reading.ts.timestamp())


def test_station_without_some_sensors_sends_nulls() -> None:
    obs = Observation.model_validate(
        {"geo_id_insee": "97101015", "validity_time": "2026-11-20T03:48:00Z", "t": 300.15}
    )

    reading = to_telemetry(obs, STATION)

    assert reading is not None
    assert reading.temperature_c == 27.0
    assert reading.humidity_pct is None and reading.pressure_hpa is None


def test_all_null_reading_converts_to_nothing() -> None:
    obs = Observation.model_validate(
        {"geo_id_insee": "97101015", "validity_time": "2026-11-20T03:48:00Z", "t": None}
    )

    assert to_telemetry(obs, STATION) is None


def test_every_recorded_observation_converts_or_is_empty() -> None:
    registry = {s.mf_id: s for s in load_registry(REGISTRY_PATH)}
    converted = [
        to_telemetry(obs, registry[obs.station])
        for obs in (Observation.model_validate(o) for o in PACKAGE)
        if obs.station in registry
    ]
    assert len([r for r in converted if r is not None]) > 20


@respx.mock
def test_package_sends_the_token_and_keeps_the_antilles() -> None:
    token_route = respx.post(TOKEN_URL).mock(return_value=token("t1"))
    package_route = respx.get(PACKAGE_URL).mock(return_value=httpx.Response(200, json=PACKAGE))
    client = MeteoFranceClient(httpx.Client(), APPLICATION_ID, FakeClock())

    observations = client.package(SLOT)

    token_request = token_route.calls.last.request
    assert token_request.headers["Authorization"] == f"Basic {APPLICATION_ID}"
    assert token_request.content == b"grant_type=client_credentials"
    request = package_route.calls.last.request
    assert request.headers["Authorization"] == "Bearer t1"
    assert request.url.params["date"] == SLOT.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert request.url.params["format"] == "json"
    # The recorded package holds stations of mainland France too.
    assert len(observations) < len(PACKAGE)
    assert observations and all(o.station[:3] in ("971", "972") for o in observations)


@respx.mock
def test_token_is_reused_until_a_minute_before_it_expires() -> None:
    token_route = respx.post(TOKEN_URL).mock(side_effect=[token("t1"), token("t2")])
    package_route = respx.get(PACKAGE_URL).mock(return_value=httpx.Response(200, json=[]))
    clock = FakeClock()
    client = MeteoFranceClient(httpx.Client(), APPLICATION_ID, clock)

    client.package(SLOT)
    clock.now += 3600 - 61
    client.package(SLOT)
    assert token_route.call_count == 1
    clock.now += 2
    client.package(SLOT)

    assert token_route.call_count == 2
    assert package_route.calls.last.request.headers["Authorization"] == "Bearer t2"


@respx.mock
def test_expired_token_is_renewed_once() -> None:
    respx.post(TOKEN_URL).mock(side_effect=[token("revoked"), token("fresh")])

    def answer(request: httpx.Request) -> httpx.Response:
        if request.headers["Authorization"] == "Bearer fresh":
            return httpx.Response(200, json=PACKAGE)
        return httpx.Response(401, json={"code": "900901", "message": "Invalid Credentials"})

    package_route = respx.get(PACKAGE_URL).mock(side_effect=answer)
    client = MeteoFranceClient(httpx.Client(), APPLICATION_ID, FakeClock())

    assert client.package(SLOT)
    assert package_route.call_count == 2


@respx.mock
def test_a_second_401_is_an_error_not_a_loop() -> None:
    token_route = respx.post(TOKEN_URL).mock(return_value=token("t"))
    package_route = respx.get(PACKAGE_URL).mock(return_value=httpx.Response(401))
    client = MeteoFranceClient(httpx.Client(), APPLICATION_ID, FakeClock())

    with pytest.raises(httpx.HTTPStatusError):
        client.package(SLOT)
    assert package_route.call_count == 2
    assert token_route.call_count == 2
