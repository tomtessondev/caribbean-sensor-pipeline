"""The read-only stations endpoint on a real TimescaleDB, through the `site_reader` role."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.conninfo import make_conninfo

from sensor_pipeline.api import SITE_ORIGINS, connect, create_app
from sensor_pipeline.db import Store, migrate
from sensor_pipeline.messages import Telemetry

pytestmark = pytest.mark.integration

NOW = datetime.now(UTC).replace(microsecond=0)
FIELDS = {
    "station_id",
    "name",
    "territory",
    "lat",
    "lon",
    "ts",
    "temperature_c",
    "humidity_pct",
    "pressure_hpa",
    "pm10_ugm3",
    "pm2_5_ugm3",
    "source",
    "status",
}


def reading(station: str, minutes_ago: float, pm10: float, temperature: float = 27.0) -> Telemetry:
    ts = NOW - timedelta(minutes=minutes_ago)
    return Telemetry(
        station_id=station,
        ts=ts,
        seq=int(ts.timestamp()) // 10,
        temperature_c=temperature,
        humidity_pct=70,
        pressure_hpa=1013,
        pm10_ugm3=pm10,
        pm2_5_ugm3=round(pm10 / 2, 1),
        battery_v=3.9,
        rssi_dbm=-70,
    )


@pytest.fixture(scope="module")
def site_dsn(dsn: str) -> str:
    migrate(dsn, site_password="site-test")
    store = Store.connect(dsn)
    store.insert_readings(
        [
            reading("gp-pap-01", 30, 20.0),
            reading("gp-pap-01", 1, 22.0, temperature=28.5),  # the latest: online
            reading("mq-fdf-01", 7, 41.0),  # 7 min of silence, threshold 5: offline
            reading("gp-bte-01", 60 * 25, 15.0),  # older than 24 h: no latest reading
        ]
    )
    store.close()
    # A spike flagged by the quality rules: the endpoint serves the clean value.
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "insert into reading_flags (station_id, ts, flag) values ('mq-fdf-01', %s, 'pm_spike')",
            (NOW - timedelta(minutes=7),),
        )
    return make_conninfo(dsn, user="site_reader", password="site-test")


@pytest.fixture(scope="module")
def client(site_dsn: str) -> TestClient:
    return TestClient(create_app(site_dsn))


def stations(client: TestClient) -> dict[str, dict[str, object]]:
    body = client.get("/v1/stations").json()
    return {s["station_id"]: s for s in body["stations"]}


def test_latest_reading_per_station(client: TestClient) -> None:
    response = client.get("/v1/stations")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=60"
    body = response.json()
    generated = datetime.fromisoformat(body["generated_at"])
    assert abs((generated - datetime.now(UTC)).total_seconds()) < 60
    by_id = {s["station_id"]: s for s in body["stations"]}
    assert len(by_id) == 6  # every station, with or without a recent reading
    pap = by_id["gp-pap-01"]
    assert datetime.fromisoformat(pap["ts"]) == NOW - timedelta(minutes=1)
    assert pap["temperature_c"] == 28.5 and pap["pm10_ugm3"] == 22.0
    assert pap["pm2_5_ugm3"] == 11.0 and pap["source"] == "simulated"
    assert pap["name"] == "Pointe-à-Pitre" and pap["territory"] == "Guadeloupe"
    assert (pap["lat"], pap["lon"]) == (16.2411, -61.5331)
    assert by_id["mq-fdf-01"]["pm10_ugm3"] is None  # flagged spike
    assert by_id["mq-fdf-01"]["temperature_c"] == 27.0  # the rest of the reading stays
    assert by_id["gp-bte-01"]["ts"] is None


def test_offline_after_the_stations_threshold(client: TestClient) -> None:
    by_id = stations(client)

    assert by_id["gp-pap-01"]["status"] == "online"
    assert by_id["mq-fdf-01"]["status"] == "offline"
    assert by_id["gp-bte-01"]["status"] == "offline"


def test_no_column_outside_the_list_leaks(client: TestClient) -> None:
    body = client.get("/v1/stations").json()

    assert set(body) == {"generated_at", "stations"}
    for station in body["stations"]:
        assert set(station) == FIELDS


@pytest.mark.parametrize("origin", SITE_ORIGINS)
def test_cors_allows_the_site(client: TestClient, origin: str) -> None:
    response = client.get("/v1/stations", headers={"Origin": origin})
    assert response.headers["access-control-allow-origin"] == origin

    preflight = client.options(
        "/v1/stations",
        headers={"Origin": origin, "Access-Control-Request-Method": "GET"},
    )
    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == origin


@pytest.mark.parametrize(
    "origin",
    [
        "https://evil.example",
        "http://localhost:3001",
        "https://antilles-data-pipeline.vercel.app.evil.example",
    ],
)
def test_cors_refuses_other_origins(client: TestClient, origin: str) -> None:
    response = client.get("/v1/stations", headers={"Origin": origin})
    assert "access-control-allow-origin" not in response.headers

    preflight = client.options(
        "/v1/stations",
        headers={"Origin": origin, "Access-Control-Request-Method": "GET"},
    )
    assert "access-control-allow-origin" not in preflight.headers


def test_read_only(client: TestClient) -> None:
    assert client.post("/v1/stations").status_code == 405
    assert client.delete("/v1/stations").status_code == 405


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_site_reader_sees_the_view_only(site_dsn: str) -> None:
    with psycopg.connect(site_dsn) as conn:
        assert conn.execute("select count(*) from station_latest").fetchone() == (6,)
        for table in ("telemetry", "stations", "station_status", "ingest_errors"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(f"select * from {table} limit 1")
            conn.rollback()


@pytest.mark.parametrize(
    "headers", [{}, {"Origin": SITE_ORIGINS[0]}, {"Origin": "https://evil.example"}]
)
def test_answers_vary_on_origin(client: TestClient, headers: dict[str, str]) -> None:
    """The CORS header depends on Origin: a cache must not serve one origin's answer to
    another, including the answer to a request without Origin."""
    response = client.get("/v1/stations", headers=headers)

    assert "origin" in [v.strip().lower() for v in response.headers["vary"].split(",")]


def test_queries_are_bounded_in_time(site_dsn: str) -> None:
    with connect(site_dsn) as conn:
        assert conn.execute("show statement_timeout").fetchone() == ("2s",)
        with pytest.raises(psycopg.errors.QueryCanceled):
            conn.execute("select pg_sleep(3)")


def test_no_api_description_is_published(client: TestClient) -> None:
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path).status_code == 404


def test_health_is_503_when_the_database_is_unreachable() -> None:
    # 10.255.255.1 is not routed: without connect_timeout the request would hang.
    client = TestClient(create_app("postgresql://site_reader:x@10.255.255.1:5432/sensors"))

    started = time.monotonic()
    response = client.get("/health")

    assert response.status_code == 503
    assert time.monotonic() - started < 6


def test_stations_is_503_when_the_database_is_unreachable() -> None:
    client = TestClient(create_app("postgresql://site_reader:x@127.0.0.1:1/sensors"))

    response = client.get("/v1/stations")

    assert response.status_code == 503
    assert "origin" in response.headers["vary"].lower()
