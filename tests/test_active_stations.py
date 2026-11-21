"""A database that switched station source: the stations of the other source stay in the
table (their history is kept) but no longer appear on the map, in the fleet views or in
the offline alert."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from psycopg.conninfo import make_conninfo

from sensor_pipeline.api import create_app
from sensor_pipeline.db import Store, migrate
from sensor_pipeline.messages import Telemetry
from sensor_pipeline.registry import load_air_quality_registry, load_registry
from sensor_pipeline.stations import STATIONS
from tests.support import query
from tests.test_grafana import GRAFANA

pytestmark = pytest.mark.integration

SIMULATED = {s.station_id for s in STATIONS}
REAL = {s.station_id for s in load_registry()}
AIR_QUALITY = {s.station_id for s in load_air_quality_registry()}


def test_only_the_active_source_is_served(dsn: str) -> None:
    migrate(dsn, source="simulated", site_password="site-test")
    store = Store.connect(dsn)
    store.insert_readings([Telemetry(station_id="gp-pap-01", ts=datetime.now(UTC), seq=1)])
    store.close()

    migrate(dsn, source="meteofrance", site_password="site-test")

    client = TestClient(create_app(make_conninfo(dsn, user="site_reader", password="site-test")))
    served = {s["station_id"] for s in client.get("/v1/stations").json()["stations"]}
    assert served == REAL
    assert not served & SIMULATED
    health = {row[0] for row in query(dsn, "select station_id from station_health")}
    assert health == REAL
    # Retired, not deleted: the simulated history stays.
    assert query(dsn, "select count(*) from telemetry where station_id = 'gp-pap-01'") == [(1,)]
    assert query(dsn, "select count(*) from stations where not active") == [(len(SIMULATED),)]


def test_switching_back_reactivates_the_stations(dsn: str) -> None:
    migrate(dsn, source="meteofrance")
    migrate(dsn, source="simulated")

    health = {row[0] for row in query(dsn, "select station_id from station_health")}
    assert health == SIMULATED


def test_real_source_serves_both_registries_with_their_source(dsn: str) -> None:
    migrate(dsn, source="real", site_password="site-test")

    client = TestClient(create_app(make_conninfo(dsn, user="site_reader", password="site-test")))
    served = {s["station_id"]: s["source"] for s in client.get("/v1/stations").json()["stations"]}
    assert set(served) == REAL | AIR_QUALITY
    assert {served[i] for i in REAL} == {"meteofrance"}
    assert {served[i] for i in AIR_QUALITY} == {"airquality"}

    migrate(dsn, source="meteofrance")
    health = {row[0] for row in query(dsn, "select station_id from station_health")}
    assert health == REAL  # the particle stations retired, not deleted
    assert query(dsn, "select source from stations where station_id = 'gp-pap-02'") == [
        ("airquality",)
    ]


def dashboard_variable(name: str) -> str:
    board = json.loads((GRAFANA / "dashboards" / "air-quality.json").read_text(encoding="utf-8"))
    return next(v["query"] for v in board["templating"]["list"] if v["name"] == name)


def test_air_quality_dashboard_splits_particle_and_weather_stations(dsn: str) -> None:
    """With the real sources, the particle panels list the air-quality stations and the
    weather panels the Météo-France ones; the simulated stations measure both."""
    migrate(dsn, source="real")
    assert {r[1] for r in query(dsn, dashboard_variable("station"))} == AIR_QUALITY
    assert {r[1] for r in query(dsn, dashboard_variable("weather"))} == REAL

    migrate(dsn, source="simulated")
    assert {r[1] for r in query(dsn, dashboard_variable("station"))} == SIMULATED
    assert {r[1] for r in query(dsn, dashboard_variable("weather"))} == SIMULATED
