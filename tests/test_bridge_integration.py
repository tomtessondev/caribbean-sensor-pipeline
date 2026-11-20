"""A recorded Météo-France package through the bridge, a real Mosquitto, the ingestor and
TimescaleDB. Only HTTP is mocked."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx

from sensor_pipeline.bridge import Bridge, mqtt_sessions
from sensor_pipeline.config import Settings
from sensor_pipeline.db import Store, migrate
from sensor_pipeline.ingest import ingest
from sensor_pipeline.meteofrance import PACKAGE_URL, TOKEN_URL, MeteoFranceClient
from sensor_pipeline.registry import REGISTRY_PATH, load_registry
from tests.support import query, wait_until

pytestmark = pytest.mark.integration

PACKAGE = json.loads(
    (Path(__file__).parent / "fixtures" / "meteofrance" / "package_infrahoraire_6m.json").read_text(
        encoding="utf-8"
    )
)
SLOT = datetime.fromisoformat(PACKAGE[0]["validity_time"])


def expected_rows() -> set[str]:
    registered = {s.mf_id: s.station_id for s in load_registry(REGISTRY_PATH)}
    return {
        registered[o["geo_id_insee"]]
        for o in PACKAGE
        if o["geo_id_insee"] in registered and any(o[k] is not None for k in ("t", "u", "pmer"))
    }


@respx.mock
def test_bridge_readings_reach_timescale(dsn: str, broker: tuple[str, int]) -> None:
    respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "t", "expires_in": 3600})
    )
    respx.get(PACKAGE_URL).mock(return_value=httpx.Response(200, json=PACKAGE))
    host, port = broker
    settings = Settings(mqtt_host=host, mqtt_port=port, db_dsn=dsn)
    migrate(dsn, source="meteofrance")
    store = Store.connect(dsn)
    stop_ingest = threading.Event()
    ingestor = threading.Thread(
        target=ingest, args=(settings, store, stop_ingest), kwargs={"max_wait_s": 0.5}
    )
    ingestor.start()
    time.sleep(2)  # let the ingestor subscribe before anything is published

    bridge = Bridge(
        MeteoFranceClient(httpx.Client(), "app-id", time.monotonic),
        load_registry(REGISTRY_PATH),
        mqtt_sessions(settings),
        lambda: SLOT + timedelta(minutes=10, seconds=30),
    )
    try:
        published = bridge.tick()
        # Both passes got the same recorded package: each reading went out once.
        assert published == len(expected_rows())
        wait_until(lambda: query(dsn, "select count(*) from telemetry") == [(published,)])
    finally:
        bridge.close()
        time.sleep(1)
        stop_ingest.set()
        ingestor.join(timeout=10)
        store.close()

    rows = query(
        dsn,
        "select station_id, ts, seq, temperature_c, pm10_ugm3, battery_v from telemetry",
    )
    assert {r[0] for r in rows} == expected_rows()
    assert {r[1] for r in rows} == {SLOT}
    assert {r[2] for r in rows} == {int(SLOT.timestamp()) // 360}
    assert all(15 < r[3] < 40 for r in rows if r[3] is not None)
    assert all(r[4] is None and r[5] is None for r in rows)
    # Each station logged in on its own and said goodbye.
    assert query(dsn, "select count(*), min(status) from station_status") == [
        (published, "offline")
    ]
