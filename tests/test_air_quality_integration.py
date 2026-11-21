"""A trimmed real air-quality day file through the poller, a real Mosquitto, the ingestor
and TimescaleDB. Only HTTP is mocked."""

from __future__ import annotations

import threading
import time
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx

from sensor_pipeline.airquality import AirQualityClient, file_url
from sensor_pipeline.bridge import AirQualityBridge, mqtt_sessions
from sensor_pipeline.config import Settings
from sensor_pipeline.db import Store, migrate
from sensor_pipeline.ingest import ingest
from sensor_pipeline.registry import load_air_quality_registry
from tests.support import query, wait_until

pytestmark = pytest.mark.integration

CONTENT = (Path(__file__).parent / "fixtures" / "airquality" / "FR_E2_2026-11-20.csv").read_bytes()
NOW = datetime(2026, 11, 21, 2, 0, tzinfo=UTC)  # 22:00 on the 20th in the Antilles
REGISTRY = load_air_quality_registry()


@respx.mock
def test_air_quality_readings_reach_timescale(dsn: str, broker: tuple[str, int]) -> None:
    respx.get(file_url(date(2026, 11, 19))).mock(return_value=httpx.Response(404))
    respx.get(file_url(date(2026, 11, 20))).mock(
        return_value=httpx.Response(
            200, content=CONTENT, headers={"Last-Modified": "Sat, 21 Nov 2026 01:30:00 GMT"}
        )
    )
    host, port = broker
    settings = Settings(mqtt_host=host, mqtt_port=port, db_dsn=dsn)
    migrate(dsn, source="real")
    store = Store.connect(dsn)
    stop_ingest = threading.Event()
    ingestor = threading.Thread(
        target=ingest, args=(settings, store, stop_ingest), kwargs={"max_wait_s": 0.5}
    )
    ingestor.start()
    time.sleep(2)  # let the ingestor subscribe before anything is published

    poller = AirQualityBridge(
        AirQualityClient(httpx.Client()), REGISTRY, mqtt_sessions(settings), lambda: NOW
    )
    try:
        assert poller.tick() == 36  # 12 stations, 3 hours
        wait_until(lambda: query(dsn, "select count(*) from telemetry") == [(36,)])
    finally:
        poller.close()
        time.sleep(1)
        stop_ingest.set()
        ingestor.join(timeout=10)
        store.close()

    victoire = next(s.station_id for s in REGISTRY if s.site_code == "FR37006")
    rows = query(
        dsn,
        "select ts, seq, pm10_ugm3, pm2_5_ugm3, temperature_c, battery_v from telemetry"
        f" where station_id = '{victoire}' order by ts",
    )
    # 19:00, 20:00 and 21:00 in Pointe-à-Pitre, in UTC.
    assert [r[0] for r in rows] == [
        datetime(2026, 11, 20, 23, tzinfo=UTC),
        datetime(2026, 11, 21, 0, tzinfo=UTC),
        datetime(2026, 11, 21, 1, tzinfo=UTC),
    ]
    assert rows[-1][1:] == (int(rows[-1][0].timestamp()) // 3600, 14.3, 6.8, None, None)
    assert query(dsn, "select count(distinct station_id) from telemetry") == [(12,)]
    assert query(dsn, "select count(*) from telemetry where pm2_5_ugm3 is not null") == [(24,)]
    # Each station logged in on its own and said goodbye.
    assert query(dsn, "select count(*), min(status) from station_status") == [(12, "offline")]
