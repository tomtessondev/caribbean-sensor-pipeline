"""The public, read-only endpoint behind the portfolio site's live map: the latest reading
of each station. It logs in as `site_reader`, which can read the `station_latest` view and
nothing else, answers GET only, lets only the site's origins call it from a browser and
asks caches to keep each answer a minute (stations publish every 6 minutes at most)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

import psycopg
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import class_row
from pydantic import AwareDatetime, BaseModel

SITE_ORIGINS = ("https://antilles-data-pipeline.vercel.app", "http://localhost:3000")
CACHE_CONTROL = "public, max-age=60"


class StationOut(BaseModel):
    station_id: str
    name: str
    territory: str
    lat: float
    lon: float
    ts: AwareDatetime | None
    temperature_c: float | None
    humidity_pct: float | None
    pressure_hpa: float | None
    pm10_ugm3: float | None
    status: Literal["online", "offline"]


class StationsOut(BaseModel):
    generated_at: AwareDatetime
    stations: list[StationOut]


def create_app(dsn: str) -> FastAPI:
    app = FastAPI(
        title="Antilles stations",
        description="Latest reading of each station of the caribbean-sensor-pipeline.",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(SITE_ORIGINS),
        allow_methods=["GET"],
        allow_credentials=False,
    )

    @app.get("/v1/stations")
    def stations(response: Response) -> StationsOut:
        with psycopg.connect(dsn) as conn, conn.cursor(row_factory=class_row(StationOut)) as cur:
            cur.execute(
                "select station_id, name, territory, lat, lon, ts, temperature_c,"
                " humidity_pct, pressure_hpa, pm10_ugm3, status"
                " from station_latest order by territory, station_id"
            )
            rows = cur.fetchall()
        response.headers["Cache-Control"] = CACHE_CONTROL
        return StationsOut(generated_at=datetime.now(UTC), stations=rows)

    @app.get("/health")
    def health() -> dict[str, str]:
        with psycopg.connect(dsn) as conn:
            conn.execute("select 1 from station_latest limit 1")
        return {"status": "ok"}

    return app
