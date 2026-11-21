"""The public, read-only endpoint behind the portfolio site's live map: the latest reading
of each station. It logs in as `site_reader`, which can read the `station_latest` view and
nothing else, answers GET only, lets only the site's origins call it from a browser and
asks caches to keep each answer a minute (stations publish every 6 minutes at most)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Literal

import psycopg
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg.rows import TupleRow, class_row
from pydantic import AwareDatetime, BaseModel
from starlette.responses import Response as StarletteResponse

from sensor_pipeline.registry import Origin

SITE_ORIGINS = ("https://antilles-data-pipeline.vercel.app", "http://localhost:3000")
CACHE_CONTROL = "public, max-age=60"
# A database that does not answer must not hold the request (and a worker) for long.
CONNECT_TIMEOUT_S = 3
STATEMENT_TIMEOUT_MS = 2000


def connect(dsn: str) -> psycopg.Connection[TupleRow]:
    return psycopg.connect(
        dsn,
        connect_timeout=CONNECT_TIMEOUT_S,
        options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
    )


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
    pm2_5_ugm3: float | None
    status: Literal["online", "offline"]
    source: Origin


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

    @app.middleware("http")
    async def vary_on_origin(
        request: Request, call_next: Callable[[Request], Awaitable[StarletteResponse]]
    ) -> StarletteResponse:
        """The CORS answer depends on Origin, so every /v1 answer must say so to caches,
        including the one to a request without Origin (CORSMiddleware only marks the
        others)."""
        response = await call_next(request)
        if request.url.path.startswith("/v1/"):
            vary = [v.strip().lower() for v in response.headers.get("vary", "").split(",")]
            if "origin" not in vary:
                response.headers.append("Vary", "Origin")
        return response

    @app.get("/v1/stations")
    def stations(response: Response) -> StationsOut:
        with connect(dsn) as conn, conn.cursor(row_factory=class_row(StationOut)) as cur:
            cur.execute(
                "select station_id, name, territory, lat, lon, ts, temperature_c,"
                " humidity_pct, pressure_hpa, pm10_ugm3, pm2_5_ugm3, status, source"
                " from station_latest order by territory, station_id"
            )
            rows = cur.fetchall()
        response.headers["Cache-Control"] = CACHE_CONTROL
        return StationsOut(generated_at=datetime.now(UTC), stations=rows)

    @app.get("/health")
    def health() -> dict[str, str]:
        with connect(dsn) as conn:
            conn.execute("select 1 from station_latest limit 1")
        return {"status": "ok"}

    @app.exception_handler(psycopg.OperationalError)
    def database_unavailable(_request: Request, _exc: psycopg.OperationalError) -> JSONResponse:
        """Database down, unreachable or too slow (statement timeout): a temporary state,
        not a bug of the endpoint."""
        return JSONResponse({"detail": "database unavailable"}, status_code=503)

    return app
