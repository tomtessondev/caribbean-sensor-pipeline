"""TimescaleDB access: migrations and the three writes the ingestor needs."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from importlib.resources import files

import psycopg
from psycopg import sql
from psycopg.rows import TupleRow

from sensor_pipeline.messages import StationStatus, Telemetry
from sensor_pipeline.stations import STATIONS

logger = logging.getLogger(__name__)

TELEMETRY_COLUMNS = (
    "station_id",
    "ts",
    "seq",
    "temperature_c",
    "humidity_pct",
    "pressure_hpa",
    "pm2_5_ugm3",
    "pm10_ugm3",
    "battery_v",
    "rssi_dbm",
)


def migrate(
    dsn: str, grafana_password: str | None = None, ingestor_password: str | None = None
) -> list[str]:
    """Apply the SQL files not applied yet, in name order, register the stations and let
    the least-privileged roles log in when given a password: `grafana_reader` (read-only)
    and `sensor_ingestor` (insert-only). Returns the files applied."""
    # Autocommit: TimescaleDB policies and continuous aggregates refuse explicit
    # transaction blocks.
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "create table if not exists schema_migrations"
            " (name text primary key, applied_at timestamptz not null default now())"
        )
        done = {row[0] for row in conn.execute("select name from schema_migrations")}
        applied = []
        scripts = sorted(
            (f for f in files("sensor_pipeline.sql").iterdir() if f.name.endswith(".sql")),
            key=lambda f: f.name,
        )
        for script in scripts:
            if script.name in done:
                continue
            logger.info("Applying %s", script.name)
            conn.execute(script.read_text(encoding="utf-8").encode())
            conn.execute("insert into schema_migrations (name) values (%s)", (script.name,))
            applied.append(script.name)
        conn.execute(
            """
            insert into stations (station_id, town, territory, latitude, longitude)
            select * from unnest(%s::text[], %s::text[], %s::text[], %s::float8[], %s::float8[])
            on conflict (station_id) do update set
                town = excluded.town, territory = excluded.territory,
                latitude = excluded.latitude, longitude = excluded.longitude
            """,
            (
                [s.station_id for s in STATIONS],
                [s.town for s in STATIONS],
                [s.territory for s in STATIONS],
                [s.latitude for s in STATIONS],
                [s.longitude for s in STATIONS],
            ),
        )
        for role, password in (
            ("grafana_reader", grafana_password),
            ("sensor_ingestor", ingestor_password),
        ):
            if password is not None:
                # ALTER ROLE takes no bind parameters: quote the password as a SQL literal.
                conn.execute(
                    sql.SQL("alter role {} with login password {}").format(
                        sql.Identifier(role), sql.Literal(password)
                    )
                )
    return applied


@dataclass(frozen=True)
class Rejected:
    received_at: datetime
    topic: str
    payload: str
    error: str


class Store:
    """The ingestor's writes. Each call is one transaction."""

    def __init__(self, conn: psycopg.Connection[TupleRow]) -> None:
        self._conn = conn

    @classmethod
    def connect(cls, dsn: str) -> Store:
        return cls(psycopg.connect(dsn))

    def close(self) -> None:
        self._conn.close()

    def insert_readings(self, readings: Sequence[Telemetry]) -> int:
        """Insert readings, ignoring ones already stored. Returns the rows inserted."""
        if not readings:
            return 0
        placeholders = ", ".join(["%s"] * len(TELEMETRY_COLUMNS))
        with self._conn.transaction(), self._conn.cursor() as cur:
            # No conflict target: the primary key (station_id, ts) is the only unique
            # constraint, and naming it would need SELECT, which the ingestor role lacks.
            cur.executemany(
                f"insert into telemetry ({', '.join(TELEMETRY_COLUMNS)}) values ({placeholders})"
                " on conflict do nothing",
                [tuple(getattr(r, c) for c in TELEMETRY_COLUMNS) for r in readings],
            )
            return cur.rowcount

    def insert_rejected(self, rejected: Sequence[Rejected]) -> None:
        if not rejected:
            return
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.executemany(
                "insert into ingest_errors (received_at, topic, payload, error)"
                " values (%s, %s, %s, %s)",
                [(r.received_at, r.topic, r.payload, r.error) for r in rejected],
            )

    def set_status(self, station_id: str, status: StationStatus, at: datetime) -> None:
        with self._conn.transaction():
            self._conn.execute(
                """
                insert into station_status (station_id, status, changed_at)
                values (%s, %s, %s)
                on conflict (station_id) do update
                set status = excluded.status, changed_at = excluded.changed_at
                """,
                (station_id, status, at),
            )
