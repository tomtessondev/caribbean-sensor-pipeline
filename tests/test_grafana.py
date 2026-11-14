"""Grafana as code: the provisioning files are well formed, and every SQL query they hold
runs against the real schema with the read-only role Grafana uses."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from psycopg.conninfo import make_conninfo

from sensor_pipeline.db import Store, migrate
from sensor_pipeline.messages import Telemetry
from tests.support import query

GRAFANA = Path(__file__).resolve().parents[1] / "grafana"
DASHBOARDS = sorted((GRAFANA / "dashboards").glob("*.json"))
ALERT_RULES = GRAFANA / "provisioning" / "alerting" / "rules.yaml"
DATASOURCE_UID = "timescale"


def dashboard_queries() -> list[tuple[str, str]]:
    queries = []
    for path in DASHBOARDS:
        board = json.loads(path.read_text(encoding="utf-8"))
        for variable in board.get("templating", {}).get("list", []):
            queries.append((f"{path.stem}/${variable['name']}", variable["query"]))
        for panel in board["panels"]:
            for target in panel.get("targets", []):
                queries.append((f"{path.stem}/{panel['title']}", target["rawSql"]))
    return queries


def alert_queries() -> list[tuple[str, str]]:
    rules = yaml.safe_load(ALERT_RULES.read_text(encoding="utf-8"))
    return [
        (f"alert/{rule['title']}", step["model"]["rawSql"])
        for group in rules["groups"]
        for rule in group["rules"]
        for step in rule["data"]
        if step["datasourceUid"] == DATASOURCE_UID
    ]


ALL_QUERIES = dashboard_queries() + alert_queries()


def render(sql: str) -> str:
    """Expand the Grafana macros and variables the queries use, as Grafana would."""
    sql = re.sub(r"\$__timeFilter\(([^)]+)\)", r"\1 > now() - interval '6 hours'", sql)
    sql = sql.replace("$__interval", "1 minute").replace("$station", "'gp-pap-01','mq-fdf-01'")
    assert "$" not in sql, f"unexpanded macro in {sql}"
    return sql


def test_dashboards_are_well_formed() -> None:
    assert [p.stem for p in DASHBOARDS] == ["air-quality", "fleet-health"]
    for path in DASHBOARDS:
        board = json.loads(path.read_text(encoding="utf-8"))
        ids = [panel["id"] for panel in board["panels"]]
        assert len(ids) == len(set(ids)), f"duplicate panel id in {path.name}"
        for panel in board["panels"]:
            assert panel["datasource"]["uid"] == DATASOURCE_UID, panel["title"]
            assert panel["targets"], panel["title"]


def test_alert_rules_are_well_formed() -> None:
    rules = yaml.safe_load(ALERT_RULES.read_text(encoding="utf-8"))
    titles = [rule["title"] for group in rules["groups"] for rule in group["rules"]]
    assert titles == ["Saharan dust episode", "Station offline", "Particle sensor stuck"]
    for group in rules["groups"]:
        for rule in group["rules"]:
            refs = {step["refId"] for step in rule["data"]}
            assert rule["condition"] in refs, rule["title"]


def reading(station: str, ts: datetime, seq: int, pm10: float) -> Telemetry:
    return Telemetry(station_id=station, ts=ts, seq=seq, pm10_ugm3=pm10, battery_v=4.0)


@pytest.fixture(scope="module")
def reader_dsn(dsn: str) -> str:
    migrate(dsn, grafana_password="reader-test")
    now = datetime.now(UTC).replace(microsecond=0)
    store = Store.connect(dsn)
    # Pointe-à-Pitre: readings 3 and 4 never arrived.
    store.insert_readings(
        [
            reading("gp-pap-01", now - timedelta(minutes=60 - 5 * i), seq, 20.0 + seq)
            for i, seq in enumerate([0, 1, 2, 5, 6, 7])
        ]
    )
    # Fort-de-France: the particle counter repeats the same value for 24 minutes.
    store.insert_readings(
        [reading("mq-fdf-01", now - timedelta(minutes=24 - 2 * i), i, 33.3) for i in range(12)]
    )
    store.set_status("gp-pap-01", "online", now)
    store.close()
    return make_conninfo(dsn, user="grafana_reader", password="reader-test")


@pytest.mark.integration
def test_station_health_measures_loss_and_stuck_sensors(reader_dsn: str) -> None:
    rows = query(
        reader_dsn,
        "select station_id, status, loss_pct_24h, pm10_stuck from station_health"
        " where station_id in ('gp-pap-01', 'mq-fdf-01', 'gp-bte-01') order by station_id",
    )
    assert [(r[0], r[1], float(r[2]) if r[2] is not None else None, r[3]) for r in rows] == [
        ("gp-bte-01", "unknown", None, False),  # no data at all
        ("gp-pap-01", "online", 25.0, False),  # 2 lost out of 8
        ("mq-fdf-01", "unknown", 0.0, True),
    ]


@pytest.mark.integration
@pytest.mark.parametrize(("name", "sql"), ALL_QUERIES, ids=[name for name, _ in ALL_QUERIES])
def test_every_grafana_query_runs_as_the_reader_role(reader_dsn: str, name: str, sql: str) -> None:
    rows: list[Any]
    with psycopg.connect(reader_dsn) as conn:
        rows = conn.execute(render(sql).encode()).fetchall()
    assert rows is not None, name


@pytest.mark.integration
def test_reader_role_cannot_write(reader_dsn: str) -> None:
    with psycopg.connect(reader_dsn) as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("delete from telemetry")
