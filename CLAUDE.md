# CLAUDE.md

Context for Claude Code working in this repository.

## Purpose

Portfolio project (phase 4 of the antilles-data-pipeline roadmap): an IoT pipeline for
air-quality stations in Guadeloupe and Martinique. The owner is an FPGA / embedded engineer
moving toward freelance data engineering, so code quality, tests and clear docs matter more
than feature count. Every change should be something a recruiter can read.

## Stack

- Python 3.12+ package in `src/sensor_pipeline` (src layout, hatchling)
  - `messages.py`: the MQTT contract (topics, versioned pydantic `Telemetry`, status). The
    future ESP32 firmware must follow it; change it only with a new `v`.
  - `stations.py`, `simulator.py` (physics + faults, deterministic per seed),
    `publisher.py` (one MQTT session per station, last will)
  - `ingest.py`: paho thread enqueues, main thread validates and batches; `db.py` writes
  - `sql/NNN_*.sql`: migrations applied in order by `sensor-pipeline migrate`
- TimescaleDB (hypertable `telemetry`, continuous aggregate `telemetry_hourly`), Mosquitto
- pytest (unit; integration marked `integration`, real containers via testcontainers),
  ruff, mypy --strict
- Docker Compose: broker, db (host port 5433), migrate, ingest, simulator, Grafana (host
  port 3001, anonymous read-only)
- `grafana/`: provisioned datasource (read-only role `grafana_reader`, password from
  `GRAFANA_DB_PASSWORD`), dashboards JSON, alert rules YAML. Every SQL query in them is
  executed by `tests/test_grafana.py`: keep them runnable after schema changes.

## Commands

```bash
python -m venv .venv && source .venv/bin/activate
make install       # pip install -e ".[dev]"
make check         # ruff + mypy + unit tests
make integration   # needs Docker
make up / down     # full stack
make psql          # SQL shell on the database
```

## Rules

- Run `make check` and `make integration` before every commit; both must pass.
- Ingestion must stay idempotent (primary key `(station_id, ts)` + `on conflict do nothing`).
- Invalid messages go to `ingest_errors`, never silently dropped.
- Schema changes are new numbered files in `src/sensor_pipeline/sql/`, never edits of
  applied ones.
- Keep mypy strict; no `type: ignore` without a comment explaining why.
- Code, comments, commits and README in English.
- Small commits with clear messages; open a PR per feature so CI runs.

## Roadmap

See `docs/ROADMAP.md`. Work through it in order unless told otherwise.
