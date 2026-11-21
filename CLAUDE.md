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
  - `registry.py` + `data/meteofrance_antilles.csv`: the real Météo-France stations
    (built once by `scripts/build_registry.py`), and `data/air_quality_antilles.csv`: the
    12 official particle stations (Gwad'Air, Madininair; written by hand, ids checked by
    `make_air_quality_ids`); `STATION_SOURCE=simulated|meteofrance|real` (real =
    Météo-France + air quality, the deployed stack) picks what `migrate` seeds (and marks
    `active`, the other stations become inactive, `sql/007`; `stations.source` says
    simulated / meteofrance / airquality, `sql/009`), what `provision-broker` creates
    logins for and what the bridge polls. Station ids are
    permanent: the commune letters come from `registry.COMMUNES` (INSEE code -> one
    unique code), simulator ids are skipped, air-quality counters continue after
    Météo-France's; never renumber a deployed station.
    `offline_after_s` comes from observed cadence (`scripts/classify_cadence.py`, run on
    a database with a few hours of real data): 6000 s for 6-minute stations, 10800 s
    for hourly or never-seen ones
  - `meteofrance.py` (API client, OAuth2 token from the application id in
    `METEOFRANCE_APPLICATION_ID`, else `secrets/meteofrance_application_id`, never printed; invalid observations skipped one by one) and `bridge.py` (each slot
    read twice, at +10 and +80 min, each pass catching up on missed slots up to 3 h
    back; one MQTT session per station, acks awaited once per tick; a tick never stops
    the bridge). Tests replay `tests/fixtures/meteofrance/`; never call the real API in
    tests
  - `airquality.py` (LCSQA / INERIS real-time day files on data.gouv.fr, no key:
    timestamps in the territory's local time, converted to UTC; A/1 rows only; PM10 and
    PM2.5 merged per station and hour, `seq` = hours since the epoch; a PM10-only hour of
    a station that reports PM2.5 is held up to 3 h for it) and
    `bridge.AirQualityBridge` (current and previous local day every 15 min with
    If-Modified-Since / If-None-Match, 304 when unchanged; 72 h "already sent" window).
    `bridge.run_all` runs both pollers in threads; a crashed one stops the bridge. Tests replay
    `tests/fixtures/airquality/` (a trimmed real file); never call data.gouv.fr in tests.
    Attribution: « Données temps réel non validées : LCSQA / INERIS (Géod'Air), produites
    par Gwad'Air et Madininair, data.gouv.fr, Licence Ouverte 2.0 »
  - `api.py`: FastAPI, read-only `GET /v1/stations` (latest values incl. PM2.5, and
    `source`) + `/health` for the portfolio site,
    DB role `site_reader` (select on view `station_latest` only, `sql/006`), CORS limited
    to the site's origins, `Vary: Origin`, 3 s connect / 2 s statement timeouts, 503
    when the database is unavailable; served by `sensor-pipeline api`
  - `ingest.py`: paho thread enqueues, main thread validates and batches; `db.py` writes
  - `sql/NNN_*.sql`: migrations applied in order by `sensor-pipeline migrate`
- TimescaleDB (hypertable `telemetry`, continuous aggregate `telemetry_hourly`), Mosquitto
- Data quality (`sql/003_data_quality.sql`, rules replaced by `sql/008`: temperature
  5-40 °C for the mountain stations, then by `sql/010`: pm_spike judged only with 3
  following readings, lookback 12 h, so hourly stations are judged once their next hours
  arrive; pm_stuck / `pm10_stuck` count readings and are inert at hourly cadence):
  procedure `flag_readings` scheduled with `add_job` every 5 min, writes `reading_flags`; never modify `telemetry` to correct data,
  add a rule instead. Dashboards read `telemetry_clean` for PM values.
- `firmware/`: PlatformIO project for the ESP32 station. Logic in `lib/telemetry` (no
  Arduino dependency) tested with `pio test -e native`; `src/main.cpp` is hardware glue.
  `test/golden/telemetry_v1.json` must stay byte-identical to what the serialiser emits and
  valid for `Telemetry` (`tests/test_firmware_contract.py`). `pio` may be blocked locally
  by lean-ctx: CI runs it.
- pytest (unit; integration marked `integration`, real containers via testcontainers),
  ruff, mypy --strict
- Security: `provision.py` generates CA, broker cert, passwords, ACL (never commit them;
  `secrets/` is ignored). Broker is TLS-only on 8883 with ACL `antilles/%u/...`;
  ingestor uses DB role `sensor_ingestor` (insert-only) and must keep `on conflict do
  nothing` without a conflict target. `tests/test_security.py` attacks a real broker.
- Docker Compose: provision, broker, db (host port 5433), migrate, ingest, simulator (profile
  `simulated`) or bridge (profiles `meteofrance` and `real`), api (host port 8010), Grafana (host port 3001,
  anonymous read-only)
- Production: `docker-compose.vps.yml` for Coolify (`STATION_SOURCE=real`, no `ports:`, Coolify-generated
  `SERVICE_PASSWORD_*`, memory limits, broker and Grafana config baked in by
  `docker/*.Dockerfile`), documented in `docs/deploy-vps.md`
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
