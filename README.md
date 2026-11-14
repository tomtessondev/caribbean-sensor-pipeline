# Caribbean Sensor Pipeline

An IoT data pipeline for air-quality sensor stations in **Guadeloupe and Martinique**:
devices publish over **MQTT**, an ingestion service validates and batches the readings into
**TimescaleDB**, and **Grafana** shows air quality and fleet health and raises alerts, all
provisioned as code. Six stations are simulated with realistic physics and faults, and an
[ESP32 firmware](firmware/) (BME280 + PMS5003) speaks the same contract, tested on the host
and built in CI.

It is the companion of [antilles-data-pipeline](https://github.com/tomtessondev/antilles-data-pipeline)
(batch: APIs, S3 lake, dbt, Dagster): same islands, the streaming side, closer to the
embedded world I come from.

## Architecture

```
 stations (simulated or ESP32)            Mosquitto                 ingestor                     TimescaleDB
 ─────────────────────────────────►  antilles/<id>/telemetry  ──►  validate (pydantic)  ──►  telemetry (hypertable)
   BME280 + PMS5003 + battery         antilles/<id>/status         batch: 500 rows / 2 s      telemetry_hourly (continuous aggregate)
   QoS 1, last will "offline"         (retained)                   rejects ──────────────►    ingest_errors, station_status
                                                                                                         │ station_health (view)
                                                                                     Grafana ◄───────────┘ read-only role
                                                                            dashboards + alerts as code
```

| Part | Tooling | Where |
| --- | --- | --- |
| Message contract | JSON over MQTT, versioned, validated with pydantic | `src/sensor_pipeline/messages.py` |
| Stations | Simulator: diurnal cycle, atmospheric tide, Saharan dust plumes, solar battery, faults | `src/sensor_pipeline/simulator.py`, `publisher.py` |
| Firmware | ESP32, C++17, PlatformIO: PMS5003 frame decoder, store-and-forward ring buffer, esp-mqtt QoS 1; Unity tests on the host; golden message shared with the Python tests | `firmware/` |
| Broker | Eclipse Mosquitto 2 | `docker/mosquitto.conf` |
| Ingestion | paho-mqtt, batching, idempotent inserts, dead-letter table | `src/sensor_pipeline/ingest.py`, `db.py` |
| Storage | TimescaleDB: hypertable, compression, retention, continuous aggregate | `src/sensor_pipeline/sql/` |
| Monitoring | Grafana 12: provisioned datasource, two dashboards, three alert rules; read-only database role | `grafana/`, `src/sensor_pipeline/sql/002_station_health.sql` |
| Quality | pytest (unit + integration on real containers), ruff, mypy `--strict` | `tests/`, `.github/workflows/ci.yml` |

## Quick start

```bash
docker compose up -d --build        # broker, database, migrations, ingestor, simulator, Grafana
docker compose logs -f ingest       # "Wrote 500 new readings ..."
```

Grafana on http://localhost:3001 (read-only without login; `admin` / `admin` to edit):

- **Air quality — Antilles**: stations online, stations above the EU PM10 limit, a map of
  the latest PM10, PM10 against the 50 µg/m³ limit, PM2.5, temperature, humidity, pressure.
- **Fleet health**: per-station status, minutes since the last reading, battery, radio
  signal, message loss and stuck-sensor flag; battery and RSSI over time, loss per hour,
  the latest rejected messages.
- **Alerts** (Alerting > Alert rules): *Saharan dust episode* (PM10 hourly mean above 50 for
  15 min), *Station offline* (no reading for 5 min), *Particle sensor stuck* (same value
  for 30 min). One alert instance per station.

```bash
docker compose exec db psql -U sensors -c \
  "select station_id, bucket, round(pm10_avg_ugm3::numeric, 1) pm10, readings
   from telemetry_hourly order by bucket desc, station_id limit 12"
```

Development:

```bash
python -m venv .venv && source .venv/bin/activate
make install        # pip install -e ".[dev]"
make check          # ruff, mypy, unit tests
make integration    # tests against real Mosquitto and TimescaleDB (needs Docker)
```

## Design choices

- **A versioned contract.** Firmware in the field cannot be updated in lockstep with the
  server, so every telemetry message carries `v`. Ranges in the schema are the sensors'
  physical limits: outside them the message is broken; an odd but possible value is data.
- **At-least-once, stored exactly once.** Stations publish with QoS 1, so the broker may
  deliver a reading twice; the `(station_id, ts)` key turns duplicates into no-ops. The
  ingestor keeps a persistent session, so the broker queues messages while it restarts.
- **Sequence numbers measure loss.** `seq` advances even when a reading is lost on the way:
  gaps per station are the radio link's loss rate, not a guess. A lower number than the
  previous one is a reboot, not a loss. The simulator numbers readings by their slot on a
  fixed time grid, so a restart republishes identical history instead of a second,
  interleaved sequence (which first showed up as a bogus 87 % loss on the fleet dashboard).
- **Nothing is silently dropped.** Messages that fail validation land verbatim in
  `ingest_errors` with the reason.
- **Batching with a deadline.** One transaction per 500 readings or per 2 seconds,
  whichever comes first: throughput without unbounded latency.
- **Station liveness from MQTT itself.** Each station's last will is a retained "offline"
  on its status topic: the broker announces a dead station without any polling.
- **TimescaleDB, not a separate time-series engine.** It is PostgreSQL: SQL, joins with
  reference tables, the usual tooling, plus daily chunks, columnar compression after 7 days,
  180-day retention and an hourly rollup refreshed in the background.
- **Monitoring as code.** Datasource, dashboards and alert rules are files in `grafana/`,
  loaded at start; UI edits are not kept. Grafana connects with a `select`-only role whose
  password comes from the environment. CI runs every dashboard and alert query against a
  real TimescaleDB with that role, so a renamed column breaks the build, not a panel.

## Roadmap

See [docs/ROADMAP.md](docs/ROADMAP.md).

## Author

Tom Tesson, data engineer and FPGA/embedded engineer, based in Guadeloupe (UTC−4).
