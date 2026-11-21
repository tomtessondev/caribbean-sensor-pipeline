# Caribbean Sensor Pipeline

An IoT data pipeline for air-quality sensor stations in **Guadeloupe and Martinique**:
devices publish over **MQTT**, an ingestion service validates and batches the readings into
**TimescaleDB**, and **Grafana** shows air quality and fleet health and raises alerts, all
provisioned as code. The stations are either six simulated ones (realistic physics and
faults) or real ones: the 80 automatic weather stations of **Météo-France** in Guadeloupe,
Martinique, Saint-Barthélemy and Saint-Martin, bridged from its API, and the 12 official
**PM10 / PM2.5** stations of Gwad'Air and Madininair, bridged from the LCSQA / INERIS
real-time files; an
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
                                                                         flag_readings (job, every 5 min) ──► reading_flags
                                                                                                         │ telemetry_clean, station_health,
                                                                                     Grafana ◄───────────┘ station_quality_daily (views)
                                                                            dashboards + alerts as code    read-only role
```

| Part | Tooling | Where |
| --- | --- | --- |
| Message contract | JSON over MQTT, versioned, validated with pydantic | `src/sensor_pipeline/messages.py` |
| Stations | Simulator: diurnal cycle, atmospheric tide, Saharan dust plumes, solar battery, faults | `src/sensor_pipeline/simulator.py`, `publisher.py` |
| Real stations | Météo-France Package Observations API (OAuth2, httpx), a versioned registry of 80 stations, one MQTT session per station | `src/sensor_pipeline/meteofrance.py`, `bridge.py`, `registry.py`, `data/` |
| Official air quality | LCSQA / INERIS real-time CSV files on data.gouv.fr (conditional GET, local time to UTC), 12 particle stations | `src/sensor_pipeline/airquality.py`, `bridge.py`, `data/air_quality_antilles.csv` |
| Stations endpoint | FastAPI, read-only: the latest clean reading of each station for the portfolio site's map; its own database role, CORS limited to the site | `src/sensor_pipeline/api.py`, `sql/006_site_reader.sql` |
| Firmware | ESP32, C++17, PlatformIO: PMS5003 frame decoder, store-and-forward ring buffer, esp-mqtt QoS 1; Unity tests on the host; golden message shared with the Python tests | `firmware/` |
| Broker | Eclipse Mosquitto 2: TLS only, a login per client, per-station ACL | `docker/mosquitto.conf`, `docker/broker-entrypoint.sh` |
| Security material | Local CA and broker certificate (cryptography, ECDSA P-256), random passwords, ACL; generated, never committed | `src/sensor_pipeline/provision.py` |
| Ingestion | paho-mqtt, batching, idempotent inserts, dead-letter table; insert-only database role | `src/sensor_pipeline/ingest.py`, `db.py` |
| Storage | TimescaleDB: hypertable, compression, retention, continuous aggregate | `src/sensor_pipeline/sql/` |
| Data quality | SQL rules run by a TimescaleDB job: stuck sensor, spike, implausible weather; flags, a validated view, a daily quality score per station | `src/sensor_pipeline/sql/003_data_quality.sql` |
| Monitoring | Grafana 12: provisioned datasource, two dashboards, three alert rules; read-only database role | `grafana/`, `src/sensor_pipeline/sql/002_station_health.sql` |
| Quality | pytest (unit + integration on real containers), ruff, mypy `--strict` | `tests/`, `.github/workflows/ci.yml` |

## Quick start

```bash
make up                             # certificates, broker, database, migrations, ingestor, simulator, Grafana
docker compose logs -f ingest       # "Wrote 500 new readings ..."
```

Real stations instead of the simulator (on a fresh stack, `make down` then remove the
volumes, or at least the `broker-secrets` one):

```bash
echo -n '<application id>' > secrets/meteofrance_application_id   # from portail-api.meteofrance.fr, never committed
make up STATION_SOURCE=meteofrance  # registers the 80 stations, one broker login each, starts the bridge
docker compose logs -f bridge       # "Published 47 readings"
```

`make up STATION_SOURCE=real` adds the 12 official particle stations (the deployed stack):
the bridge then also reads the air-quality files every 15 minutes. Switching from
`meteofrance` to `real` on an existing stack works without removing anything: `provision`
adds the new broker logins and `migrate` registers the new stations, but the broker reads
its logins when it starts, so recreate it (`docker compose up -d --force-recreate broker`).

The broker listens on `localhost:8883`, TLS only, with a login. On the first start the
`provision` service writes the CA, the broker certificate, one password per station and the
ACL to the `broker-secrets` volume; later starts keep them. To reach the broker from your
machine, copy `ca.crt` and `credentials.json` out of that volume.

Grafana on http://localhost:3001 (read-only without login; `admin` / `admin` to edit):

- **Air quality — Antilles**: stations online, stations above the EU PM10 limit (24 h
  mean), a map of the latest PM10, PM10 against the 50 µg/m³ limit, PM2.5 (particle
  stations: the official ones or the simulated ones), temperature, humidity, pressure
  (weather stations: Météo-France's or the simulated ones).
- **Fleet health**: per-station status, minutes since the last reading, battery, radio
  signal, message loss and stuck-sensor flag; battery and RSSI over time, loss per hour,
  the latest rejected messages.
- **Alerts** (Alerting > Alert rules): *Saharan dust episode* (PM10 mean of the last
  3 hours above 50 for 15 min: the official hourly means arrive up to 2.5 h late),
  *Station offline* (no reading for longer than the station's own threshold: 5 min for a
  simulated station, 100 min or 3 h for a Météo-France one, 4 h for an official particle
  station), *Particle sensor stuck*
  (same value for 30 min). One alert instance per station, active stations only.

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

Production: `docker-compose.vps.yml` runs the real stations (`STATION_SOURCE=real`:
Météo-France and the official particle stations) on a VPS with Coolify
(no host port, generated passwords, a memory limit per container); see
[docs/deploy-vps.md](docs/deploy-vps.md).

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
- **A station can only speak for itself.** Every client logs in (no anonymous access), over
  TLS verified against the project's own CA. A station's username is its id and the ACL
  pattern `antilles/%u/...` lets it publish on its own topics only: a stolen or
  misconfigured device cannot inject readings for another station. The ingestor may
  subscribe, not publish. Integration tests attack a real broker set up like Compose's:
  plain MQTT, wrong password, another station's password, anonymous login, a spoofed topic
  and a publishing ingestor are all refused.
- **Least privilege in the database too.** The ingestor writes with an insert-only role:
  it cannot read, update or delete readings, nor read the quality flags. Duplicates still
  vanish without SELECT because the insert uses `ON CONFLICT DO NOTHING` without naming the
  key (naming it requires SELECT, and a column-level grant does not survive TimescaleDB's
  compressed internal table, which is how this was found).
- **Generated secrets.** The CA, certificates and passwords are created by
  `sensor-pipeline provision-broker` (owner-only files, ECDSA P-256 keys that an ESP32
  handles cheaply). Rerunning keeps the CA and the passwords already flashed into devices.
- **TimescaleDB, not a separate time-series engine.** It is PostgreSQL: SQL, joins with
  reference tables, the usual tooling, plus daily chunks, columnar compression after 7 days,
  180-day retention and an hourly rollup refreshed in the background.
- **Monitoring as code.** Datasource, dashboards and alert rules are files in `grafana/`,
  loaded at start; UI edits are not kept. Grafana connects with a `select`-only role whose
  password comes from the environment. CI runs every dashboard and alert query against a
  real TimescaleDB with that role, so a renamed column breaks the build, not a panel.
- **Flag, don't delete.** Three SQL rules run every 5 minutes as a TimescaleDB job:
  *pm_stuck* (the same PM10 value 30 times in a row), *pm_spike* (more than 3 times the
  mean of the 30 surrounding readings, so a Saharan plume rising over hours is not
  flagged but an isolated glitch is; judged only once 3 following readings exist, so an
  hourly station's latest value is not condemned before the next hours arrive) and *implausible_weather* (fine for the sensor,
  impossible for a Caribbean station: temperature outside 5-40 °C, which leaves room for
  the mountain stations' cold nights, humidity under 25 %, pressure outside 950-1040 hPa).
  Flags go to `reading_flags`; the raw table is never modified; `telemetry_clean` hides
  only the suspect values (a stuck PM sensor does not invalidate the temperature) and the
  air-quality charts read it.
- **Checked by arrival, not by timestamp.** The rules look at everything received in the
  last 12 hours, whatever its timestamp, so a station replaying an hour of buffered readings after an
  outage gets checked too.
- **One quality number.** `station_quality_daily` gives, per station and local day, the
  share of expected readings (one per slot) that arrived and passed every rule: lost
  messages and suspect values both lower it. The rules also caught the simulator itself:
  its first fault rates had the particle sensors stuck 20 % of the time.

## Real stations: the Météo-France bridge

- **A registry, not a guess.** `src/sensor_pipeline/data/meteofrance_antilles.csv` lists
  the 80 stations of the four territories that report a temperature (of 96 listed), with
  their Météo-France id, a contract id and their offline threshold. It was built once from
  the API by `scripts/build_registry.py` and is reviewed like code.
- **Ids that never change meaning.** A contract id (`gp-aby-02`, Le Raizet in Les Abymes)
  is the territory, three letters for the commune and a counter. The letters come from a
  hand-checked table keyed by the INSEE commune code (`registry.COMMUNES`), not from
  station names, which spell one commune several ways: one commune has one code and no two
  communes share one (La Désirade `des`, Deshaies `dsh`). Ids the simulator uses
  (`mq-fdf-01`...) are skipped, so the two sources can never merge histories.
- **Two deliveries, two reads, no gap.** Every station measures every 6 minutes, but about
  half publish each reading within minutes and the others send the past hour in one batch.
  The bridge reads the package of each 6-minute slot 10 minutes after it and again 80
  minutes after it, and publishes each reading once. Each read remembers its last slot:
  after a laptop sleep or an API outage the next tick reads every slot it missed, up to
  3 hours back. `seq` is the number of 6-minute slots since the epoch, so a missing
  reading shows as a gap, as for a device.
- **A bridge that keeps running.** An API error, a malformed package or a broker outage
  is logged and retried on the next tick; an observation that does not parse is skipped
  alone. Readings are queued on their session and acknowledgements awaited once per tick,
  under one 30 s deadline; while the broker is away paho keeps them and resends them on
  reconnection, and each session republishes its retained "online" on every connection.
- **A station is a station.** Each one gets its own MQTT session, broker login and last
  will: the ACL, the ingestor, the quality rules and Grafana do not know the difference.
  Units follow the contract: temperature K to °C, humidity in %, sea-level pressure Pa to
  hPa (the station pressure of a 1141 m mountain station would fail the plausibility rule).
- **Offline is per station, from observed cadence.** `stations.offline_after_s` is
  5 minutes for a simulated station. For a Météo-France one, `scripts/classify_cadence.py`
  reads a few hours of real telemetry: a station with readings off the hour measures every
  6 minutes and is offline after 100 minutes (its newest reading is at most about
  90 minutes old when it arrives in a batch read 80 minutes late); one seen only on the
  hour, or not yet, gets 3 hours. On 19 hours of data all 80 stations report every
  6 minutes. The fleet table and the *Station offline* alert use the threshold.
- **Only the active source.** A database that switched from the simulator to Météo-France
  keeps the simulated stations and their history, but `migrate` marks them inactive
  (`stations.active`): the map, the fleet views and the offline alert show the stations
  of `STATION_SOURCE` only.
- **The API is never called in tests.** A real package recorded by
  `scripts/record_package.py` is replayed with respx, through a real broker and database
  in the integration test. The token is cached until a minute before it expires and a 401
  gets one new token and one retry.

## Real particles: the official air-quality files

The Météo-France stations measure the weather, not particles. The official PM10 and PM2.5
of the Antilles come from the networks approved for air-quality monitoring, Gwad'Air
(Guadeloupe, Saint-Martin) and Madininair (Martinique), through the real-time files that
LCSQA / INERIS publish on data.gouv.fr. The bridge runs a second poller for them, next to
the Météo-France one (`STATION_SOURCE=real`).

- **One file per local day for all of France** (about 12 MB, 47 000 rows), rewritten every
  hour, and the files of the previous days rewritten too while late values arrive. The
  poller reads the current and the previous local day every 15 minutes, sending back the
  `Last-Modified` and `ETag` of the last read (`If-Modified-Since`, `If-None-Match`):
  data.gouv.fr redirects to an S3 storage that answers 304 with no body when the file is
  unchanged, so a poll costs a few hundred bytes, not 12 MB. The version of a file is kept
  only once its readings went out, so a broker outage does not lose them until the next
  rewrite. Only the Antilles rows are decoded: the whole file as text would not fit the
  bridge's memory limit.
- **Local time, not UTC.** The timestamps are the territory's local time (UTC−4, no
  daylight saving), even though the companion XML says `+00:00`; this was checked against
  Gwad'Air's own service in true UTC. 21:00 in Pointe-à-Pitre becomes 01:00 UTC the next
  day.
- **One reading per station and hour.** PM10 and PM2.5 of the same hour are merged into
  one message (the other contract fields stay empty); an hour is sent once, so when a
  station that measures PM2.5 has only its PM10 in the file yet, that hour waits up to
  3 hours for the PM2.5, then goes out alone; `ts` is the start of the hour and
  `seq` the number of hours since the epoch. Only rows marked valid (`code qualité` A,
  `validité` 1) with a value are kept; the unit is spelled `µg-m3` or `µg/m3`, anything
  else is logged and skipped.
- **A 72-hour memory.** Values arrive up to 2.5 hours late and files are rewritten for
  three days, so what was sent is remembered for 72 hours and nothing older is published.
- **Raw values.** They are hourly means not validated yet by the networks (`moyenne
  horaire brute`): the quality rules of this pipeline still apply to them. A spike is
  judged once three later hours arrived (`sql/010`); the stuck-sensor rules count
  readings (30 in a row, 10 in 30 minutes) and stay inert at one reading per hour.
- **Same ids, same contract.** The 12 stations are in
  `src/sensor_pipeline/data/air_quality_antilles.csv` with ids from the same commune table,
  continuing its counters (Les Abymes has `gp-aby-01` and `-02` from Météo-France, its
  particle station is `gp-aby-03`); each has its own broker login and MQTT session, and an
  offline threshold of 4 hours.

Données temps réel non validées : LCSQA / INERIS (Géod'Air), produites par Gwad'Air et
Madininair, data.gouv.fr, Licence Ouverte 2.0.

## The stations endpoint

`GET /v1/stations` (on http://localhost:8010 with Docker Compose) feeds the live map of the
portfolio site:

```json
{"generated_at": "2026-11-20T04:12:31Z",
 "stations": [{"station_id": "gp-aby-02", "name": "Le Raizet Aero", "territory": "Guadeloupe",
               "lat": 16.264, "lon": -61.516333, "ts": "2026-11-20T04:00:00Z",
               "temperature_c": 24.6, "humidity_pct": 93.0, "pressure_hpa": 1015.2,
               "pm10_ugm3": null, "pm2_5_ugm3": null, "status": "online",
               "source": "meteofrance"}]}
```

Every station of the active source is listed, with where its readings come from
(`source`: `meteofrance`, `airquality` or `simulated`); `ts` and the values are those of its latest
reading of the last 24 hours, with flagged values set to null as in `telemetry_clean`;
`status` is `offline` past the station's own threshold. It connects as `site_reader`, a
role that can read the `station_latest` view and nothing else (password from
`SITE_DB_PASSWORD`), with a 3 s connect timeout and a 2 s statement timeout, answers GET
only, allows browser calls from `https://antilles-data-pipeline.vercel.app` and
`http://localhost:3000` only, and sends `Cache-Control: public, max-age=60` and
`Vary: Origin`. `GET /health` checks the database; both answer 503 while it is
unavailable. No OpenAPI description or docs page is published.

## Roadmap

See [docs/ROADMAP.md](docs/ROADMAP.md).

## Author

Tom Tesson, data engineer and FPGA/embedded engineer, based in Guadeloupe (UTC−4).
