# Deploying on a VPS with Coolify

`docker-compose.vps.yml` runs the real stations (no simulator) on a VPS
managed by [Coolify](https://coolify.io), behind Coolify's Traefik proxy with Let's
Encrypt certificates. `STATION_SOURCE=real` (set in the file for `provision`, `migrate` and
`bridge`) means the 80 Météo-France weather stations plus the 12 official PM10 / PM2.5
stations of Gwad'Air and Madininair (LCSQA / INERIS real-time files, no key needed):

| URL | Service |
| --- | --- |
| https://capteurs.horizon-studio.dev | Grafana (dashboards readable without login) |
| https://capteurs.horizon-studio.dev/v1/stations | Stations endpoint for the portfolio site's map |

Nothing else is reachable from the internet: the broker and the database have no route
and no host port.

## Coolify settings

1. **Projects > Démos > + New > Public/Private Repository** (with the GitHub App if the
   repository is private), repository `tomtessondev/caribbean-sensor-pipeline`, branch
   `main`.
2. **Build Pack: Docker Compose**, base directory `/`, Docker Compose location
   `/docker-compose.vps.yml`. Save: Coolify reads the file and lists its services.
3. **Domains**, per service:
   - `grafana`: `https://capteurs.horizon-studio.dev:3000`
   - `api`: `https://capteurs.horizon-studio.dev:8000/v1`

   The `:3000` / `:8000` suffix tells Coolify which container port to route to (it is not
   published on the host); `https://` makes it request a
   Let's Encrypt certificate. Traefik sends `/v1/...` to the API and everything else to
   Grafana.
4. **Strip Prefixes: off** (Advanced, or the per-service option, depending on the Coolify
   version). The API serves `/v1/stations` itself: with the prefix stripped it would
   receive `/stations` and answer 404.
5. **Environment Variables**: enter one value only,
   `METEOFRANCE_APPLICATION_ID` (the application id from portail-api.meteofrance.fr,
   marked as secret / not available at build time). Leave every `SERVICE_PASSWORD_*`
   alone: Coolify generates them on the first deploy and keeps them.
6. **Deploy**. The first deploy builds three images (the application, the broker, Grafana),
   then starts the services in order: `provision` (certificates, broker logins) →
   `broker` and `db` → `migrate` (schema, the 92 stations, the passwords of the three
   restricted roles) → `ingest` → `bridge`, with `api` and `grafana` after `migrate`.
   The first readings arrive within a minute; every station has one within about
   10 minutes. The particle stations publish hourly means about 1.5 h after the hour:
   on start the bridge publishes the hours of the current and the previous local day at
   once, then each new hour.

### Variables

| Variable | Set by | Used for |
| --- | --- | --- |
| `METEOFRANCE_APPLICATION_ID` | the owner, in Coolify's UI | the bridge's OAuth2 token (never logged) |
| `SERVICE_PASSWORD_POSTGRES` | Coolify | database owner `sensors` (migrations, backups) |
| `SERVICE_PASSWORD_INGESTOR` | Coolify | role `sensor_ingestor` (insert-only) |
| `SERVICE_PASSWORD_SITEREADER` | Coolify | role `site_reader` (the API, one view) |
| `SERVICE_PASSWORD_GRAFANADB` | Coolify | role `grafana_reader` (select-only) |
| `SERVICE_PASSWORD_GRAFANAADMIN` | Coolify | Grafana `admin` login, to edit |

`migrate` sets the three role passwords on every deploy, so they always match Coolify's.
Two are read once, when their volume is created: the database owner password
(`db-data`) and the Grafana admin password (`grafana-data`). Do not regenerate those two
in Coolify on a running deployment, or change them in the database / Grafana first.

## Memory budget

Each container has a hard limit (`deploy.resources.limits.memory`); the stack stays
around 1 GB even if every service grows to its limit.

| Service | Limit | Seen after start |
| --- | --- | --- |
| db (TimescaleDB, `shared_buffers` tuned to 96 MB from the limit) | 384 MB | 62 MB |
| grafana | 192 MB | 117-157 MB |
| broker | 64 MB | 7 MB |
| ingest | 128 MB | 50 MB |
| bridge (peak about 92 MB while reading an air-quality file) | 128 MB | 76 MB |
| api | 128 MB | 53 MB |
| **long-running total** | **1024 MB** | **about 400 MB** |
| provision, migrate (one-shot, exit in seconds) | 128 MB each | |

## Checking that no port is published

On the VPS:

```bash
sudo ss -tlnp
```

Only Coolify's own listeners should appear (Traefik on 80/443, SSH, Coolify itself):
nothing on 3000, 8000, 5432 or 8883. `docker ps --format '{{.Names}} {{.Ports}}'`
shows `3000/tcp` and `8000/tcp` without a `0.0.0.0:` mapping for grafana and api, and
nothing for the others.

## Database backup

Coolify names each container `<service>-<resource uuid>` (shown on the resource's page).
One line for a nightly script, as the database owner, in PostgreSQL's custom format:

```bash
docker exec "$(docker ps -qf name=^db-<resource uuid>)" pg_dump -U sensors -Fc sensors > /var/backups/sensors/sensors-$(date +%F).dump
```

`pg_dump` warns about circular foreign keys in TimescaleDB's catalog: that is expected.
To restore into an empty database of the same TimescaleDB version, run
`select timescaledb_pre_restore();`, then `pg_restore -U sensors -d sensors <file>`, then
`select timescaledb_post_restore();`.

## Updating

Merge to `main`, then **Redeploy** in Coolify (or turn on automatic deploys on push).
`provision` keeps the existing certificates and passwords, `migrate` applies only the
new migrations, and the broker's persistent sessions keep the readings published while
the ingestor restarts.

### Moving an existing deployment to `STATION_SOURCE=real`

A deployment made with `STATION_SOURCE=meteofrance` takes the change with a normal
redeploy, nothing to do by hand:

- `provision` runs on every deploy and keeps what exists: the CA and the 80 existing
  logins stay, the 12 particle stations get theirs (`credentials.json`, `passwords`).
- The broker reads its logins only when it starts (`docker/broker-entrypoint.sh` copies
  and hashes `passwords`, then starts Mosquitto). A Coolify redeploy recreates every
  container, the broker included, so it starts with the new logins. On a host where only
  some containers are recreated (a plain `docker compose up -d`), recreate the broker
  too: `docker compose up -d --force-recreate broker`; otherwise the bridge's particle
  sessions are refused and logged as not acknowledged.
- `migrate` applies `009_station_source.sql` (the `source` column, PM2.5 and `source` in
  `station_latest`) and registers the 12 new stations as active.
- The bridge refuses to start if a station has no login, with a message naming the
  `STATION_SOURCE` to provision with.
