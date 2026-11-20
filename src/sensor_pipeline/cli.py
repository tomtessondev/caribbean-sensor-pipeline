"""Command-line entry point:
`sensor-pipeline migrate | provision-broker | simulate | bridge | ingest | api`."""

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
import typer
import uvicorn

from sensor_pipeline.api import create_app
from sensor_pipeline.bridge import POLL_S, Bridge, mqtt_sessions
from sensor_pipeline.bridge import run as run_bridge
from sensor_pipeline.config import Settings
from sensor_pipeline.db import Store, migrate
from sensor_pipeline.ingest import ingest as run_ingest
from sensor_pipeline.meteofrance import MeteoFranceClient
from sensor_pipeline.provision import BROKER_HOSTNAMES, provision_broker
from sensor_pipeline.publisher import simulate as run_simulation
from sensor_pipeline.registry import StationSource, load_registry, seeded_stations
from sensor_pipeline.stations import STATIONS

app = typer.Typer(help="Antilles sensor pipeline", no_args_is_help=True)


@app.callback()
def main(verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging")) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


SOURCE_OPTION = typer.Option(
    "simulated",
    envvar="STATION_SOURCE",
    help="Station network: the six simulated stations, or the registry of real "
    "Météo-France stations",
)


def _stop_on_signals() -> threading.Event:
    """SIGTERM (docker stop) and SIGINT (Ctrl-C) end the loop cleanly."""
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    return stop


@app.command("migrate")
def migrate_command(
    grafana_password: str | None = typer.Option(
        None, envvar="GRAFANA_DB_PASSWORD", help="Enables the read-only grafana_reader login"
    ),
    ingestor_password: str | None = typer.Option(
        None, envvar="INGESTOR_DB_PASSWORD", help="Enables the insert-only sensor_ingestor login"
    ),
    site_password: str | None = typer.Option(
        None, envvar="SITE_DB_PASSWORD", help="Enables the site_reader login (stations endpoint)"
    ),
    source: StationSource = SOURCE_OPTION,
) -> None:
    """Create or upgrade the TimescaleDB schema and register the stations."""
    applied = migrate(Settings().db_dsn, grafana_password, ingestor_password, source, site_password)
    typer.echo(f"Applied {len(applied)} migration(s): {', '.join(applied) or 'none'}")


@app.command("provision-broker")
def provision_broker_command(
    out: Path = typer.Option(..., help="Directory for the certificates, passwords and ACL"),
    hostname: list[str] = typer.Option(
        [],
        help="Extra name or IP the broker is reached at (e.g. its LAN address for real "
        "stations); 'broker', 'localhost' and 127.0.0.1 are always included",
    ),
    source: StationSource = SOURCE_OPTION,
) -> None:
    """Generate the broker's TLS certificates, one password per station and the ACL.
    Keeps what already exists: rerunning never invalidates a device's credentials."""
    written = provision_broker(
        out, [s.station_id for s in seeded_stations(source)], [*BROKER_HOSTNAMES, *hostname]
    )
    typer.echo(f"Wrote {', '.join(written)} to {out}")


@app.command()
def simulate(
    seed: int = typer.Option(42, help="Same seed, same weather"),
    interval: float = typer.Option(10.0, min=1, help="Seconds between readings"),
    backfill_hours: float = typer.Option(48.0, min=0, help="History published at start"),
) -> None:
    """Run the six simulated stations until stopped."""
    published = run_simulation(
        Settings(),
        STATIONS,
        seed=seed,
        interval_s=interval,
        backfill_hours=backfill_hours,
        stop=_stop_on_signals(),
    )
    typer.echo(f"Published {published} readings")


@app.command()
def bridge(
    poll: float = typer.Option(POLL_S, min=10, help="Seconds between two looks at the API"),
    application_id_file: Path = typer.Option(
        Path("secrets/meteofrance_application_id"),
        envvar="METEOFRANCE_APPLICATION_ID_FILE",
        help="File holding the Météo-France application id (or set METEOFRANCE_APPLICATION_ID)",
    ),
) -> None:
    """Publish the readings of Météo-France's automatic stations until stopped, each
    station on its own MQTT session (provision the broker with STATION_SOURCE=meteofrance)."""
    application_id = os.getenv("METEOFRANCE_APPLICATION_ID")
    if not application_id:
        # Docker mounts a missing file as an empty directory: say what is missing
        # instead of crash-looping on IsADirectoryError.
        if not application_id_file.is_file():
            raise typer.BadParameter(
                f"{application_id_file} is not a file: put the Météo-France application id "
                "in secrets/meteofrance_application_id (or set METEOFRANCE_APPLICATION_ID)",
                param_hint="--application-id-file",
            )
        application_id = application_id_file.read_text(encoding="utf-8").strip()
    settings = Settings()
    registry = load_registry()
    sessions = mqtt_sessions(settings, registry)
    with httpx.Client(timeout=60) as http:
        run_bridge(
            Bridge(
                MeteoFranceClient(http, application_id, time.monotonic),
                registry,
                sessions,
                lambda: datetime.now(UTC),
            ),
            _stop_on_signals(),
            poll,
        )


@app.command()
def ingest(
    max_batch: int = typer.Option(500, min=1, help="Readings per transaction"),
    max_wait: float = typer.Option(2.0, min=0.1, help="Seconds before a partial batch is written"),
    ready_file: Path | None = typer.Option(None, help="Created once subscribed (health check)"),
) -> None:
    """Consume MQTT and write to TimescaleDB until stopped."""
    settings = Settings()
    store = Store.connect(settings.db_dsn)
    try:
        run_ingest(
            settings,
            store,
            _stop_on_signals(),
            max_batch=max_batch,
            max_wait_s=max_wait,
            ready_file=ready_file,
        )
    finally:
        store.close()


@app.command()
def api(
    host: str = typer.Option("127.0.0.1", help="Interface to listen on"),
    port: int = typer.Option(8000, help="Port to listen on"),
) -> None:
    """Serve the latest reading of each station (GET /v1/stations) until stopped. Connect
    it as site_reader (SENSOR_DB_DSN), the role that can read that and nothing else."""
    uvicorn.run(create_app(Settings().db_dsn), host=host, port=port, access_log=False)


if __name__ == "__main__":
    app()
