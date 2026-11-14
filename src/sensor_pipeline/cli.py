"""Command-line entry point: `sensor-pipeline migrate | provision-broker | simulate | ingest`."""

from __future__ import annotations

import logging
import signal
import threading
from pathlib import Path

import typer

from sensor_pipeline.config import Settings
from sensor_pipeline.db import Store, migrate
from sensor_pipeline.ingest import ingest as run_ingest
from sensor_pipeline.provision import BROKER_HOSTNAMES, provision_broker
from sensor_pipeline.publisher import simulate as run_simulation
from sensor_pipeline.stations import STATIONS

app = typer.Typer(help="Antilles sensor pipeline", no_args_is_help=True)


@app.callback()
def main(verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging")) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
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
) -> None:
    """Create or upgrade the TimescaleDB schema and register the stations."""
    applied = migrate(Settings().db_dsn, grafana_password, ingestor_password)
    typer.echo(f"Applied {len(applied)} migration(s): {', '.join(applied) or 'none'}")


@app.command("provision-broker")
def provision_broker_command(
    out: Path = typer.Option(..., help="Directory for the certificates, passwords and ACL"),
    hostname: list[str] = typer.Option(
        [],
        help="Extra name or IP the broker is reached at (e.g. its LAN address for real "
        "stations); 'broker', 'localhost' and 127.0.0.1 are always included",
    ),
) -> None:
    """Generate the broker's TLS certificates, one password per station and the ACL.
    Keeps what already exists: rerunning never invalidates a device's credentials."""
    written = provision_broker(
        out, [s.station_id for s in STATIONS], [*BROKER_HOSTNAMES, *hostname]
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


if __name__ == "__main__":
    app()
