"""Build the registry of real stations (src/sensor_pipeline/data/meteofrance_antilles.csv)
from Météo-France's station list and a few observation packages.

Run once by hand, review the diff, commit the CSV; tests never run it. It reads the
application id from secrets/meteofrance_application_id and prints counts only.

    .venv/bin/python scripts/build_registry.py

A station is kept when it reported a temperature in a complete package (two hours old).
Its offline threshold is not guessed from one package: a station already in the registry
keeps its own, a new one gets the cautious hourly threshold until
`scripts/classify_cadence.py` has seen a few hours of its telemetry.
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from sensor_pipeline.meteofrance import PACKAGE_URL, SLOT_S, TOKEN_URL
from sensor_pipeline.registry import (
    OFFLINE_AFTER_S,
    REGISTRY_PATH,
    TERRITORIES,
    RegistryStation,
    load_registry,
    make_station_ids,
    save_registry,
    territory_prefix,
)

ROOT = Path(__file__).resolve().parents[1]
STATIONS_URL = "https://public-api.meteofrance.fr/public/DPObs/v2/liste-stations"


def slot(t: datetime) -> datetime:
    return datetime.fromtimestamp(t.timestamp() // SLOT_S * SLOT_S, UTC)


def main() -> None:
    application_id = (ROOT / "secrets" / "meteofrance_application_id").read_text().strip()
    with httpx.Client(timeout=60) as http:
        token = http.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            headers={"Authorization": f"Basic {application_id}"},
        ).raise_for_status()
        headers = {"Authorization": f"Bearer {token.json()['access_token']}"}

        listing = http.get(STATIONS_URL, headers=headers).raise_for_status()
        rows = [
            r
            for r in csv.DictReader(io.StringIO(listing.content.decode("latin-1")), delimiter=";")
            if r["Id_station"][:3] in ("971", "972")
        ]

        def package(at: datetime) -> list[dict[str, Any]]:
            response = http.get(
                PACKAGE_URL,
                params={"date": at.strftime("%Y-%m-%dT%H:%M:%SZ"), "format": "json"},
                headers=headers,
            ).raise_for_status()
            data: list[dict[str, Any]] = response.json()
            return data

        now = datetime.now(UTC)
        complete = [
            slot(now - timedelta(hours=2)) - k * timedelta(seconds=SLOT_S) for k in range(5)
        ]
        with_temperature = {
            obs["geo_id_insee"] for at in complete for obs in package(at) if obs["t"] is not None
        }

    known = {s.mf_id: s.offline_after_s for s in load_registry()} if REGISTRY_PATH.exists() else {}
    kept = [r for r in rows if r["Id_station"] in with_temperature]
    ids = make_station_ids((r["Id_station"], r["Nom_usuel"]) for r in kept)
    save_registry(
        RegistryStation(
            station_id=ids[r["Id_station"]],
            mf_id=r["Id_station"],
            name=r["Nom_usuel"].replace("_", " ").title(),
            territory=TERRITORIES[territory_prefix(r["Id_station"])],
            latitude=float(r["Latitude"]),
            longitude=float(r["Longitude"]),
            offline_after_s=known.get(r["Id_station"], OFFLINE_AFTER_S["hourly"]),
        )
        for r in kept
    )
    new = sum(1 for r in kept if r["Id_station"] not in known)
    print(
        f"{len(rows)} stations listed, {len(kept)} with a temperature sensor kept "
        f"({new} new, hourly threshold until classify_cadence.py has seen them)"
    )


if __name__ == "__main__":
    main()
