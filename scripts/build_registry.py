"""Build the registry of real stations (src/sensor_pipeline/data/meteofrance_antilles.csv)
from Météo-France's station list and a few observation packages.

Run once by hand, review the diff, commit the CSV; tests never run it. It reads the
application id from secrets/meteofrance_application_id and prints counts only.

    .venv/bin/python scripts/build_registry.py

Every station measures every 6 minutes, but they do not deliver alike: some send each
reading within minutes, others send the past hour's readings in one batch, up to an hour
late. A station is kept when it reported a temperature in a complete package (two hours
old) and counts as "prompt" when it is already in the package of 12 minutes ago.
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
    make_station_ids,
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
        prompt = {obs["geo_id_insee"] for obs in package(slot(now - timedelta(minutes=12)))}

    kept = [r for r in rows if r["Id_station"] in with_temperature]
    ids = make_station_ids((r["Id_station"], r["Nom_usuel"]) for r in kept)
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REGISTRY_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(
            ["station_id", "mf_id", "name", "territory", "latitude", "longitude", "offline_after_s"]
        )
        for r in sorted(kept, key=lambda r: ids[r["Id_station"]]):
            mf_id = r["Id_station"]
            writer.writerow(
                [
                    ids[mf_id],
                    mf_id,
                    r["Nom_usuel"].replace("_", " ").title(),
                    TERRITORIES[territory_prefix(mf_id)],
                    r["Latitude"],
                    r["Longitude"],
                    OFFLINE_AFTER_S["prompt" if mf_id in prompt else "hourly_batch"],
                ]
            )
    n_prompt = sum(1 for r in kept if r["Id_station"] in prompt)
    print(
        f"{len(rows)} stations listed, {len(kept)} with a temperature sensor kept "
        f"({n_prompt} prompt, {len(kept) - n_prompt} in hourly batches)"
    )


if __name__ == "__main__":
    main()
