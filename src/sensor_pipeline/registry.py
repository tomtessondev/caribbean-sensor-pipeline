"""The stations a deployment knows: the six simulated ones, or the registry of real
Météo-France automatic weather stations (a versioned CSV built once by
`scripts/build_registry.py`). `STATION_SOURCE` picks one; `migrate` seeds it into the
`stations` table and `provision-broker` creates one broker login per station."""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from sensor_pipeline.stations import STATIONS

StationSource = Literal["simulated", "meteofrance"]

REGISTRY_PATH = Path(__file__).parent / "data" / "meteofrance_antilles.csv"

TERRITORIES = {
    "gp": "Guadeloupe",
    "mq": "Martinique",
    "bl": "Saint-Barthélemy",
    "mf": "Saint-Martin",
}

# The simulator publishes every 10 s: five silent minutes is a real outage.
SIMULATED_OFFLINE_AFTER_S = 300
# Météo-France stations all measure every 6 minutes but deliver in two ways. Prompt ones
# are published about 8 minutes later and the bridge reads each slot 10 minutes after it:
# 30 minutes of silence is an outage. The others send the past hour in one batch, up to an
# hour late, which the bridge picks up on a second read 80 minutes after the slot.
OFFLINE_AFTER_S = {"prompt": 1800, "hourly_batch": 6000}


@dataclass(frozen=True)
class RegistryStation:
    station_id: str
    mf_id: str  # Météo-France station id: INSEE commune code + 3 digits
    name: str
    territory: str
    latitude: float
    longitude: float
    # Silence after which the station counts as offline (OFFLINE_AFTER_S).
    offline_after_s: int


@dataclass(frozen=True)
class SeededStation:
    """A row of the `stations` table."""

    station_id: str
    town: str
    territory: str
    latitude: float
    longitude: float
    offline_after_s: int


def load_registry(path: Path = REGISTRY_PATH) -> list[RegistryStation]:
    with path.open(encoding="utf-8", newline="") as f:
        return [
            RegistryStation(
                station_id=row["station_id"],
                mf_id=row["mf_id"],
                name=row["name"],
                territory=row["territory"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                offline_after_s=int(row["offline_after_s"]),
            )
            for row in csv.DictReader(f)
        ]


def seeded_stations(source: StationSource) -> list[SeededStation]:
    if source == "simulated":
        return [
            SeededStation(
                s.station_id,
                s.town,
                s.territory,
                s.latitude,
                s.longitude,
                SIMULATED_OFFLINE_AFTER_S,
            )
            for s in STATIONS
        ]
    if source == "meteofrance":
        return [
            SeededStation(
                s.station_id, s.name, s.territory, s.latitude, s.longitude, s.offline_after_s
            )
            for s in load_registry()
        ]
    raise ValueError(f"unknown STATION_SOURCE {source!r}: expected simulated or meteofrance")


def territory_prefix(mf_id: str) -> str:
    """Saint-Barthélemy (97123) and Saint-Martin (97127) are reported under department 971
    but are territories of their own."""
    if mf_id.startswith("97123"):
        return "bl"
    if mf_id.startswith("97127"):
        return "mf"
    if mf_id.startswith("971"):
        return "gp"
    if mf_id.startswith("972"):
        return "mq"
    raise ValueError(f"{mf_id} is not a station of the French Antilles")


_ARTICLES = {"LE", "LA", "LES"}
_SAINTS = {"ST", "STE", "SAINT", "SAINTE"}


def commune_letters(name: str) -> str:
    """Three letters for the commune that starts a Météo-France station name, e.g.
    "FORT-DE-FRANCE DESAIX" -> fdf, "LES ABYMES CHAZEAU_SAPC" -> aby,
    "ST-BARTHELEMY METEO" -> sba, "MORNE-A-L'EAU BLANCHET" -> mal."""
    words = [w.strip("-") for w in re.split(r"[\s_]+", name.upper()) if w.strip("-")]
    commune = words[0]
    # "SAINT FRANCOIS", "STE- LUCE", "LES ABYMES": the commune spans two words.
    if commune in _ARTICLES | _SAINTS and len(words) > 1:
        commune = f"{commune}-{words[1]}"
    parts = [p for p in (re.sub("[^A-Z]", "", p) for p in commune.split("-")) if p]
    if parts[0] in _ARTICLES and len(parts) > 1:
        parts = parts[1:]
    if parts[0] in _SAINTS and len(parts) > 1:
        letters = "s" + parts[1][:2]
    elif len(parts) >= 3:
        letters = "".join(p[0] for p in parts[:3])
    elif len(parts) == 2:
        letters = parts[0][0] + parts[1][:2]
    else:
        letters = parts[0][:3]
    if len(letters) != 3:
        raise ValueError(f"cannot derive three letters from {name!r}")
    return letters.lower()


def make_station_ids(stations: Iterable[tuple[str, str]]) -> dict[str, str]:
    """Map (Météo-France id, name) pairs to contract ids `<territory>-<commune>-NN`, the
    counter running in Météo-France id order so that the result does not depend on the
    input order."""
    counters: dict[str, int] = defaultdict(int)
    ids = {}
    for mf_id, name in sorted(stations):
        stem = f"{territory_prefix(mf_id)}-{commune_letters(name)}"
        counters[stem] += 1
        ids[mf_id] = f"{stem}-{counters[stem]:02d}"
    return ids
