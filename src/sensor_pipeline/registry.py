"""The stations a deployment knows: the six simulated ones, or the real ones: Météo-France's
automatic weather stations (a versioned CSV built once by `scripts/build_registry.py`) and
the particle stations of Gwad'Air and Madininair (`data/air_quality_antilles.csv`, written
by hand). `STATION_SOURCE` picks the network; `migrate` seeds it into the `stations` table
and `provision-broker` creates one broker login per station."""

from __future__ import annotations

import csv
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from sensor_pipeline.stations import STATIONS

# simulated: the six simulated stations. meteofrance: the Météo-France weather stations.
# real: Météo-France plus the official air-quality stations (the deployed stack).
StationSource = Literal["simulated", "meteofrance", "real"]
# Where a station's readings come from (stations.source, served by /v1/stations).
Origin = Literal["simulated", "meteofrance", "airquality"]

REGISTRY_PATH = Path(__file__).parent / "data" / "meteofrance_antilles.csv"
AIR_QUALITY_PATH = Path(__file__).parent / "data" / "air_quality_antilles.csv"

TERRITORIES = {
    "gp": "Guadeloupe",
    "mq": "Martinique",
    "bl": "Saint-Barthélemy",
    "mf": "Saint-Martin",
}

# The simulator publishes every 10 s: five silent minutes is a real outage.
SIMULATED_OFFLINE_AFTER_S = 300
# Météo-France stations, by the cadence observed in their telemetry
# (scripts/classify_cadence.py). A station with readings between the hours measures every
# 6 minutes; whether it delivers them promptly or in an hourly batch, the bridge has
# published its newest reading at most about 90 minutes after it was taken (second read
# 80 minutes after the slot, plus a poll): 100 minutes of silence is an outage. A station
# seen only on the hour may be read on that second pass only, so its newest reading can be
# 140 minutes old; it gets 3 hours, as does a station never seen yet.
OFFLINE_AFTER_S = {"six_minutes": 6000, "hourly": 10800}
# The particle stations publish hourly means about 1.5 h after the hour, 2.5 h at worst:
# four hours of silence is an outage.
AIR_QUALITY_OFFLINE_AFTER_S = 14400
FIELDS = ("station_id", "mf_id", "name", "territory", "latitude", "longitude", "offline_after_s")


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
class AirQualityStation:
    """A particle station of the LCSQA / INERIS real-time files (Gwad'Air, Madininair)."""

    station_id: str
    site_code: str  # `code site` in the files: FR37xxx Gwad'Air, FR39xxx Madininair
    insee: str  # INSEE code of its commune, which gives the id's letters
    name: str
    territory: str
    latitude: float
    longitude: float
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
    source: Origin


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


def load_air_quality_registry(path: Path = AIR_QUALITY_PATH) -> list[AirQualityStation]:
    with path.open(encoding="utf-8", newline="") as f:
        return [
            AirQualityStation(
                station_id=row["station_id"],
                site_code=row["site_code"],
                insee=row["insee"],
                name=row["name"],
                territory=row["territory"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                offline_after_s=int(row["offline_after_s"]),
            )
            for row in csv.DictReader(f)
        ]


def save_registry(stations: Iterable[RegistryStation], path: Path = REGISTRY_PATH) -> None:
    """Write the registry sorted by station id, as `scripts/build_registry.py` built it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(FIELDS)
        for s in sorted(stations, key=lambda s: s.station_id):
            writer.writerow(
                [
                    s.station_id,
                    s.mf_id,
                    s.name,
                    s.territory,
                    f"{s.latitude:.6f}",
                    f"{s.longitude:.6f}",
                    s.offline_after_s,
                ]
            )


def with_observed_cadence(
    stations: Iterable[RegistryStation], sub_hourly: Mapping[str, bool]
) -> list[RegistryStation]:
    """The stations with the offline threshold of their observed cadence. `sub_hourly`
    maps a station id to whether any of its readings was taken off the hour; a station
    absent from it was never seen and keeps the cautious hourly threshold."""
    return [
        replace(
            s,
            offline_after_s=OFFLINE_AFTER_S[
                "six_minutes" if sub_hourly.get(s.station_id, False) else "hourly"
            ],
        )
        for s in stations
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
                "simulated",
            )
            for s in STATIONS
        ]
    if source not in ("meteofrance", "real"):
        raise ValueError(
            f"unknown STATION_SOURCE {source!r}: expected simulated, meteofrance or real"
        )
    seeded = [
        SeededStation(
            s.station_id,
            s.name,
            s.territory,
            s.latitude,
            s.longitude,
            s.offline_after_s,
            "meteofrance",
        )
        for s in load_registry()
    ]
    if source == "real":
        seeded += [
            SeededStation(
                s.station_id,
                s.name,
                s.territory,
                s.latitude,
                s.longitude,
                s.offline_after_s,
                "airquality",
            )
            for s in load_air_quality_registry()
        ]
    return seeded


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


# Three letters per commune, keyed by its INSEE code (the first five digits of a Météo-France
# station id). Station ids are permanent once deployed, so the letters are chosen by hand,
# never derived from station names (which spell the same commune several ways): one commune,
# one code, and no two communes share a code, across the four territories.
COMMUNES: dict[str, tuple[str, str]] = {
    "97101": ("aby", "Les Abymes"),
    "97102": ("abe", "Anse-Bertrand"),
    "97103": ("bma", "Baie-Mahault"),
    "97104": ("bai", "Baillif"),
    "97105": ("bte", "Basse-Terre"),
    "97106": ("bou", "Bouillante"),
    "97107": ("cbe", "Capesterre-Belle-Eau"),
    "97108": ("cmg", "Capesterre-de-Marie-Galante"),
    "97109": ("gou", "Gourbeyre"),
    "97110": ("des", "La Désirade"),
    "97111": ("dsh", "Deshaies"),
    "97112": ("gbo", "Grand-Bourg"),
    "97113": ("gos", "Le Gosier"),
    "97114": ("goy", "Goyave"),
    "97115": ("lmn", "Lamentin"),
    "97116": ("mal", "Morne-à-l'Eau"),
    "97117": ("mou", "Le Moule"),
    "97118": ("pbo", "Petit-Bourg"),
    "97119": ("pca", "Petit-Canal"),
    "97120": ("pap", "Pointe-à-Pitre"),
    "97121": ("pno", "Pointe-Noire"),
    "97122": ("plo", "Port-Louis"),
    "97123": ("sba", "Saint-Barthélemy"),
    "97124": ("scl", "Saint-Claude"),
    "97125": ("sfr", "Saint-François"),
    "97126": ("slo", "Saint-Louis"),
    "97127": ("sma", "Saint-Martin"),
    "97128": ("san", "Sainte-Anne (Guadeloupe)"),
    "97129": ("sro", "Sainte-Rose"),
    "97130": ("tdb", "Terre-de-Bas"),
    "97131": ("tdh", "Terre-de-Haut"),
    "97132": ("trv", "Trois-Rivières"),
    "97133": ("vfo", "Vieux-Fort"),
    "97134": ("vha", "Vieux-Habitants"),
    "97201": ("abo", "L'Ajoupa-Bouillon"),
    "97202": ("ans", "Les Anses-d'Arlet"),
    "97203": ("bpo", "Basse-Pointe"),
    "97204": ("car", "Le Carbet"),
    "97205": ("cpi", "Case-Pilote"),
    "97206": ("dia", "Le Diamant"),
    "97207": ("duc", "Ducos"),
    "97208": ("fsd", "Fonds-Saint-Denis"),
    "97209": ("fdf", "Fort-de-France"),
    "97210": ("fra", "Le François"),
    "97211": ("gri", "Grand'Rivière"),
    "97212": ("gmo", "Gros-Morne"),
    "97213": ("lam", "Le Lamentin"),
    "97214": ("lor", "Le Lorrain"),
    "97215": ("mac", "Macouba"),
    "97216": ("mgt", "Le Marigot"),
    "97217": ("mar", "Le Marin"),
    "97218": ("mro", "Le Morne-Rouge"),
    "97219": ("pre", "Le Prêcheur"),
    "97220": ("rpi", "Rivière-Pilote"),
    "97221": ("rsa", "Rivière-Salée"),
    "97222": ("rob", "Le Robert"),
    "97223": ("ses", "Saint-Esprit"),
    "97224": ("sjo", "Saint-Joseph"),
    "97225": ("spi", "Saint-Pierre"),
    "97226": ("sae", "Sainte-Anne (Martinique)"),
    "97227": ("slu", "Sainte-Luce"),
    "97228": ("smr", "Sainte-Marie"),
    "97229": ("sch", "Schœlcher"),
    "97230": ("tri", "La Trinité"),
    "97231": ("til", "Les Trois-Îlets"),
    "97232": ("vau", "Le Vauclin"),
    "97233": ("mve", "Le Morne-Vert"),
    "97234": ("bel", "Bellefontaine"),
}


def make_station_ids(mf_ids: Iterable[str]) -> dict[str, str]:
    """Map Météo-France ids to contract ids `<territory>-<commune letters>-NN`, the counter
    running in Météo-France id order so that the result does not depend on the input
    order. An id the simulator uses (mq-fdf-01...) is skipped: mixing the two sources must
    never merge two stations' histories."""
    return _assign_ids({mf_id: mf_id[:5] for mf_id in mf_ids}, {s.station_id for s in STATIONS})


def make_air_quality_ids(communes: Mapping[str, str]) -> dict[str, str]:
    """Map air-quality site codes to contract ids, from the INSEE code of each site's
    commune, after the ids of the simulator and of the Météo-France registry: the commune
    counters continue (Les Abymes has gp-aby-01 and -02 from Météo-France, its particle
    station is gp-aby-03)."""
    taken = {s.station_id for s in STATIONS} | {s.station_id for s in load_registry()}
    return _assign_ids(communes, taken)


def _assign_ids(communes: Mapping[str, str], taken: set[str]) -> dict[str, str]:
    """`communes` maps a source's station key to its INSEE commune code; the counter of
    each commune runs in key order and skips the ids in `taken`."""
    counters: dict[str, int] = defaultdict(int)
    ids = {}
    for key in sorted(communes):
        insee = communes[key]
        commune = COMMUNES.get(insee)
        if commune is None:
            raise ValueError(f"commune {insee} of station {key} is not in COMMUNES")
        stem = f"{territory_prefix(insee)}-{commune[0]}"
        station_id = stem
        while station_id == stem or station_id in taken:
            counters[stem] += 1
            station_id = f"{stem}-{counters[stem]:02d}"
        ids[key] = station_id
    return ids
