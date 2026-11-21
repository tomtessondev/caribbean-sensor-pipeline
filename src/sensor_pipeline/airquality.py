"""The official particle measurements of Gwad'Air (Guadeloupe, Saint-Martin) and Madininair
(Martinique), from the real-time files LCSQA / INERIS publish on data.gouv.fr (no key,
Licence Ouverte 2.0), and their conversion to the telemetry contract.

One CSV per local day for all of France (about 12 MB), rewritten every hour; the previous
days are rewritten too while late values arrive. Values are raw hourly means, not yet
validated by the networks. The timestamps are the territory's local time, whatever the
companion XML says: 21:00 in Pointe-à-Pitre is 01:00 UTC the next day.
"""

from __future__ import annotations

import csv
import io
import itertools
import logging
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal
from zoneinfo import ZoneInfo

import httpx
from pydantic import ValidationError

from sensor_pipeline.messages import Telemetry
from sensor_pipeline.registry import AirQualityStation

logger = logging.getLogger(__name__)

BASE_URL = (
    "https://files.data.gouv.fr/ineris/lcsqa/"
    "concentrations-de-polluants-atmospheriques-reglementes/temps-reel"
)
# The networks of the Antilles and the time zone their timestamps are in (UTC-4 all year).
ORGANISMS = {
    "GWAD'AIR": ZoneInfo("America/Guadeloupe"),
    "MADININAIR": ZoneInfo("America/Martinique"),
}
ORGANISM_BYTES = tuple(f'"{o}"'.encode() for o in ORGANISMS)
# The local day of the files read: both territories share it.
LOCAL_TIME = ZoneInfo("America/Guadeloupe")
Pollutant = Literal["PM10", "PM2.5"]
POLLUTANTS: tuple[Pollutant, ...] = ("PM10", "PM2.5")
UNITS = ("µg-m3", "µg/m3")  # both spellings occur, sometimes in the same network
TIME_FORMAT = "%Y/%m/%d %H:%M:%S"
COLUMNS = (
    "Date de début",
    "Organisme",
    "code site",
    "Polluant",
    "valeur",
    "unité de mesure",
    "code qualité",
    "validité",
)
HOUR_S = 3600
# The whole download, redirect included: httpx's own timeout bounds each read only, so a
# body that keeps trickling would hold the poller for ever. 12 MB takes a few seconds.
DOWNLOAD_TIMEOUT_S = 60.0


def file_url(day: date) -> str:
    return f"{BASE_URL}/{day.year}/FR_E2_{day.isoformat()}.csv"


@dataclass(frozen=True)
class HourlyMean:
    site_code: str
    start: datetime  # start of the hour, UTC
    pollutant: Pollutant
    value: float  # µg/m³


@dataclass(frozen=True)
class Version:
    """What the storage said of a file it served: sent back on the next request, it answers
    304 Not Modified, with no body, when the file has not been rewritten since."""

    last_modified: str | None = None
    etag: str | None = None


@dataclass(frozen=True)
class DayFile:
    content: bytes
    version: Version


def parse_day_file(content: bytes) -> list[HourlyMean]:
    """The valid particle means of the Antilles networks in a day file. Valid means code
    qualité A, validité 1 and a value. A row that does not parse is logged and skipped;
    a file without the expected columns raises ValueError."""
    lines = io.BytesIO(content)
    header = lines.readline().decode("utf-8-sig")
    # Only the Antilles rows are decoded: the whole file as text would take several times
    # its 12 MB, too much for the bridge's memory limit.
    antilles = (line.decode("utf-8") for line in lines if any(o in line for o in ORGANISM_BYTES))
    reader = csv.DictReader(itertools.chain([header], antilles), delimiter=";")
    missing = [c for c in COLUMNS if c not in (reader.fieldnames or ())]
    if missing:
        raise ValueError(f"air-quality file without the columns {missing}")
    means = []
    try:
        for row in reader:
            zone = ORGANISMS.get(row["Organisme"])
            pollutant = row["Polluant"]
            if zone is None or pollutant not in POLLUTANTS:
                continue
            if row["code qualité"] != "A" or row["validité"] != "1" or not row["valeur"]:
                continue
            if row["unité de mesure"] not in UNITS:
                logger.warning(
                    "%s %s at %s skipped: unit %r",
                    row["code site"],
                    pollutant,
                    row["Date de début"],
                    row["unité de mesure"],
                )
                continue
            try:
                local = datetime.strptime(row["Date de début"], TIME_FORMAT)
                value = float(row["valeur"])
            except ValueError as exc:
                logger.warning("%s %s skipped: %s", row["code site"], pollutant, exc)
                continue
            means.append(
                HourlyMean(
                    row["code site"],
                    local.replace(tzinfo=zone).astimezone(UTC),
                    "PM10" if pollutant == "PM10" else "PM2.5",
                    value,
                )
            )
    except csv.Error as exc:
        raise ValueError(f"air-quality file does not parse: {exc}") from exc
    return means


def hourly_readings(
    means: Iterable[HourlyMean], stations: Mapping[str, AirQualityStation]
) -> list[Telemetry]:
    """One reading per station and hour, PM10 and PM2.5 together, the other contract
    fields empty; `seq` counts hours since the epoch. Sites not in `stations` are left
    out. A value outside the contract's bounds (a negative raw mean, or above the
    1000 µg/m³ the contract takes from the PMS5003 range) drops the whole reading of that
    station and hour, its other pollutant too, with a warning; the other readings go on."""
    hours: dict[tuple[str, datetime], dict[Pollutant, float]] = defaultdict(dict)
    for mean in means:
        if mean.site_code in stations:
            hours[(mean.site_code, mean.start)][mean.pollutant] = mean.value
    readings = []
    for (site_code, start), values in hours.items():
        station = stations[site_code]
        try:
            readings.append(
                Telemetry(
                    station_id=station.station_id,
                    ts=start,
                    seq=int(start.timestamp()) // HOUR_S,
                    pm10_ugm3=values.get("PM10"),
                    pm2_5_ugm3=values.get("PM2.5"),
                )
            )
        except ValidationError as exc:
            logger.warning("Reading of %s outside the contract: %s", station.station_id, exc)
    return readings


class AirQualityClient:
    def __init__(self, http: httpx.Client, total_timeout_s: float = DOWNLOAD_TIMEOUT_S) -> None:
        self._http = http
        self._total_timeout_s = total_timeout_s

    def day_file(self, day: date, known: Version | None) -> DayFile | None:
        """The file of a local day, or None when it is the `known` version (the one read
        last) or is not published yet. data.gouv.fr redirects to the storage that serves
        the file; the conditional headers go along and the storage answers them."""
        headers = {}
        if known is not None and known.last_modified is not None:
            headers["If-Modified-Since"] = known.last_modified
        if known is not None and known.etag is not None:
            headers["If-None-Match"] = known.etag
        deadline = time.monotonic() + self._total_timeout_s
        with self._http.stream(
            "GET", file_url(day), headers=headers, follow_redirects=True
        ) as response:
            if response.status_code in (httpx.codes.NOT_MODIFIED, httpx.codes.NOT_FOUND):
                return None
            response.raise_for_status()
            content = bytearray()
            for chunk in response.iter_bytes():
                if time.monotonic() > deadline:
                    raise httpx.ReadTimeout(
                        f"download longer than {self._total_timeout_s} s",
                        request=response.request,
                    )
                content += chunk
        return DayFile(
            bytes(content),
            Version(response.headers.get("Last-Modified"), response.headers.get("ETag")),
        )
