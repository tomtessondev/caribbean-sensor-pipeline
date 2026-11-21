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
import logging
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


def file_url(day: date) -> str:
    return f"{BASE_URL}/{day.year}/FR_E2_{day.isoformat()}.csv"


@dataclass(frozen=True)
class HourlyMean:
    site_code: str
    start: datetime  # start of the hour, UTC
    pollutant: Pollutant
    value: float  # µg/m³


@dataclass(frozen=True)
class DayFile:
    content: bytes
    last_modified: str | None


def parse_day_file(content: bytes) -> list[HourlyMean]:
    """The valid particle means of the Antilles networks in a day file. Valid means code
    qualité A, validité 1 and a value. A row that does not parse is logged and skipped;
    a file without the expected columns raises ValueError."""
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")), delimiter=";")
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
    out; a value outside the contract (a negative raw mean) drops that reading only."""
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
    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    def day_file(self, day: date, if_modified_since: str | None) -> DayFile | None:
        """The file of a local day, or None when it has not changed since
        `if_modified_since` (the Last-Modified of the previous read) or is not published
        yet. data.gouv.fr redirects to the storage that serves the file."""
        headers = {"If-Modified-Since": if_modified_since} if if_modified_since else {}
        response = self._http.get(file_url(day), headers=headers, follow_redirects=True)
        if response.status_code in (httpx.codes.NOT_MODIFIED, httpx.codes.NOT_FOUND):
            return None
        response.raise_for_status()
        return DayFile(response.content, response.headers.get("Last-Modified"))
