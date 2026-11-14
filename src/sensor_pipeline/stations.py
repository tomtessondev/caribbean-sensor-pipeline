"""The simulated station network: one station per town of the antilles-data-pipeline project."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Station:
    station_id: str
    town: str
    territory: str
    latitude: float
    longitude: float
    # Simulation parameters: mean temperature, background PM10, radio link quality.
    base_temperature_c: float
    background_pm10_ugm3: float
    base_rssi_dbm: float


STATIONS: tuple[Station, ...] = (
    Station("gp-pap-01", "Pointe-à-Pitre", "Guadeloupe", 16.2411, -61.5331, 27.5, 18.0, -67.0),
    Station("gp-bte-01", "Basse-Terre", "Guadeloupe", 15.9985, -61.7255, 26.0, 14.0, -74.0),
    Station("gp-mou-01", "Le Moule", "Guadeloupe", 16.3333, -61.3443, 27.0, 16.0, -81.0),
    Station("mq-fdf-01", "Fort-de-France", "Martinique", 14.6161, -61.0588, 27.8, 19.0, -63.0),
    Station("mq-lam-01", "Le Lamentin", "Martinique", 14.6133, -60.9997, 27.6, 20.0, -70.0),
    Station("mq-san-01", "Sainte-Anne", "Martinique", 14.4400, -60.8817, 27.2, 15.0, -78.0),
)


def station_by_id(station_id: str) -> Station:
    for station in STATIONS:
        if station.station_id == station_id:
            return station
    raise KeyError(station_id)
