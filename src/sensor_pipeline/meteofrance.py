"""Météo-France's observation API (Package Observations v2) and the conversion of its
readings to the telemetry contract.

Authentication is OAuth2 client credentials: the application id from the portal is sent
as a Basic header to get a bearer token valid one hour. The token is cached until a minute
before it expires; a 401 (token revoked or expired early) gets one new token and one retry.
Neither the application id nor the token is ever logged.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from sensor_pipeline.messages import Telemetry
from sensor_pipeline.registry import RegistryStation

logger = logging.getLogger(__name__)

TOKEN_URL = "https://portail-api.meteofrance.fr/token"
# Every station of France for one 6-minute slot (`date` required). The department package
# (v1, `id-departement`) is not part of the subscription.
PACKAGE_URL = (
    "https://public-api.meteofrance.fr/public/DPPaquetObs/v2/paquet/stations/infrahoraire-6m"
)
# Automatic stations measure every 6 minutes, on the clock (hh:00, hh:06, ...).
SLOT_S = 360
DEPARTMENTS = ("971", "972")
TOKEN_MARGIN_S = 60


class Observation(BaseModel):
    """One 6-minute reading of one station, as the package returns it. Only the fields the
    contract uses are kept: temperature (K), relative humidity (%), station and sea-level
    pressure (Pa)."""

    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    station: str = Field(alias="geo_id_insee")
    validity_time: AwareDatetime
    t: float | None = None
    u: float | None = None
    pres: float | None = None
    pmer: float | None = None


class _Token(BaseModel):
    access_token: str
    expires_in: int = 3600


def to_telemetry(obs: Observation, station: RegistryStation) -> Telemetry | None:
    """The reading in the contract, or None when the station measured nothing usable.
    Pressure is the sea-level value: the raw station pressure of a mountain station
    (1141 m at Saint-Claude) would look like a broken sensor next to the coast ones."""
    if obs.t is None and obs.u is None and obs.pmer is None:
        return None
    return Telemetry(
        station_id=station.station_id,
        ts=obs.validity_time,
        seq=int(obs.validity_time.timestamp()) // SLOT_S,
        temperature_c=round(obs.t - 273.15, 2) if obs.t is not None else None,
        humidity_pct=obs.u,
        pressure_hpa=round(obs.pmer / 100, 2) if obs.pmer is not None else None,
    )


class MeteoFranceClient:
    def __init__(self, http: httpx.Client, application_id: str, clock: Callable[[], float]) -> None:
        self._http = http
        self._application_id = application_id
        self._clock = clock
        self._token: str | None = None
        self._expires_at = 0.0

    def package(self, at: datetime) -> list[Observation]:
        """The readings of every Antilles station for the 6-minute slot `at` (UTC). An
        observation that does not parse is logged and skipped: one broken station must not
        hide the others."""
        observations = []
        for item in self.raw_package(at):
            if str(item.get("geo_id_insee", ""))[:3] not in DEPARTMENTS:
                continue
            try:
                observations.append(Observation.model_validate(item))
            except ValidationError as exc:
                logger.warning(
                    "Observation of %s at %s skipped: %s", item.get("geo_id_insee"), at, exc
                )
        return observations

    def raw_package(self, at: datetime) -> list[dict[str, Any]]:
        """The package as returned: every station of France, every field. Raises
        ValueError when the body is not a list of observations (or not JSON)."""
        response = self._get(
            PACKAGE_URL, {"date": at.strftime("%Y-%m-%dT%H:%M:%SZ"), "format": "json"}
        )
        items: object = response.json()
        if not isinstance(items, list) or not all(isinstance(i, dict) for i in items):
            raise ValueError(f"unexpected package shape for {at}: {str(items)[:200]}")
        return items

    def _get(self, url: str, params: dict[str, str]) -> httpx.Response:
        response = self._http.get(url, params=params, headers=self._auth())
        if response.status_code == httpx.codes.UNAUTHORIZED:
            logger.info("Météo-France refused the token, fetching a new one")
            self._token = None
            response = self._http.get(url, params=params, headers=self._auth())
        return response.raise_for_status()

    def _auth(self) -> dict[str, str]:
        if self._token is None or self._clock() >= self._expires_at:
            response = self._http.post(
                TOKEN_URL,
                data={"grant_type": "client_credentials"},
                headers={"Authorization": f"Basic {self._application_id}"},
            ).raise_for_status()
            token = _Token.model_validate(response.json())
            self._token = token.access_token
            self._expires_at = self._clock() + token.expires_in - TOKEN_MARGIN_S
        return {"Authorization": f"Bearer {self._token}"}
