"""Météo-France's observation API (DPObs / Package Observations v2)."""

from __future__ import annotations

TOKEN_URL = "https://portail-api.meteofrance.fr/token"
# Every station of France for one 6-minute slot (`date` required). The department package
# (v1, `id-departement`) is not part of the subscription.
PACKAGE_URL = (
    "https://public-api.meteofrance.fr/public/DPPaquetObs/v2/paquet/stations/infrahoraire-6m"
)
# Automatic stations measure every 6 minutes, on the clock (hh:00, hh:06, ...).
SLOT_S = 360
