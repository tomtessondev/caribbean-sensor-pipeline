"""The registry of real Météo-France stations and the choice of station source."""

from __future__ import annotations

import re

import pytest

from sensor_pipeline.registry import (
    OFFLINE_AFTER_S,
    REGISTRY_PATH,
    TERRITORIES,
    load_registry,
    make_station_ids,
    seeded_stations,
)
from sensor_pipeline.stations import STATIONS

ID_PATTERN = re.compile(r"^[a-z]{2}-[a-z]{3}-\d{2}$")


def test_registry_ids_follow_the_contract() -> None:
    registry = load_registry(REGISTRY_PATH)

    assert len(registry) > 50
    ids = [s.station_id for s in registry]
    assert all(ID_PATTERN.match(i) for i in ids), ids
    assert len(set(ids)) == len(ids)
    mf_ids = [s.mf_id for s in registry]
    assert len(set(mf_ids)) == len(mf_ids)
    for station in registry:
        assert TERRITORIES[station.station_id[:2]] == station.territory, station
        assert station.mf_id[:3] in ("971", "972"), station
        assert station.offline_after_s in OFFLINE_AFTER_S.values(), station
        assert 14 < station.latitude < 19 and -64 < station.longitude < -60, station


def test_station_ids_come_from_the_commune_code_and_name() -> None:
    ids = make_station_ids(
        [
            ("97101014", "LES ABYMES CHAZEAU_SAPC"),
            ("97123001", "ST-BARTHELEMY METEO"),
            ("97127002", "ST-MARTIN MARIGOT GENDARMERIE"),
            ("97209002", "FORT-D-FRANCE-DONIS"),
            ("97209004", "FORT-DE-FRANCE DESAIX"),
            ("97116002", "MORNE-A-L'EAU BLANCHET"),
            ("97125011", "SAINT FRANCOIS AERODROME"),
            ("97125001", "ST-FRANCOIS RENEVILLE"),
            ("97227001", "STE- LUCE_CTM"),
            ("97206001", "DIAMANT - JACQUA"),
        ]
    )

    assert ids == {
        "97101014": "gp-aby-01",
        "97123001": "bl-sba-01",
        "97127002": "mf-sma-01",
        "97209002": "mq-fdf-01",
        "97209004": "mq-fdf-02",
        "97116002": "gp-mal-01",
        # Counted in Météo-France id order, whatever the input order.
        "97125001": "gp-sfr-01",
        "97125011": "gp-sfr-02",
        "97227001": "mq-slu-01",
        "97206001": "mq-dia-01",
    }


def test_simulated_source_keeps_the_six_stations_and_their_threshold() -> None:
    seeded = seeded_stations("simulated")

    assert [s.station_id for s in seeded] == [s.station_id for s in STATIONS]
    assert {s.offline_after_s for s in seeded} == {300}


def test_meteofrance_source_seeds_the_registry_only() -> None:
    seeded = seeded_stations("meteofrance")

    assert [s.station_id for s in seeded] == [s.station_id for s in load_registry(REGISTRY_PATH)]
    # Some ids exist in both networks (mq-fdf-01 is Fort-de-France either way): what
    # matters is that no simulated station is registered.
    assert not {s.town for s in STATIONS} & {s.town for s in seeded}


def test_unknown_source_is_refused() -> None:
    with pytest.raises(ValueError, match="STATION_SOURCE"):
        seeded_stations("aqicn")  # type: ignore[arg-type]  # deliberately outside the Literal
