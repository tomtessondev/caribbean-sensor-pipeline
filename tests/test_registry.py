"""The registry of real Météo-France stations and the choice of station source."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from sensor_pipeline.registry import (
    AIR_QUALITY_OFFLINE_AFTER_S,
    AIR_QUALITY_PATH,
    COMMUNES,
    OFFLINE_AFTER_S,
    REGISTRY_PATH,
    TERRITORIES,
    load_air_quality_registry,
    load_registry,
    make_air_quality_ids,
    make_station_ids,
    save_registry,
    seeded_stations,
    with_observed_cadence,
)
from sensor_pipeline.stations import STATIONS

ID_PATTERN = re.compile(r"^[a-z]{2}-[a-z]{3}-\d{2}$")
SIMULATED_IDS = {s.station_id for s in STATIONS}


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


def test_station_ids_come_from_the_commune_code() -> None:
    ids = make_station_ids(
        ["97101014", "97101015", "97123001", "97127002", "97125011", "97125001", "97206001"]
    )

    assert ids == {
        # Le Raizet is in Les Abymes: same commune, same letters.
        "97101014": "gp-aby-01",
        "97101015": "gp-aby-02",
        "97123001": "bl-sba-01",
        "97127002": "mf-sma-01",
        # Counted in Météo-France id order, whatever the input order.
        "97125001": "gp-sfr-01",
        "97125011": "gp-sfr-02",
        "97206001": "mq-dia-01",
    }


def test_an_id_of_the_simulator_is_skipped() -> None:
    """mq-fdf-01 is the simulated Fort-de-France station: a real one must never take it,
    or switching sources would merge two histories."""
    ids = make_station_ids(["97209002", "97209004", "97213004"])

    assert ids == {"97209002": "mq-fdf-02", "97209004": "mq-fdf-03", "97213004": "mq-lam-02"}


def test_each_commune_has_its_own_letters() -> None:
    letters = [code for code, _name in COMMUNES.values()]

    assert len(set(letters)) == len(letters)
    assert all(re.fullmatch("[a-z]{3}", code) for code in letters)


def test_registry_ids_are_unambiguous() -> None:
    registry = load_registry(REGISTRY_PATH)
    letters_of_commune: dict[str, set[str]] = {}
    communes_of_letters: dict[str, set[str]] = {}
    for s in registry:
        letters = s.station_id.split("-")[1]
        letters_of_commune.setdefault(s.mf_id[:5], set()).add(letters)
        communes_of_letters.setdefault(letters, set()).add(s.mf_id[:5])

    assert all(len(v) == 1 for v in letters_of_commune.values()), letters_of_commune
    assert all(len(v) == 1 for v in communes_of_letters.values()), communes_of_letters
    assert not SIMULATED_IDS & {s.station_id for s in registry}
    # The committed ids are the ones make_station_ids gives.
    assert make_station_ids(s.mf_id for s in registry) == {s.mf_id: s.station_id for s in registry}


def test_commune_outside_the_table_is_refused() -> None:
    with pytest.raises(ValueError, match="97199"):
        make_station_ids(["97199001"])


def test_simulated_source_keeps_the_six_stations_and_their_threshold() -> None:
    seeded = seeded_stations("simulated")

    assert [s.station_id for s in seeded] == [s.station_id for s in STATIONS]
    assert {s.offline_after_s for s in seeded} == {300}


def test_meteofrance_source_seeds_the_registry_only() -> None:
    seeded = seeded_stations("meteofrance")

    assert [s.station_id for s in seeded] == [s.station_id for s in load_registry(REGISTRY_PATH)]
    # Neither a simulated town nor a simulated id.
    assert not {s.town for s in STATIONS} & {s.town for s in seeded}
    assert not SIMULATED_IDS & {s.station_id for s in seeded}


def test_unknown_source_is_refused() -> None:
    with pytest.raises(ValueError, match="STATION_SOURCE"):
        seeded_stations("aqicn")  # type: ignore[arg-type]  # deliberately outside the Literal


def test_threshold_follows_the_observed_cadence() -> None:
    registry = load_registry(REGISTRY_PATH)[:3]
    every_6_min, on_the_hour, never_seen = (s.station_id for s in registry)

    classified = with_observed_cadence(registry, {every_6_min: True, on_the_hour: False})

    assert [s.offline_after_s for s in classified] == [6000, 10800, 10800]
    assert [s.station_id for s in classified] == [every_6_min, on_the_hour, never_seen]


def test_saving_the_registry_rewrites_it_identically(tmp_path: Path) -> None:
    copy = tmp_path / "registry.csv"

    save_registry(load_registry(REGISTRY_PATH), copy)

    assert copy.read_text(encoding="utf-8") == REGISTRY_PATH.read_text(encoding="utf-8")


AIR_QUALITY = load_air_quality_registry(AIR_QUALITY_PATH)
METEOFRANCE_IDS = {s.station_id for s in load_registry(REGISTRY_PATH)}


def test_air_quality_registry_holds_the_twelve_particle_stations() -> None:
    assert {s.site_code for s in AIR_QUALITY} == {
        *(f"FR370{n}" for n in ("01", "06", "10", "40", "41", "95")),
        *(f"FR390{n}" for n in ("08", "09", "11", "14", "15", "16")),
    }
    ids = [s.station_id for s in AIR_QUALITY]
    assert all(ID_PATTERN.match(i) for i in ids), ids
    assert len(set(ids)) == len(ids)
    for station in AIR_QUALITY:
        assert TERRITORIES[station.station_id[:2]] == station.territory, station
        assert station.offline_after_s == AIR_QUALITY_OFFLINE_AFTER_S
        assert 14 < station.latitude < 19 and -64 < station.longitude < -60, station


def test_air_quality_ids_never_collide_with_another_source() -> None:
    ids = {s.station_id for s in AIR_QUALITY}

    assert not ids & SIMULATED_IDS
    assert not ids & METEOFRANCE_IDS
    # The committed ids are the ones make_air_quality_ids gives.
    assert make_air_quality_ids({s.site_code: s.insee for s in AIR_QUALITY}) == {
        s.site_code: s.station_id for s in AIR_QUALITY
    }


def test_air_quality_ids_continue_the_commune_counters() -> None:
    """Pointe-à-Pitre's 01 is the simulator's, Les Abymes' 01 and 02 are Météo-France's."""
    ids = make_air_quality_ids({"FR37006": "97120", "FR37040": "97101", "FR39008": "97229"})

    assert ids == {"FR37006": "gp-pap-02", "FR37040": "gp-aby-03", "FR39008": "mq-sch-01"}


def test_real_source_seeds_both_registries() -> None:
    seeded = seeded_stations("real")

    assert [s.station_id for s in seeded] == [
        *(s.station_id for s in load_registry(REGISTRY_PATH)),
        *(s.station_id for s in AIR_QUALITY),
    ]
    by_id = {s.station_id: s for s in seeded}
    assert {by_id[i].source for i in METEOFRANCE_IDS} == {"meteofrance"}
    assert {by_id[s.station_id].source for s in AIR_QUALITY} == {"airquality"}
    assert {s.source for s in seeded_stations("simulated")} == {"simulated"}
    assert {s.source for s in seeded_stations("meteofrance")} == {"meteofrance"}
