"""The LCSQA / INERIS real-time files: parsing a trimmed real file and fetching with
If-Modified-Since. HTTP is mocked: tests never call data.gouv.fr."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
import respx

from sensor_pipeline.airquality import (
    AirQualityClient,
    HourlyMean,
    file_url,
    hourly_readings,
    parse_day_file,
)
from sensor_pipeline.registry import load_air_quality_registry

FIXTURE = Path(__file__).parent / "fixtures" / "airquality" / "FR_E2_2026-11-20.csv"
CONTENT = FIXTURE.read_bytes()
HEADER = CONTENT.decode("utf-8-sig").splitlines()[0]
STATIONS = {s.site_code: s for s in load_air_quality_registry()}


def row(
    *,
    start: str = "2026/11/20 21:00:00",
    organism: str = "GWAD'AIR",
    site: str = "FR37006",
    pollutant: str = "PM10",
    value: str = "12.5",
    unit: str = "µg/m3",
    quality: str = "A",
    validity: str = "1",
) -> str:
    return (
        f'"{start}";"2026/11/20 22:00:00";"{organism}";"FR01ZAG01";"ZAG";"{site}";"name";'
        f'"Urbaine";"{pollutant}";"Fond";"2";"Oui";"mesures fixes";"Auto";'
        f'"moyenne horaire brute";"{value}";"{value}";"{unit}";;;;"{quality}";"{validity}"'
    )


def csv_file(*rows: str, bom: bool = True) -> bytes:
    text = "\n".join([HEADER, *rows]) + "\n"
    return text.encode("utf-8-sig" if bom else "utf-8")


def test_url_of_a_day() -> None:
    assert file_url(date(2026, 11, 20)) == (
        "https://files.data.gouv.fr/ineris/lcsqa/"
        "concentrations-de-polluants-atmospheriques-reglementes/temps-reel/2026/"
        "FR_E2_2026-11-20.csv"
    )


def test_real_file_keeps_the_valid_antilles_particles_only() -> None:
    means = parse_day_file(CONTENT)

    # 12 PM10 and 8 PM2.5 series, three hours each. Not kept: the other organisms, the
    # Antilles NO2 row, the N / -1 row without a value.
    assert len(means) == 60
    assert {m.site_code[:4] for m in means} == {"FR37", "FR39"}
    assert {m.pollutant for m in means} == {"PM10", "PM2.5"}
    assert {m.site_code for m in means} == set(STATIONS)


def test_local_time_becomes_utc() -> None:
    """The files give the territory's local time (UTC-4): 21:00 in Pointe-à-Pitre on the
    20th is 01:00 UTC on the 21st."""
    means = parse_day_file(CONTENT)

    victoire = [m for m in means if m.site_code == "FR37006" and m.pollutant == "PM2.5"]
    assert victoire[-1] == HourlyMean(
        "FR37006", datetime(2026, 11, 21, 1, tzinfo=UTC), "PM2.5", 6.8
    )
    assert {m.start.hour for m in means} == {23, 0, 1}  # 19:00, 20:00, 21:00 local


def test_martinique_rows_use_martinique_time() -> None:
    means = parse_day_file(csv_file(row(organism="MADININAIR", site="FR39008")))

    assert [m.start for m in means] == [datetime(2026, 11, 21, 1, tzinfo=UTC)]


@pytest.mark.parametrize("unit", ["µg-m3", "µg/m3"])
def test_both_spellings_of_the_unit(unit: str) -> None:
    assert [m.value for m in parse_day_file(csv_file(row(unit=unit)))] == [12.5]


def test_other_unit_is_skipped_and_logged(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="sensor_pipeline.airquality"):
        means = parse_day_file(csv_file(row(unit="mg-m3"), row(site="FR37010")))

    assert [m.site_code for m in means] == ["FR37010"]
    assert "mg-m3" in caplog.text


@pytest.mark.parametrize(
    "fields",
    [
        {"quality": "N", "validity": "-1", "value": ""},
        {"quality": "R", "validity": "1"},
        {"quality": "A", "validity": "4"},
        {"value": ""},
    ],
)
def test_invalid_rows_are_skipped(fields: dict[str, str]) -> None:
    assert parse_day_file(csv_file(row(**fields))) == []


def test_file_without_bom_parses_too() -> None:
    assert len(parse_day_file(csv_file(row(), bom=False))) == 1


def test_broken_row_does_not_hide_the_others(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="sensor_pipeline.airquality"):
        means = parse_day_file(
            csv_file(row(value="n/a"), row(start="20/11/2026"), row(site="FR37010"))
        )

    assert [m.site_code for m in means] == ["FR37010"]
    assert caplog.text.count("skipped") == 2


def test_file_without_the_expected_columns_is_refused() -> None:
    with pytest.raises(ValueError, match="valeur"):
        parse_day_file('"Date de début";"Organisme"\n"2026/11/20 21:00:00";"GWAD\'AIR"\n'.encode())


def test_pm10_and_pm25_of_an_hour_make_one_reading() -> None:
    hour = datetime(2026, 11, 21, 1, tzinfo=UTC)
    station = STATIONS["FR37006"]

    readings = hourly_readings(
        [
            HourlyMean("FR37006", hour, "PM10", 14.0),
            HourlyMean("FR37006", hour, "PM2.5", 6.8),
            HourlyMean("FR37001", hour, "PM10", 20.0),
        ],
        STATIONS,
    )

    by_station = {r.station_id: r for r in readings}
    victoire = by_station[station.station_id]
    assert (victoire.pm10_ugm3, victoire.pm2_5_ugm3) == (14.0, 6.8)
    assert victoire.ts == hour
    assert victoire.seq == int(hour.timestamp()) // 3600
    assert victoire.temperature_c is None and victoire.battery_v is None
    assert by_station[STATIONS["FR37001"].station_id].pm2_5_ugm3 is None


def test_real_file_gives_one_reading_per_station_and_hour() -> None:
    readings = hourly_readings(parse_day_file(CONTENT), STATIONS)

    assert len(readings) == 36  # 12 stations, 3 hours
    assert sum(r.pm2_5_ugm3 is not None for r in readings) == 24


@respx.mock
def test_client_sends_the_last_modified_date_back() -> None:
    url = file_url(date(2026, 11, 20))
    stamp = "Sat, 21 Nov 2026 03:30:27 GMT"
    route = respx.get(url).mock(
        side_effect=[
            httpx.Response(200, content=CONTENT, headers={"Last-Modified": stamp}),
            httpx.Response(304),
        ]
    )
    client = AirQualityClient(httpx.Client())

    first = client.day_file(date(2026, 11, 20), None)
    second = client.day_file(date(2026, 11, 20), stamp)

    assert first is not None and first.content == CONTENT and first.last_modified == stamp
    assert second is None
    assert "if-modified-since" not in route.calls[0].request.headers
    assert route.calls[1].request.headers["if-modified-since"] == stamp


@respx.mock
def test_client_follows_the_redirect_to_the_storage() -> None:
    storage = "https://storage.example/FR_E2_2026-11-20.csv"
    respx.get(file_url(date(2026, 11, 20))).mock(
        return_value=httpx.Response(302, headers={"Location": storage})
    )
    respx.get(storage).mock(return_value=httpx.Response(200, content=CONTENT))

    day = AirQualityClient(httpx.Client()).day_file(date(2026, 11, 20), None)

    assert day is not None and day.content == CONTENT and day.last_modified is None


@respx.mock
def test_file_not_published_yet_is_not_an_error() -> None:
    respx.get(file_url(date(2026, 11, 21))).mock(return_value=httpx.Response(404))

    assert AirQualityClient(httpx.Client()).day_file(date(2026, 11, 21), None) is None


@respx.mock
def test_server_error_raises() -> None:
    respx.get(file_url(date(2026, 11, 21))).mock(return_value=httpx.Response(503))

    with pytest.raises(httpx.HTTPStatusError):
        AirQualityClient(httpx.Client()).day_file(date(2026, 11, 21), None)


def test_value_outside_the_contract_drops_that_reading_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    hour = datetime(2026, 11, 21, 1, tzinfo=UTC)
    with caplog.at_level(logging.WARNING, logger="sensor_pipeline.airquality"):
        readings = hourly_readings(
            [HourlyMean("FR37006", hour, "PM10", -0.4), HourlyMean("FR37001", hour, "PM10", 9)],
            STATIONS,
        )

    assert [r.station_id for r in readings] == [STATIONS["FR37001"].station_id]
    assert STATIONS["FR37006"].station_id in caplog.text
