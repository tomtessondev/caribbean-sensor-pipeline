from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sensor_pipeline.messages import Telemetry
from sensor_pipeline.simulator import DustEpisode, FaultRates, StationSimulator, dust_episodes
from sensor_pipeline.stations import STATIONS, station_by_id

START = datetime(2026, 11, 1, tzinfo=UTC)
NO_FAULTS = FaultRates(drop=0, stuck_start=0, spike=0)


def run(sim: StationSimulator, hours: float, step_s: float = 300) -> list[Telemetry | None]:
    steps = int(hours * 3600 / step_s)
    return [sim.reading(START + timedelta(seconds=i * step_s)) for i in range(steps)]


def readings(items: list[Telemetry | None]) -> list[Telemetry]:
    return [r for r in items if r is not None]


def test_same_seed_same_weather() -> None:
    station = station_by_id("gp-pap-01")
    first = run(StationSimulator(station, [], seed=1), hours=6)
    second = run(StationSimulator(station, [], seed=1), hours=6)
    other = run(StationSimulator(station, [], seed=2), hours=6)

    assert first == second
    assert first != other


def test_afternoons_are_warmer_than_nights() -> None:
    sim = StationSimulator(station_by_id("gp-pap-01"), [], seed=1, faults=NO_FAULTS)
    by_hour: dict[int, list[float]] = {}
    for r in readings(run(sim, hours=72)):
        local_hour = (r.ts - timedelta(hours=4)).hour
        assert r.temperature_c is not None
        by_hour.setdefault(local_hour, []).append(r.temperature_c)

    def mean(hour: int) -> float:
        return sum(by_hour[hour]) / len(by_hour[hour])

    assert mean(15) - mean(4) > 4


def test_dust_plume_raises_pm10_and_reaches_the_east_first() -> None:
    plume = [DustEpisode(START + timedelta(hours=12), hours=24, peak_pm10_ugm3=150)]
    east = StationSimulator(station_by_id("mq-san-01"), plume, seed=1, faults=NO_FAULTS)
    west = StationSimulator(station_by_id("gp-bte-01"), plume, seed=1, faults=NO_FAULTS)
    at = START + timedelta(hours=24)  # the plume peaks over the east coast

    calm = east.reading(START)
    east_now, west_now = east.reading(at), west.reading(at)

    assert calm is not None and east_now is not None and west_now is not None
    assert calm.pm10_ugm3 is not None and calm.pm10_ugm3 < 30
    assert east_now.pm10_ugm3 is not None and west_now.pm10_ugm3 is not None
    assert east_now.pm10_ugm3 > 60
    assert east_now.pm10_ugm3 > west_now.pm10_ugm3


def test_lost_messages_leave_gaps_in_the_sequence() -> None:
    sim = StationSimulator(STATIONS[0], [], seed=3, faults=FaultRates(drop=0.3))
    received = readings(run(sim, hours=10))
    seqs = [r.seq for r in received]

    assert seqs == sorted(seqs)
    assert len(received) < 120 and seqs[-1] >= 115  # ~30 % lost, seq still counts them


def test_stuck_sensor_repeats_its_last_value() -> None:
    sim = StationSimulator(STATIONS[0], [], seed=4, faults=FaultRates(drop=0, stuck_start=1))
    values = [r.pm10_ugm3 for r in readings(run(sim, hours=2))]

    assert len(set(values[1:20])) == 1


def test_dust_episodes_are_deterministic_and_plausible() -> None:
    episodes = dust_episodes(seed=7, start=START, days=60)

    assert episodes == dust_episodes(seed=7, start=START, days=60)
    assert 4 <= len(episodes) <= 15
    assert all(24 <= e.hours <= 96 and 40 <= e.peak_pm10_ugm3 <= 250 for e in episodes)


def test_every_simulated_reading_satisfies_the_contract() -> None:
    episodes = dust_episodes(seed=1, start=START, days=30)
    for station in STATIONS:
        sim = StationSimulator(station, episodes, seed=1, faults=FaultRates(spike=0.05))
        for r in readings(run(sim, hours=24 * 7, step_s=900)):
            # Round-trip through JSON as the ingestor will.
            assert Telemetry.model_validate_json(r.model_dump_json()) == r
