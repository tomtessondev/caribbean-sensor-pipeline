"""Simulated sensor stations: plausible physics, real-world faults, deterministic per seed.

Each station behaves like a BME280 (temperature, humidity, pressure) plus a PMS5003
particle counter on a solar-charged battery:

- temperature follows the day (minimum before dawn, maximum mid-afternoon) plus AR(1)
  noise; humidity moves the opposite way;
- pressure shows the semi-diurnal atmospheric tide (two highs a day, ~1 hPa in the
  tropics) on a slow random walk;
- PM10 sits on a local background until a Saharan dust plume crosses the islands; plumes
  are shared by every station and reach the eastern coasts first;
- the battery charges in daylight and drains at night.

Faults the ingestor and the dashboards must survive: lost messages (the sequence number
still advances), a particle sensor stuck on its last value, isolated spikes.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from sensor_pipeline.messages import Telemetry
from sensor_pipeline.stations import Station


@dataclass(frozen=True)
class DustEpisode:
    start: datetime
    hours: float
    peak_pm10_ugm3: float

    def load(self, ts: datetime) -> float:
        """Plume concentration at `ts`: a smooth rise and fall over the episode."""
        elapsed = (ts - self.start).total_seconds() / 3600
        if not 0 <= elapsed <= self.hours:
            return 0.0
        return self.peak_pm10_ugm3 * math.sin(math.pi * elapsed / self.hours) ** 2


# Plumes are drawn from this date on, so every run of a given seed sees the same ones.
DUST_EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


def dust_episodes(seed: int, start: datetime, days: int) -> list[DustEpisode]:
    """Roughly one plume a week, lasting one to four days, 40 to 250 µg/m³ at its peak."""
    rng = random.Random(f"dust-{seed}")
    episodes: list[DustEpisode] = []
    cursor = start
    end = start + timedelta(days=days)
    while cursor < end:
        cursor += timedelta(hours=rng.uniform(72, 216))
        hours = rng.uniform(24, 96)
        episodes.append(DustEpisode(cursor, hours, rng.uniform(40, 250)))
        cursor += timedelta(hours=hours)
    return episodes


@dataclass(frozen=True)
class FaultRates:
    """Per-reading probabilities. At one reading per 10 s: about 1 % of messages lost, a
    particle sensor hanging for 5 to 30 minutes about every 14 hours, and a glitch about
    every 6 hours. (A 0.2 % hang rate had the sensors stuck 20 % of the time, which the
    data-quality rules duly reported.)"""

    drop: float = 0.01
    stuck_start: float = 0.0002
    spike: float = 0.0005


# Plumes travel westward: one hour of delay per tenth of a degree west of Sainte-Anne.
_EASTERNMOST_LONGITUDE = -60.88


@dataclass
class StationSimulator:
    station: Station
    episodes: list[DustEpisode]
    seed: int
    faults: FaultRates = field(default_factory=FaultRates)
    # Readings fall on a fixed time grid and are numbered by their slot on it. A restarted
    # simulator therefore republishes the very same (station, ts, seq) triples, which the
    # database ignores, instead of a second sequence interleaved with the first.
    interval_s: float = 10.0
    _rng: random.Random = field(init=False)
    _temperature_noise: float = field(init=False, default=0.0)
    _pressure_drift: float = field(init=False, default=0.0)
    _stuck_for: int = field(init=False, default=0)
    _last_pm: tuple[float, float] = field(init=False, default=(0.0, 0.0))

    def __post_init__(self) -> None:
        self._rng = random.Random(f"{self.seed}-{self.station.station_id}")

    def _dust(self, ts: datetime) -> float:
        lag = timedelta(hours=(_EASTERNMOST_LONGITUDE - self.station.longitude) * 10)
        return sum(episode.load(ts - lag) for episode in self.episodes)

    def reading(self, ts: datetime) -> Telemetry | None:
        """The reading at `ts`, or None when the message is lost on the way."""
        rng = self._rng
        seq = int(ts.timestamp() // self.interval_s)
        # Local solar time: the islands are about four hours behind UTC.
        hour = (ts.hour + ts.minute / 60 + self.station.longitude / 15) % 24

        self._temperature_noise = 0.95 * self._temperature_noise + rng.gauss(0, 0.15)
        daily = 3.2 * math.sin(2 * math.pi * (hour - 9) / 24)
        temperature = self.station.base_temperature_c + daily + self._temperature_noise
        humidity = min(
            100.0,
            max(
                35.0, 78 - 2.2 * (temperature - self.station.base_temperature_c) + rng.gauss(0, 1.5)
            ),
        )

        self._pressure_drift = 0.999 * self._pressure_drift + rng.gauss(0, 0.05)
        pressure = 1013.5 + 0.9 * math.cos(2 * math.pi * (hour - 10) / 12) + self._pressure_drift

        if self._stuck_for > 0:
            self._stuck_for -= 1
            pm2_5, pm10 = self._last_pm
        else:
            pm10 = max(0.0, self.station.background_pm10_ugm3 + self._dust(ts) + rng.gauss(0, 2.5))
            if rng.random() < self.faults.spike:
                pm10 *= rng.uniform(3, 6)
            pm2_5 = max(0.0, 0.32 * pm10 + rng.gauss(0, 1.0))
            if rng.random() < self.faults.stuck_start:
                self._stuck_for = rng.randint(30, 180)
        self._last_pm = (pm2_5, pm10)

        sunlight = max(0.0, math.sin(2 * math.pi * (hour - 6) / 24))
        battery = 3.65 + 0.5 * sunlight + rng.gauss(0, 0.01)
        rssi = round(self.station.base_rssi_dbm + rng.gauss(0, 3))

        if rng.random() < self.faults.drop:
            return None
        return Telemetry(
            station_id=self.station.station_id,
            ts=ts,
            seq=seq,
            temperature_c=round(temperature, 2),
            humidity_pct=round(humidity, 1),
            pressure_hpa=round(pressure, 2),
            pm2_5_ugm3=round(min(pm2_5, 1000.0), 1),
            pm10_ugm3=round(min(pm10, 1000.0), 1),
            battery_v=round(battery, 3),
            rssi_dbm=max(-120, min(0, rssi)),
        )
