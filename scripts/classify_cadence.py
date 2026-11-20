"""Set each station's offline threshold in the registry
(src/sensor_pipeline/data/meteofrance_antilles.csv) from the cadence its telemetry shows.

Run by hand against a database that has received a few hours of real readings, review the
diff, commit the CSV; tests never run it. The query is read-only and only counts are
printed (never the DSN, which holds a password).

    .venv/bin/python scripts/classify_cadence.py --dsn postgresql://...@localhost:5433/sensors

A station with any reading taken off the hour measures every 6 minutes; one seen only on
the hour reports hourly; one never seen keeps the cautious hourly threshold
(sensor_pipeline.registry.OFFLINE_AFTER_S). Only readings shaped like the bridge's are
counted (no battery, no RSSI): a database that ran the simulator first holds simulated
history under some ids too.
"""

from __future__ import annotations

import argparse
import os

import psycopg

from sensor_pipeline.registry import (
    OFFLINE_AFTER_S,
    REGISTRY_PATH,
    load_registry,
    save_registry,
    with_observed_cadence,
)

QUERY = """
select station_id, bool_or(extract(minute from ts) <> 0)
from telemetry
where battery_v is null and rssi_dbm is null
group by station_id
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dsn", default=os.getenv("SENSOR_DB_DSN"), help="Database to read")
    dsn = parser.parse_args().dsn
    if not dsn:
        parser.error("--dsn or SENSOR_DB_DSN is required")
    with psycopg.connect(dsn, options="-c default_transaction_read_only=on") as conn:
        sub_hourly = {str(sid): bool(sub) for sid, sub in conn.execute(QUERY).fetchall()}

    registry = with_observed_cadence(load_registry(REGISTRY_PATH), sub_hourly)
    save_registry(registry, REGISTRY_PATH)
    never_seen = sum(1 for s in registry if s.station_id not in sub_hourly)
    for cadence, threshold in OFFLINE_AFTER_S.items():
        count = sum(1 for s in registry if s.offline_after_s == threshold)
        print(f"{cadence}: {count} stations (offline after {threshold} s)")
    print(f"{never_seen} stations without any reading (kept hourly)")


if __name__ == "__main__":
    main()
