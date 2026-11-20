"""Record one real Météo-France package as a test fixture and print what the bridge would
do with it. Run by hand; tests never call the API. Prints counts only, never the
application id or the token.

    .venv/bin/python scripts/record_package.py

The package of a slot two hours old (complete, batches included) is reduced to the
Antilles stations plus three of mainland France (to keep the department filter tested)
and written to tests/fixtures/meteofrance/package_infrahoraire_6m.json. It holds no secret.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from sensor_pipeline.bridge import slot
from sensor_pipeline.meteofrance import DEPARTMENTS, MeteoFranceClient, Observation, to_telemetry
from sensor_pipeline.registry import load_registry

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "meteofrance" / "package_infrahoraire_6m.json"


def main() -> None:
    application_id = (ROOT / "secrets" / "meteofrance_application_id").read_text().strip()
    at = slot(datetime.now(UTC) - timedelta(hours=2))
    with httpx.Client(timeout=60) as http:
        raw = MeteoFranceClient(http, application_id, time.monotonic).raw_package(at)

    antilles = [o for o in raw if o["geo_id_insee"][:3] in DEPARTMENTS]
    mainland = [o for o in raw if o["geo_id_insee"][:3] not in DEPARTMENTS][:3]
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps(antilles + mainland, indent=1) + "\n", encoding="utf-8")

    registry = {s.mf_id: s for s in load_registry()}
    observations = [Observation.model_validate(o) for o in antilles]
    known = [o for o in observations if o.station in registry]
    readings = [r for o in known if (r := to_telemetry(o, registry[o.station])) is not None]
    print(
        f"slot {at:%H:%M} UTC: {len(raw)} stations in France, {len(antilles)} in the Antilles, "
        f"{len(known)} registered, {len(readings)} readings to publish"
    )


if __name__ == "__main__":
    main()
