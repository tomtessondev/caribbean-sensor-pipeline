"""Runtime settings, read from environment variables with local-development defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    mqtt_host: str = field(default_factory=lambda: os.getenv("MQTT_HOST", "localhost"))
    mqtt_port: int = field(default_factory=lambda: int(os.getenv("MQTT_PORT", "1883")))
    db_dsn: str = field(
        default_factory=lambda: os.getenv(
            "SENSOR_DB_DSN", "postgresql://sensors:sensors@localhost:5432/sensors"
        )
    )
