"""Runtime settings, read from environment variables with local-development defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _optional(name: str) -> str | None:
    return os.getenv(name) or None


@dataclass(frozen=True)
class Settings:
    mqtt_host: str = field(default_factory=lambda: os.getenv("MQTT_HOST", "localhost"))
    mqtt_port: int = field(default_factory=lambda: int(os.getenv("MQTT_PORT", "1883")))
    # Broker security (all unset: plain, anonymous MQTT, as in the unit tests).
    mqtt_tls_ca: Path | None = field(
        default_factory=lambda: Path(p) if (p := _optional("MQTT_TLS_CA")) else None
    )
    mqtt_username: str | None = field(default_factory=lambda: _optional("MQTT_USERNAME"))
    mqtt_password: str | None = field(default_factory=lambda: _optional("MQTT_PASSWORD"))
    # credentials.json from provision-broker: the simulator logs each station in with its own.
    mqtt_credentials_file: Path | None = field(
        default_factory=lambda: Path(p) if (p := _optional("MQTT_CREDENTIALS_FILE")) else None
    )
    db_dsn: str = field(
        default_factory=lambda: os.getenv(
            "SENSOR_DB_DSN", "postgresql://sensors:sensors@localhost:5432/sensors"
        )
    )
