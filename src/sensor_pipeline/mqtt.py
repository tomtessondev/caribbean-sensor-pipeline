"""MQTT client construction shared by the stations and the ingestor."""

from __future__ import annotations

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from sensor_pipeline.config import Settings


def new_client(
    settings: Settings,
    client_id: str,
    *,
    username: str | None,
    password: str | None,
    clean_session: bool,
) -> mqtt.Client:
    """A paho client with TLS (when a CA is configured) and credentials (when given).
    TLS verifies the broker's certificate against the CA, hostname included."""
    client = mqtt.Client(
        CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=clean_session
    )
    if settings.mqtt_tls_ca is not None:
        client.tls_set(ca_certs=str(settings.mqtt_tls_ca))
    if username is not None:
        client.username_pw_set(username, password)
    return client
