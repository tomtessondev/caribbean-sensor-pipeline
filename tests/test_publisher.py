"""A station's MQTT session on a real Mosquitto: its retained status follows reconnections."""

from __future__ import annotations

import paho.mqtt.client as mqtt
import pytest
from paho.mqtt.enums import CallbackAPIVersion

from sensor_pipeline.config import Settings
from sensor_pipeline.messages import status_topic
from sensor_pipeline.publisher import StationClient
from tests.support import wait_until

pytestmark = pytest.mark.integration


def retained_status(host: str, port: int, station_id: str) -> str | None:
    """The retained value of the station's status topic, as a new subscriber sees it."""
    received: list[str] = []
    client = mqtt.Client(CallbackAPIVersion.VERSION2)
    client.on_message = lambda _c, _u, msg: received.append(msg.payload.decode())
    client.connect(host, port)
    client.subscribe(status_topic(station_id), qos=1)
    client.loop_start()
    try:
        wait_until(lambda: bool(received), timeout=5)
    except AssertionError:
        return None
    finally:
        client.disconnect()
        client.loop_stop()
    return received[0]


def test_status_is_online_again_after_a_reconnection(broker: tuple[str, int]) -> None:
    host, port = broker
    station = StationClient("gp-pap-01", Settings(mqtt_host=host, mqtt_port=port))
    try:
        wait_until(lambda: retained_status(host, port, "gp-pap-01") == "online", timeout=10)
        # The connection drops: the broker publishes the last will ("offline", retained).
        overwrite = mqtt.Client(CallbackAPIVersion.VERSION2)
        overwrite.connect(host, port)
        overwrite.loop_start()
        overwrite.publish(
            status_topic("gp-pap-01"), "offline", qos=1, retain=True
        ).wait_for_publish(timeout=5)
        overwrite.disconnect()
        overwrite.loop_stop()
        assert retained_status(host, port, "gp-pap-01") == "offline"

        station._client.reconnect()  # what paho's loop does after a drop

        wait_until(lambda: retained_status(host, port, "gp-pap-01") == "online", timeout=10)
    finally:
        station.close()
