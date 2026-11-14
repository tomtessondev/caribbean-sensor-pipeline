# Station firmware (ESP32)

One air-quality station: an ESP32 DevKit, a **BME280** (temperature, humidity, pressure)
and a **Plantower PMS5003** (PM2.5, PM10), publishing the v1 telemetry contract over MQTT
to the broker of this repository. It is a drop-in replacement for one simulated station.

## Wiring

| Part | Pin | ESP32 |
| --- | --- | --- |
| BME280 | SDA / SCL | GPIO 21 / GPIO 22 (I2C address 0x76) |
| BME280 | VIN / GND | 3V3 / GND |
| PMS5003 | TX → | GPIO 16 (UART2 RX), 9600 baud |
| PMS5003 | RX ← | GPIO 17 (UART2 TX) |
| PMS5003 | VCC / GND | 5V / GND (the fan needs 5 V; its logic is 3.3 V) |
| Battery | through a 1:2 divider | GPIO 35 (ADC1, usable while Wi-Fi runs) |

## Build, test, flash

```bash
pip install -e "..[firmware]"          # PlatformIO
cp include/station_config.example.h include/station_config.h   # Wi-Fi, broker, station id
pio test -e native                     # logic tests on the computer, no board needed
pio run -e esp32dev -t upload -t monitor
```

`station_config.h` is git-ignored; without it the firmware builds with the placeholders,
which is what CI does.

## How it works

```
 PMS5003 ─UART─► Decoder (byte-fed, checksum) ─┐
 BME280  ─I2C──────────────────────────────────┼─► Reading every 10 s slot ─► RingBuffer ─► esp-mqtt QoS 1
 battery ─ADC──────────────────────────────────┘    (NTP time, seq = slot)    (1 h, RAM)    + last will
```

- **Time slots, not a free-running counter.** Readings are taken when the NTP clock enters
  a new 10-second slot and numbered by that slot, like the simulator. Every reading has a
  unique `(station_id, ts)` key; a slot missing from the database is a reading that never
  arrived, whatever the cause (radio, power, reboot). No reading is taken before NTP has
  answered: a timestamp of 1970 would be worse than nothing.
- **Store and forward.** Readings always go through a fixed-size ring buffer (one hour,
  about 20 KB of RAM, no heap). While Wi-Fi or the broker is down they wait there, and are
  published oldest first once the connection is back. When an outage outlasts the buffer,
  the oldest readings are overwritten: the recent past matters more.
- **At-least-once delivery.** Telemetry is published at QoS 1 through ESP-IDF's esp-mqtt
  client, which keeps unacknowledged messages in its outbox and resends them. Duplicates
  are harmless: the ingestor's primary key absorbs them.
- **Liveness.** The connection carries a retained "offline" last will on the station's
  status topic, and "online" is published on every (re)connection.
- **A dead sensor does not stop the station.** A missing BME280 or a PMS5003 that stopped
  talking (no valid frame for 5 s) leaves its fields out of the message; the contract makes
  every measurement optional.
- **The PMS5003 decoder** is a byte-at-a-time state machine: it resynchronises on the
  `0x42 0x4D` header, checks the frame length and the 16-bit checksum, and counts corrupt
  frames, so power-up noise or a half frame never yields a bogus value.

## Tests

The logic (`lib/telemetry`) has no Arduino dependency and runs on the host with Unity
(`pio test -e native`): JSON serialisation, UTC timestamps (including a leap day), the
PMS5003 decoder (valid frame, garbage and split frames, corrupted checksum) and the ring
buffer (FIFO order, overwrite when full).

The contract is checked across languages: the firmware test must produce
`test/golden/telemetry_v1.json` byte for byte, and `tests/test_firmware_contract.py` in the
Python suite validates that same file with the ingestor's pydantic model.

CI runs the host tests and builds the firmware for the ESP32 on every pull request.

## Not done yet

- Simulating the whole board in CI with [Wokwi](https://wokwi.com) (needs an account token).
- TLS and per-station credentials (roadmap step 5).
- Deep sleep between readings to save the battery: the ESP32 currently stays awake.
