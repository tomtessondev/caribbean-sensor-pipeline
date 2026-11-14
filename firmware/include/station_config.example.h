// Copy to station_config.h (git-ignored) and fill in. Without it the firmware builds with
// these placeholders, which is what CI does.
#pragma once

#include <cstdint>

// Also the station's MQTT username: the broker's ACL only lets it publish on
// antilles/<STATION_ID>/telemetry and antilles/<STATION_ID>/status.
#define STATION_ID "gp-pap-01"

#define WIFI_SSID "your-ssid"
#define WIFI_PASSWORD "your-password"

// TLS only. The address must appear in the broker certificate:
//   sensor-pipeline provision-broker --out <dir> --hostname 192.168.1.10
#define MQTT_URI "mqtts://192.168.1.10:8883"
// This station's entry in credentials.json, written by provision-broker.
#define MQTT_PASSWORD "paste-the-station-password"

// ca.crt from provision-broker: the station trusts this CA and nothing else, so it cannot
// be pointed at an impostor broker.
static const char kBrokerCaPem[] = R"PEM(-----BEGIN CERTIFICATE-----
paste ca.crt here
-----END CERTIFICATE-----
)PEM";

// Same cadence as the simulator: one reading per 10-second slot.
constexpr uint32_t kIntervalS = 10;

// Wiring (ESP32 DevKit): BME280 on the default I2C pins (SDA 21, SCL 22) at 0x76,
// PMS5003 TX on GPIO 16, battery through a 1:2 divider on GPIO 35 (ADC1).
constexpr int kPmsRxPin = 16;
constexpr int kPmsTxPin = 17;
constexpr int kBatteryPin = 35;
constexpr float kBatteryDivider = 2.0f;
constexpr uint8_t kBme280Address = 0x76;
