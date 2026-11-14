// Copy to station_config.h (git-ignored) and fill in. Without it the firmware builds with
// these placeholders, which is what CI does.
#pragma once

#include <cstdint>

#define STATION_ID "gp-pap-01"

#define WIFI_SSID "your-ssid"
#define WIFI_PASSWORD "your-password"

// The broker of docker-compose.yml, seen from the station's network.
#define MQTT_URI "mqtt://192.168.1.10:1883"

// Same cadence as the simulator: one reading per 10-second slot.
constexpr uint32_t kIntervalS = 10;

// Wiring (ESP32 DevKit): BME280 on the default I2C pins (SDA 21, SCL 22) at 0x76,
// PMS5003 TX on GPIO 16, battery through a 1:2 divider on GPIO 35 (ADC1).
constexpr int kPmsRxPin = 16;
constexpr int kPmsTxPin = 17;
constexpr int kBatteryPin = 35;
constexpr float kBatteryDivider = 2.0f;
constexpr uint8_t kBme280Address = 0x76;
