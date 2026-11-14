// One air-quality station: BME280 + PMS5003 on an ESP32, publishing the v1 telemetry
// contract over MQTT.
//
// - A reading per 10-second slot, timestamped by NTP and numbered by its slot, exactly
//   like the simulator: a missing slot in the database is a lost reading.
// - Readings go through a RAM ring buffer: while Wi-Fi or the broker is down they wait
//   there (up to an hour), and are replayed oldest first on reconnection.
// - QoS 1 with esp-mqtt's outbox: a reading the broker has not acknowledged is resent.
// - Last will "offline" (retained) on the status topic; "online" on every connection.
#include <Adafruit_BME280.h>
#include <Arduino.h>
#include <WiFi.h>
#include <Wire.h>
#include <mqtt_client.h>

#include <atomic>
#include <ctime>

#include "pms5003.h"
#include "ring_buffer.h"
#include "telemetry.h"

#if __has_include("station_config.h")
#include "station_config.h"
#else
#include "station_config.example.h"
#endif

namespace {

// Any time before this means NTP has not answered yet: no reading without a real clock.
constexpr time_t kClockValidAfter = 1700000000;  // 2023-11-14
// A particle value older than this is reported as missing rather than stale.
constexpr uint32_t kPmMaxAgeMs = 5000;

Adafruit_BME280 bme;
bool bmeOk = false;
pms5003::Decoder pms;
pms5003::Sample latestPm = {};
uint32_t latestPmAtMs = 0;
bool havePm = false;

esp_mqtt_client_handle_t mqtt = nullptr;
std::atomic<bool> mqttConnected{false};
char telemetryTopic[64];
char statusTopic[64];

RingBuffer<telemetry::Reading, 3600 / kIntervalS> pending;
int64_t lastSlot = -1;

void onMqttEvent(void*, esp_event_base_t, int32_t eventId, void*) {
  switch (static_cast<esp_mqtt_event_id_t>(eventId)) {
    case MQTT_EVENT_CONNECTED:
      esp_mqtt_client_publish(mqtt, statusTopic, "online", 0, 1, 1);
      mqttConnected = true;
      Serial.println("MQTT connected");
      break;
    case MQTT_EVENT_DISCONNECTED:
      mqttConnected = false;
      Serial.println("MQTT disconnected");
      break;
    default:
      break;
  }
}

void startMqtt() {
  telemetry::telemetryTopic(STATION_ID, telemetryTopic, sizeof telemetryTopic);
  telemetry::statusTopic(STATION_ID, statusTopic, sizeof statusTopic);

  esp_mqtt_client_config_t config = {};
  config.uri = MQTT_URI;
  config.client_id = STATION_ID;
  config.keepalive = 30;
  config.lwt_topic = statusTopic;
  config.lwt_msg = "offline";
  config.lwt_qos = 1;
  config.lwt_retain = 1;
  mqtt = esp_mqtt_client_init(&config);
  esp_mqtt_client_register_event(mqtt, static_cast<esp_mqtt_event_id_t>(ESP_EVENT_ANY_ID),
                                 onMqttEvent, nullptr);
  esp_mqtt_client_start(mqtt);  // reconnects on its own after any drop
}

float batteryVolts() {
  return analogReadMilliVolts(kBatteryPin) * kBatteryDivider / 1000.0f;
}

telemetry::Reading takeReading(int64_t slot) {
  telemetry::Reading r;
  std::snprintf(r.station_id, sizeof r.station_id, "%s", STATION_ID);
  r.ts = slot * kIntervalS;
  r.seq = static_cast<uint32_t>(slot);
  if (bmeOk) {
    r.temperature_c = bme.readTemperature();
    r.humidity_pct = bme.readHumidity();
    r.pressure_hpa = bme.readPressure() / 100.0f;
  }
  if (havePm && millis() - latestPmAtMs < kPmMaxAgeMs) {
    r.pm2_5_ugm3 = latestPm.pm2_5;
    r.pm10_ugm3 = latestPm.pm10;
  }
  r.battery_v = batteryVolts();
  if (WiFi.status() == WL_CONNECTED) r.rssi_dbm = static_cast<int16_t>(WiFi.RSSI());
  return r;
}

// Publishes buffered readings, oldest first, while the broker is reachable.
void flushPending() {
  char json[telemetry::kMaxJson];
  while (mqttConnected && !pending.empty()) {
    const std::size_t length = telemetry::toJson(pending.front(), json, sizeof json);
    if (length == 0 ||
        esp_mqtt_client_publish(mqtt, telemetryTopic, json, static_cast<int>(length), 1, 0) < 0) {
      break;  // keep it for the next attempt
    }
    pending.pop();
  }
}

}  // namespace

void setup() {
  Serial.begin(115200);
  Serial2.begin(9600, SERIAL_8N1, kPmsRxPin, kPmsTxPin);
  bmeOk = bme.begin(kBme280Address);
  if (!bmeOk) Serial.println("BME280 not found: temperature, humidity, pressure omitted");
  analogReadResolution(12);

  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  configTime(0, 0, "pool.ntp.org", "time.google.com");  // UTC
  startMqtt();
}

void loop() {
  while (Serial2.available() > 0) {
    if (pms.feed(static_cast<uint8_t>(Serial2.read()))) {
      latestPm = pms.sample();
      latestPmAtMs = millis();
      havePm = true;
    }
  }

  const time_t now = time(nullptr);
  if (now > kClockValidAfter) {
    const int64_t slot = static_cast<int64_t>(now) / kIntervalS;
    if (slot != lastSlot) {
      lastSlot = slot;
      pending.push(takeReading(slot));
      if (pending.overwritten() > 0 && pending.size() == pending.capacity()) {
        Serial.printf("Offline buffer full, %u readings overwritten so far\n",
                      static_cast<unsigned>(pending.overwritten()));
      }
    }
  }
  flushPending();
  delay(20);
}
