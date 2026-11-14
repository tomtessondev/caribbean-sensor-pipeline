// The MQTT telemetry contract, version 1 (see src/sensor_pipeline/messages.py).
//
// Pure C++ with no Arduino dependency, so it is unit tested on the host.
#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>

namespace telemetry {

constexpr int kVersion = 1;
constexpr std::size_t kMaxJson = 320;

// A missing measurement is NaN (floats) or kNoRssi, and is left out of the JSON: the
// contract makes every measurement optional so a dead sensor does not drop the reading.
constexpr int16_t kNoRssi = INT16_MIN;

struct Reading {
  char station_id[16] = {};
  int64_t ts = 0;   // seconds since the Unix epoch, UTC
  uint32_t seq = 0;
  float temperature_c = NAN;
  float humidity_pct = NAN;
  float pressure_hpa = NAN;
  float pm2_5_ugm3 = NAN;
  float pm10_ugm3 = NAN;
  float battery_v = NAN;
  int16_t rssi_dbm = kNoRssi;
};

// "2026-11-14T10:00:00Z". `out` must hold 21 bytes.
void formatTimestamp(int64_t epoch_seconds, char* out);

// Serialises `reading` into `out`. Returns the length, or 0 if `out` is too small.
std::size_t toJson(const Reading& reading, char* out, std::size_t out_size);

// "antilles/<station_id>/telemetry" and ".../status".
std::size_t telemetryTopic(const char* station_id, char* out, std::size_t out_size);
std::size_t statusTopic(const char* station_id, char* out, std::size_t out_size);

}  // namespace telemetry
