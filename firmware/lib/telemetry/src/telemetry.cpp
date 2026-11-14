#include "telemetry.h"

#include <ArduinoJson.h>

#include <cstdio>
#include <ctime>

namespace telemetry {

namespace {

// Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm): no timegm/gmtime_r,
// whose availability differs between newlib on the ESP32 and the host libc.
void civilFromDays(int64_t days, int& year, unsigned& month, unsigned& day) {
  days += 719468;
  const int64_t era = (days >= 0 ? days : days - 146096) / 146097;
  const unsigned doe = static_cast<unsigned>(days - era * 146097);
  const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const unsigned mp = (5 * doy + 2) / 153;
  day = doy - (153 * mp + 2) / 5 + 1;
  month = mp < 10 ? mp + 3 : mp - 9;
  year = static_cast<int>(yoe + era * 400 + (month <= 2 ? 1 : 0));
}

// Fixed decimals, as the simulator sends them; avoids float noise like 28.370001.
double rounded(float value, int decimals) {
  const double scale = std::pow(10.0, decimals);
  return std::round(static_cast<double>(value) * scale) / scale;
}

void putIfPresent(JsonDocument& doc, const char* key, float value, int decimals) {
  if (!std::isnan(value)) doc[key] = rounded(value, decimals);
}

}  // namespace

void formatTimestamp(int64_t epoch_seconds, char* out) {
  int64_t days = epoch_seconds / 86400;
  int64_t rest = epoch_seconds % 86400;
  if (rest < 0) {
    rest += 86400;
    days -= 1;
  }
  int year;
  unsigned month, day;
  civilFromDays(days, year, month, day);
  std::snprintf(out, 21, "%04d-%02u-%02uT%02u:%02u:%02uZ", year, month, day,
                static_cast<unsigned>(rest / 3600), static_cast<unsigned>(rest % 3600 / 60),
                static_cast<unsigned>(rest % 60));
}

std::size_t toJson(const Reading& reading, char* out, std::size_t out_size) {
  char ts[21];
  formatTimestamp(reading.ts, ts);

  JsonDocument doc;
  doc["v"] = kVersion;
  doc["station_id"] = reading.station_id;
  doc["ts"] = ts;
  doc["seq"] = reading.seq;
  putIfPresent(doc, "temperature_c", reading.temperature_c, 2);
  putIfPresent(doc, "humidity_pct", reading.humidity_pct, 1);
  putIfPresent(doc, "pressure_hpa", reading.pressure_hpa, 2);
  putIfPresent(doc, "pm2_5_ugm3", reading.pm2_5_ugm3, 1);
  putIfPresent(doc, "pm10_ugm3", reading.pm10_ugm3, 1);
  putIfPresent(doc, "battery_v", reading.battery_v, 3);
  if (reading.rssi_dbm != kNoRssi) doc["rssi_dbm"] = reading.rssi_dbm;

  if (measureJson(doc) + 1 > out_size) return 0;
  return serializeJson(doc, out, out_size);
}

std::size_t telemetryTopic(const char* station_id, char* out, std::size_t out_size) {
  const int n = std::snprintf(out, out_size, "antilles/%s/telemetry", station_id);
  return n > 0 && static_cast<std::size_t>(n) < out_size ? static_cast<std::size_t>(n) : 0;
}

std::size_t statusTopic(const char* station_id, char* out, std::size_t out_size) {
  const int n = std::snprintf(out, out_size, "antilles/%s/status", station_id);
  return n > 0 && static_cast<std::size_t>(n) < out_size ? static_cast<std::size_t>(n) : 0;
}

}  // namespace telemetry
