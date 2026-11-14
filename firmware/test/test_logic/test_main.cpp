// Host-side tests of the firmware logic: `pio test -e native`.
#include <unity.h>

#include <cmath>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "pms5003.h"
#include "ring_buffer.h"
#include "telemetry.h"

void setUp() {}
void tearDown() {}

// --- telemetry ---------------------------------------------------------------------------

telemetry::Reading fullReading() {
  telemetry::Reading r;
  std::strcpy(r.station_id, "gp-pap-01");
  r.ts = 1794650400;  // 2026-11-14T10:00:00Z
  r.seq = 179465040;
  r.temperature_c = 28.374f;
  r.humidity_pct = 76.42f;
  r.pressure_hpa = 1013.876f;
  r.pm2_5_ugm3 = 7.06f;
  r.pm10_ugm3 = 21.94f;
  r.battery_v = 4.1234f;
  r.rssi_dbm = -67;
  return r;
}

std::string readFile(const char* path) {
  std::FILE* f = std::fopen(path, "rb");
  TEST_ASSERT_NOT_NULL_MESSAGE(f, path);
  std::string content;
  char buf[256];
  std::size_t n;
  while ((n = std::fread(buf, 1, sizeof buf, f)) > 0) content.append(buf, n);
  std::fclose(f);
  while (!content.empty() && (content.back() == '\n' || content.back() == '\r')) content.pop_back();
  return content;
}

void test_timestamp_is_utc_iso8601() {
  char ts[21];
  telemetry::formatTimestamp(0, ts);
  TEST_ASSERT_EQUAL_STRING("1970-01-01T00:00:00Z", ts);
  telemetry::formatTimestamp(1794650400, ts);
  TEST_ASSERT_EQUAL_STRING("2026-11-14T10:00:00Z", ts);
  telemetry::formatTimestamp(951782400, ts);  // leap day
  TEST_ASSERT_EQUAL_STRING("2000-02-29T00:00:00Z", ts);
}

// The exact bytes a station sends. The Python side validates the same file with the
// ingestor's pydantic model (tests/test_firmware_contract.py): if either side drifts from
// the contract, one of the two test suites fails.
void test_full_reading_matches_the_golden_message() {
  char json[telemetry::kMaxJson];
  const std::size_t n = telemetry::toJson(fullReading(), json, sizeof json);
  TEST_ASSERT_GREATER_THAN(0, n);
  TEST_ASSERT_EQUAL_STRING(readFile(GOLDEN_TELEMETRY_PATH).c_str(), json);
}

void test_missing_measurements_are_left_out() {
  telemetry::Reading r = fullReading();
  r.pm10_ugm3 = NAN;  // particle sensor not answering
  r.rssi_dbm = telemetry::kNoRssi;
  char json[telemetry::kMaxJson];
  telemetry::toJson(r, json, sizeof json);
  TEST_ASSERT_NULL(std::strstr(json, "pm10_ugm3"));
  TEST_ASSERT_NULL(std::strstr(json, "rssi_dbm"));
  TEST_ASSERT_NOT_NULL(std::strstr(json, "\"temperature_c\":28.37"));
}

void test_too_small_buffer_is_refused() {
  char json[16];
  TEST_ASSERT_EQUAL(0, telemetry::toJson(fullReading(), json, sizeof json));
}

void test_topics() {
  char topic[64];
  telemetry::telemetryTopic("mq-fdf-01", topic, sizeof topic);
  TEST_ASSERT_EQUAL_STRING("antilles/mq-fdf-01/telemetry", topic);
  telemetry::statusTopic("mq-fdf-01", topic, sizeof topic);
  TEST_ASSERT_EQUAL_STRING("antilles/mq-fdf-01/status", topic);
}

// --- PMS5003 -------------------------------------------------------------------------------

std::vector<uint8_t> frame(uint16_t pm1, uint16_t pm25, uint16_t pm10) {
  std::vector<uint8_t> f = {0x42, 0x4D, 0x00, 0x1C};
  const uint16_t words[13] = {static_cast<uint16_t>(pm1 + 1), static_cast<uint16_t>(pm25 + 1),
                              static_cast<uint16_t>(pm10 + 1), pm1, pm25, pm10,
                              900, 300, 60, 10, 2, 1, 0};
  for (uint16_t w : words) {
    f.push_back(static_cast<uint8_t>(w >> 8));
    f.push_back(static_cast<uint8_t>(w & 0xFF));
  }
  uint16_t sum = 0;
  for (uint8_t b : f) sum = static_cast<uint16_t>(sum + b);
  f.push_back(static_cast<uint8_t>(sum >> 8));
  f.push_back(static_cast<uint8_t>(sum & 0xFF));
  return f;
}

int feedAll(pms5003::Decoder& decoder, const std::vector<uint8_t>& bytes) {
  int frames = 0;
  for (uint8_t b : bytes) frames += decoder.feed(b) ? 1 : 0;
  return frames;
}

void test_pms_decodes_a_frame() {
  pms5003::Decoder decoder;
  TEST_ASSERT_EQUAL(1, feedAll(decoder, frame(5, 12, 31)));
  TEST_ASSERT_EQUAL(5, decoder.sample().pm1_0);
  TEST_ASSERT_EQUAL(12, decoder.sample().pm2_5);
  TEST_ASSERT_EQUAL(31, decoder.sample().pm10);
}

void test_pms_resynchronises_after_garbage_and_split_frames() {
  pms5003::Decoder decoder;
  // Power-up noise, a frame cut in the middle, a stray header byte, then whole frames.
  std::vector<uint8_t> stream = {0x00, 0xFF, 0x42, 0x13, 0x42};
  std::vector<uint8_t> first = frame(1, 2, 3);
  stream.insert(stream.end(), first.begin() + 10, first.end());
  std::vector<uint8_t> second = frame(4, 8, 16), third = frame(5, 9, 17);
  stream.insert(stream.end(), second.begin(), second.end());
  stream.insert(stream.end(), third.begin(), third.end());

  TEST_ASSERT_EQUAL(2, feedAll(decoder, stream));
  TEST_ASSERT_EQUAL(17, decoder.sample().pm10);
}

void test_pms_rejects_a_corrupted_frame() {
  pms5003::Decoder decoder;
  std::vector<uint8_t> bad = frame(5, 12, 31);
  bad[14] ^= 0x01;  // one flipped bit in the PM2.5 value
  TEST_ASSERT_EQUAL(0, feedAll(decoder, bad));
  TEST_ASSERT_EQUAL(1, decoder.checksumErrors());
  TEST_ASSERT_EQUAL(1, feedAll(decoder, frame(5, 12, 31)));
}

// --- ring buffer ---------------------------------------------------------------------------

void test_ring_buffer_is_fifo() {
  RingBuffer<int, 4> buffer;
  for (int i = 1; i <= 3; ++i) buffer.push(i);
  TEST_ASSERT_EQUAL(3, buffer.size());
  TEST_ASSERT_EQUAL(1, buffer.front());
  buffer.pop();
  TEST_ASSERT_EQUAL(2, buffer.front());
}

void test_ring_buffer_overwrites_the_oldest_when_full() {
  RingBuffer<int, 3> buffer;
  for (int i = 1; i <= 5; ++i) buffer.push(i);
  TEST_ASSERT_EQUAL(3, buffer.size());
  TEST_ASSERT_EQUAL(2, buffer.overwritten());
  std::vector<int> drained;
  while (!buffer.empty()) {
    drained.push_back(buffer.front());
    buffer.pop();
  }
  TEST_ASSERT_EQUAL(3, drained.size());
  TEST_ASSERT_EQUAL(3, drained[0]);
  TEST_ASSERT_EQUAL(5, drained[2]);
  buffer.pop();  // popping an empty buffer is harmless
  TEST_ASSERT_TRUE(buffer.empty());
}

int main() {
  UNITY_BEGIN();
  RUN_TEST(test_timestamp_is_utc_iso8601);
  RUN_TEST(test_full_reading_matches_the_golden_message);
  RUN_TEST(test_missing_measurements_are_left_out);
  RUN_TEST(test_too_small_buffer_is_refused);
  RUN_TEST(test_topics);
  RUN_TEST(test_pms_decodes_a_frame);
  RUN_TEST(test_pms_resynchronises_after_garbage_and_split_frames);
  RUN_TEST(test_pms_rejects_a_corrupted_frame);
  RUN_TEST(test_ring_buffer_is_fifo);
  RUN_TEST(test_ring_buffer_overwrites_the_oldest_when_full);
  return UNITY_END();
}
