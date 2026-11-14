// Decoder for the Plantower PMS5003 particle sensor's serial frames.
//
// The sensor streams 32-byte frames at 9600 baud, unsolicited, roughly once a second:
//   0x42 0x4D | length (2, = 28) | 13 big-endian uint16 values | checksum (2)
// The checksum is the 16-bit sum of every byte before it. Bytes arrive one at a time
// from the UART, possibly starting mid-frame, so the decoder is a byte-fed state machine
// that resynchronises on the 0x42 0x4D header.
#pragma once

#include <cstddef>
#include <cstdint>

namespace pms5003 {

struct Sample {
  // "Atmospheric environment" concentrations, the ones to report outdoors, in µg/m³.
  uint16_t pm1_0;
  uint16_t pm2_5;
  uint16_t pm10;
};

class Decoder {
 public:
  // Feeds one byte. Returns true when it completed a valid frame, now in sample().
  bool feed(uint8_t byte);
  const Sample& sample() const { return sample_; }
  uint32_t checksumErrors() const { return checksum_errors_; }

 private:
  static constexpr std::size_t kFrameSize = 32;
  uint8_t frame_[kFrameSize] = {};
  std::size_t pos_ = 0;
  Sample sample_ = {};
  uint32_t checksum_errors_ = 0;
};

}  // namespace pms5003
