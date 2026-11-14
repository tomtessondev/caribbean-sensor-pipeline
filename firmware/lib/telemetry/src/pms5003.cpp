#include "pms5003.h"

namespace pms5003 {

namespace {

uint16_t be16(const uint8_t* p) { return static_cast<uint16_t>((p[0] << 8) | p[1]); }

}  // namespace

bool Decoder::feed(uint8_t byte) {
  // Header: 0x42 0x4D. Anything else restarts the search.
  if (pos_ == 0 && byte != 0x42) return false;
  if (pos_ == 1 && byte != 0x4D) {
    pos_ = byte == 0x42 ? 1 : 0;
    return false;
  }
  frame_[pos_++] = byte;
  if (pos_ == 4 && be16(&frame_[2]) != kFrameSize - 4) {
    pos_ = 0;  // not a PMS5003 data frame (or a misaligned header): resynchronise
    return false;
  }
  if (pos_ < kFrameSize) return false;

  pos_ = 0;
  uint16_t sum = 0;
  for (std::size_t i = 0; i < kFrameSize - 2; ++i) sum = static_cast<uint16_t>(sum + frame_[i]);
  if (sum != be16(&frame_[kFrameSize - 2])) {
    ++checksum_errors_;
    return false;
  }
  // Data words start at byte 4: PM1.0/2.5/10 "CF=1" (words 0-2), then atmospheric (3-5).
  sample_ = {be16(&frame_[4 + 2 * 3]), be16(&frame_[4 + 2 * 4]), be16(&frame_[4 + 2 * 5])};
  return true;
}

}  // namespace pms5003
