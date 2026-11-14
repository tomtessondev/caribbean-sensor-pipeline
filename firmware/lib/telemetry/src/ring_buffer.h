// Fixed-size FIFO for readings taken while the broker is unreachable.
//
// No heap: the capacity is a compile-time constant sized for the RAM budget. When full,
// the oldest reading is overwritten: after a long outage the recent past matters more,
// and the sequence numbers will show exactly what was lost.
#pragma once

#include <cstddef>
#include <cstdint>

template <typename T, std::size_t Capacity>
class RingBuffer {
  static_assert(Capacity > 0, "capacity must be positive");

 public:
  void push(const T& item) {
    items_[(head_ + size_) % Capacity] = item;
    if (size_ < Capacity) {
      ++size_;
    } else {
      head_ = (head_ + 1) % Capacity;
      ++overwritten_;
    }
  }

  // Oldest item; only valid when !empty().
  const T& front() const { return items_[head_]; }

  void pop() {
    if (size_ == 0) return;
    head_ = (head_ + 1) % Capacity;
    --size_;
  }

  bool empty() const { return size_ == 0; }
  std::size_t size() const { return size_; }
  static constexpr std::size_t capacity() { return Capacity; }
  uint32_t overwritten() const { return overwritten_; }

 private:
  T items_[Capacity] = {};
  std::size_t head_ = 0;
  std::size_t size_ = 0;
  uint32_t overwritten_ = 0;
};
