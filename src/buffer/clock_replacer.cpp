//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  auto i = clock.begin();
  bool cycle_start = false;
  std::list<node>::iterator it;
  for (it = clock.begin(); it != clock.end(); ++it) {
    if (it->frame_id == i->frame_id && cycle_start) {
      break;
    }
    cycle_start = true;

    if (it->reference_bit) {
      clock_table.insert({it->frame_id, false});
      it->reference_bit = false;
      clock.splice(clock.end(), clock, it);
    } else {
      *frame_id = it->frame_id;
      clock_table.erase(it->frame_id);
      clock.erase(it);
      return true;
    }
  }
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  clock_table.erase(frame_id);
  for (auto it = clock.begin(); it != clock.end(); ++it) {
    if (it->frame_id == frame_id) {
      clock.erase(it);
      break;
    }
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  auto got = clock_table.find(frame_id);
  if(got == clock_table.end()) {
    clock_table.insert({frame_id, false});
    clock.push_back({frame_id, false});
  }
}

size_t ClockReplacer::Size() { return clock_table.size(); }

}  // namespace bustub
