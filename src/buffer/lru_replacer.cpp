//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) 
 :num_pages_(num_pages),
  free_list_(),
  frame_table_() {

  }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
  LockGuard lock(latch_);
  if (free_list_.empty()) {
    return false;
  }

  *frame_id = free_list_.back();
  free_list_.pop_back();
  frame_table_.erase(*frame_id);

  return true; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  LockGuard lock(latch_);
  auto search_iter = frame_table_.find(frame_id);
  if (search_iter == frame_table_.end()) {
    return;
  }

  free_list_.erase(search_iter->second);
  frame_table_.erase(search_iter->first);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  LockGuard lock(latch_);
  auto search_iter = frame_table_.find(frame_id);
  if (search_iter != frame_table_.end()) {
    return;

  } else {
    // cache is full.
    if (free_list_.size() == num_pages_) {
      return;
    }
  
    // allocate free frame
     free_list_.push_front(frame_id);
    // mapping frame_id and iterator
    frame_table_[frame_id] = free_list_.begin();
  }
}

size_t LRUReplacer::Size() { 
  LockGuard lock(latch_);
  return free_list_.size();
}  // namespace bustub
}
