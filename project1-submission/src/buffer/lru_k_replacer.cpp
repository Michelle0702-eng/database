//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include <stdexcept>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k), frames_(num_frames) {
  if (k == 0) {
    throw std::invalid_argument("LRU-K requires a positive k");
  }
}

void LRUKReplacer::ValidateFrameId(frame_id_t frame_id) const {
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::out_of_range("Frame id is outside the replacer");
  }
}

void LRUKReplacer::EraseCandidate(frame_id_t frame_id) {
  const auto &frame = frames_[frame_id];
  if (!frame.is_evictable_ || frame.history_.empty()) {
    return;
  }
  auto &candidates = frame.history_.size() < k_ ? history_candidates_ : cache_candidates_;
  candidates.erase({frame.history_.front(), frame_id});
}

void LRUKReplacer::InsertCandidate(frame_id_t frame_id) {
  const auto &frame = frames_[frame_id];
  if (!frame.is_evictable_ || frame.history_.empty()) {
    return;
  }
  auto &candidates = frame.history_.size() < k_ ? history_candidates_ : cache_candidates_;
  candidates.emplace(frame.history_.front(), frame_id);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  // Maximizing backward k-distance is equivalent to choosing the earliest kth
  // access.
  auto &candidates = history_candidates_.empty() ? cache_candidates_ : history_candidates_;
  *frame_id = candidates.begin()->second;
  candidates.erase(candidates.begin());
  frames_[*frame_id].history_.clear();
  frames_[*frame_id].is_evictable_ = false;
  --curr_size_;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  ValidateFrameId(frame_id);
  EraseCandidate(frame_id);
  auto &history = frames_[frame_id].history_;
  history.push_back(++current_timestamp_);
  if (history.size() > k_) {
    history.pop_front();
  }
  InsertCandidate(frame_id);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  ValidateFrameId(frame_id);
  auto &frame = frames_[frame_id];
  if (frame.history_.empty() || frame.is_evictable_ == set_evictable) {
    return;
  }
  if (set_evictable) {
    frame.is_evictable_ = true;
    InsertCandidate(frame_id);
    ++curr_size_;
  } else {
    EraseCandidate(frame_id);
    frame.is_evictable_ = false;
    --curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  ValidateFrameId(frame_id);
  auto &frame = frames_[frame_id];
  if (frame.history_.empty()) {
    return;
  }
  if (!frame.is_evictable_) {
    throw std::invalid_argument("Cannot remove a non-evictable frame");
  }
  EraseCandidate(frame_id);
  frame.history_.clear();
  frame.is_evictable_ = false;
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
