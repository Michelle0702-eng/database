//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager_instance.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::AcquireFrame(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(frame_id)) {
      return false;
    }
    auto &victim = pages_[*frame_id];
    if (victim.is_dirty_) {
      disk_manager_->WritePage(victim.page_id_, victim.GetData());
    }
    page_table_->Remove(victim.page_id_);
  }

  auto &page = pages_[*frame_id];
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  return true;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!AcquireFrame(&frame_id)) {
    return nullptr;
  }

  // Allocate an id only after a frame is available.
  *page_id = AllocatePage();
  auto &page = pages_[frame_id];
  page.page_id_ = *page_id;
  page.pin_count_ = 1;
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  if (page_id < 0) {
    return nullptr;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    auto &page = pages_[frame_id];
    ++page.pin_count_;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &page;
  }
  if (!AcquireFrame(&frame_id)) {
    return nullptr;
  }

  auto &page = pages_[frame_id];
  disk_manager_->ReadPage(page_id, page.GetData());
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  auto &page = pages_[frame_id];
  if (page.pin_count_ <= 0) {
    return false;
  }
  // A clean unpin must not erase an earlier writer's dirty flag.
  page.is_dirty_ = page.is_dirty_ || is_dirty;
  --page.pin_count_;
  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  auto &page = pages_[frame_id];
  disk_manager_->WritePage(page_id, page.GetData());
  page.is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  // Write directly while holding the latch; FlushPgImp would acquire the same
  // mutex again.
  for (size_t frame_id = 0; frame_id < pool_size_; ++frame_id) {
    auto &page = pages_[frame_id];
    if (page.page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page.page_id_, page.GetData());
      page.is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  auto &page = pages_[frame_id];
  if (page.pin_count_ > 0) {
    return false;
  }
  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
