//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  auto got = page_table_.find(page_id);
  frame_id_t *frame_id = nullptr;
  Page *victim_page = nullptr;

  if (free_list_.empty()) {
    for (uint64_t i = 0; i < sizeof(&pages_); i++) {
      if (pages_[i].GetPinCount() == 0) {
        victim_page = &pages_[i];
        break;
      }
    }
    if (victim_page == nullptr) {
      return nullptr;
    }
  }

  if (got != page_table_.end()) {
    frame_id = &got->second;
    victim_page = &pages_[*frame_id];
  }

  if (victim_page == nullptr) {
    if (!free_list_.empty()) {
      frame_id = &free_list_.front();
      free_list_.pop_front();
      victim_page = &pages_[*frame_id];
    } else if (replacer_->Victim(frame_id)) {
      victim_page = &pages_[*frame_id];
      if (victim_page->IsDirty()) {
        disk_manager_->WritePage(victim_page->page_id_, victim_page->GetData());
      }
    }
  }
  victim_page->pin_count_ = victim_page->GetPinCount() + 1;
  replacer_->Pin(*frame_id);
  page_table_.erase(victim_page->page_id_);
  page_table_.insert({page_id, *frame_id});
  disk_manager_->ReadPage(page_id, victim_page->data_);

  return victim_page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  Page *page = &pages_[page_table_.at(page_id)];
  page->pin_count_ = page->GetPinCount() - 1;
  page->is_dirty_ = is_dirty;
  if (is_dirty) {
    disk_manager_->WritePage(page_id, page->GetData());
  }
  if (page->pin_count_ == 0) {
    replacer_->Unpin(page_table_.at(page_id));
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  frame_id_t frame_id = page_table_.at(page_id);
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  page_id_t new_page_id = disk_manager_->AllocatePage();

  frame_id_t frame_id = 0;
  Page *victim_page = nullptr;

  if (free_list_.empty()) {
    for (uint64_t i = 0; i < sizeof(&pages_); i++) {
      if (pages_[i].GetPinCount() == 0) {
        victim_page = &pages_[i];
        break;
      }
    }
    if (victim_page == nullptr) {
      return nullptr;
    }
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    victim_page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    victim_page = &pages_[frame_id];
    if (victim_page->IsDirty()) {
      disk_manager_->WritePage(victim_page->page_id_, victim_page->GetData());
    }
  }

  victim_page->page_id_ = new_page_id;
  victim_page->is_dirty_ = false;
  victim_page->pin_count_ = 1;
  replacer_->Pin(frame_id);
  victim_page->ResetMemory();

  page_table_.insert({new_page_id, frame_id});
  *page_id = new_page_id;
  return victim_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free
  // list.
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

}  // namespace bustub
