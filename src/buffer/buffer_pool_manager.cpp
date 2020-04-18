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
#include "storage/disk/disk_manager.h"
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
  // 1.     Search the page table for the requested page (P).
  auto got = page_table_.find(page_id);
  Page* p= nullptr;
  if ( got != page_table_.end() ) {
    // 1.1    If P exists, pin it and return it immediately.
    frame_id_t frame_id = got->second;
    p = &pages_[frame_id];
    p->pin_count_ = p->GetPinCount()+1;
    return p;
  }
  std::cout << "not found";
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.

  // 2.     If R is dirty, write it back to the disk.
  if(p->IsDirty()){
    FlushPage(page_id);
  }

  // 3.     Delete R from the page table and insert P.

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  Page *page = &pages_[page_table_.at(page_id)];
  if(page->GetPinCount()>0) {
    page->pin_count_ = page->GetPinCount() - 1;
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  auto got = page_table_.find(page_id);
  frame_id_t frame_id = got->second;
  Page* p = &pages_[frame_id];
  disk_manager_->WritePage(page_id,p->GetData());
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  page_id_t new_page_id  = disk_manager_->AllocatePage();

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if(free_list_.empty()&& getUnpinnedPage()== nullptr){
    return nullptr;
  }


  frame_id_t available_frame_index = BUSTUB_INT32_NULL;
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if(!free_list_.empty()){
    available_frame_index = free_list_.front();
    free_list_.pop_front();
  }else{
    //TODO: Remove from replacer
    replacer_->Size();
    available_frame_index = 5;
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page *victimPage = &pages_[available_frame_index];
  if(victimPage!= nullptr) {
    victimPage->ResetMemory();
    victimPage->page_id_ = new_page_id;
    victimPage->pin_count_ = victimPage->GetPinCount()+1;
    page_table_.insert(std::pair<page_id_t ,frame_id_t >(new_page_id, available_frame_index));
  }

  // 4.   Set the page ID output parameter. Return a pointer to P.

  return victimPage;
}

Page *BufferPoolManager::getUnpinnedPage() const {
  Page *empty_page = nullptr;
  for(uint64_t i=0;i<sizeof(&pages_);i++){
    if(pages_[i].GetPinCount()==0){
      empty_page = &pages_[i];
      break;
    }
  }
  return empty_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!

  // 1.   Search the page table for the requested page (P).
  auto got = page_table_.find(page_id);
  // 1.   If P does not exist, return true.
  if ( got != page_table_.end() ) {
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t frame_id = got->second;
  Page *p = &pages_[frame_id];
  if(p->GetPinCount()>0){
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  disk_manager_->DeallocatePage(page_id);
  page_table_.erase(page_id);
  p->ResetMemory();
  free_list_.push_back(frame_id);

  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

}  // namespace bustub
