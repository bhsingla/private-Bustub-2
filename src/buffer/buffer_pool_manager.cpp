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

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

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
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  frame_id_t frame_id;
  
  if(!(page_table_.find(page_id) == page_table_.cend())) {
    frame_id = page_table_.at(page_id);
    Page* p = &pages_[frame_id];
    p->pin_count_++;
    p->is_dirty_ = true;
    replacer_->Pin(frame_id);
    latch_.unlock();
    return p;
  } 
  Page* p;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    p = &pages_[frame_id];
  }else if (replacer_->Victim(&frame_id)) {
    p = &pages_[frame_id];
    if (p->IsDirty()){ 
      disk_manager_->WritePage(p->page_id_, p->GetData());
    }
    page_table_.erase(p->GetPageId());
  } else{  
    latch_.unlock();
    return nullptr;
  }
  p->page_id_ = page_id;
  p->pin_count_++;
  p->is_dirty_ = false;
  page_table_[page_id] = frame_id;
  replacer_->Pin(frame_id);
  disk_manager_->ReadPage(page_id, p->GetData());
  latch_.unlock();
  return p;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  frame_id_t frame_id;
  if(!(page_table_.find(page_id) == page_table_.cend())){
    frame_id = page_table_.at(page_id);
    Page* p = &pages_[frame_id];
    if (p->pin_count_ == 0){
      return false;
    }
    p->pin_count_--;
    p->is_dirty_ = is_dirty;
    if (p->pin_count_ == 0) {
      replacer_->Unpin(frame_id);
      if (p->IsDirty()){
        disk_manager_->WritePage(page_id, p->GetData());
      }
    }
    latch_.unlock();
    return true;
  } 
  latch_.unlock();
  return false;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  if(!(page_table_.find(page_id) == page_table_.cend())){
    frame_id_t frame_id = page_table_.at(page_id);
    Page* p = &pages_[frame_id];
    if (p->IsDirty()){
      disk_manager_->WritePage(page_id, p->GetData());
    }
    p->is_dirty_ = false;
  }
  latch_.unlock();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  for(size_t i = 0; i < pool_size_; ++i){
    if(pages_[i].GetPinCount() == 0){
      break;
    }
    if(i == pool_size_-1){
      latch_.unlock();
      return nullptr;
    }
  }
  page_id_t pageID = (*disk_manager_).AllocatePage();
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }else if (replacer_->Victim(&frame_id)) {
    if (pages_[frame_id].IsDirty()){
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
    }
    page_table_.erase(pages_[frame_id].page_id_);
    pages_[frame_id].is_dirty_ = false;
  } else{
    latch_.unlock();
    return nullptr;
  }
  Page* p = &pages_[frame_id];
  p->pin_count_++;
  p->page_id_ = pageID;
  page_table_[pageID] = frame_id;
  *page_id = pageID;
  memset(p->GetData(), 0, PAGE_SIZE);
  latch_.unlock();
  return p;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  if(!(page_table_.find(page_id) == page_table_.end())){
    frame_id_t frame_id = page_table_.at(page_id);
    Page* p = &pages_[frame_id];
    if (p->GetPinCount() != 0){
        latch_.unlock();
        return false;
    }
    page_table_.erase(page_id);
    p->page_id_ = INVALID_PAGE_ID;
    p->is_dirty_ = false;
    memset(p->GetData(), 0, PAGE_SIZE);
    free_list_.push_back(frame_id);
  }
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  latch_.lock();
  for(auto p : page_table_){
    this->FlushPage(p.first);
  }
  latch_.unlock();
}

}  // namespace bustub
