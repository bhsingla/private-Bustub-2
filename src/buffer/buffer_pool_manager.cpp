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
#include "iostream"
using namespace std;
#include <list>
#include <unordered_map>

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
  for(size_t i = 0; i < pool_size_; ++i){
    if(pages_[i].GetPinCount() == 0) break;
    if(i == pool_size_-1) return nullptr;
  }
  if(!(page_table_.find(page_id) == page_table_.end())){
    frame_id_t frameID = page_table_.at(page_id);
    size_t t = static_cast<size_t>(frameID);
    Page* p = &pages_[t];
    p->pin_count_++;
    replacer_->Pin(frameID);
    return p;
  } 
  frame_id_t frameID;
  Page* p;
  if(!free_list_.empty()){
    frameID = free_list_.front();
    free_list_.pop_front();
    size_t t = static_cast<size_t>(frameID);
    p = &pages_[t];
  } else{
    bool replace = replacer_->Victim(&frameID);
    if(!replace) return nullptr;
    size_t t = static_cast<size_t>(frameID);
    p = &pages_[t];
    if((p) != nullptr){
      if(p->IsDirty()){
      this->FlushPageImpl((*p).GetPageId());
      page_table_.erase((*p).GetPageId());
      }
   }
}
  p->page_id_ = page_id;
  p->pin_count_++;
  page_table_[page_id] = frameID;
  replacer_->Pin(frameID);

  return p;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  if(!(page_table_.find(page_id) == page_table_.end())){
    frame_id_t frameID = page_table_.at(page_id);
    size_t t = static_cast<size_t>(frameID);
    Page* p = &pages_[t];
    p->pin_count_--;
    if(is_dirty)  p->is_dirty_ = is_dirty;
    if(p->pin_count_ == 0) replacer_->Unpin(page_id);
    //page_table_.erase(page_id);
    return true;
  }
  return false; 
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if(!(page_table_.find(page_id) == page_table_.end())){
    frame_id_t frameID = page_table_.at(page_id);
    size_t t = static_cast<size_t>(frameID);
    Page* p = &pages_[t];
    if(p->IsDirty())  (*disk_manager_).WritePage(page_id, p->data_);
    //page_table_.erase(page_id);
    return true;
  } 
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  for(size_t i = 0; i < pool_size_; ++i){
    if(pages_[i].GetPinCount() == 0) break;
    if(i == pool_size_-1) return nullptr;
  }
  page_id_t pageID = (*disk_manager_).AllocatePage();
  frame_id_t frameID;
  Page* p;
  if(!free_list_.empty()){
    frameID = free_list_.front();
    free_list_.pop_front();
    size_t t = static_cast<size_t>(frameID);
    p = &pages_[t];
    p->page_id_ = pageID;
    p->pin_count_++;
    page_table_[pageID] = frameID;
    *page_id = pageID;
  } else{
    bool replace = replacer_->Victim(&frameID);
    if(!replace) return nullptr;
    size_t t = static_cast<size_t>(frameID);
    p = &pages_[t];
    this->FlushPage(p->GetPageId());
    //page_table_.erase(p->GetPageId());
    p->is_dirty_ = false;
    p->page_id_ = pageID;
    p->pin_count_++;
    page_table_[pageID] = frameID;
    *page_id = pageID;
  }
  return p;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

}  // namespace bustub
