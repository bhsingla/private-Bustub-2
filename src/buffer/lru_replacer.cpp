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

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    if(!frames.empty()){
        *frame_id = frames.front();
        frames.remove(*frame_id);
        return true;
    }
    return false; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    frames.remove(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    bool list_contains_frame = std::find(frames.begin(), frames.end(), frame_id) != frames.end();
    if(!list_contains_frame){
        frames.push_back(frame_id);
    }
}

size_t LRUReplacer::Size() { 
    return frames.size(); 
 }

}  // namespace bustub
