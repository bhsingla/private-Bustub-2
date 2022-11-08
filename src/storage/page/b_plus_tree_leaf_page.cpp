//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  //TODO: Set next page id: SetNextPageId();
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { 
  //return INVALID_PAGE_ID; 
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  this.next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const { 
  for (int cur = 1; cur < GetSize(); cur++) {
    if (comparator(key, array[cur].first) <= 0) {
      return curr;
    }
  }
  return curr;
  //return 0; 
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  //KeyType key{};
  KeyType key{array[index].first};
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
  //return array[0];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  BUSTUB_ASSERT((uint64_t)GetSize() < LEAF_PAGE_SIZE, "inserting into non full leaf node");
  int index = KeyIndex(key);
  int cur = GetSize();
  while (cur >= index) {
    array[cur] = array[cur - 1];
    cur--;
  }
  array[cur + 1] = MappingType(new_key, new_value);
  IncreaseSize(1);
  return GetSize();
  //return 0;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array + (GetSize() + 1) / 2, GetSize() / 2, buffer_pool_manager);
  recipient->SetSize(GetSize() / 2);
  SetSize((GetSize() + 1) / 2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  BUSTUB_ASSERT(GetSize() == 0, "entries will be overwritten");
  for (int cur = 0; cur < size; cur++) {
    array[cur] = items[cur];
    BPlusTreeInternalPage *child_page =
        reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager->FetchPage(array[cur].second)->GetData());
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  for (int cur = 1; cur < GetSize(); cur++) {
    if (comparator(key, array[cur].first) == 0) {
      array[cur].second = value;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) { 
  if(!Lookup) return GetSize();
  //TODO: Check if need to return size or 0
  //KeyIndex would give index with matching key, since key is present
  int index = KeyIndex(key);
  for(int cur=index; cur< GetSize(); cur++){
    array[cur] = array[cur+1];
  }
  IncreaseSize(-1);
  return GetSize();


  //return 0;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  BUSTUB_ASSERT(recipient->GetSize() + GetSize() <= recipient->GetMaxSize(), "recipient does not have room");
  int cur = recipient->GetSize();
  recipient->array[cur].first = middle_key;
  recipient->array[cur].second = array[0].second;
  BPlusTreeInternalPage *child_page =
      reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager->FetchPage(array[0].second)->GetData());
  child_page->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  cur++;
  for (int i = 1; i < GetSize(); i++, cur++) {
    recipient->array[cur] = array[i];
    child_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager->FetchPage(array[i].second)->GetData());
    child_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  recipient->IncreaseSize(GetSize());
  /*Unsure if its correct*/recipient->SetNextPageId() = GetNextPageId();
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  BUSTUB_ASSERT(recipient->GetSize() < recipient->GetMaxSize(), "no room in recipient");
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  //Moving the first element would lead to empty key, setting it as middle key
  recipient->array[recipient->GetSize() - 1].first = middle_key;
  //shift elements left one step
  for (int i = 0; i < GetSize(); i++) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[GetSize()] = pair;
  IncreaseSize(1);
  BPlusTreeInternalPage *child_page =
      reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager->FetchPage(pair.second)->GetData());
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  BUSTUB_ASSERT(recipient->GetSize() < recipient->GetMaxSize(), "no room in recipient");
  recipient->CopyFirstFrom(array[GetSize() - 1], buffer_pool_manager);
  //TODO: Check should we set middle key as 1st or 0th element of array
  recipient->array[1].first = middle_key;
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  for (int i = GetSize(); i > 0; i--) {
    array[i] = array[i - 1];
  }
  array[1].first = pair.first;
  array[0].second = pair.second;
  IncreaseSize(1);
  BPlusTreeInternalPage *child_page =
      reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager->FetchPage(pair.second)->GetData());
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
