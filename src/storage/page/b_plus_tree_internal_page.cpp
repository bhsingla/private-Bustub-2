//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/*
 * Helper method to get the key stored at the given index.
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  KeyType key{array[index].first};
  return key;
}

/*
 * Helper method to set the key stored at the given index.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * Helper method to find and return array index where the value
 * equals to parameter value. Return -1 if value not found.
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int cur = 0; cur < GetSize() + 1; cur++) {
    if (value == array[cur].second) {
      return cur;
    }
  }
  // BUSTUB_ASSERT(false, "matching value not found");
  return -1;
}

/*
 * Helper method to get the value stored at the given index.
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int cur = 1;
  for (; cur < GetSize(); cur++) {
    // the first entry > than our key indicates the previous pointer contains key
    // Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
    // K(i) <= K < K(i+1).
    if (comparator(key, array[cur].first) < 0) {
      return array[cur - 1].second;
    }
  }
  // cur == size, no entry > key, take last pointer
  return array[cur - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way up to the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 * where old_value is page_id of old root, new_value is page_id of new split
 * page (successor sibling to old root) and new_key is the middle key.
 * NOTE: Make sure to update the size of this node.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  // I think this only makes sense if it is called on the root page
  // root must have split, old_value is pointer to old root
  // so assuming the new node is the successor, old value should be first pointer
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  BUSTUB_ASSERT((uint64_t)GetSize() < INTERNAL_PAGE_SIZE, "inserting into full internal node");
  int index = ValueIndex(old_value);
  // shift everything to the right of index over
  int cur = GetSize();
  while (cur > index) {
    array[cur] = array[cur - 1];
    cur--;
  }
  array[cur + 1] = MappingType(new_key, new_value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // have this node get the majority because size counts the key-less pointer at index 0
  // LOG_INFO("Moving half of page %d to page %d, starting with %ld:%d", GetPageId(), recipient->GetPageId(),
  //          array[(GetSize() + 1) / 2].first.ToInt64(), array[(GetSize() + 1) / 2].second);
  recipient->CopyNFrom(array + (GetSize() + 1) / 2, GetSize() / 2, buffer_pool_manager);
  recipient->SetSize(GetSize() / 2);
  SetSize((GetSize() + 1) / 2);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // assume I am an empty page
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
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // shift everything left starting at index
  int cur = index;
  while (cur < GetSize() + 1) {
    array[cur] = array[cur + 1];
    cur++;
  }
  IncreaseSize(-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // assume recipient is a predecessor (i.e., middle key goes at the end, then everything from this node)
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
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  BUSTUB_ASSERT(recipient->GetSize() < recipient->GetMaxSize(), "no room in recipient");
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  recipient->array[recipient->GetSize() - 1].first = middle_key;
  for (int i = 0; i < GetSize(); i++) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array[GetSize()] = pair;
  IncreaseSize(1);
  BPlusTreeInternalPage *child_page =
      reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager->FetchPage(pair.second)->GetData());
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  BUSTUB_ASSERT(recipient->GetSize() < recipient->GetMaxSize(), "no room in recipient");
  recipient->CopyFirstFrom(array[GetSize() - 1], buffer_pool_manager);
  recipient->array[1].first = middle_key;
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
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

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
