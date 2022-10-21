/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int i) {
  buffer_pool_manager_ = bpm;
  current_page_id_ = page_id;
  index_ = i;
}

// INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::~IndexIterator() {
//   if (current_page_) { // for end(), current_page_ will be null
//     buffer_pool_manager_->UnpinPage(current_page_->GetPageId(), true);
//   }
// }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return current_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return current_page_id_ == itr.current_page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return current_page_id_ != itr.current_page_id_ || index_ != itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  Page *page = buffer_pool_manager_->FetchPage(current_page_id_);
  page->RLatch();
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  const MappingType &val = leaf->GetItem(index_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return val;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  index_++;
  Page *page = buffer_pool_manager_->FetchPage(current_page_id_);
  page->RLatch();
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  if (index_ >= leaf->GetSize()) {
    current_page_id_ = leaf->GetNextPageId();
    index_ = 0;
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
