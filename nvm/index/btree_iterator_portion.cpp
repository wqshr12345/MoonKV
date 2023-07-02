#include "btree_iterator_portion.h"

#include "btree_node.h"

namespace ROCKSDB_NAMESPACE {

bool BtreePortionIterator::include_in_sub_run() {
  return sub_run_set.find(valueaddr::get_run_from_slice(
             BtreeIterator::native_value())) != sub_run_set.end();
}

void BtreePortionIterator::Seek(const Slice& target) {
  BtreeIterator::Seek(target);
  if (!include_in_sub_run()) {
    Next();
  }
}
void BtreePortionIterator::Next() {
  do {
    BtreeIterator::Next();
  } while (BtreeIterator::Valid() && !include_in_sub_run());
}

void BtreePortionIterator::Prev() {
  do {
    BtreeIterator::Prev();
  } while (BtreeIterator::Valid() && !include_in_sub_run());
}

void BtreePortionIterator::SeekToFirst() {
  BtreeIterator::SeekToFirst();
  if (!include_in_sub_run()) {
    Next();
  }
}

void BtreePortionIterator::SeekToLast() {
  BtreeIterator::SeekToLast();
  if (!include_in_sub_run()) {
    Prev();
  }
}

}  // namespace ROCKSDB_NAMESPACE