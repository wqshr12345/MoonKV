#pragma once

#include "btree_iterator.h"

namespace ROCKSDB_NAMESPACE {

class BtreePortionIterator : public BtreeIterator {
 public:
  BtreePortionIterator(NvmBtree* btree, const std::vector<uint32_t>& sub_runs,
                       const ReadOptions& read_options,
                       const InternalKeyComparator& icomp)
      : BtreeIterator(btree, read_options, icomp) {
    for (const auto& run : sub_runs) {
      for (uint i = 0; i < btree->sub_runs.size(); ++i) {
        if (run == btree->sub_runs[i]) {
          sub_run_set.emplace(i);
          break;
        }
      }
    }
    SeekToFirst();
  }

  void Seek(const Slice& target) override;
  void Next() override;
  void Prev() override;
  void SeekToFirst() override;
  void SeekToLast() override;

 private:
  std::unordered_set<uint8_t> sub_run_set;

  bool include_in_sub_run();
};

}  // namespace ROCKSDB_NAMESPACE