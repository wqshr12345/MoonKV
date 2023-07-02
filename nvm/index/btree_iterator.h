#pragma once

#include "btree_node.h"
#include "db/dbformat.h"
#include "nvm_btree.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// 模仿IterKey写的用于在iterator中缓存value的对象
class IterValue {
 public:
  IterValue(std::vector<uint32_t>& sub_run)
      : file_seq_((uint32_t*)buf_),
        value_offset_((uint8_t*)buf_ + sizeof(uint32_t)),
        sub_run_(sub_run) {}
  IterValue(const IterValue&) = delete;
  void operator=(const IterValue&) = delete;
  ~IterValue() {}

  void SetValue(const Slice& native_value) {
    auto* v = (valueaddr*)native_value.data();
    assert(v->run < sub_run_.size());
    uint32_t file_sequence = sub_run_[v->run];

    *file_seq_ = file_sequence;
    memcpy(value_offset_, v->value_offset, VALUEADDR_OFFSET_SIZE);
  }

  Slice GetValue() const {
    return {buf_, sizeof(uint32_t) + VALUEADDR_OFFSET_SIZE};
  }

  uint32_t GetFileSeq() const { return *file_seq_; }

  uint64_t GetValueOffset() const {
    uint64_t res = 0;
    for (int i = 0; i < VALUEADDR_OFFSET_SIZE; ++i) {
      res |= value_offset_[i] << (8 * i);
    }
    return res;
  }

 private:
  // Btree中存放value的字段是一个固定的valueaddr字段，是不需要考虑空间不够的
  char buf_[16];
  uint32_t* file_seq_;
  uint8_t* value_offset_;
  std::vector<uint32_t>& sub_run_;
};

enum EstimateSearchMode {
  BackTrack = 0x0,
  LinearDetection = 0x1
};

class BtreeIterator : public InternalIterator {
 public:
  BtreeIterator(NvmBtree* btree, const ReadOptions& read_options,
                const InternalKeyComparator& icomp)
      : btree(btree),
        node_ptr(0),
        node_iter(btree->pmemaddr, icomp),
        value_buf_(btree->sub_runs),
        read_options_(read_options),
        icomp_(icomp),
        user_comparator_(icomp.user_comparator()) {
    bool b = read_options_.total_order_seek;
    key_buf_.SetInternalKey(node_iter.peek_key());
    value_buf_.SetValue(node_iter.peek_value());
  }

  ~BtreeIterator() = default;

  bool Valid() const override;
  void Seek(const Slice& target) override;
  void Next() override;
  void Prev() override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void SeekForPrev(const Slice& target) override;
  Status status() const override;
  Slice key() const override;
  Slice user_key() const override;
  Slice value() const override;
  
  Slice value(bool vertical_merge_type, const std::set<uint32_t>& tables = std::set<uint32_t>()) const override {
    assert(false);
    return Slice();
  }
  
  Slice native_value() const;
  uint32_t value_file_seq() const;
  uint64_t value_offset() const;
  /*
   * Estimate search相关函数，一个是返回当前iter的位置，另一个是根据给出的偏移查找位置
   * GetKeyPosition的参数n是用于指定获取当前这个iter所在的位置，其倒数第n层的block偏移位置
   * 0表示leaf node的block偏移，1表示其parent node的block偏移……
   */
  uint32_t GetKeyPosition(int n) const;
  double GetKeyPosition(const uint32_t low_bound, const uint32_t up_bound,uint32_t position) const;
  uint32_t EstimateSeek(const Slice& target, const int estimate_block, const EstimateSearchMode mode = EstimateSearchMode::BackTrack);
  uint32_t GetLevels() const;
  int32_t GetNodePtr() const;

 private:
  NvmBtree* btree;
  int32_t node_ptr;
  node_iterator node_iter;

  IterKey key_buf_;
  IterValue value_buf_;
  Slice key_;
  Slice value_;
  Status status_;

  const ReadOptions& read_options_;
  const InternalKeyComparator& icomp_;
  UserComparatorWrapper user_comparator_;

  inline int Compare(const Slice& a, const Slice& b) const {
    return icomp_.Compare(a, b);
  }
};

}  // namespace ROCKSDB_NAMESPACE