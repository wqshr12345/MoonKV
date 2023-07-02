//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/flush_block_policy.h"

#include <cassert>
#include <mutex>

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/customizable_util.h"
#include "table/block_based/block_based_table_reader.h"
#include "nvm/table/nvm_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/flush_block_policy.h"
#include "table/format.h"


namespace ROCKSDB_NAMESPACE {

// Flush block by size
class FlushBlockBySizePolicy : public FlushBlockPolicy {
 public:
  // @params block_size:           Approximate size of user data packed per
  //                               block.
  // @params block_size_deviation: This is used to close a block before it
  //                               reaches the configured
  FlushBlockBySizePolicy(const uint64_t block_size,
                         const uint64_t block_size_deviation,
                         const bool align,
                         const BlockBuilder& data_block_builder,
                         const NvmDataBlockBuilder& data_block_builder2)
      : block_size_(block_size),
        block_size_deviation_limit_(
            ((block_size * (100 - block_size_deviation)) + 99) / 100),
        align_(align),
        data_block_builder_(data_block_builder),
        data_block_builder2_(data_block_builder2) {}

  // func: 用于评估加上value后，是否到达了block_size，是否应该flush。
  // note: 这里为了兼容RocksDB的原有接口，入参中保留了key，但是实际并不会用到这个参数。
  bool Update(const Slice& key, const Slice& value) override {
    // it makes no sense to flush when the data block is empty
    if (data_block_builder2_.empty()) {
      return false;
    }

    auto curr_size = data_block_builder2_.CurrentSizeEstimate();

    // Do flush if one of the below two conditions is true:
    // 1) if the current estimated size already exceeds the block size,
    // 2) block_size_deviation is set and the estimated size after appending
    // the kv will exceed the block size and the current size is under the
    // the deviation.
    // return curr_size >= block_size_ || BlockAlmostFull(key, value);
    return curr_size >= block_size_ || BlockAlmostFull2(value);
  }

 private:
  bool BlockAlmostFull(const Slice& key, const Slice& value) const {
    if (block_size_deviation_limit_ == 0) {
      return false;
    }

    const auto curr_size = data_block_builder_.CurrentSizeEstimate();
    auto estimated_size_after =
        data_block_builder_.EstimateSizeAfterKV(key, value);

    if (align_) {
      estimated_size_after += BlockBasedTable::kBlockTrailerSize;
      return estimated_size_after > block_size_;
    }

    return estimated_size_after > block_size_ &&
           curr_size > block_size_deviation_limit_;
  }
  // func: 加入下一个value后，判断是否达到了block_size，以适配我们的逻辑
  bool BlockAlmostFull2(const Slice& value){
    if(block_size_deviation_limit_  == 0){
      return false;
    }
    const auto curr_size = data_block_builder2_.CurrentSizeEstimate();
    auto estimated_size_after = 
        data_block_builder2_.EstimateSizeAfterValue(value);
    // 可以看到，只有用了block_align策略，才会在计算block是否应该flush的时候加入末尾的5bytes
    // 如果是非block_align策略，block仅仅是用来分区管理的东西，不用考虑对齐，自然不需要加入这5bytes
    if(align_){
        // 预估大小要加上kBlockTrailerSize，一共5byte。包括1byte的compression type和4byte的checksum
        estimated_size_after += NvmTable::kBlockTrailerSize;
        return estimated_size_after > block_size_;
    }
    return estimated_size_after > block_size_ &&
           curr_size > block_size_deviation_limit_;
  }

  const uint64_t block_size_;
  const uint64_t block_size_deviation_limit_;
  const bool align_;
  const BlockBuilder& data_block_builder_;
  const NvmDataBlockBuilder& data_block_builder2_;
};

FlushBlockPolicy* FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
    const BlockBasedTableOptions& table_options,
    const BlockBuilder& data_block_builder) const {
  NvmDataBlockBuilder data_block_builder2;
  return new FlushBlockBySizePolicy(
      table_options.block_size, table_options.block_size_deviation,
      table_options.block_align, data_block_builder,data_block_builder2);
}

FlushBlockPolicy* FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
        const BlockBasedTableOptions& table_options,
        const NvmDataBlockBuilder& data_block_builder2) const {
  BlockBuilder data_block_builder(10);
  return new FlushBlockBySizePolicy(
      table_options.block_size, table_options.block_size_deviation,
      table_options.block_align,data_block_builder,data_block_builder2);  
}

FlushBlockPolicy* FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
    const uint64_t size, const int deviation,
    const BlockBuilder& data_block_builder) {
  NvmDataBlockBuilder data_block_builder2;
  return new FlushBlockBySizePolicy(size, deviation, false, data_block_builder,data_block_builder2);
}

#ifndef ROCKSDB_LITE
static int RegisterFlushBlockPolicyFactories(ObjectLibrary& library,
                                             const std::string& /*arg*/) {
  library.AddFactory<FlushBlockPolicyFactory>(
      FlushBlockBySizePolicyFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<FlushBlockPolicyFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new FlushBlockBySizePolicyFactory());
        return guard->get();
      });
  library.AddFactory<FlushBlockPolicyFactory>(
      FlushBlockEveryKeyPolicyFactory::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<FlushBlockPolicyFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new FlushBlockEveryKeyPolicyFactory());
        return guard->get();
      });
  return 2;
}
#endif  // ROCKSDB_LITE

static bool LoadFlushPolicyFactory(
    const std::string& id, std::shared_ptr<FlushBlockPolicyFactory>* result) {
  if (id.empty()) {
    result->reset(new FlushBlockBySizePolicyFactory());
#ifdef ROCKSDB_LITE
  } else if (id == FlushBlockBySizePolicyFactory::kClassName()) {
    result->reset(new FlushBlockBySizePolicyFactory());
  } else if (id == FlushBlockEveryKeyPolicyFactory::kClassName()) {
    result->reset(new FlushBlockEveryKeyPolicyFactory());
#endif  // ROCKSDB_LITE
  } else {
    return false;
  }
  return true;
}

FlushBlockBySizePolicyFactory::FlushBlockBySizePolicyFactory()
    : FlushBlockPolicyFactory() {}

Status FlushBlockPolicyFactory::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<FlushBlockPolicyFactory>* factory) {
#ifndef ROCKSDB_LITE
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterFlushBlockPolicyFactories(*(ObjectLibrary::Default().get()), "");
  });
#endif  // ROCKSDB_LITE
  return LoadSharedObject<FlushBlockPolicyFactory>(
      config_options, value, LoadFlushPolicyFactory, factory);
}
}  // namespace ROCKSDB_NAMESPACE
