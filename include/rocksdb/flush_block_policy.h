// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/table.h"
#include "nvm/table/nvm_data_block_builder.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class BlockBuilder;
struct ConfigOptions;
struct Options;

// FlushBlockPolicy provides a configurable way to determine when to flush a
// block in the block based tables.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class FlushBlockPolicy {
 public:
  // Keep track of the key/value sequences and return the boolean value to
  // determine if table builder should flush current data block.
  virtual bool Update(const Slice& key, const Slice& value) = 0;

  virtual ~FlushBlockPolicy() {}
};

class FlushBlockPolicyFactory : public Customizable {
 public:
  static const char* Type() { return "FlushBlockPolicyFactory"; }

  // Creates a FlushBlockPolicyFactory based on the input value.
  // By default, this method can create EveryKey or BySize PolicyFactory,
  // which take now config_options.
  static Status CreateFromString(
      const ConfigOptions& config_options, const std::string& value,
      std::shared_ptr<FlushBlockPolicyFactory>* result);

  // Return a new block flush policy that flushes data blocks by data size.
  // FlushBlockPolicy may need to access the metadata of the data block
  // builder to determine when to flush the blocks.
  //
  // Callers must delete the result after any database that is using the
  // result has been closed.
  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& table_options,
      const BlockBuilder& data_block_builder) const = 0;

  // func: 基于新的data block格式，构造FlushBlockPolicy
  // 增加这个函数的目的是因为原有函数存在和BlockBuilder的耦合，为了增加基于我们的NvmDataBlockBuilder的策略，必须增加一个对应的虚函数
  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& table_options,
      const NvmDataBlockBuilder& data_block_builder) const = 0;
  virtual ~FlushBlockPolicyFactory() {}
};

// func: FLushBlockPolicyFactory的一个实现，依据当前BLock size判断是否要flush，返回了默认使用的FLushBlockPolicy
// 这里工厂模式的实现是一个工厂类生产一种产品(耐克)，而不是一个工厂类生产所有产品(耐克、阿迪、安踏)
class FlushBlockBySizePolicyFactory : public FlushBlockPolicyFactory {
 public:
  FlushBlockBySizePolicyFactory();

  static const char* kClassName() { return "FlushBlockBySizePolicyFactory"; }
  const char* Name() const override { return kClassName(); }

  FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& table_options,
      const BlockBuilder& data_block_builder) const override;

  FlushBlockPolicy* NewFlushBlockPolicy(
        const BlockBasedTableOptions& table_options,
        const NvmDataBlockBuilder& data_block_builder) const override;

  static FlushBlockPolicy* NewFlushBlockPolicy(
      const uint64_t size, const int deviation,
      const BlockBuilder& data_block_builder);
};

}  // namespace ROCKSDB_NAMESPACE
