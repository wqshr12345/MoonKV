//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef NVM_TABLE_BUILDER_H_
#define NVM_TABLE_BUILDER_H_
#pragma once
#include <stdint.h>

#include <array>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "db/version_edit.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

class NvmDataBlockBuilder;
class BlockHandle;
class WritableFile;
struct BlockBasedTableOptions;

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;

class NvmTableBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  NvmTableBuilder(const BlockBasedTableOptions& table_options,
                         const TableBuilderOptions& table_builder_options,
                         WritableFileWriter* file);

  // No copying allowed
  NvmTableBuilder(const NvmTableBuilder&) = delete;
  NvmTableBuilder& operator=(const NvmTableBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~NvmTableBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  void Add(const Slice& key, const Slice& value, bool real_value) override {};

  uint64_t Add2(const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Return non-ok iff some error happens during IO.
  IOStatus io_status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  bool IsEmpty() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  uint64_t TableFileSize() const override{ return 0; };

  // Estimated size of the file generated so far. This is used when
  // FileSize() cannot estimate final SST size, e.g. parallel compression
  // is enabled.
  uint64_t EstimatedFileSize() const override;

  bool NeedCompact() const override;

  // Get table properties
  TableProperties GetTableProperties() const override;

  // Get file checksum
  std::string GetFileChecksum() const override;

  // Get file checksum function name
  const char* GetFileChecksumFuncName() const override;

  void SetSeqnoTimeTableProperties(
      const std::string& encoded_seqno_to_time_mapping,
      uint64_t oldest_ancestor_time) override;

 private:
  bool ok() const { return status().ok(); }

  // Transition state from buffered to unbuffered. See `Rep::State` API comment
  // for details of the states.
  // REQUIRES: `rep_->state == kBuffered`
  // Noused
  void EnterUnbuffered();

  // Call block's Finish() method and then
  // - in buffered mode, buffer the uncompressed block contents.
  // - in unbuffered mode, write the compressed block contents to file.
  // func: 将内存中的value按照block大小刷入磁盘
  void WriteBlock(NvmDataBlockBuilder* block, BlockHandle* handle,
                  BlockType blocktype);

  // Compress and write block content to the file.
  // func: 同上，是上一个函数会调用的函数。将一个block大小的内容压缩后(如有必要)刷入磁盘
  void WriteBlock(const Slice& block_contents, BlockHandle* handle,
                  BlockType block_type);
  // Directly write data to the file.
  // func: 与文件系统交互，将block_contents中的内容真正写入磁盘
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle,
                     BlockType block_type, const Slice* raw_data = nullptr);

  // Noused
  // void SetupCacheKeyPrefix(const TableBuilderOptions& tbo);

  template <typename TBlocklike>
  Status InsertBlockInCache(const Slice& block_contents,
                            const BlockHandle* handle, BlockType block_type);

  Status InsertBlockInCacheHelper(const Slice& block_contents,
                                  const BlockHandle* handle,
                                  BlockType block_type);

  Status InsertBlockInCompressedCache(const Slice& block_contents,
                                      const CompressionType type,
                                      const BlockHandle* handle);

  void WriteFilterBlock(MetaIndexBuilder* meta_index_builder);
  void WriteIndexBlock(MetaIndexBuilder* meta_index_builder,
                       BlockHandle* index_block_handle);
  void WritePropertiesBlock(MetaIndexBuilder* meta_index_builder);
  // Noused
  void WriteCompressionDictBlock(MetaIndexBuilder* meta_index_builder);
  // Noused
  void WriteRangeDelBlock(MetaIndexBuilder* meta_index_builder);
  void WriteFooter(BlockHandle& metaindex_block_handle,
                   BlockHandle& index_block_handle);

  struct Rep;
  class BlockBasedTablePropertiesCollectorFactory;
  class BlockBasedTablePropertiesCollector;
  Rep* rep_;

  struct ParallelCompressionRep;

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  void Flush();

  // Some compression libraries fail when the raw size is bigger than int. If
  // uncompressed size is bigger than kCompressionSizeLimit, don't compress it
  const uint64_t kCompressionSizeLimit = std::numeric_limits<int>::max();

  // Get blocks from mem-table walking thread, compress them and
  // pass them to the write thread. Used in parallel compression mode only
  // Noused
  void BGWorkCompression(const CompressionContext& compression_ctx,
                         UncompressionContext* verify_ctx);

  // Given raw block content, try to compress it and return result and
  // compression type
  void CompressAndVerifyBlock(const Slice& raw_block_contents,
                              bool is_data_block,
                              const CompressionContext& compression_ctx,
                              UncompressionContext* verify_ctx,
                              std::string* compressed_output,
                              Slice* result_block_contents,
                              CompressionType* result_compression_type,
                              Status* out_status);

  // Get compressed blocks from BGWorkCompression and write them into SST
  // Noused
  void BGWorkWriteRawBlock();

  // Initialize parallel compression context and
  // start BGWorkCompression and BGWorkWriteRawBlock threads
  // Noused
  void StartParallelCompression();

  // Stop BGWorkCompression and BGWorkWriteRawBlock threads
  // Noused
  void StopParallelCompression();
};

Slice CompressBlock(const Slice& raw, const CompressionInfo& info,
                    CompressionType* type, uint32_t format_version,
                    bool do_sample, std::string* compressed_output,
                    std::string* sampled_output_fast,
                    std::string* sampled_output_slow);

}  // namespace ROCKSDB_NAMESPACE
#endif