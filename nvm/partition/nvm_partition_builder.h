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
#define PARTITION_BUILDER
#include "nvm/index/btree_builder.h"
#include "util/compression.h"


namespace ROCKSDB_NAMESPACE {

class BlockBuilder;
class BlockHandle;
class WritableFile;
struct BlockBasedTableOptions;

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;

class NvmPartitionBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  // 理论上来说 file中的file_name包含了table file
  // seq，但是它没有提供一个接口返回 然而在构造index file的时候需要这个table
  // file seq。所以不得不在这里传入
  // 如果本次value_compaction为true，file和table_file_seq才有意义
  // file: 指向本次要生成的table file的文件指针(如果有value_compaction)
  NvmPartitionBuilder(const BlockBasedTableOptions& table_options,
                      const TableBuilderOptions& table_builder_options,
                      WritableFileWriter* file, uint64_t table_file_seq_,
                      uint64_t index_file_seq,
                      std::vector<table_information_collect>& sub_run,
                      bool gc_compaction, bool vertical_compaction);

  // No copying allowed
  NvmPartitionBuilder(const NvmPartitionBuilder&) = delete;
  NvmPartitionBuilder& operator=(const NvmPartitionBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~NvmPartitionBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  void Add(const Slice& key, const Slice& value, bool real_value) override;

  uint64_t Add2(const Slice& value) override{
    return 0;
  }
  
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

  uint64_t TableNumEntries() const override;

  uint64_t MergeEntries() const override;

  bool IsEmpty() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  uint64_t TableFileSize() const;

  bool NeedCompact() const override;

  void SetSeqnoTimeTableProperties(
      const std::string& encoded_seqno_to_time_mapping,
      uint64_t oldest_ancestor_time) override{};
    
  // WQTODO 这个要返回谁的TableProperties？返回table的不太合适。但是index又没有这个东西
  TableProperties GetTableProperties() const override;

  // WQTODO 设计这个函数
  std::string GetFileChecksum() const override{
    return "";
  };
  // WQTODO 设计这个函数
  const char* GetFileChecksumFuncName() const override{
    return "";
  };

  std::vector<table_information_collect> *GetInternalTableInform() const;
 private:
  bool ok() const { return status().ok(); }
  
  struct Rep;
  Rep* rep_;


  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  void Flush();

  // Some compression libraries fail when the raw size is bigger than int. If
  // uncompressed size is bigger than kCompressionSizeLimit, don't compress it
  const uint64_t kCompressionSizeLimit = std::numeric_limits<int>::max();
};
struct NvmPartitionBuilder::Rep {
  const ImmutableOptions ioptions;
  const MutableCFOptions moptions;
  const BlockBasedTableOptions table_options;
  TableBuilder* table_builder = nullptr;
  BtreeBuilder* index_builder = nullptr;
  uint64_t merge_entries_ = 0; // 统计本次构造的Partition中的merge_entries_数量(理论上应该放在IndexBuilder中统计，但是太麻烦了...先这样吧)

 Status GetStatus() {
    // We need to make modifications of status visible when status_ok is set
    // to false, and this is ensured by status_mutex, so no special memory
    // order for status_ok is required.
    if (status_ok.load(std::memory_order_relaxed)) {
      return Status::OK();
    } else {
      return CopyStatus();
    }
  }

  Status CopyStatus() {
    std::lock_guard<std::mutex> lock(status_mutex);
    return status;
  }

  IOStatus GetIOStatus() {
    // We need to make modifications of io_status visible when status_ok is set
    // to false, and this is ensured by io_status_mutex, so no special memory
    // order for io_status_ok is required.
    if (io_status_ok.load(std::memory_order_relaxed)) {
      return IOStatus::OK();
    } else {
      return CopyIOStatus();
    }
  }

  IOStatus CopyIOStatus() {
    std::lock_guard<std::mutex> lock(io_status_mutex);
    return io_status;
  }

  // Never erase an existing status that is not OK.
  void SetStatus(Status s) {
    if (!s.ok() && status_ok.load(std::memory_order_relaxed)) {
      // Locking is an overkill for non compression_opts.parallel_threads
      // case but since it's unlikely that s is not OK, we take this cost
      // to be simplicity.
      std::lock_guard<std::mutex> lock(status_mutex);
      status = s;
      status_ok.store(false, std::memory_order_relaxed);
    }
  }

  // Never erase an existing I/O status that is not OK.
  // Calling this will also SetStatus(ios)
  void SetIOStatus(IOStatus ios) {
    if (!ios.ok() && io_status_ok.load(std::memory_order_relaxed)) {
      // Locking is an overkill for non compression_opts.parallel_threads
      // case but since it's unlikely that s is not OK, we take this cost
      // to be simplicity.
      std::lock_guard<std::mutex> lock(io_status_mutex);
      io_status = ios;
      io_status_ok.store(false, std::memory_order_relaxed);
    }
    SetStatus(ios);
  }
  bool GetValueCompaction(){
    return gc_compaction_;
  }

  bool GetVerticalCompaction(){
    return vertical_compaction_;
  }

  uint64_t GetTableFileSeq() { return table_file_seq_; }
  Rep(const BlockBasedTableOptions& table_opt, const TableBuilderOptions& tbo,
      WritableFileWriter* f, uint64_t table_file_seq, uint64_t index_file_seq,
      std::vector<table_information_collect>& sub_run, bool gc_compaction,bool vertical_compaction)
      : ioptions(tbo.ioptions),
        moptions(tbo.moptions),
        table_options(table_opt),
        index_builder(new BtreeBuilder(index_file_seq, tbo, sub_run)),
        gc_compaction_(gc_compaction),
        vertical_compaction_(vertical_compaction),
        table_file_seq_(table_file_seq) {
    // 只有当触发gc compaction或者vertical compaction的时候，才会构造table_builder
    if (gc_compaction_ || vertical_compaction_) {
      // 注意，这里table_builder实质上用的就是NvmPartitionFactory中得到的tbo
      table_builder = ioptions.second_table_factory->NewTableBuilder(tbo, f);
    }
  }

  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;
  ~Rep(){
    if(table_builder)
      delete table_builder;
    if(index_builder)
      delete index_builder;
  }

 private:
  // Synchronize status & io_status accesses across threads from main thread,
  // compression thread and write thread in parallel compression.
  std::mutex status_mutex;
  std::atomic<bool> status_ok;
  Status status;
  std::mutex io_status_mutex;
  std::atomic<bool> io_status_ok;
  IOStatus io_status;
  bool gc_compaction_ = false;
  bool vertical_compaction_ = false;
  uint64_t table_file_seq_ = 0; // 仅当gc_compaction || vertical_compaction时有效
};

}  // namespace ROCKSDB_NAMESPACE
