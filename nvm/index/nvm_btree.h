#pragma once

#include <iostream>
#include <string>

#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

const uint32_t NVM_BLOCK_SIZE = 256;
const uint64_t BTREE_FILE_MAGIC_NUMBER = 114514;

struct btree_file_meta {
  uint32_t nrun;         // number of runs be indexed
  uint32_t sub_run_off;  //存索引的所有Table的Sequence Number
  uint32_t
      table_valid_key_num_off;  //存每个Table中引用的key的数量，和sub_run_off中的Table是一一对应的
  uint32_t
      table_total_size_off;  //存每个Table的文件整体大小，依然和sub_run_off中的Table一一对应

  uint32_t first_last_key_offset;  // 存放此索引文件的first和last key的offset

  uint32_t seq;            // the table file's sequence number
  uint32_t total_kv;       // include invalid kv
  uint32_t index_off;      // head node of B+ tree
  uint32_t leaf_node_num;  // number of leaf node which store the value position
  uint32_t levels;         // B+tree's total levels
  uint32_t file_size;
  uint64_t magic;
};

class NvmBtree : public TableReader {
  friend class BtreeIterator;
  friend class BtreePortionIterator;
  friend class NvmPartition;

 private:
  void* pmemaddr;
  btree_file_meta* meta;

  void* index_off;
  uint32_t leaf_node_num;
  uint8_t levels;

  std::string first_key;
  std::string last_key;

  std::vector<uint32_t> sub_runs;
  std::vector<uint32_t> table_valid_key_num;
  std::vector<uint32_t> table_total_size;

  const ReadOptions& read_options_;
  const ImmutableOptions &ioptions_;
  const InternalKeyComparator& icomp_;
  UserComparatorWrapper user_comparator_;

  uint64_t file_size_;
  void* get_leaf_node_addr(uint32_t n) {
    return (uint8_t*)pmemaddr + (n * NVM_BLOCK_SIZE);
  }

 public:
  NvmBtree(uint32_t seq, const ReadOptions& read_options,
           const ImmutableOptions& ioptions,
           const InternalKeyComparator& internal_key_comparator);
  ~NvmBtree();

  InternalIterator* NewIterator(const ReadOptions& read_options,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size,
                                bool allow_unprepared_value) override;

  InternalIterator* NewPortionIterator(const ReadOptions& read_options,
                                       const SliceTransform* prefix_extractor,
                                       Arena* arena, bool skip_filters,
                                       TableReaderCaller caller,
                                       size_t compaction_readahead_size,
                                       bool allow_unprepared_value,
                                       std::vector<uint32_t>& select_tables);

  uint64_t ApproximateOffsetOf(const Slice& key,
                               TableReaderCaller caller) override;
  uint64_t ApproximateSize(const Slice& start, const Slice& end,
                           TableReaderCaller caller) override;
  void SetupForCompaction() override;
  std::shared_ptr<const TableProperties> GetTableProperties() const override;
  size_t ApproximateMemoryUsage() const override;
  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters, const ImmutableOptions& ioptions = ImmutableOptions()) override;
  Status Get2(const ReadOptions& readOptions, const uint64_t& offset,
              GetContext* get_context, Slice* value,
              const SliceTransform* prefix_extractor,
              bool skip_filters) override;

  /*
   * 在打开Btree文件之后，用于返回其对应的所有Table的基本信息
   * 返回的信息格式又table_information_collect设置，可以灵活变更
   */
  std::vector<table_information_collect> GetTableInformation();
};

}  // namespace ROCKSDB_NAMESPACE