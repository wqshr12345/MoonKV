#include "nvm/partition/nvm_partition_builder.h"

#include <assert.h>
#include <stdio.h>

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>

#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "cache/cache_reservation_manager.h"
#include "db/dbformat.h"
#include "table/block_based/index_builder.h"
#include "logging/logging.h"
#include "memory/memory_allocator.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/table.h"
#include "rocksdb/types.h"
#include "table/block_based/block.h"
#include "nvm/table/nvm_data_block_builder.h"
#include "table/block_based/block_like_traits.h"
#include "table/block_based/filter_block.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/full_filter_block.h"
#include "table/block_based/partitioned_filter_block.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/work_queue.h"

#include "nvm/table/nvm_table_builder.h"
// #define PARTITION_BUILDER
// #include "nvm/index/btree_builder.h"
namespace ROCKSDB_NAMESPACE {

// func: 用作NvmPartitionBuilder的一些辅助变量

NvmPartitionBuilder::NvmPartitionBuilder(
    const BlockBasedTableOptions& table_options, const TableBuilderOptions& tbo,
    WritableFileWriter* file, uint64_t table_file_seq, uint64_t index_file_seq,
    std::vector<table_information_collect>& sub_run, bool gc_compaction, bool vertical_compaction) {
  BlockBasedTableOptions sanitized_table_options(table_options);
  // if (sanitized_table_options.format_version == 0 &&
  //     sanitized_table_options.checksum != kCRC32c) {
  //   ROCKS_LOG_WARN(
  //       tbo.ioptions.logger,
  //       "Silently converting format_version to 1 because checksum is "
  //       "non-default");
  //   // silently convert format_version to 1 to keep consistent with current
  //   // behavior
  //   sanitized_table_options.format_version = 1;
  // }

  rep_ = new Rep(sanitized_table_options, tbo, file, table_file_seq, index_file_seq, sub_run, gc_compaction, vertical_compaction);

//   TEST_SYNC_POINT_CALLBACK(
//       "NvmTableBuilder::NvmTableBuilder:PreSetupBaseCacheKey",
//       const_cast<TableProperties*>(&rep_->props));

  // Extremely large files use atypical cache key encoding, and we don't
  // know ahead of time how big the file will be. But assuming it's less
  // than 4TB, we will correctly predict the cache keys.
// WQTODO 关于SetupBaseCacheKey，暂时注释掉。未来如果需要用到再来回看以下
//   NvmPartitionBuilder::SetupBaseCacheKey(
//       &rep_->props, tbo.db_session_id, tbo.cur_file_num,
//       NvmTable::kMaxFileSizeStandardEncoding, &rep_->base_cache_key);
}

NvmPartitionBuilder::~NvmPartitionBuilder(){
    delete rep_;
}

// 这个接口不应该被调用
void NvmPartitionBuilder::Add(const Slice& key,const Slice& value){
  assert(false);   
}

// func: 构造一个partition的内容(包括index和data的构造/生成)
void NvmPartitionBuilder::Add(const Slice& key, const Slice& value, bool real_value) {
  // 统计当前kTypeMerge的数量
  ParsedInternalKey parsed_key;
  if(ParseInternalKey(key, &parsed_key,true) != Status::OK()){
    assert(false);
  }
  if(parsed_key.type == kTypeMerge) {
    rep_->merge_entries_ ++;
  }
  // 1. 如果是real value 则本次add的key value需要被添加到新的table中，同时将table的file number和当前的offset添加到index中
  if( real_value ){
    uint64_t value_offset = rep_->table_builder->Add2(value);
    rep_->index_builder->Add(key, rep_->GetTableFileSeq(), value_offset);
  }
  // 2. 如果非real value 则直接将本次的vaddr添加到index中
  else {
    rep_->index_builder->Add(key, value);
  }
}

Status NvmPartitionBuilder::status() const {
    return rep_->GetStatus();
}

IOStatus NvmPartitionBuilder::io_status() const {
    return rep_->GetIOStatus();
}

// func: 结束一个Partition的生成，在磁盘和nvm分别生成data和index
Status NvmPartitionBuilder::Finish(){
    Status s;
    // WQTODO 这二者并不一定是串行的，可以多线程优化？有无必要？
    // 1.磁盘文件的finish，构造metablock，并将所有文件内容刷到磁盘 只有当value_compaction或者vertical compaction时才有意义
    // WQTODO 减少原引用key的逻辑在哪里写？
    if(rep_->GetValueCompaction()||rep_->GetVerticalCompaction()){
      s = rep_->table_builder->Finish();
      std::vector<table_information_collect>* table_information = rep_->index_builder->GetInternalTableInform();
      // 目前来看只能用遍历 WQTOOD
      // 这里要用&，不然修改了值没办法作用到原来的地方orz
      for(auto& i : *table_information) {
      if(i.table_file_number == rep_->GetTableFileSeq()){
        i.table_file_size = rep_->table_builder->FileSize();
      }
      }
    }
    // 2.索引文件的finish，进行一些善后工作
    rep_->index_builder->Finish();
    // 3.索引文件的format，构造metadata等信息
    rep_->index_builder->Format();
    // 4.索引文件的flush，将所有内容刷到nvm
    rep_->index_builder->Flush();
    return s;
}

void NvmPartitionBuilder::Abandon(){
    assert(false);
}

uint64_t NvmPartitionBuilder::NumEntries() const {
    //return rep_->table_builder.NumEntries();
    // 我们用index_builder中的NumEntries，因为只有这个能代表本partition中的总的kv数 如果本次Partition
    //的构造不涉及value的compaction的话，那么根本不会调用table_builder，其中的NumEntries自然也为0
    return rep_->index_builder->NumEntries();
}
uint64_t NvmPartitionBuilder::TableNumEntries() const {
  assert(rep_->GetValueCompaction() || rep_->GetVerticalCompaction());
  return rep_->table_builder->NumEntries();
}

uint64_t NvmPartitionBuilder::MergeEntries() const {
  return rep_->merge_entries_;
}

bool NvmPartitionBuilder::IsEmpty() const {
    return rep_->index_builder->NumEntries() == 0;
}

uint64_t  NvmPartitionBuilder::FileSize() const {
  return rep_->index_builder->FileSize();
}

uint64_t NvmPartitionBuilder::TableFileSize() const {
  return rep_->table_builder->FileSize();
}

TableProperties NvmPartitionBuilder::GetTableProperties() const {
    if(rep_->GetValueCompaction()){
      return rep_->table_builder->GetTableProperties();
      // WQTODO 设计非value compaction情况下的TableProperties()
    }else {
      return TableProperties();
    }
}

bool NvmPartitionBuilder::NeedCompact() const {
  return false;
}

  std::vector<table_information_collect> *NvmPartitionBuilder::GetInternalTableInform() const{
    return rep_->index_builder->GetInternalTableInform();
  };

}