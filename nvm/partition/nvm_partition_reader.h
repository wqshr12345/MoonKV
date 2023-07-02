#pragma once

#include <cstdint>
#include <memory>

#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "cache/cache_reservation_manager.h"
#include "db/range_tombstone_fragmenter.h"
#include "file/filename.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table_properties.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_type.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/filter_block.h"
#include "table/block_based/uncompression_dict_reader.h"
#include "table/format.h"
#include "table/persistent_cache_options.h"
#include "table/table_properties_internal.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/coro_utils.h"
#include "util/hash_containers.h"
#include "db/version_edit.h"

#include "nvm/index/nvm_btree.h"
#include "nvm/table/nvm_table_reader.h"
#include  "rocksdb/cache.h"
namespace ROCKSDB_NAMESPACE {

class NvmPartition : public TableReader{
friend class NvmPartitionIterator;

public:
  // func: 创造一个NvmPartition对象，以便在工厂类中返回。这本质上相当于NvmPartition的一个构造函数
  static Status Open(
      const ReadOptions& ro, const ImmutableOptions& ioptions,
      const EnvOptions& env_options,
      const BlockBasedTableOptions& table_options,
      const InternalKeyComparator& internal_key_comparator,
      std::unique_ptr<RandomAccessFileReader>&& file,
      std::unique_ptr<TableReader>* table_reader,
      std::shared_ptr<CacheReservationManager> table_reader_cache_res_mgr =
          nullptr,
      const std::shared_ptr<const SliceTransform>& prefix_extractor = nullptr,
      bool prefetch_index_and_filter_in_cache = true, bool skip_filters = false,
      int level = -1, const bool immortal_table = false,
      const SequenceNumber largest_seqno = 0,
      bool force_direct_prefetch = false,
      TailPrefetchStats2* tail_prefetch_stats = nullptr,
      BlockCacheTracer* const block_cache_tracer = nullptr,
      size_t max_file_size_for_l0_meta_pin = 0,
      const std::string& cur_db_session_id = "", uint64_t cur_file_num = 0,TableCache* table_cache = nullptr);  
  ~NvmPartition();
  
  //func: 本iterator返回一个key为key，value为value的iter
  // 实际上，这里的7个参数不是必须的，只需要第一个参数就好了。
  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size = 0,
                                bool allow_unprepared_value = false) override;
  //func：本iterator返回一个key为key，value为offset的iter
  InternalIterator* NewIterator2(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size = 0,
                                bool allow_unprepared_value = false){ return nullptr; };

  //func： 无用函数。NvmPartition并不会真正实现这个函数
  Status Get(const ReadOptions& readOptions,const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false, const ImmutableOptions& ioptions = ImmutableOptions()) override;

  //func: 无用函数。仅在NvmTable中有用
  Status Get2(const ReadOptions& readOptions, const uint64_t& offset,
                     GetContext* get_context,Slice* value,
                     const SliceTransform* prefix_extractor,
                     bool skip_filters = false) override{return Status::OK();};

 //以下为暂时无用函数 仅仅是重载虚函数
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
        const ReadOptions& read_options) override {
    return nullptr;
  };
  Status Prefetch(const Slice* begin, const Slice* end) override{
    return Status::OK();
  };
  uint64_t ApproximateOffsetOf(const Slice& key,
                                TableReaderCaller caller) override{
    return 0;
  };
  uint64_t ApproximateSize(const Slice& start, const Slice& end,
                           TableReaderCaller caller) override{
    return 0;                          
  };

  Status ApproximateKeyAnchors(const ReadOptions& read_options,
                               std::vector<Anchor>& anchors) override{
    return Status::OK();
  };
  void SetupForCompaction() override{};

  std::shared_ptr<const TableProperties> GetTableProperties() const override{return nullptr;};

  size_t ApproximateMemoryUsage() const override{return 0;};

  Status DumpTable(WritableFile* out_file) override{return Status::OK();};

  Status VerifyChecksum(const ReadOptions& readOptions,
                        TableReaderCaller caller) override{return Status::OK();};

  uint64_t file_number;
protected:
  explicit NvmPartition(NvmBtree* nvm_btree,std::map<uint32_t,NvmTable*> nvm_tables, std::vector<Cache::Handle*> table_handles,TableCache* table_cache)
      : nvm_btree_(nvm_btree),nvm_tables_(nvm_tables),table_handles_(table_handles),table_cache_(table_cache) {}
  explicit NvmPartition(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
private:
  NvmBtree* nvm_btree_;
  std::map<uint32_t,NvmTable*> nvm_tables_;
  std::vector<Cache::Handle*> table_handles_;
  TableCache* table_cache_;
};

}