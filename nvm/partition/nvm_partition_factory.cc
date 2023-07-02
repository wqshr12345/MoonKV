#include <stdint.h>

#include <cinttypes>
#include <memory>
#include <string>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "logging/logging.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_type.h"

#include "nvm/partition/nvm_partition_factory.h"
#include "nvm/partition/nvm_partition_reader.h"
#include "nvm/partition/nvm_partition_builder.h"

#include "nvm/table/nvm_table_factory.h"

namespace ROCKSDB_NAMESPACE {
    NvmPartitionFactory::NvmPartitionFactory(
        const BlockBasedTableOptions& _table_options)
        : table_options_(_table_options){
      // 这里的初始化不是必要的。因为我们的NvmTable是通过其second_table_factory生成的，所以其option在那里自己初始化
      //InitializeOptions();
    //   RegisterOptions(&table_options_, &nvm_table_type_info);
    // 构造函数貌似什么也不需要做...因为核心功能在NvmTable中已经实现了
    }

    void NvmPartitionFactory::InitializeOptions() {
    if (table_options_.flush_block_policy_factory == nullptr) {
      table_options_.flush_block_policy_factory.reset(
          new FlushBlockBySizePolicyFactory());
    }
    // NvmPartition用不到block_cache
    // if (table_options_.no_block_cache) {
    //   table_options_.block_cache.reset();
    // } 
    // else if (table_options_.block_cache == nullptr) {
    //   LRUCacheOptions co;
    //   co.capacity = 8 << 20;
    //   // It makes little sense to pay overhead for mid-point insertion while the
    //   // block size is only 8MB.
    //   co.high_pri_pool_ratio = 0.0;
    //   table_options_.block_cache = NewLRUCache(co);
    // }
    if (table_options_.block_size_deviation < 0 ||
        table_options_.block_size_deviation > 100) {
      table_options_.block_size_deviation = 0;
    }
    if (table_options_.block_restart_interval < 1) {
      table_options_.block_restart_interval = 1;
    }
    if (table_options_.index_block_restart_interval < 1) {
      table_options_.index_block_restart_interval = 1;
    }
    if (table_options_.index_type == BlockBasedTableOptions::kHashSearch &&
        table_options_.index_block_restart_interval != 1) {
      // Currently kHashSearch is incompatible with index_block_restart_interval > 1
      table_options_.index_block_restart_interval = 1;
    }
    if (table_options_.partition_filters &&
        table_options_.index_type !=
            BlockBasedTableOptions::kTwoLevelIndexSearch) {
      // We do not support partitioned filters without partitioning indexes
      table_options_.partition_filters = false;
    }
    auto& options_overrides =
        table_options_.cache_usage_options.options_overrides;
    const auto options = table_options_.cache_usage_options.options;
    for (std::uint32_t i = 0; i < kNumCacheEntryRoles; ++i) {
      CacheEntryRole role = static_cast<CacheEntryRole>(i);
      auto options_overrides_iter = options_overrides.find(role);
      if (options_overrides_iter == options_overrides.end()) {
        options_overrides.insert({role, options});
      } else if (options_overrides_iter->second.charged ==
                CacheEntryRoleOptions::Decision::kFallback) {
        options_overrides_iter->second.charged = options.charged;
      }
    }
  }

    Status NvmPartitionFactory::NewTableReader2(
    const ReadOptions& ro, const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file,
    std::unique_ptr<TableReader>* table_reader,TableCache* table_cache,
    bool prefetch_index_and_filter_in_cache) const {
        // 返回一个使用Open函数构造的NvmPartition对象
        // 在Open函数中，会首先构造一个NvmBtree对象；但不会生成NvmTable对象
        // NvmTable对象是在后续的Get流程中会用到的，随用生成，生命周期由Cache维护
        return NvmPartition::Open(
            ro, table_reader_options.ioptions, table_reader_options.env_options,
            table_options_, table_reader_options.internal_comparator, std::move(file),
            table_reader, table_reader_cache_res_mgr_,
            table_reader_options.prefix_extractor, prefetch_index_and_filter_in_cache,
            table_reader_options.skip_filters, table_reader_options.level,
            table_reader_options.immortal, table_reader_options.largest_seqno,
            table_reader_options.force_direct_prefetch,nullptr,
            table_reader_options.block_cache_tracer,
            table_reader_options.max_file_size_for_l0_meta_pin,
            table_reader_options.cur_db_session_id,
            table_reader_options.cur_file_num,table_cache);
    }   

    TableBuilder* NvmPartitionFactory::NewTableBuilder(
        const TableBuilderOptions& table_builder_options,
        WritableFileWriter* file, uint64_t table_file_seq,
        uint64_t index_file_seq,
        std::vector<table_information_collect>& sub_run,
        bool gc_compaction, bool vertical_compaction) const {
      return new NvmPartitionBuilder(table_options_, table_builder_options,
                                     file, table_file_seq, index_file_seq,
                                     sub_run, gc_compaction, vertical_compaction);
    }

    TableFactory* NewNvmPartitionFactory(
    const BlockBasedTableOptions& _table_options) {
  return new NvmPartitionFactory(_table_options);
    }
}