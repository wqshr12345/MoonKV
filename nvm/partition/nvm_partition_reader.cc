#include "nvm/partition/nvm_partition_reader.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "cache/sharded_cache.h"
#include "db/compaction/compaction_picker.h"
#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "file/file_prefetch_buffer.h"
#include "file/file_util.h"
#include "file/random_access_file_reader.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "port/lang.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/trace_record.h"
#include "table/block_based/binary_search_index_reader.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_iterator.h"
#include "table/block_based/block_like_traits.h"
#include "table/block_based/block_prefix_index.h"
#include "table/block_based/block_type.h"
#include "table/block_based/filter_block.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/full_filter_block.h"
#include "table/block_based/hash_index_reader.h"
#include "table/block_based/partitioned_filter_block.h"
#include "table/block_based/partitioned_index_reader.h"
#include "table/block_fetcher.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/multiget_context.h"
#include "table/persistent_cache_helper.h"
#include "table/persistent_cache_options.h"
#include "table/sst_file_writer_collectors.h"
#include "table/two_level_iterator.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

#include "nvm/index/btree_node.h"
#include "nvm/index/btree_iterator.h"
#include "nvm/partition/nvm_partition_iterator.h"

namespace ROCKSDB_NAMESPACE{

NvmPartition::~NvmPartition() {
  delete nvm_btree_;
  // 仅仅删除句柄就好。具体NvmTable的生命周期由cache控制。
  for(auto handle:table_handles_){
    if (handle != nullptr) {
      table_cache_->ReleaseHandle(handle);
    }
  }
}

Status NvmPartition::Open(
    const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const EnvOptions& env_options, const BlockBasedTableOptions& table_options,
    const InternalKeyComparator& internal_comparator,
    std::unique_ptr<RandomAccessFileReader>&& file,
    std::unique_ptr<TableReader>* table_reader,
    std::shared_ptr<CacheReservationManager> table_reader_cache_res_mgr,
    const std::shared_ptr<const SliceTransform>& prefix_extractor,
    const bool prefetch_index_and_filter_in_cache, const bool skip_filters,
    const int level, const bool immortal_table,
    const SequenceNumber largest_seqno, const bool force_direct_prefetch,
    TailPrefetchStats2* tail_prefetch_stats,
    BlockCacheTracer* const block_cache_tracer,
    size_t max_file_size_for_l0_meta_pin, const std::string& cur_db_session_id,
    uint64_t cur_file_num,TableCache* table_cache) {
  table_reader->reset();
  Status s;
  // 传统BlockBased或者Nvm的table，在Open的时候会提前打开Footer/Metablock/index block等，预取一下
  // 我们打开一个NvmPartition的时候，首先需要提前打开index file(NvmBtree)，将其mmap映射一下，减少系统调用开销
  // 然后提前打开本次用到的所有Table

  // 2.构造NvmPartition对象
  // 2.1 构造NvmBtree对象（cur_file_num参数即为file sequence） 其生命周期由NvmPartition控制
  NvmBtree* btree = new NvmBtree(cur_file_num, read_options, ioptions, internal_comparator);
  // 2.2 构造NvmTable对象
  // 从FindTable打开 过期问题由控制
  std::map<uint32_t,NvmTable*> nvm_tables;
  std::vector<Cache::Handle*> table_handles;
  std::vector<table_information_collect> table_info = btree->GetTableInformation();
  // 就这么设计吧。NvmPartition的构造函数生成各个table，然后析构函数释放。
  for(auto info:table_info){
    uint32_t table_number = info.table_file_number, file_size = info.table_file_size;
    NvmTable* table;
    Cache::Handle* handle = nullptr;
    // WQTODO 这里的fd理论上从VersionStorageInfo中拿最好，但是如果我这么做，对原有代码的侵入性太大了。所以直接构造一个新的fd
    // table file的路径一定是0
    FileDescriptor fd(table_number,0,file_size);
    s = table_cache->FindTable(read_options,table_cache->file_options_ /*file_options*/, internal_comparator,fd, &handle,
                      prefix_extractor,
                      read_options.read_tier == kBlockCacheTier /* no_io */,
                      false /* record_read_stats */,nullptr, /*file_read_hist*/ skip_filters,
                      -1/*level 对NvmTable来说无意义*/, true /* prefetch_index_and_filter_in_cache */,
                      max_file_size_for_l0_meta_pin,Temperature::kUnknown,true);
      if(s.ok()){
        // 将从cache中查到的value进行类型转换
        table = static_cast<NvmTable*>(table_cache->GetTableReaderFromHandle(handle));
      }
    nvm_tables.insert({table_number,table});
    table_handles.push_back(handle);
  }
  // 2.3 构造NvmPartition对象
  std::unique_ptr<NvmPartition> new_table(
    new NvmPartition(btree,nvm_tables,table_handles,table_cache));
  new_table->file_number = cur_file_num;
  // 中间可能可以加入一些预取的逻辑.
  // 3. 返回NvmPartition对象
  if (s.ok()) {
    *table_reader = std::move(new_table);
  }
  return s;
}

  Status NvmPartition::Get(
              const ReadOptions& readOptions, const Slice& key,
              GetContext* get_context,const SliceTransform* prefix_extractor,
                bool skip_filters, const ImmutableOptions& ioptions) {
    assert(key.size() >= 8);  // key must be internal key
    assert(get_context != nullptr);
    Status s;
    get_context->set_btree_iter(static_cast<BtreeIterator*>(nvm_btree_->NewIterator(readOptions,prefix_extractor,nullptr,false,kUserGet,0,false)));
    uint32_t seek_blocks = 0; // 本次查询涉及的blocks
    uint64_t elapsed = 0;
    uint32_t total_blocks = nvm_btree_->levels;
    StopWatch* sw1 = new StopWatch(ioptions.clock, ioptions.stats, DB_INDEX_GET, &elapsed);
    seek_blocks = get_context->EstimateSeekIfNeed(key, total_blocks, ioptions.enable_estimate_seek, ioptions.estimate_threshold, ioptions.clock, ioptions.stats);
    delete sw1;
    BtreeIterator* iter = get_context->get_btree_iter();
       //1.获取btree index的iterator
    //1.1 去index iterator获取本key的table file和offset。注意，此处应该使用UserKey
    
    // double estimate_position; // 本次estimate_search用到的估计位置
    // 如果当前是l0内的查询，则使用fake_estimation进行查询；否则，使用正常的position查询    
    // LAST_VERSION
    // if(get_context->get_level() == 0 && get_context->get_last_level() == 0){
    //   estimate_position = get_context->get_fake_estimate_position();
    // } else {
    // estimate_position = get_context->get_estimate_position();
    // }
    // 只有在三种情况下不会用EstimateSeek
    // 1.没有启用EstimateSeek
    // 2.当前level和上一level均在L0内（这是因为并发的l0 build 难以构造children_rank）
    // 3.当前level并不是上一level+1（这说明查询过程中跳过了一个level 此时children_rank预估的值无用
    
    // if(ioptions.enable_estimate_seek && get_context->is_match()) {
    //   get_context->get_context_stats_.estimate_search_hit += 1;
    //   seek_blocks = iter->EstimateSeek(key, estimate_position, LinearDetection);
    //   #ifndef NDEBUG
    //   std::cout <<"this position " << estimate_position << std::endl;
    //   #endif
    // } else {
    //   get_context->get_context_stats_.estimate_search_miss += 1;
    //   iter->Seek(key);
    //   seek_blocks = total_blocks;
    // }
    // if(!ioptions.enable_estimate_seek || (get_context->get_level() == 0 && get_context->get_last_level() == 0) || get_context->get_level() != get_context->get_last_level()+1){
    //   iter->Seek(key);
    //   seek_blocks = total_blocks;
    // } else {
    //   seek_blocks = iter->EstimateSeek(key, estimate_position, LinearDetection);
    //   #ifndef NDEBUG
    //   std::cout <<"this position " << estimate_position << std::endl;
    //   #endif
    // }
    // LAST_VERSION
    // uint32_t seek_blocks = iter->EstimateSeek(key, estimate_position);
    #ifndef NDEBUG
    std::cout << "read seek_blocks "<< seek_blocks << std::endl;
    #endif
    // 增加相关统计信息

    // uint32_t position = iter->GetKeyPosition(0);
    // uint32_t level3_position = iter->GetKeyPosition(3);
    // 设置fake_estimation为当前position在当前file内的区间
    // if (get_context->get_level() == 0) {
    //   get_context->set_fake_estimate_position(iter->GetKeyPosition(3));
    // }
    // if(ioptions.enable_estimate_seek) {
    //   StopWatch* sw7 = new StopWatch(ioptions.clock,ioptions.stats,DB_CHILDREN_RANK_GET);
    //   // 设置estimation
    //   // 设置在当前level判断的estimate_position_
    //   std::vector<PositionKeyList> children_ranks = get_context->get_children_ranks();
    //   // 1.如果当前children_rank为空，或者当前position已经超过下一个children_rank的范围，那么设置position为-1，直接退出
    //   // LAST_VERSION 只是把所有的first替换为front().list_.LowerLevelPos 所有的second替换为back().list_.LowerLevelPos
    //   // if(children_ranks.size() == 0 || position < children_ranks.front().first || position > children_ranks.back().second) {
    //   if(children_ranks.size() == 0 || position < children_ranks.front().front().LowerLevelPos || position > children_ranks.back().back().LowerLevelPos) {
    //     get_context->set_estimate_position(-1);
    //   } else {
    //     // 2.二分 寻找children_ranks中第一个back值大于等于position的区间
    //     int left = 0, right = children_ranks.size() - 1;
    //     while(left < right) {
    //       int mid = (left + right) >> 1;
    //       // if(children_ranks[mid].first > position) {
    //       if(children_ranks[mid].back().LowerLevelPos >= position) {
    //         right = mid;
    //       } else {
    //         left = mid + 1;
    //       }
    //     }
    //     // // 2.1 如果position在children_ranks[left].first和children_ranks[left].second之间，那么在这个区间之内估计
    //     // // 否则，需要将left--，在其前面那个区间估计
    //     // // if(children_ranks[left].first > position) {
    //     // if(children_ranks[left].front().LowerLevelPos > position) {
    //     //   left--;
    //     // }
    //     // 2.2 在查找到的区间内估计position
    //     // 2.2.1 如果当前position比查找到的区间的最小值还小,那么它不会在下一level被查到
    //     // 此时应该设置estimate_position为-1 这是为了避免在下下level查找时，错误的estimate_position造成错误的查找
    //     // if(position > children_ranks[left].second) {
    //     if(position < children_ranks[left].front().LowerLevelPos) {
    //       get_context->set_estimate_position(-1);
    //     } 
    //     // 2.2.2 如果当前position在查找到的区间之内 那么它会在下一level被查到，需要在这个区间内估计
    //     else {
    //       // if (children_ranks[left].first == children_ranks[left].second) {
    //       // if (children_ranks[left].front().LowerLevelPos == children_ranks[left].back().LowerLevelPos) {
    //       //   get_context->set_estimate_position(-1);
    //       // }
    //       get_context->set_estimate_position(children_ranks[left].GetEstimatePos(key.ToString(), position));
    //       // LAST_VERSION
    //       // get_context->set_estimate_position(iter->GetKeyPosition(children_ranks[left].first,children_ranks[left].second,position));
    //     }
    //   }
    //   delete sw7;
    // }
    bool matched = false; 
    StopWatch* sw6 = new StopWatch(ioptions.clock,ioptions.stats,DB_ITER_NEXT_GET);
    // WQTODOIMP 这里的num_seek_levels的语义是查过一次level就算？不管查没查到？
    // 如果不管查没查到就算，那么就应该在这里++，否则应该在查到的时候++
    bool seek = false;
    get_context->get_context_stats_.num_seek_levels += 1;
    for (; iter->Valid(); iter->Next()) {
      seek = true;
      StopWatch* sw4 = new StopWatch(ioptions.clock, ioptions.stats, DB_FIND_TABLE_GET);
      ParsedInternalKey parsed_key;
      Status pik_status =
          ParseInternalKey(iter->key(), &parsed_key, false /* log_err_key */);
      if (!pik_status.ok()) {
        s = pik_status;
      }

      // 2. 根据从index获得到的file
      // seq和offset，去磁盘查找具体文件得到结果，并存入get_context中。
      uint32_t file_seq = iter->value_file_seq();
      uint64_t offset = iter->value_offset();
      TableReader* nvm_table = nullptr;
      Cache::Handle* handle = nullptr;
      // 2.1 使用FindTable方法，在cache中查找是否有对应的NvmTable对象
      nvm_table = nvm_tables_.find(file_seq)->second;
      delete sw4;
      Slice value;
      //2.3 调用Get2接口，将真实的value存储到value对象中 
      //注意，这里的Get2虽然也传入了get_context，但是它在此处仅负责统计信息，不参与value的存储
      StopWatch* sw2 = new StopWatch(ioptions.clock, ioptions.stats, DB_DATA_GET);
      nvm_table->Get2(readOptions,offset,get_context,&value,nullptr,skip_filters);
      delete sw2;
      // 2.4将value对象传给get_context，完成一次查询
      // 找不到完整的值(Merge)返回true;找到一个完整的值，或者找到下一个user_key了还没找到，返回false。此时需要在下一个level继续查找
      StopWatch* sw5 = new StopWatch(ioptions.clock, ioptions.stats, DB_SAVE_VALUE_GET);
      bool temp = get_context->SaveValue(parsed_key, value, &matched,
                                  iter->IsValuePinned() ? iter : nullptr);
      delete sw5;
      StopWatch sw3(ioptions.clock,ioptions.stats,DB_POSITION_GET);
      //  如果找到full value，那么直接break
      if (!temp) {


        // // 2. 存在第一个大于等于position的值的区间，该下标的前一个
        // for(size_t i = 0;i<children_ranks.size();i++){
        //   if(position <= children_ranks[i]){
        //     // 增加这个额外的if判断是为了避免当target == smallest key时的特殊情况
        //     if(i == 0){
        //       get_context->set_estimate_position(iter->GetKeyPosition(children_ranks[i],children_ranks[i+1],position));
        //     } else {
        //       get_context->set_estimate_position(iter->GetKeyPosition(children_ranks[i-1],children_ranks[i],position));
        //     }
        //     break;
        //   }
        // }
        break;
      }
    }
    if (seek) {
      get_context->get_context_stats_.num_seek_times += 1;
    }
    delete sw6;
    s = iter->status();
    // delete iter; // 无需在此时删除 非最后file会在estiamte_seek()中delete 最后file会在GetContext的析构函数中delete

    return s;
  }

  // void NvmPartition::GetValueFromTable(uint32_t file_seq,uint64_t offset,TableCache* table_cache){
  //         //2. 根据从index获得到的file seq和offset，去磁盘查找具体文件得到结果，并存入get_context中。
  //     TableReader* nvm_table;
  //     Cache::Handle* handle = nullptr;
  //     //2.1 使用FindTable方法，在cache中查找是否有对应的NvmTable对象

  //     //WQTODO 这里要不要再引入一层缓存(比如一个map)？类似Fmd中的table_reader对象一样，这样可以减少一次cache的查找
  //     //不过这样做的好处并不大，多查一次cache应该没有太大坏处。另外， 我再引入一个map不仅增加代码复杂度，而且map中的
  //     //TableReader*我无法感知它是否还有效，直接从cache中拿得了。
  //     // FindTable函数的FileDescriptor参数基本无用。这个参数传进去就是得到一个number和一个Temperature的，我直接从这里得到
  //     // WQTODO 如果未来为每一个table构造一个FileMetadata的话，这里可以增加根据file_seq获得filemetadata进而获得fd的逻辑
  //     // 这样的话就无需在FindTable的最后面传入file_seq参数
  //     // WQTODO 为了测试写死的东西，以后再改
  //     //WQTODOIMP 这里的文件路径...不一定是0，政洪的那个设置，要改一下!!!
  //     // FileDescriptor fd = version
  //     FileDescriptor& fd = table_cache
  //     s = table_cache->FindTable(readOptions, file_options_, internal_comparator,fd, &handle,
  //                     prefix_extractor,
  //                     readOptions.read_tier == kBlockCacheTier /* no_io */,
  //                     true /* record_read_stats */, file_read_hist, skip_filters,
  //                     level, true /* prefetch_index_and_filter_in_cache */,
  //                     max_file_size_for_l0_meta_pin,Temperature::kUnknown,true);
  //     if(s.ok()){
  //       // 将从cache中查到的value进行类型转换
  //       nvm_table = table_cache->GetTableReaderFromHandle(handle);
  //     }

  //     Slice value;
  //     //2.3 调用Get2接口，将真实的value存储到value对象中 
  //     //注意，这里的Get2虽然也传入了get_context，但是它在此处仅负责统计信息，不参与value的存储
  //     nvm_table->Get2(readOptions,offset,get_context,&value,nullptr,skip_filters);
  //     // 释放针对NvmTable的句柄，因为这是从Cache中拿到的
  //     if (handle != nullptr) {
  //       table_cache->ReleaseHandle(handle);
  //     }
  // }
  InternalIterator* NvmPartition::NewIterator(const ReadOptions& read_options,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size,
                                bool allow_unprepared_value){
 if (arena == nullptr) {
    return new NvmPartitionIterator(
        this, read_options, nvm_btree_->icomp_);
  } else {
    auto* mem = arena->AllocateAligned(sizeof(NvmPartitionIterator));
    return new (mem) NvmPartitionIterator(
        this, read_options, nvm_btree_->icomp_);
  }
  }
}