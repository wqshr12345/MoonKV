//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "db/blob/blob_file_builder.h"
#include "db/compaction/compaction_iterator.h"
#include "db/event_helpers.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/output_validator.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"

#include "nvm/partition/nvm_partition_builder.h"
#include "nvm/partition/nvm_partition_iterator.h"

namespace ROCKSDB_NAMESPACE {

class TableFactory;
TableBuilder* NewTableBuilder(const TableBuilderOptions& tboptions,
                              WritableFileWriter* file) {
  assert((tboptions.column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         tboptions.column_family_name.empty());
  return tboptions.ioptions.table_factory->NewTableBuilder(tboptions, file);
}
TableBuilder* NewTableBuilder(const TableBuilderOptions& tboptions,
                              WritableFileWriter* file, uint64_t table_file_seq,
                              uint64_t index_file_seq,
                              std::vector<table_information_collect>& sub_run,
                              bool gc_compaction,bool vertical_compaction) {
  assert((tboptions.column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         tboptions.column_family_name.empty());
  // return tboptions.ioptions.table_factory->NewTableBuilder(tboptions, file);
  return tboptions.ioptions.table_factory->NewTableBuilder(
      tboptions, file, table_file_seq, index_file_seq, sub_run,
      gc_compaction,vertical_compaction);
}

Status BuildTable(
    const std::string& dbname, VersionSet* versions,
    const ImmutableDBOptions& db_options, const TableBuilderOptions& tboptions,
    const FileOptions& file_options, TableCache* table_cache,
    InternalIterator* iter,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters,
    FileMetaData* meta, std::vector<BlobFileAddition>* blob_file_additions,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SequenceNumber job_snapshot, SnapshotChecker* snapshot_checker,
    bool paranoid_file_checks, InternalStats* internal_stats,
    IOStatus* io_status, const std::shared_ptr<IOTracer>& io_tracer,
    BlobFileCreationReason blob_creation_reason,
    const SeqnoToTimeMapping& seqno_to_time_mapping, FileMetaData* table_meta,EventLogger* event_logger,
    int job_id, const Env::IOPriority io_priority,
    TableProperties* table_properties, Env::WriteLifeTimeHint write_hint,
    const std::string* full_history_ts_low,
    BlobFileCompletionCallback* blob_callback, uint64_t* num_input_entries,
    uint64_t* memtable_payload_bytes, uint64_t* memtable_garbage_bytes,Version* base_version) {
  assert((tboptions.column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         tboptions.column_family_name.empty());
  auto& mutable_cf_options = tboptions.moptions;
  auto& ioptions = tboptions.ioptions;
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  // OutputValidator用来验证k/v的顺序
  OutputValidator output_validator(
      tboptions.internal_comparator,
      /*enable_order_check=*/
      mutable_cf_options.check_flush_compaction_key_order,
      /*enable_hash=*/paranoid_file_checks);
  Status s;
  meta->fd.file_size = 0;
  table_meta->fd.file_size = 0;
  // 1.把指向imm的迭代器置为first的位置
  iter->SeekToFirst();
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&tboptions.internal_comparator,
                                       snapshots));
  uint64_t num_unfragmented_tombstones = 0;
  uint64_t total_tombstone_payload_bytes = 0;
  for (auto& range_del_iter : range_del_iters) {
    num_unfragmented_tombstones +=
        range_del_iter->num_unfragmented_tombstones();
    total_tombstone_payload_bytes +=
        range_del_iter->total_tombstone_payload_bytes();
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }

  std::string index_name = TableFileName(ioptions.cf_paths, meta->fd.GetNumber(),
                                    meta->fd.GetPathId());
  // 此处的fname是写入磁盘的文件名 需要使用table_meta的number
  std::string fname = TableFileName(ioptions.cf_paths, table_meta->fd.GetNumber(),
                                    table_meta->fd.GetPathId());
  std::vector<std::string> blob_file_paths;
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(ioptions.listeners, dbname,
                                               tboptions.column_family_name,
                                               fname, job_id, tboptions.reason);
#endif  // !ROCKSDB_LITE
  Env* env = db_options.env;
  assert(env);
  FileSystem* fs = db_options.fs.get();
  assert(fs);

  TableProperties tp;
  bool table_file_created = false;
  // 2.核心逻辑之创建table
  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    std::unique_ptr<CompactionFilter> compaction_filter;
    if (ioptions.compaction_filter_factory != nullptr &&
        ioptions.compaction_filter_factory->ShouldFilterTableFileCreation(
            tboptions.reason)) {
      CompactionFilter::Context context;
      context.is_full_compaction = false;
      context.is_manual_compaction = false;
      context.column_family_id = tboptions.column_family_id;
      context.reason = tboptions.reason;
      compaction_filter =
          ioptions.compaction_filter_factory->CreateCompactionFilter(context);
      if (compaction_filter != nullptr &&
          !compaction_filter->IgnoreSnapshots()) {
        s.PermitUncheckedError();
        return Status::NotSupported(
            "CompactionFilter::IgnoreSnapshots() = false is not supported "
            "anymore.");
      }
    }

    TableBuilder* builder;
    std::vector<table_information_collect> sub_runs;
    std::unique_ptr<WritableFileWriter> file_writer;
    {
      std::unique_ptr<FSWritableFile> file;
#ifndef NDEBUG
      bool use_direct_writes = file_options.use_direct_writes;
      TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
      IOStatus io_s = NewWritableFile(fs, fname, &file, file_options);
      assert(s.ok());
      s = io_s;
      if (io_status->ok()) {
        *io_status = io_s;
      }
      if (!s.ok()) {
        EventHelpers::LogAndNotifyTableFileCreationFinished(
            event_logger, ioptions.listeners, dbname,
            tboptions.column_family_name, fname, job_id, table_meta->fd,
            kInvalidBlobFileNumber, tp, tboptions.reason, s, file_checksum,
            file_checksum_func_name);
        return s;
      }

      table_file_created = true;
      FileTypeSet tmp_set = ioptions.checksum_handoff_file_types;
      file->SetIOPriority(io_priority);
      file->SetWriteLifeTimeHint(write_hint);
      file_writer.reset(new WritableFileWriter(
          std::move(file), fname, file_options, ioptions.clock, io_tracer,
          ioptions.stats, ioptions.listeners,
          ioptions.file_checksum_gen_factory.get(),
          tmp_set.Contains(FileType::kTableFile), false));

      // 当前Flush默认有value compaction 不然就无法生成table_file了
      builder = NewTableBuilder(tboptions, file_writer.get(),table_meta->fd.GetNumber(),meta->fd.GetNumber(),sub_runs,true,false);
      
    }

    MergeHelper merge(
        env, tboptions.internal_comparator.user_comparator(),
        ioptions.merge_operator.get(), compaction_filter.get(), ioptions.logger,
        true /* internal key corruption is not ok */,
        snapshots.empty() ? 0 : snapshots.back(), snapshot_checker);

    std::unique_ptr<BlobFileBuilder> blob_file_builder(
        (mutable_cf_options.enable_blob_files &&
         tboptions.level_at_creation >=
             mutable_cf_options.blob_file_starting_level &&
         blob_file_additions)
            ? new BlobFileBuilder(
                  versions, fs, &ioptions, &mutable_cf_options, &file_options,
                  tboptions.db_id, tboptions.db_session_id, job_id,
                  tboptions.column_family_id, tboptions.column_family_name,
                  io_priority, write_hint, io_tracer, blob_callback,
                  blob_creation_reason, &blob_file_paths, blob_file_additions)
            : nullptr);

    const std::atomic<bool> kManualCompactionCanceledFalse{false};
    CompactionIterator c_iter(
        iter, tboptions.internal_comparator.user_comparator(), &merge,
        kMaxSequenceNumber, &snapshots, earliest_write_conflict_snapshot,
        job_snapshot, snapshot_checker, env,
        ShouldReportDetailedTime(env, ioptions.stats),
        true /* internal key corruption is not ok */, range_del_agg.get(),
        blob_file_builder.get(), ioptions.allow_data_in_errors,
        ioptions.enforce_single_del_contracts,
        /*manual_compaction_canceled=*/kManualCompactionCanceledFalse,
        /*compaction=*/nullptr, compaction_filter.get(),
        /*shutting_down=*/nullptr, db_options.info_log, full_history_ts_low, kMaxSequenceNumber, true);

    c_iter.SeekToFirst();
    // 2.1 真正遍历imm同时构造table的地方
    // 不需要user_key相同时要去重的逻辑
    for (; c_iter.Valid(); c_iter.Next()) {
      const Slice& key = c_iter.key();
      const Slice& value = c_iter.value();
      const ParsedInternalKey& ikey = c_iter.ikey();
      // Generate a rolling 64-bit hash of the key and values
      // Note :
      // Here "key" integrates 'sequence_number'+'kType'+'user key'.
      s = output_validator.Add(key, value);
      if (!s.ok()) {
        break;
      }
      // flush时全部使用real value构造数据
      builder->Add(key, value, true);

      s = meta->UpdateBoundaries(key, value, ikey.sequence, ikey.type);
      // table_meta也需要更新上下界
      s = table_meta->UpdateBoundaries(key, value, ikey.sequence, ikey.type);
      if (!s.ok()) {
        break;
      }

      // TODO(noetzli): Update stats after flush, too.
      if (io_priority == Env::IO_HIGH &&
          IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        ThreadStatusUtil::SetThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
      }
    }
    //遍历结束
    if (!s.ok()) {
      c_iter.status().PermitUncheckedError();
    } else if (!c_iter.status().ok()) {
      s = c_iter.status();
    }

    if (s.ok()) {
      auto range_del_it = range_del_agg->NewIterator();
      for (range_del_it->SeekToFirst(); range_del_it->Valid();
           range_del_it->Next()) {
        auto tombstone = range_del_it->Tombstone();
        auto kv = tombstone.Serialize();
        builder->Add(kv.first.Encode(), kv.second);
        meta->UpdateBoundariesForRange(kv.first, tombstone.SerializeEndKey(),
                                       tombstone.seq_,
                                       tboptions.internal_comparator);
        table_meta->UpdateBoundariesForRange(kv.first, tombstone.SerializeEndKey(),
                                       tombstone.seq_,
                                       tboptions.internal_comparator);
      }
    }

    TEST_SYNC_POINT("BuildTable:BeforeFinishBuildTable");
    const bool empty = builder->IsEmpty();
    if (num_input_entries != nullptr) {
      *num_input_entries =
          c_iter.num_input_entry_scanned() + num_unfragmented_tombstones;
    }
    if (!s.ok() || empty) {
      builder->Abandon();
    } else {
      std::string seqno_time_mapping_str;
      seqno_to_time_mapping.Encode(
          seqno_time_mapping_str, meta->fd.smallest_seqno,
          meta->fd.largest_seqno, meta->file_creation_time);
      builder->SetSeqnoTimeTableProperties(
          seqno_time_mapping_str,
          ioptions.compaction_style == CompactionStyle::kCompactionStyleFIFO
              ? meta->file_creation_time
              : meta->oldest_ancester_time);
      s = builder->Finish();
    }
    if (io_status->ok()) {
      *io_status = builder->io_status();
    }

    if (s.ok() && !empty) {
      uint64_t file_size = builder->FileSize();
      // flush时，meta和table_meta都是同样的大小
      meta->fd.file_size = file_size;
      table_meta->fd.file_size = builder->TableFileSize();
      meta->marked_for_compaction = builder->NeedCompact();
      assert(meta->fd.GetFileSize() > 0);
      tp = builder->GetTableProperties(); // refresh now that builder is finished
      meta->total_entries_ = tp.num_entries;
      meta->merge_entries_ = builder->MergeEntries();
      table_meta->total_entries_ = tp.num_entries;
      meta->fd.sub_number_to_reference_key.insert({table_meta->fd.GetNumber(),table_meta->total_entries_});
      // 仅table_meta的reference_entries有意义 无需在此生成
      // meta和table_meta的生成map的逻辑无需在这里实现。AddFile和AddTableFile时会自动实现。
      if (memtable_payload_bytes != nullptr &&
          memtable_garbage_bytes != nullptr) {
        const CompactionIterationStats& ci_stats = c_iter.iter_stats();
        uint64_t total_payload_bytes = ci_stats.total_input_raw_key_bytes +
                                       ci_stats.total_input_raw_value_bytes +
                                       total_tombstone_payload_bytes;
        uint64_t total_payload_bytes_written =
            (tp.raw_key_size + tp.raw_value_size);
        // Prevent underflow, which may still happen at this point
        // since we only support inserts, deletes, and deleteRanges.
        if (total_payload_bytes_written <= total_payload_bytes) {
          *memtable_payload_bytes = total_payload_bytes;
          *memtable_garbage_bytes =
              total_payload_bytes - total_payload_bytes_written;
        } else {
          *memtable_payload_bytes = 0;
          *memtable_garbage_bytes = 0;
        }
      }
      if (table_properties) {
        *table_properties = tp;
      }
    }
    RecordTick(ioptions.statistics.get(), FLUSH_ENTRIES, builder->NumEntries());
    delete builder;

    // Finish and check for file errors
    TEST_SYNC_POINT("BuildTable:BeforeSyncTable");
    if (s.ok() && !empty) {
      StopWatch sw(ioptions.clock, ioptions.stats, TABLE_SYNC_MICROS);
      *io_status = file_writer->Sync(ioptions.use_fsync);
    }
    TEST_SYNC_POINT("BuildTable:BeforeCloseTableFile");
    if (s.ok() && io_status->ok() && !empty) {
      *io_status = file_writer->Close();
    }
    if (s.ok() && io_status->ok() && !empty) {
      // Add the checksum information to file metadata.
      meta->file_checksum = file_writer->GetFileChecksum();
      table_meta->file_checksum = file_writer->GetFileChecksum();
      meta->file_checksum_func_name = file_writer->GetFileChecksumFuncName();
      table_meta->file_checksum_func_name = file_writer->GetFileChecksumFuncName();
      file_checksum = meta->file_checksum;
      file_checksum_func_name = meta->file_checksum_func_name;
      // Set unique_id only if db_id and db_session_id exist
      if (!tboptions.db_id.empty() && !tboptions.db_session_id.empty()) {
        if (!GetSstInternalUniqueId(tboptions.db_id, tboptions.db_session_id,
                                    meta->fd.GetNumber(), &(meta->unique_id))
                 .ok()) {
          // if failed to get unique id, just set it Null
          meta->unique_id = kNullUniqueId64x2;
        }
      }
    }

    if (s.ok()) {
      s = *io_status;
    }

    if (blob_file_builder) {
      if (s.ok()) {
        s = blob_file_builder->Finish();
      } else {
        blob_file_builder->Abandon(s);
      }
      blob_file_builder.reset();
    }

    // TODO Also check the IO status when create the Iterator.

    TEST_SYNC_POINT("BuildTable:BeforeOutputValidation");
    // 3.构造NvmPartition结束，同时构造本NvmPartition的Reader，存入table_cacahe，同时存入fmd中。
    if (s.ok() && !empty) {
      // Verify that the table is usable
      // We set for_compaction to false and don't OptimizeForCompactionTableRead
      // here because this is a special case after we finish the table building.
      // No matter whether use_direct_io_for_flush_and_compaction is true,
      // the goal is to cache it here for further user reads.
      ReadOptions read_options;
      read_options.verify_checksums = false;
      // 构造it使用到了table_cache的NewIterator，这会调用FindTable()构造当前Partition的TableReader然后存入fmd中
      std::unique_ptr<InternalIterator> it(table_cache->NewIterator(
          read_options, file_options, tboptions.internal_comparator, *meta,
          nullptr /* range_del_agg */, mutable_cf_options.prefix_extractor,
          nullptr,
          (internal_stats == nullptr) ? nullptr
                                      : internal_stats->GetFileReadHist(0),
          TableReaderCaller::kFlush, /*arena=*/nullptr,
          /*skip_filter=*/false, tboptions.level_at_creation,
          MaxFileSizeForL0MetaPin(mutable_cf_options),
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key*/ nullptr,
          /*allow_unprepared_value*/ false));
      // 4. 使用构造后生成的iterator，为当前partition维护其下一个level的guard内的ranks
      // std::vector<std::pair<uint32_t,uint32_t>> children_ranks;
      // if (ioptions.enable_estimate_seek) {
      //   std::vector<PositionKeyList> children_ranks;
      //   const std::vector<FileMetaData*>& l1_files = base_version->storage_info()->LevelFiles(1);
      //   NvmPartitionIterator* partition_father_it = static_cast<NvmPartitionIterator*>(it.get());
      //   StopWatch* sw = new StopWatch(ioptions.clock, ioptions.stats, CHILDREN_RANK_COMPUTE);
      //   std::vector<std::pair<InternalIterator*,std::pair<std::string,std::string>>> childrenit_range_keys;
      //   // 4.1 遍历l1_files，为每一个l1_file生成一个iterator
      //   for (auto& f : l1_files) {
      //     // 4.1.1 如果children file与father file没有overlap，直接跳过
      //     if(tboptions.internal_comparator.user_comparator()->Compare(f->largest.user_key(),meta->smallest.user_key()) < 0 ||
      //       tboptions.internal_comparator.user_comparator()->Compare(f->smallest.user_key(),meta->largest.user_key()) > 0)
      //         continue;
      //     // 4.1.2 构造children file的iterator，添加到children_its中
      //     children_ranks.emplace_back(meta->fd.GetNumber(), f->fd.GetNumber()); // 
      //     childrenit_range_keys.push_back(std::pair<InternalIterator*,std::pair<std::string,std::string>>(table_cache->NewIterator(
      //       read_options, file_options, tboptions.internal_comparator, *f,
      //       nullptr /* range_del_agg */, mutable_cf_options.prefix_extractor,
      //       nullptr,
      //       (internal_stats == nullptr) ? nullptr
      //                                   : internal_stats->GetFileReadHist(0),
      //       TableReaderCaller::kFlush, /*arena=*/nullptr,
      //       /*skip_filter=*/false, tboptions.level_at_creation,
      //       MaxFileSizeForL0MetaPin(mutable_cf_options),
      //       /*smallest_compaction_key=*/nullptr,
      //       /*largest_compaction_key*/ nullptr,
      //       /*allow_unprepared_value*/ false),std::pair<std::string,std::string>()));
      //   }

      //   // 4.2 遍历children_its，为每一个子file生成对应的初始children_rank(即只有first key和last key)
      //   for(int i = 0; i < childrenit_range_keys.size(); i++ ) {
      //     InternalIterator* children_iter = childrenit_range_keys[i].first;
      //     // 4.2.1 查询子iterator的first key和last key 得到对应的level3_block
      //     children_iter->SeekToFirst();
      //     std::string left_key = children_iter->key().ToString();
      //     uint32_t level3_left_block = static_cast<NvmPartitionIterator*>(children_iter)->GetKeyPosition(3);
      //     children_iter->SeekToLast();
      //     std::string right_key = children_iter->key().ToString();
      //     uint32_t level3_right_block = static_cast<NvmPartitionIterator*>(children_iter)->GetKeyPosition(3);
      //     // 4.2.2 在father iterator中查询对应key的position
      //     partition_father_it->Seek(left_key);
      //     if (!partition_father_it->Valid())
      //     {
      //       partition_father_it->SeekToFirst();
      //     }
      //     uint32_t left_pos = partition_father_it->GetKeyPosition(0);

      //     partition_father_it->Seek(right_key);
      //     if (!partition_father_it->Valid())
      //     {
      //       partition_father_it->SeekToLast();
      //     }
      //     uint32_t right_pos = partition_father_it->GetKeyPosition(0);

      //     childrenit_range_keys[i].second = {left_key, right_key};
      //     // WQTODO 2023.06.01
      //     children_ranks[i].AddPosKey(left_key, left_pos, level3_left_block);
      //     children_ranks[i].AddPosKey(right_key, right_pos, level3_right_block);
      //   }
      //   // 4.3 遍历father_it，进行EstimateSeek，同时动态添加children_ranks
      //   if(childrenit_range_keys.size() != 0) {
      //     int times = 0;
      //     for (partition_father_it->SeekToFirst(); partition_father_it->Valid(); partition_father_it->Next()) {
      //       // 4.3.1 按照estimate_interval选择father_iter中的不同key进行EstimateSeek
      //       if(times++ % ioptions.estimate_interval == 0) {
      //         Slice temp_key = partition_father_it->key();
      //         // 4.3.2 如果当前key与children_ranks中的key range没有overlap，直接跳过
      //         if (tboptions.internal_comparator.user_comparator()->Compare(temp_key, childrenit_range_keys.front().second.first) < 0 ||
      //           tboptions.internal_comparator.user_comparator()->Compare(temp_key, childrenit_range_keys.back().second.second) > 0)
      //           continue;
      //         //4.3.3 二分 寻找childrenit_range_keys中第一个大于等于temp_key的值的largest_key
      //         int left = 0, right = childrenit_range_keys.size() - 1;
      //         while (left < right) {
      //           int mid = (left + right) >> 1;
      //           if(tboptions.internal_comparator.user_comparator()->Compare(childrenit_range_keys[mid].second.second, temp_key) >= 0) {
      //             right = mid;
      //           } else {
      //             left = mid + 1;
      //           }
      //         }
      //         // 此时left位置一定是第一个大于等于temp_key的range
      //         // 4.3.4 如果temp_key比children_range_keys[left].second.first小 那么直接continue
      //         if (tboptions.internal_comparator.user_comparator()->Compare(temp_key, childrenit_range_keys[left].second.first) < 0)
      //           continue;
      //         // 4.3.5 此时，temp_key在指定区间内部，可以进行estimate_seek
      //         int estimate_pos = children_ranks[left].GetEstimatePos(temp_key.ToString(), partition_father_it->GetKeyPosition(0));
      //         uint32_t seek_levels = static_cast<NvmPartitionIterator*>(childrenit_range_keys[left].first)->EstimateSeek(temp_key, estimate_pos, LinearDetection);
      //         if (seek_levels >= ioptions.estimate_threshold) {
      //           children_ranks[left].AddPosKey(temp_key.ToString(), partition_father_it->GetKeyPosition(0), static_cast<NvmPartitionIterator*>(childrenit_range_keys[left].first)->GetKeyPosition(3));
      //         }
      //       }
      //     }
      //   }
      //   // for(auto& f:l1_files){
      //   //   // 4.2.1 如果l1_file与当前file没有任何overlap 那么无需生成对应children_ranks_
      //   //   if(tboptions.internal_comparator.user_comparator()->Compare(f->largest.user_key(),meta->smallest.user_key()) < 0 ||
      //   //     tboptions.internal_comparator.user_comparator()->Compare(f->smallest.user_key(),meta->largest.user_key()) > 0)
      //   //       continue;
      //   //   // LAST_VERSION
      //   //   // if(tboptions.internal_comparator.user_comparator()->Compare(f->smallest.user_key(),meta->smallest.user_key())>0 && 
      //   //   //   tboptions.internal_comparator.user_comparator()->Compare(f->largest.user_key(),meta->largest.user_key())<0) {
      //   //     // 为每一个children_file生成一个PositionKeyList
      //   //   children_ranks.emplace_back(0, 0);
      //   //   std::unique_ptr<InternalIterator> children_it(table_cache->NewIterator(
      //   //     read_options, file_options, tboptions.internal_comparator, *f,
      //   //     nullptr /* range_del_agg */, mutable_cf_options.prefix_extractor,
      //   //     nullptr,
      //   //     (internal_stats == nullptr) ? nullptr
      //   //                                 : internal_stats->GetFileReadHist(0),
      //   //     TableReaderCaller::kFlush, /*arena=*/nullptr,
      //   //     /*skip_filter=*/false, tboptions.level_at_creation,
      //   //     MaxFileSizeForL0MetaPin(mutable_cf_options),
      //   //     /*smallest_compaction_key=*/nullptr,
      //   //     /*largest_compaction_key*/ nullptr,
      //   //     /*allow_unprepared_value*/ false));
      //   //   // 注意 此处并不会改变children_it的所有权 partition_children_it不需要负责释放该指针
      //   //   NvmPartitionIterator* partition_children_it = static_cast<NvmPartitionIterator*>(children_it.get());
      //   //   int times = 0;
      //   //   for(partition_children_it->SeekToFirst(); partition_children_it->Valid(); partition_children_it->Next()) {
      //   //     if(times++ % ioptions.estimate_interval == 0) {
      //   //     // int estimate_pos2 = pos_list[i].GetEstimatePos(search_key.ToString(), root->GetKeyPosition(0));

      //   //     // uint32_t seek_level2 = child[i]->EstimateSeek(search_key, estimate_pos2, LinearDetection);
      //   //     // if (seek_level2 >= 5) {
      //   //       std::string temp_key = partition_children_it->key().ToString();
      //   //       uint32_t level3_block = partition_children_it->GetKeyPosition(3);

      //   //       partition_father_it->Seek(temp_key);
      //   //       if (!partition_father_it->Valid())
      //   //       {
      //   //         partition_father_it->SeekToFirst();
      //   //       }
      //   //       uint32_t father_pos = partition_father_it->GetKeyPosition(0);
      //   //       children_ranks.back().AddPosKey(temp_key, father_pos, level3_block);
      //   //     }
      //   //     // LAST_VERSION
      //   //     // children_ranks.push_back({});
      //   //     // partition_father_it->Seek(f->smallest.Encode());
      //   //     // children_ranks.back().first = partition_father_it->GetKeyPosition();
      //   //     // partition_father_it->Seek(f->largest.Encode());
      //   //     // children_ranks.back().second = partition_father_it->GetKeyPosition();
      //   //     // LAST_VERSION
      //   //   }
      //   //   // 最后再添加last_key
      //   //   partition_children_it->SeekToLast();
      //   //   std::string temp_key = partition_children_it->key().ToString();
      //   //   uint32_t level3_block = partition_children_it->GetKeyPosition(3);

      //   //   partition_father_it->Seek(temp_key);
      //   //   if (!partition_father_it->Valid()) {
      //   //     partition_father_it->SeekToFirst();
      //   //   }
      //   //   uint32_t father_pos = partition_father_it->GetKeyPosition(0);
      //   //   children_ranks.back().AddPosKey(temp_key, father_pos, level3_block);
      //   //   // }
      //   // }
      //   delete sw;
      //   // 使用move 避免vector的深拷贝
      //   meta->children_ranks_ = std::move(children_ranks);
      
      //   for (auto childrenit_range_key : childrenit_range_keys ) {
      //     delete childrenit_range_key.first;
      //   }
      // }
      s = it->status();
      if (s.ok() && paranoid_file_checks) {
        OutputValidator file_validator(tboptions.internal_comparator,
                                       /*enable_order_check=*/true,
                                       /*enable_hash=*/true);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          // Generate a rolling 64-bit hash of the key and values
          file_validator.Add(it->key(), it->value()).PermitUncheckedError();
        }
        s = it->status();
        if (s.ok() && !output_validator.CompareValidator(file_validator)) {
          s = Status::Corruption("Paranoid checksums do not match");
        }
      }
    }
  }
  //WQTODO l0 compaction
  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (!s.ok() || meta->fd.GetFileSize() == 0) {
    TEST_SYNC_POINT("BuildTable:BeforeDeleteFile");

    constexpr IODebugContext* dbg = nullptr;

    if (table_file_created) {
      Status ignored = fs->DeleteFile(fname, IOOptions(), dbg);
      ignored.PermitUncheckedError();
    }

    assert(blob_file_additions || blob_file_paths.empty());

    if (blob_file_additions) {
      for (const std::string& blob_file_path : blob_file_paths) {
        Status ignored = DeleteDBFile(&db_options, blob_file_path, dbname,
                                      /*force_bg=*/false, /*force_fg=*/false);
        ignored.PermitUncheckedError();
        TEST_SYNC_POINT("BuildTable::AfterDeleteFile");
      }
    }
  }

  Status status_for_listener = s;
  if (meta->fd.GetFileSize() == 0) {
    fname = "(nil)";
    if (s.ok()) {
      status_for_listener = Status::Aborted("Empty SST file not kept");
    }
  }
  // Output to event logger and fire events.
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger, ioptions.listeners, dbname, tboptions.column_family_name,
      fname, job_id, table_meta->fd, meta->oldest_blob_file_number, tp,
      tboptions.reason, status_for_listener, file_checksum,
      file_checksum_func_name);

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
