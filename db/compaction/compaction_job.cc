//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_job.h"

#include <algorithm>
#include <cinttypes>
#include <memory>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "db/blob/blob_counting_iterator.h"
#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_builder.h"
#include "db/builder.h"
#include "db/compaction/clipping_iterator.h"
#include "db/compaction/compaction_state.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/history_trimming_iterator.h"
#include "db/log_writer.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_type.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"

#include "nvm/partition/nvm_partition_iterator.h"

namespace ROCKSDB_NAMESPACE {

const char* GetCompactionReasonString(CompactionReason compaction_reason) {
  switch (compaction_reason) {
    case CompactionReason::kUnknown:
      return "Unknown";
    case CompactionReason::kLevelL0FilesNum:
      return "LevelL0FilesNum";
    case CompactionReason::kLevelMaxLevelSize:
      return "LevelMaxLevelSize";
    case CompactionReason::kUniversalSizeAmplification:
      return "UniversalSizeAmplification";
    case CompactionReason::kUniversalSizeRatio:
      return "UniversalSizeRatio";
    case CompactionReason::kUniversalSortedRunNum:
      return "UniversalSortedRunNum";
    case CompactionReason::kFIFOMaxSize:
      return "FIFOMaxSize";
    case CompactionReason::kFIFOReduceNumFiles:
      return "FIFOReduceNumFiles";
    case CompactionReason::kFIFOTtl:
      return "FIFOTtl";
    case CompactionReason::kManualCompaction:
      return "ManualCompaction";
    case CompactionReason::kFilesMarkedForCompaction:
      return "FilesMarkedForCompaction";
    case CompactionReason::kBottommostFiles:
      return "BottommostFiles";
    case CompactionReason::kTtl:
      return "Ttl";
    case CompactionReason::kFlush:
      return "Flush";
    case CompactionReason::kExternalSstIngestion:
      return "ExternalSstIngestion";
    case CompactionReason::kPeriodicCompaction:
      return "PeriodicCompaction";
    case CompactionReason::kChangeTemperature:
      return "ChangeTemperature";
    case CompactionReason::kForcedBlobGC:
      return "ForcedBlobGC";
    case CompactionReason::kNumOfReasons:
      // fall through
    default:
      assert(false);
      return "Invalid";
  }
}

CompactionJob::CompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const MutableDBOptions& mutable_db_options, const FileOptions& file_options,
    VersionSet* versions, const std::atomic<bool>* shutting_down,
    LogBuffer* log_buffer, FSDirectory* db_directory,
    FSDirectory* output_directory, FSDirectory* blob_output_directory,
    Statistics* stats, InstrumentedMutex* db_mutex,
    ErrorHandler* db_error_handler,
    std::vector<SequenceNumber> existing_snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const SnapshotChecker* snapshot_checker, JobContext* job_context,
    std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
    bool paranoid_file_checks, bool measure_io_stats, const std::string& dbname,
    CompactionJobStats* compaction_job_stats, Env::Priority thread_pri,
    const std::shared_ptr<IOTracer>& io_tracer,
    const std::atomic<bool>& manual_compaction_canceled,
    const std::string& db_id, const std::string& db_session_id,
    std::string full_history_ts_low, std::string trim_ts,
    BlobFileCompletionCallback* blob_callback, int* bg_compaction_scheduled,
    int* bg_bottom_compaction_scheduled)
    : compact_(new CompactionState(compaction)),
      compaction_stats_(compaction->compaction_reason(), 1),
      db_options_(db_options),
      mutable_db_options_copy_(mutable_db_options),
      log_buffer_(log_buffer),
      output_directory_(output_directory),
      stats_(stats),
      bottommost_level_(false),
      write_hint_(Env::WLTH_NOT_SET),
      compaction_job_stats_(compaction_job_stats),
      job_id_(job_id),
      dbname_(dbname),
      db_id_(db_id),
      db_session_id_(db_session_id),
      file_options_(file_options),
      env_(db_options.env),
      io_tracer_(io_tracer),
      fs_(db_options.fs, io_tracer),
      file_options_for_read_(
          fs_->OptimizeForCompactionTableRead(file_options, db_options_)),
      versions_(versions),
      shutting_down_(shutting_down),
      manual_compaction_canceled_(manual_compaction_canceled),
      db_directory_(db_directory),
      blob_output_directory_(blob_output_directory),
      db_mutex_(db_mutex),
      db_error_handler_(db_error_handler),
      existing_snapshots_(std::move(existing_snapshots)),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      snapshot_checker_(snapshot_checker),
      job_context_(job_context),
      table_cache_(std::move(table_cache)),
      event_logger_(event_logger),
      paranoid_file_checks_(paranoid_file_checks),
      measure_io_stats_(measure_io_stats),
      thread_pri_(thread_pri),
      full_history_ts_low_(std::move(full_history_ts_low)),
      trim_ts_(std::move(trim_ts)),
      blob_callback_(blob_callback),
      extra_num_subcompaction_threads_reserved_(0),
      bg_compaction_scheduled_(bg_compaction_scheduled),
      bg_bottom_compaction_scheduled_(bg_bottom_compaction_scheduled) {
  assert(compaction_job_stats_ != nullptr);
  assert(log_buffer_ != nullptr);

  const auto* cfd = compact_->compaction->column_family_data();
  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
                                    db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);
  ReportStartedCompaction(compaction);
}

CompactionJob::~CompactionJob() {
  assert(compact_ == nullptr);
  ThreadStatusUtil::ResetThreadStatus();
}

void CompactionJob::ReportStartedCompaction(Compaction* compaction) {
  const auto* cfd = compact_->compaction->column_family_data();
  ThreadStatusUtil::SetColumnFamily(cfd, cfd->ioptions()->env,
                                    db_options_.enable_thread_tracking);

  ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                               job_id_);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_INPUT_OUTPUT_LEVEL,
      (static_cast<uint64_t>(compact_->compaction->start_level()) << 32) +
          compact_->compaction->output_level());

  // In the current design, a CompactionJob is always created
  // for non-trivial compaction.
  assert(compaction->IsTrivialMove() == false ||
         compaction->is_manual_compaction() == true);

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_PROP_FLAGS,
      compaction->is_manual_compaction() +
          (compaction->deletion_compaction() << 1));

  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_TOTAL_INPUT_BYTES,
      compaction->CalculateTotalInputSize());

  IOSTATS_RESET(bytes_written);
  IOSTATS_RESET(bytes_read);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, 0);
  ThreadStatusUtil::SetThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, 0);

  // Set the thread operation after operation properties
  // to ensure GetThreadList() can always show them all together.
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);

  compaction_job_stats_->is_manual_compaction =
      compaction->is_manual_compaction();
  compaction_job_stats_->is_full_compaction = compaction->is_full_compaction();
}

void CompactionJob::Prepare() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PREPARE);

  // Generate file_levels_ for compaction before making Iterator
  // 1.获取本次Compaction对象
  auto* c = compact_->compaction;
  ColumnFamilyData* cfd = c->column_family_data();
  assert(cfd != nullptr);
  assert(cfd->current()->storage_info()->NumLevelFiles(
             compact_->compaction->level()) > 0);

  write_hint_ = cfd->CalculateSSTWriteHint(c->output_level());
  bottommost_level_ = c->bottommost_level();

  // 2.为本次Compaction涉及到的文件划分区间(如果需要的话)
  if (c->ShouldFormSubcompactions()) {
      StopWatch sw(db_options_.clock, stats_, SUBCOMPACTION_SETUP_TIME);
      GenSubcompactionBoundaries();
  }
  if (boundaries_.size() >= 1) { // WQTODOIMP 这里的修改是为了避免出现vc选择时的bug...?
    for (size_t i = 0; i <= boundaries_.size(); i++) {
      // WQTODO 修改这里的时候，一定注意sub_compact_states的可能的扩容的问题
      compact_->sub_compact_states.emplace_back(
          c, (i != 0) ? std::optional<Slice>(boundaries_[i - 1]) : std::nullopt,
          (i != boundaries_.size()) ? std::optional<Slice>(boundaries_[i])
                                    : std::nullopt,
          static_cast<uint32_t>(i));
    }
    RecordInHistogram(stats_, NUM_SUBCOMPACTIONS_SCHEDULED,
                      compact_->sub_compact_states.size());
  } else {
    compact_->sub_compact_states.emplace_back(c, std::nullopt, std::nullopt,
                                              /*sub_job_id*/ 0);
  }

  // 下面的逻辑可以忽略。因为我们不会使用到这个功能
  if (c->immutable_options()->preclude_last_level_data_seconds > 0) {
    // TODO(zjay): move to a function
    seqno_time_mapping_.SetMaxTimeDuration(
        c->immutable_options()->preclude_last_level_data_seconds);
    // setup seqno_time_mapping_
    for (const auto& each_level : *c->inputs()) {
      for (const auto& fmd : each_level.files) {
        std::shared_ptr<const TableProperties> tp;
        Status s = cfd->current()->GetTableProperties(&tp, fmd, nullptr);
        if (s.ok()) {
          seqno_time_mapping_.Add(tp->seqno_to_time_mapping)
              .PermitUncheckedError();
          seqno_time_mapping_.Add(fmd->fd.smallest_seqno,
                                  fmd->oldest_ancester_time);
        }
      }
    }

    auto status = seqno_time_mapping_.Sort();
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Invalid sequence number to time mapping: Status: %s",
                     status.ToString().c_str());
    }
    int64_t _current_time = 0;
    status = db_options_.clock->GetCurrentTime(&_current_time);
    if (!status.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Failed to get current time in compaction: Status: %s",
                     status.ToString().c_str());
      penultimate_level_cutoff_seqno_ = 0;
    } else {
      penultimate_level_cutoff_seqno_ =
          seqno_time_mapping_.TruncateOldEntries(_current_time);
    }
  }
}

uint64_t CompactionJob::GetSubcompactionsLimit() {
  return extra_num_subcompaction_threads_reserved_ +
         std::max(
             std::uint64_t(1),
             static_cast<uint64_t>(compact_->compaction->max_subcompactions()));
}

void CompactionJob::AcquireSubcompactionResources(
    int num_extra_required_subcompactions) {
  TEST_SYNC_POINT("CompactionJob::AcquireSubcompactionResources:0");
  TEST_SYNC_POINT("CompactionJob::AcquireSubcompactionResources:1");
  int max_db_compactions =
      DBImpl::GetBGJobLimits(
          mutable_db_options_copy_.max_background_flushes,
          mutable_db_options_copy_.max_background_compactions,
          mutable_db_options_copy_.max_background_jobs,
          versions_->GetColumnFamilySet()
              ->write_controller()
              ->NeedSpeedupCompaction())
          .max_compactions;
  // Apply min function first since We need to compute the extra subcompaction
  // against compaction limits. And then try to reserve threads for extra
  // subcompactions. The actual number of reserved threads could be less than
  // the desired number.
  int available_bg_compactions_against_db_limit =
      std::max(max_db_compactions - *bg_compaction_scheduled_ -
                   *bg_bottom_compaction_scheduled_,
               0);
  db_mutex_->Lock();
  // Reservation only supports backgrdoun threads of which the priority is
  // between BOTTOM and HIGH. Need to degrade the priority to HIGH if the
  // origin thread_pri_ is higher than that. Similar to ReleaseThreads().
  extra_num_subcompaction_threads_reserved_ =
      env_->ReserveThreads(std::min(num_extra_required_subcompactions,
                                    available_bg_compactions_against_db_limit),
                           std::min(thread_pri_, Env::Priority::HIGH));

  // Update bg_compaction_scheduled_ or bg_bottom_compaction_scheduled_
  // depending on if this compaction has the bottommost priority
  if (thread_pri_ == Env::Priority::BOTTOM) {
    *bg_bottom_compaction_scheduled_ +=
        extra_num_subcompaction_threads_reserved_;
  } else {
    *bg_compaction_scheduled_ += extra_num_subcompaction_threads_reserved_;
  }
  db_mutex_->Unlock();
}

void CompactionJob::ShrinkSubcompactionResources(uint64_t num_extra_resources) {
  // Do nothing when we have zero resources to shrink
  if (num_extra_resources == 0) return;
  db_mutex_->Lock();
  // We cannot release threads more than what we reserved before
  int extra_num_subcompaction_threads_released = env_->ReleaseThreads(
      (int)num_extra_resources, std::min(thread_pri_, Env::Priority::HIGH));
  // Update the number of reserved threads and the number of background
  // scheduled compactions for this compaction job
  extra_num_subcompaction_threads_reserved_ -=
      extra_num_subcompaction_threads_released;
  // TODO (zichen): design a test case with new subcompaction partitioning
  // when the number of actual partitions is less than the number of planned
  // partitions
  assert(extra_num_subcompaction_threads_released == (int)num_extra_resources);
  // Update bg_compaction_scheduled_ or bg_bottom_compaction_scheduled_
  // depending on if this compaction has the bottommost priority
  if (thread_pri_ == Env::Priority::BOTTOM) {
    *bg_bottom_compaction_scheduled_ -=
        extra_num_subcompaction_threads_released;
  } else {
    *bg_compaction_scheduled_ -= extra_num_subcompaction_threads_released;
  }
  db_mutex_->Unlock();
  TEST_SYNC_POINT("CompactionJob::ShrinkSubcompactionResources:0");
}

void CompactionJob::ReleaseSubcompactionResources() {
  if (extra_num_subcompaction_threads_reserved_ == 0) {
    return;
  }
  // The number of reserved threads becomes larger than 0 only if the
  // compaction prioity is round robin and there is no sufficient
  // sub-compactions available

  // The scheduled compaction must be no less than 1 + extra number
  // subcompactions using acquired resources since this compaction job has not
  // finished yet
  assert(*bg_bottom_compaction_scheduled_ >=
             1 + extra_num_subcompaction_threads_reserved_ ||
         *bg_compaction_scheduled_ >=
             1 + extra_num_subcompaction_threads_reserved_);
  ShrinkSubcompactionResources(extra_num_subcompaction_threads_reserved_);
}

struct RangeWithSize {
  Range range;
  uint64_t size;

  RangeWithSize(const Slice& a, const Slice& b, uint64_t s = 0)
      : range(a, b), size(s) {}
};

void CompactionJob::GenSubcompactionBoundaries() {
  //RocksDB逻辑(详细见于old_rocksdb中的本方法)：
  //1.在input level和output level的所有sst中，每个sst随机选择一部分anchor
  //2.将anchor从小到大排序 去重
  //3.确定需要运行的sub compaction的个数
  //4.按照sub compaction个数，确定真正的每个边界的值boundaries
  //而RocksDB之前的逻辑中，并不是从sst中选择anchor，而是选择sst的largest和smallest key。这样的划分的粒度自然要粗很多

  //我们的逻辑：
  //1.根据当前guard中的值确定边界
  //结束，没有2.

  auto* c = compact_->compaction;
  if (c->max_subcompactions() <= 1 &&
      !(c->immutable_options()->compaction_pri == kRoundRobin &&
        c->immutable_options()->compaction_style == kCompactionStyleLevel)) {
    return;
  }
  auto* cfd = c->column_family_data();
  // const Comparator* cfd_comparator = cfd->user_comparator();
  // const InternalKeyComparator& icomp = cfd->internal_comparator();

  auto* v = compact_->compaction->input_version();
  int base_level = v->storage_info()->base_level();
  InstrumentedMutexUnlock unlock_guard(db_mutex_);


  // uint64_t total_size = 0;
  // std::vector<TableReader::Anchor> all_anchors;
  int start_lvl = c->start_level();
  int out_lvl = c->output_level();
  assert(!c->is_vertical_compaction() || start_lvl == out_lvl);
  //WQTODOIMP 这里用的是pick本次compaction时的guard，而不是当前实际guard。这样可能导致新的version中，guard和每个file之间的划分并不是严格对应的。
  // 实际上就算用current的guard，也可能出现不是严格对应。考虑一次l0到l1compaction增加了许多guard，但是原有的sst并不会突然变化范围。
  // 其实不对应不会有什么问题，我们的查找又不是基于guard，还是基于files。而且新增guard并不会造成sst的突然分裂。只会在下一次compaction的时候有变化。
  
  // guard的生成最好放在l0->l1的compaction。第一次没有guard，可以直接下来，但是下来的过程自然会遍历file，那么就得到guard。下一次l0->l1compaction就可以直接划分了。
  std::vector<std::string> guard = v->storage_info()->UpGuard(out_lvl);
  // 这里不能直接把该level的所有guard添加，因为如果一个范围内没有compaction的数据，后续在Run中也会新开启线程，造成不必要的开销。

  // 1.如果guard为空 那么直接返回
  if(guard.empty()) return;
  // 2.如果guard的范围都不在compaction的范围内，那么也直接返回，不需要划分
  if(cfd->user_comparator()->Compare(guard[0],c->GetLargestUserKey())>=0 || cfd->user_comparator()->Compare(guard[guard.size()-1],c->GetSmallestUserKey())<=0){
    return;
  }
  //3. 选择与当前compaction范围重合的guard范围
  // 分别表示与当前compaction的范围对应的guard的index范围
  int ldx = -1,rdx = -1;
  //WQTODO 理论上这里可以二分优化一下，不过我暂时没工夫，以后有机会来重写一下吧。
  for(int i = 0;i<guard.size();i++){
    // 左边界选择第一个大于smallestuserkey的index
    if(ldx == -1 && cfd->user_comparator()->Compare(guard[i],c->GetSmallestUserKey())>=0){
      ldx = i;
    }
    // 右边界选择最后一个小于largestuserkey的index 
    if(cfd->user_comparator()->Compare(guard[i],c->GetLargestUserKey())<=0){
      rdx = i;
    }
  }
  // 这里理论上ldx和rdx都不能为-1
  assert(ldx !=-1 && rdx != -1);
  boundaries_.insert(boundaries_.begin(),guard.begin()+ldx,guard.begin()+rdx+1);

  std::cout<<"Pikc Guard!";
  std::cout<<"Guard' Size: "<<boundaries_.size()<<std::endl;
  std::cout<<"Guard:[";
  for(auto i:boundaries_){
    std::cout<<i<<" ";
  }
  std::cout<<"]"<<std::endl;
}

Status CompactionJob::Run() {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);
  TEST_SYNC_POINT("CompactionJob::Run():Start");
  log_buffer_->FlushBufferToLog();
  LogCompaction();

  const size_t num_threads = compact_->sub_compact_states.size();
  assert(num_threads > 0);
  const uint64_t start_micros = db_options_.clock->NowMicros();

  // Launch a thread for each of subcompactions 1...num_threads-1
  std::vector<port::Thread> thread_pool;
  thread_pool.reserve(num_threads - 1);
  for (size_t i = 1; i < compact_->sub_compact_states.size(); i++) {
    thread_pool.emplace_back(&CompactionJob::ProcessKeyValueCompaction, this,
                             &compact_->sub_compact_states[i]);
  }

  // Always schedule the first subcompaction (whether or not there are also
  // others) in the current thread to be efficient with resources
  ProcessKeyValueCompaction(&compact_->sub_compact_states[0]);

  // Wait for all other threads (if there are any) to finish execution
  for (auto& thread : thread_pool) {
    thread.join();
  }

  compaction_stats_.SetMicros(db_options_.clock->NowMicros() - start_micros);

  for (auto& state : compact_->sub_compact_states) {
    compaction_stats_.AddCpuMicros(state.compaction_job_stats.cpu_micros);
    state.RemoveLastEmptyOutput();
  }

  RecordTimeToHistogram(stats_, COMPACTION_TIME,
                        compaction_stats_.stats.micros);
  RecordTimeToHistogram(stats_, COMPACTION_CPU_TIME,
                        compaction_stats_.stats.cpu_micros);

  TEST_SYNC_POINT("CompactionJob::Run:BeforeVerify");

  // Check if any thread encountered an error during execution
  Status status;
  IOStatus io_s;
  bool wrote_new_blob_files = false;

  for (const auto& state : compact_->sub_compact_states) {
    if (!state.status.ok()) {
      status = state.status;
      io_s = state.io_status;
      break;
    }

    if (state.Current().HasBlobFileAdditions()) {
      wrote_new_blob_files = true;
    }
  }

  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    constexpr IODebugContext* dbg = nullptr;

    if (output_directory_) {
      io_s = output_directory_->FsyncWithDirOptions(
          IOOptions(), dbg,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    }

    if (io_s.ok() && wrote_new_blob_files && blob_output_directory_ &&
        blob_output_directory_ != output_directory_) {
      io_s = blob_output_directory_->FsyncWithDirOptions(
          IOOptions(), dbg,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    }
  }
  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    status = io_s;
  }
  if (status.ok()) {
    thread_pool.clear();
    std::vector<const CompactionOutputs::Output*> files_output;
    for (const auto& state : compact_->sub_compact_states) {
      for (const auto& output : state.GetOutputs()) {
        files_output.emplace_back(&output);
      }
    }
    ColumnFamilyData* cfd = compact_->compaction->column_family_data();
    auto& prefix_extractor =
        compact_->compaction->mutable_cf_options()->prefix_extractor;
    std::atomic<size_t> next_file_idx(0);
    // compaction结束后，下面这个函数用来遍历table 将其table reader存入table cache中
    auto verify_table = [&](Status& output_status) {
      while (true) {
        size_t file_idx = next_file_idx.fetch_add(1);
        if (file_idx >= files_output.size()) {
          break;
        }
        // Verify that the table is usable
        // We set for_compaction to false and don't OptimizeForCompactionTableRead
        // here because this is a special case after we finish the table building
        // No matter whether use_direct_io_for_flush_and_compaction is true,
        // we will regard this verification as user reads since the goal is
        // to cache it here for further user reads
        ReadOptions read_options;
        InternalIterator* iter = cfd->table_cache()->NewIterator(
            read_options, file_options_, cfd->internal_comparator(),
            files_output[file_idx]->meta, /*range_del_agg=*/nullptr,
            prefix_extractor,
            /*table_reader_ptr=*/nullptr,
            cfd->internal_stats()->GetFileReadHist(
                compact_->compaction->output_level()),
            TableReaderCaller::kCompactionRefill, /*arena=*/nullptr,
            /*skip_filters=*/false, compact_->compaction->output_level(),
            MaxFileSizeForL0MetaPin(
                *compact_->compaction->mutable_cf_options()),
            /*smallest_compaction_key=*/nullptr,
            /*largest_compaction_key=*/nullptr,
            /*allow_unprepared_value=*/false);
        auto s = iter->status();
        // 1. 如果开启了enable_easimate_seek，那么执行对应逻辑
      //   if(compact_->compaction->immutable_options()->enable_estimate_seek) {
      //     StopWatch* sw = new StopWatch(db_options_.clock, db_options_.stats, CHILDREN_RANK_COMPUTE);
      //     int start_level = compact_->compaction->start_level(), output_level = compact_->compaction->output_level();
      //     const Version* base_version = compact_->compaction->input_version();
      //     // 1.1 如果output_level() == start_level()且start_level()不是0(即不是intral0compaction，一般是vertical compaction会出现这种情况) 需要为当前新生成文件维护上层的rank信息
      //     if(start_level > 0 && start_level == output_level) {
      //       const std::vector<FileMetaData*>& father_files = base_version->storage_info()->LevelFiles(start_level - 1);
      //       NvmPartitionIterator* partition_father_it = static_cast<NvmPartitionIterator*>(iter);
      //       std::vector<std::pair<InternalIterator*,std::pair<std::string,std::string>>> childrenit_range_keys;
      //       // 1.1.1 遍历father_files，为每个father_file重新生成对应的children_ranks信息
      //       for(auto& f : father_files) {
      //         // 注意 这里需要替换father_files中原有的部分children_ranks内容
      //         // 1.1.1.1 如果children file与father file没有overlap，直接跳过
      //         if(cfd->user_comparator()->Compare(f->largest.user_key(),files_output[file_idx]->meta.smallest.user_key()) < 0 ||
      //           cfd->user_comparator()->Compare(f->smallest.user_key(),files_output[file_idx]->meta.largest.user_key()) > 0)
      //             continue;
      //         InternalIterator* father_iter = cfd->table_cache()->NewIterator(
      //             read_options, file_options_, cfd->internal_comparator(),
      //             *f, /*range_del_agg=*/nullptr,
      //             prefix_extractor,
      //             /*table_reader_ptr=*/nullptr,
      //             cfd->internal_stats()->GetFileReadHist(
      //                 compact_->compaction->output_level()),
      //             TableReaderCaller::kCompactionRefill, /*arena=*/nullptr,
      //             /*skip_filters=*/false, compact_->compaction->output_level(),
      //             MaxFileSizeForL0MetaPin(
      //                 *compact_->compaction->mutable_cf_options()),
      //             /*smallest_compaction_key=*/nullptr,
      //             /*largest_compaction_key=*/nullptr,
      //             /*allow_unprepared_value=*/false);
      //         UpdateChildrenRanksForLevel(start_level-1, *f, father_iter);

      //         delete father_iter;
      //       }
      //     }

      //     #ifndef NDEBUG
      //     std::cout << "compaction reason" << (int)compact_->compaction->compaction_reason() << std::endl;
      //     #endif
      //     // 1.2 如果output_level() != start_level() 需要为当前新生成文件维护更下层的rank信息 
      //     if(output_level+1 < compact_->compaction->immutable_options()->num_levels){
      //       UpdateChildrenRanksForLevel(output_level, files_output[file_idx]->meta, iter);
      //     }

      //     // 1.3 如果start_level() == 0，且output level != start_level() (此情况多发生于l0到l1的compaction) 因为l0的反选没有选择全部的l0 file 导致剩下的那个l0 file没有被更新children_rank
      //     // 暂时不处理这种情况...在查询的时候通过father和children跳过
      //     // if (start_level == 0 && start_level != output_level) {
      //     //   for(auto l0_file : base_version->storage_info()->LevelFiles(0)) {
      //     //     const std::vector<FileMetaData*>* l0_compaction_files = compact_->compaction->inputs(0);
      //     //     for(int i = 0; i < l0_compaction_files->size(); i++) {
      //     //       if(l0_file->fd.GetNumber() == l0_compaction_files[i])
      //     //     }
      //     //     if (compact_->compaction->inputs(0)->)
      //     //   }
      //     //   UpdateChildrenRanksForLevel(output_level, );
      //     // }
      //     // children_ranks.push_back(files_output[file_idx]->meta.total_entries_);       
      //   delete sw;
      // }

        if (s.ok() && paranoid_file_checks_) {
          OutputValidator validator(cfd->internal_comparator(),
                                    /*_enable_order_check=*/true,
                                    /*_enable_hash=*/true);
          for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            s = validator.Add(iter->key(), iter->value());
            if (!s.ok()) {
              break;
            }
          }
          if (s.ok()) {
            s = iter->status();
          }
          if (s.ok() &&
              !validator.CompareValidator(files_output[file_idx]->validator)) {
            s = Status::Corruption("Paranoid checksums do not match");
          }
        }

        delete iter;

        if (!s.ok()) {
          output_status = s;
          break;
        }
      }
    };
    for (size_t i = 1; i < compact_->sub_compact_states.size(); i++) {
      thread_pool.emplace_back(verify_table,
                               std::ref(compact_->sub_compact_states[i].status));
    }
    verify_table(compact_->sub_compact_states[0].status);
    for (auto& thread : thread_pool) {
      thread.join();
    }

    for (const auto& state : compact_->sub_compact_states) {
      if (!state.status.ok()) {
        status = state.status;
        break;
      }
    }
  }

  ReleaseSubcompactionResources();
  TEST_SYNC_POINT("CompactionJob::ReleaseSubcompactionResources:0");
  TEST_SYNC_POINT("CompactionJob::ReleaseSubcompactionResources:1");

  TablePropertiesCollection tp;
  for (const auto& state : compact_->sub_compact_states) {
    for (const auto& output : state.GetOutputs()) {
      auto fn =
          TableFileName(state.compaction->immutable_options()->cf_paths,
                        output.meta.fd.GetNumber(), output.meta.fd.GetPathId());
      tp[fn] = output.table_properties;
    }
  }
  compact_->compaction->SetOutputTableProperties(std::move(tp));

  // Finish up all book-keeping to unify the subcompaction results
  compact_->AggregateCompactionStats(compaction_stats_, *compaction_job_stats_);
  UpdateCompactionStats();

  RecordCompactionIOStats();
  LogFlush(db_options_.info_log);
  TEST_SYNC_POINT("CompactionJob::Run():End");

  compact_->status = status;
  return status;
}

Status CompactionJob::Install(const MutableCFOptions& mutable_cf_options) {
  assert(compact_);

  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_INSTALL);
  db_mutex_->AssertHeld();
  Status status = compact_->status;

  ColumnFamilyData* cfd = compact_->compaction->column_family_data();
  assert(cfd);

  int output_level = compact_->compaction->output_level();
  cfd->internal_stats()->AddCompactionStats(output_level, thread_pri_,
                                            compaction_stats_);

  if (status.ok()) {
    status = InstallCompactionResults(mutable_cf_options);
  }
  if (!versions_->io_status().ok()) {
    io_status_ = versions_->io_status();
  }

  VersionStorageInfo::LevelSummaryStorage tmp;
  auto vstorage = cfd->current()->storage_info();
  const auto& stats = compaction_stats_.stats;

  double read_write_amp = 0.0;
  double write_amp = 0.0;
  double bytes_read_per_sec = 0;
  double bytes_written_per_sec = 0;

  const uint64_t bytes_read_non_output_and_blob =
      stats.bytes_read_non_output_levels + stats.bytes_read_blob;
  const uint64_t bytes_read_all =
      stats.bytes_read_output_level + bytes_read_non_output_and_blob;
  const uint64_t bytes_written_all =
      stats.bytes_written + stats.bytes_written_blob;

  if (bytes_read_non_output_and_blob > 0) {
    read_write_amp = (bytes_written_all + bytes_read_all) /
                     static_cast<double>(bytes_read_non_output_and_blob);
    write_amp =
        bytes_written_all / static_cast<double>(bytes_read_non_output_and_blob);
  }
  if (stats.micros > 0) {
    bytes_read_per_sec = bytes_read_all / static_cast<double>(stats.micros);
    bytes_written_per_sec =
        bytes_written_all / static_cast<double>(stats.micros);
  }

  const std::string& column_family_name = cfd->GetName();

  constexpr double kMB = 1048576.0;

  ROCKS_LOG_BUFFER(
      log_buffer_,
      "[%s] compacted to: %s, MB/sec: %.1f rd, %.1f wr, level %d, "
      "files in(%d, %d) out(%d +%d blob) "
      "MB in(%.1f, %.1f +%.1f blob) out(%.1f +%.1f blob), "
      "read-write-amplify(%.1f) write-amplify(%.1f) %s, records in: %" PRIu64
      ", records dropped: %" PRIu64 " output_compression: %s\n",
      column_family_name.c_str(), vstorage->LevelSummary(&tmp),
      bytes_read_per_sec, bytes_written_per_sec,
      compact_->compaction->output_level(),
      stats.num_input_files_in_non_output_levels,
      stats.num_input_files_in_output_level, stats.num_output_files,
      stats.num_output_files_blob, stats.bytes_read_non_output_levels / kMB,
      stats.bytes_read_output_level / kMB, stats.bytes_read_blob / kMB,
      stats.bytes_written / kMB, stats.bytes_written_blob / kMB, read_write_amp,
      write_amp, status.ToString().c_str(), stats.num_input_records,
      stats.num_dropped_records,
      CompressionTypeToString(compact_->compaction->output_compression())
          .c_str());

  const auto& blob_files = vstorage->GetBlobFiles();
  if (!blob_files.empty()) {
    assert(blob_files.front());
    assert(blob_files.back());

    ROCKS_LOG_BUFFER(
        log_buffer_,
        "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64 "\n",
        column_family_name.c_str(), blob_files.front()->GetBlobFileNumber(),
        blob_files.back()->GetBlobFileNumber());
  }

  if (compaction_stats_.has_penultimate_level_output) {
    ROCKS_LOG_BUFFER(
        log_buffer_,
        "[%s] has Penultimate Level output: %" PRIu64
        ", level %d, number of files: %" PRIu64 ", number of records: %" PRIu64,
        column_family_name.c_str(),
        compaction_stats_.penultimate_level_stats.bytes_written,
        compact_->compaction->GetPenultimateLevel(),
        compaction_stats_.penultimate_level_stats.num_output_files,
        compaction_stats_.penultimate_level_stats.num_output_records);
  }

  UpdateCompactionJobStats(stats);

  auto stream = event_logger_->LogToBuffer(log_buffer_, 8192);
  stream << "job" << job_id_ << "event"
         << "compaction_finished"
         << "compaction_time_micros" << stats.micros
         << "compaction_time_cpu_micros" << stats.cpu_micros << "output_level"
         << compact_->compaction->output_level() << "num_output_files"
         << stats.num_output_files << "total_output_size"
         << stats.bytes_written;

  if (stats.num_output_files_blob > 0) {
    stream << "num_blob_output_files" << stats.num_output_files_blob
           << "total_blob_output_size" << stats.bytes_written_blob;
  }

  stream << "num_input_records" << stats.num_input_records
         << "num_output_records" << stats.num_output_records
         << "num_subcompactions" << compact_->sub_compact_states.size()
         << "output_compression"
         << CompressionTypeToString(compact_->compaction->output_compression());

  stream << "num_single_delete_mismatches"
         << compaction_job_stats_->num_single_del_mismatch;
  stream << "num_single_delete_fallthrough"
         << compaction_job_stats_->num_single_del_fallthru;

  if (measure_io_stats_) {
    stream << "file_write_nanos" << compaction_job_stats_->file_write_nanos;
    stream << "file_range_sync_nanos"
           << compaction_job_stats_->file_range_sync_nanos;
    stream << "file_fsync_nanos" << compaction_job_stats_->file_fsync_nanos;
    stream << "file_prepare_write_nanos"
           << compaction_job_stats_->file_prepare_write_nanos;
  }

  stream << "lsm_state";
  stream.StartArray();
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    stream << vstorage->NumLevelFiles(level);
  }
  stream.EndArray();

  if (!blob_files.empty()) {
    assert(blob_files.front());
    stream << "blob_file_head" << blob_files.front()->GetBlobFileNumber();

    assert(blob_files.back());
    stream << "blob_file_tail" << blob_files.back()->GetBlobFileNumber();
  }

  if (compaction_stats_.has_penultimate_level_output) {
    InternalStats::CompactionStats& pl_stats =
        compaction_stats_.penultimate_level_stats;
    stream << "penultimate_level_num_output_files" << pl_stats.num_output_files;
    stream << "penultimate_level_bytes_written" << pl_stats.bytes_written;
    stream << "penultimate_level_num_output_records"
           << pl_stats.num_output_records;
    stream << "penultimate_level_num_output_files_blob"
           << pl_stats.num_output_files_blob;
    stream << "penultimate_level_bytes_written_blob"
           << pl_stats.bytes_written_blob;
  }

  CleanupCompaction();
  return status;
}

void CompactionJob::NotifyOnSubcompactionBegin(
    SubcompactionState* sub_compact) {
#ifndef ROCKSDB_LITE
  Compaction* c = compact_->compaction;

  if (db_options_.listeners.empty()) {
    return;
  }
  if (shutting_down_->load(std::memory_order_acquire)) {
    return;
  }
  if (c->is_manual_compaction() &&
      manual_compaction_canceled_.load(std::memory_order_acquire)) {
    return;
  }

  sub_compact->notify_on_subcompaction_completion = true;

  SubcompactionJobInfo info{};
  sub_compact->BuildSubcompactionJobInfo(info);
  info.job_id = static_cast<int>(job_id_);
  info.thread_id = env_->GetThreadID();

  for (const auto& listener : db_options_.listeners) {
    listener->OnSubcompactionBegin(info);
  }
  info.status.PermitUncheckedError();

#else
  (void)sub_compact;
#endif  // ROCKSDB_LITE
}

void CompactionJob::NotifyOnSubcompactionCompleted(
    SubcompactionState* sub_compact) {
#ifndef ROCKSDB_LITE

  if (db_options_.listeners.empty()) {
    return;
  }
  if (shutting_down_->load(std::memory_order_acquire)) {
    return;
  }

  if (sub_compact->notify_on_subcompaction_completion == false) {
    return;
  }

  SubcompactionJobInfo info{};
  sub_compact->BuildSubcompactionJobInfo(info);
  info.job_id = static_cast<int>(job_id_);
  info.thread_id = env_->GetThreadID();

  for (const auto& listener : db_options_.listeners) {
    listener->OnSubcompactionCompleted(info);
  }
#else
  (void)sub_compact;
#endif  // ROCKSDB_LITE
}

void CompactionJob::ProcessKeyValueCompaction(SubcompactionState* sub_compact) {
  assert(sub_compact);
  assert(sub_compact->compaction);

#ifndef ROCKSDB_LITE
  if (db_options_.compaction_service) {
    CompactionServiceJobStatus comp_status =
        ProcessKeyValueCompactionWithCompactionService(sub_compact);
    if (comp_status == CompactionServiceJobStatus::kSuccess ||
        comp_status == CompactionServiceJobStatus::kFailure) {
      return;
    }
    // fallback to local compaction
    assert(comp_status == CompactionServiceJobStatus::kUseLocal);
  }
#endif  // !ROCKSDB_LITE

  uint64_t prev_cpu_micros = db_options_.clock->CPUMicros();

  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();

  // Create compaction filter and fail the compaction if
  // IgnoreSnapshots() = false because it is not supported anymore
  const CompactionFilter* compaction_filter =
      cfd->ioptions()->compaction_filter;
  std::unique_ptr<CompactionFilter> compaction_filter_from_factory = nullptr;
  if (compaction_filter == nullptr) {
    compaction_filter_from_factory =
        sub_compact->compaction->CreateCompactionFilter();
    compaction_filter = compaction_filter_from_factory.get();
  }
  if (compaction_filter != nullptr && !compaction_filter->IgnoreSnapshots()) {
    sub_compact->status = Status::NotSupported(
        "CompactionFilter::IgnoreSnapshots() = false is not supported "
        "anymore.");
    return;
  }

  NotifyOnSubcompactionBegin(sub_compact);

  auto range_del_agg = std::make_unique<CompactionRangeDelAggregator>(
      &cfd->internal_comparator(), existing_snapshots_);

  // TODO: since we already use C++17, should use
  // std::optional<const Slice> instead.
  const std::optional<Slice> start = sub_compact->start;
  const std::optional<Slice> end = sub_compact->end;

  ReadOptions read_options;
  read_options.verify_checksums = true;
  read_options.fill_cache = false;
  read_options.rate_limiter_priority = GetRateLimiterPriority();
  // Compaction iterators shouldn't be confined to a single prefix.
  // Compactions use Seek() for
  // (a) concurrent compactions,
  // (b) CompactionFilter::Decision::kRemoveAndSkipUntil.
  read_options.total_order_seek = true;

  // Note: if we're going to support subcompactions for user-defined timestamps,
  // the timestamp part will have to be stripped from the bounds here.
  assert((!start.has_value() && !end.has_value()) ||
         cfd->user_comparator()->timestamp_size() == 0);
  if (start.has_value()) {
    read_options.iterate_lower_bound = &start.value();
  }
  if (end.has_value()) {
    read_options.iterate_upper_bound = &end.value();
  }

  // Although the v2 aggregator is what the level iterator(s) know about,
  // the AddTombstones calls will be propagated down to the v1 aggregator.
  // 1.获取本subcompact的所有key-value到iterator——raw_input中
  std::unique_ptr<InternalIterator> raw_input(versions_->MakeInputIterator(
      read_options, sub_compact->compaction, range_del_agg.get(),
      file_options_for_read_, start, end));
  InternalIterator* input = raw_input.get();

  IterKey start_ikey;
  IterKey end_ikey;
  Slice start_slice;
  Slice end_slice;

  if (start.has_value()) {
    start_ikey.SetInternalKey(start.value(), kMaxSequenceNumber,
                              kValueTypeForSeek);
    start_slice = start_ikey.GetInternalKey();
  }
  if (end.has_value()) {
    end_ikey.SetInternalKey(end.value(), kMaxSequenceNumber, kValueTypeForSeek);
    end_slice = end_ikey.GetInternalKey();
  }

  // 2.按照本次sub compaction的range范围进行clip切割
  std::unique_ptr<InternalIterator> clip;
  if (start.has_value() || end.has_value()) {
    clip = std::make_unique<ClippingIterator>(
        raw_input.get(), start.has_value() ? &start_slice : nullptr,
        end.has_value() ? &end_slice : nullptr, &cfd->internal_comparator());
    input = clip.get();
  }

  std::unique_ptr<InternalIterator> blob_counter;

  if (sub_compact->compaction->DoesInputReferenceBlobFiles()) {
    BlobGarbageMeter* meter = sub_compact->Current().CreateBlobGarbageMeter();
    blob_counter = std::make_unique<BlobCountingIterator>(input, meter);
    input = blob_counter.get();
  }

  std::unique_ptr<InternalIterator> trim_history_iter;
  if (cfd->user_comparator()->timestamp_size() > 0 && !trim_ts_.empty()) {
    trim_history_iter = std::make_unique<HistoryTrimmingIterator>(
        input, cfd->user_comparator(), trim_ts_);
    input = trim_history_iter.get();
  }

  input->SeekToFirst();

  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_PROCESS_KV);

  // I/O measurement variables
  PerfLevel prev_perf_level = PerfLevel::kEnableTime;
  const uint64_t kRecordStatsEvery = 1000;
  uint64_t prev_write_nanos = 0;
  uint64_t prev_fsync_nanos = 0;
  uint64_t prev_range_sync_nanos = 0;
  uint64_t prev_prepare_write_nanos = 0;
  uint64_t prev_cpu_write_nanos = 0;
  uint64_t prev_cpu_read_nanos = 0;
  if (measure_io_stats_) {
    prev_perf_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    prev_write_nanos = IOSTATS(write_nanos);
    prev_fsync_nanos = IOSTATS(fsync_nanos);
    prev_range_sync_nanos = IOSTATS(range_sync_nanos);
    prev_prepare_write_nanos = IOSTATS(prepare_write_nanos);
    prev_cpu_write_nanos = IOSTATS(cpu_write_nanos);
    prev_cpu_read_nanos = IOSTATS(cpu_read_nanos);
  }

  // 这个用来进行真正的compaction遍历过程中的Merge逻辑的处理。
  MergeHelper merge(
      env_, cfd->user_comparator(), cfd->ioptions()->merge_operator.get(),
      compaction_filter, db_options_.info_log.get(),
      false /* internal key corruption is expected */,
      existing_snapshots_.empty() ? 0 : existing_snapshots_.back(),
      snapshot_checker_, compact_->compaction->level(), db_options_.stats);

  const MutableCFOptions* mutable_cf_options =
      sub_compact->compaction->mutable_cf_options();
  assert(mutable_cf_options);

  std::vector<std::string> blob_file_paths;

  // TODO: BlobDB to support output_to_penultimate_level compaction, which needs
  //  2 builders, so may need to move to `CompactionOutputs`
  std::unique_ptr<BlobFileBuilder> blob_file_builder(
      (mutable_cf_options->enable_blob_files &&
       sub_compact->compaction->output_level() >=
           mutable_cf_options->blob_file_starting_level)
          ? new BlobFileBuilder(
                versions_, fs_.get(),
                sub_compact->compaction->immutable_options(),
                mutable_cf_options, &file_options_, db_id_, db_session_id_,
                job_id_, cfd->GetID(), cfd->GetName(), Env::IOPriority::IO_LOW,
                write_hint_, io_tracer_, blob_callback_,
                BlobFileCreationReason::kCompaction, &blob_file_paths,
                sub_compact->Current().GetBlobFileAdditionsPtr())
          : nullptr);

  TEST_SYNC_POINT("CompactionJob::Run():Inprogress");
  TEST_SYNC_POINT_CALLBACK(
      "CompactionJob::Run():PausingManualCompaction:1",
      reinterpret_cast<void*>(
          const_cast<std::atomic<bool>*>(&manual_compaction_canceled_)));

  const std::string* const full_history_ts_low =
      full_history_ts_low_.empty() ? nullptr : &full_history_ts_low_;
  const SequenceNumber job_snapshot_seq =
      job_context_ ? job_context_->GetJobSnapshotSequence()
                   : kMaxSequenceNumber;

  auto c_iter = std::make_unique<CompactionIterator>(
      input, cfd->user_comparator(), &merge, versions_->LastSequence(),
      &existing_snapshots_, earliest_write_conflict_snapshot_, job_snapshot_seq,
      snapshot_checker_, env_, ShouldReportDetailedTime(env_, stats_),
      /*expect_valid_internal_key=*/true, range_del_agg.get(),
      blob_file_builder.get(), db_options_.allow_data_in_errors,
      db_options_.enforce_single_del_contracts, manual_compaction_canceled_,
      sub_compact->compaction, compaction_filter, shutting_down_,
      db_options_.info_log, full_history_ts_low,
      penultimate_level_cutoff_seqno_,false,sub_compact->compaction->is_gc_compaction(), sub_compact->compaction->get_index_to_tables(), sub_compact->compaction->is_vertical_compaction(), sub_compact->compaction->vertical_start_level()); // 在此处根据是否为value compaction决定后面的merge操作流程
  c_iter->SeekToFirst();

  // Assign range delete aggregator to the target output level, which makes sure
  // it only output to single level
  sub_compact->AssignRangeDelAggregator(std::move(range_del_agg));

  if (c_iter->Valid() && sub_compact->compaction->output_level() != 0) {
    sub_compact->FillFilesToCutForTtl();
    // ShouldStopBefore() maintains state based on keys processed so far. The
    // compaction loop always calls it on the "next" key, thus won't tell it the
    // first key. So we do that here.
    sub_compact->ShouldStopBefore(c_iter->key());
  }
  const auto& c_iter_stats = c_iter->iter_stats();

  // define the open and close functions for the compaction files, which will be
  // used open/close output files when needed.
  //定义好本次sub compaction时的OpenCompaction和FinishCompaction的逻辑.
  const CompactionFileOpenFunc open_file_func =
      [this, sub_compact](CompactionOutputs& outputs) {
        return this->OpenCompactionOutputFile(sub_compact, outputs);
      };
  const CompactionFileCloseFunc close_file_func =
      [this, sub_compact](CompactionOutputs& outputs, const Status& status,
                          const Slice& next_table_min_key) {
        return this->FinishCompactionOutputFile(status, sub_compact, outputs,
                                                next_table_min_key);
      };

  Status status;
  // 3.核心中的核心. 根据iter的内容，遍历然后得到当前的key，执行具体的逻辑
  // 如果本次sub compaction不包含任何input level的数据,那么c_iter是非Valid的.不会走下面的构造逻辑

  bool first_pick_guard = false;
  int temp = first_pick_guard_->load();
  // 不需要while循环 不是每一个thread都需要进入这里的逻辑...
  if (compact_->compaction->start_level() == 0 && compact_->compaction->output_level() == 1 && temp == 1 && std::atomic_compare_exchange_strong(first_pick_guard_, &temp, 0)) {
    first_pick_guard = true;
    std::cout <<"first guard" << std::endl;
  }
  // first_pick_guard = true;
  while (status.ok() && !cfd->IsDropped() && c_iter->Valid()) {
    // Invariant: c_iter.status() is guaranteed to be OK if c_iter->Valid()
    // returns true.

    assert(!end.has_value() || cfd->user_comparator()->Compare(
                                   c_iter->user_key(), end.value()) < 0);

    if (c_iter_stats.num_input_records % kRecordStatsEvery ==
        kRecordStatsEvery - 1) {
      RecordDroppedKeys(c_iter_stats, &sub_compact->compaction_job_stats);
      c_iter->ResetRecordCounts();
      RecordCompactionIOStats();
    }

    // Add current compaction_iterator key to target compaction output, if the
    // output file needs to be close or open, it will call the `open_file_func`
    // and `close_file_func`.
    // TODO: it would be better to have the compaction file open/close moved
    // into `CompactionOutputs` which has the output file information.
    // 只有进入这里 才会第一次被设置

    status = sub_compact->AddToOutput(*c_iter, open_file_func, close_file_func, first_pick_guard);
    if (!status.ok()) {
      break;
    }

    TEST_SYNC_POINT_CALLBACK(
        "CompactionJob::Run():PausingManualCompaction:2",
        reinterpret_cast<void*>(
            const_cast<std::atomic<bool>*>(&manual_compaction_canceled_)));
    c_iter->Next();
    if (c_iter->status().IsManualCompactionPaused()) {
      break;
    }

    // TODO: Support earlier file cut for the penultimate level files. Maybe by
    //  moving `ShouldStopBefore()` to `CompactionOutputs` class. Currently
    //  the penultimate level output is only cut when it reaches the size limit.
    if (!sub_compact->Current().IsPendingClose() &&
        sub_compact->compaction->output_level() != 0 &&
        !sub_compact->compaction->SupportsPerKeyPlacement() &&
        sub_compact->ShouldStopBefore(c_iter->key())) {
      sub_compact->Current().SetPendingClose();
    }
  }

  sub_compact->compaction_job_stats.num_blobs_read =
      c_iter_stats.num_blobs_read;
  sub_compact->compaction_job_stats.total_blob_bytes_read =
      c_iter_stats.total_blob_bytes_read;
  sub_compact->compaction_job_stats.num_input_deletion_records =
      c_iter_stats.num_input_deletion_records;
  sub_compact->compaction_job_stats.num_corrupt_keys =
      c_iter_stats.num_input_corrupt_records;
  sub_compact->compaction_job_stats.num_single_del_fallthru =
      c_iter_stats.num_single_del_fallthru;
  sub_compact->compaction_job_stats.num_single_del_mismatch =
      c_iter_stats.num_single_del_mismatch;
  sub_compact->compaction_job_stats.total_input_raw_key_bytes +=
      c_iter_stats.total_input_raw_key_bytes;
  sub_compact->compaction_job_stats.total_input_raw_value_bytes +=
      c_iter_stats.total_input_raw_value_bytes;

  RecordTick(stats_, FILTER_OPERATION_TOTAL_TIME,
             c_iter_stats.total_filter_time);

  if (c_iter_stats.num_blobs_relocated > 0) {
    RecordTick(stats_, BLOB_DB_GC_NUM_KEYS_RELOCATED,
               c_iter_stats.num_blobs_relocated);
  }
  if (c_iter_stats.total_blob_bytes_relocated > 0) {
    RecordTick(stats_, BLOB_DB_GC_BYTES_RELOCATED,
               c_iter_stats.total_blob_bytes_relocated);
  }

  RecordDroppedKeys(c_iter_stats, &sub_compact->compaction_job_stats);
  RecordCompactionIOStats();

  if (status.ok() && cfd->IsDropped()) {
    status =
        Status::ColumnFamilyDropped("Column family dropped during compaction");
  }
  if ((status.ok() || status.IsColumnFamilyDropped()) &&
      shutting_down_->load(std::memory_order_relaxed)) {
    status = Status::ShutdownInProgress("Database shutdown");
  }
  if ((status.ok() || status.IsColumnFamilyDropped()) &&
      (manual_compaction_canceled_.load(std::memory_order_relaxed))) {
    status = Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }
  if (status.ok()) {
    status = input->status();
  }
  if (status.ok()) {
    status = c_iter->status();
  }

  // Call FinishCompactionOutputFile() even if status is not ok: it needs to
  // close the output files. Open file function is also passed, in case there's
  // only range-dels, no file was opened, to save the range-dels, it need to
  // create a new output file.
  status = sub_compact->CloseCompactionFiles(status, open_file_func,
                                             close_file_func);

  if (blob_file_builder) {
    if (status.ok()) {
      status = blob_file_builder->Finish();
    } else {
      blob_file_builder->Abandon(status);
    }
    blob_file_builder.reset();
    sub_compact->Current().UpdateBlobStats();
  }

  sub_compact->compaction_job_stats.cpu_micros =
      db_options_.clock->CPUMicros() - prev_cpu_micros;

  if (measure_io_stats_) {
    sub_compact->compaction_job_stats.file_write_nanos +=
        IOSTATS(write_nanos) - prev_write_nanos;
    sub_compact->compaction_job_stats.file_fsync_nanos +=
        IOSTATS(fsync_nanos) - prev_fsync_nanos;
    sub_compact->compaction_job_stats.file_range_sync_nanos +=
        IOSTATS(range_sync_nanos) - prev_range_sync_nanos;
    sub_compact->compaction_job_stats.file_prepare_write_nanos +=
        IOSTATS(prepare_write_nanos) - prev_prepare_write_nanos;
    sub_compact->compaction_job_stats.cpu_micros -=
        (IOSTATS(cpu_write_nanos) - prev_cpu_write_nanos +
         IOSTATS(cpu_read_nanos) - prev_cpu_read_nanos) /
        1000;
    if (prev_perf_level != PerfLevel::kEnableTimeAndCPUTimeExceptForMutex) {
      SetPerfLevel(prev_perf_level);
    }
  }
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
  if (!status.ok()) {
    if (c_iter) {
      c_iter->status().PermitUncheckedError();
    }
    if (input) {
      input->status().PermitUncheckedError();
    }
  }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED

  blob_counter.reset();
  clip.reset();
  raw_input.reset();
  sub_compact->status = status;
  NotifyOnSubcompactionCompleted(sub_compact);
}

uint64_t CompactionJob::GetCompactionId(SubcompactionState* sub_compact) const {
  return (uint64_t)job_id_ << 32 | sub_compact->sub_job_id;
}

void CompactionJob::RecordDroppedKeys(
    const CompactionIterationStats& c_iter_stats,
    CompactionJobStats* compaction_job_stats) {
  if (c_iter_stats.num_record_drop_user > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_USER,
               c_iter_stats.num_record_drop_user);
  }
  if (c_iter_stats.num_record_drop_hidden > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_NEWER_ENTRY,
               c_iter_stats.num_record_drop_hidden);
    if (compaction_job_stats) {
      compaction_job_stats->num_records_replaced +=
          c_iter_stats.num_record_drop_hidden;
    }
  }
  if (c_iter_stats.num_record_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_OBSOLETE,
               c_iter_stats.num_record_drop_obsolete);
    if (compaction_job_stats) {
      compaction_job_stats->num_expired_deletion_records +=
          c_iter_stats.num_record_drop_obsolete;
    }
  }
  if (c_iter_stats.num_record_drop_range_del > 0) {
    RecordTick(stats_, COMPACTION_KEY_DROP_RANGE_DEL,
               c_iter_stats.num_record_drop_range_del);
  }
  if (c_iter_stats.num_range_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_RANGE_DEL_DROP_OBSOLETE,
               c_iter_stats.num_range_del_drop_obsolete);
  }
  if (c_iter_stats.num_optimized_del_drop_obsolete > 0) {
    RecordTick(stats_, COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
               c_iter_stats.num_optimized_del_drop_obsolete);
  }
}

Status CompactionJob::FinishCompactionOutputFile(
    const Status& input_status, SubcompactionState* sub_compact,
    CompactionOutputs& outputs, const Slice& next_table_min_key) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_SYNC_FILE);
  assert(sub_compact != nullptr);
  assert(outputs.HasBuilder());

  // 获取本次compaction是否为value compaction(是的话就要去刷磁盘等一系列逻辑)
  bool gc_compaction = sub_compact->compaction->is_gc_compaction();
  bool vertical_compaction = sub_compact->compaction->is_vertical_compaction();
  FileMetaData* index_meta = outputs.GetMetaData();
  FileMetaData* table_meta;
  if(gc_compaction || vertical_compaction){
    table_meta = outputs.GetTableMetaData();
  }
  uint64_t output_number = index_meta->fd.GetNumber();
  assert(output_number != 0);

  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
  std::string file_checksum = kUnknownFileChecksum;
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;

  // Check for iterator errors
  Status s = input_status;

  // Add range tombstones
  auto earliest_snapshot = kMaxSequenceNumber;
  if (existing_snapshots_.size() > 0) {
    earliest_snapshot = existing_snapshots_[0];
  }
  // 增加范围tombstones
  if (s.ok()) {
    CompactionIterationStats range_del_out_stats;
    // if the compaction supports per_key_placement, only output range dels to
    // the penultimate level.
    // Note: Use `bottommost_level_ = true` for both bottommost and
    // output_to_penultimate_level compaction here, as it's only used to decide
    // if range dels could be dropped.
    if (outputs.HasRangeDel()) {
      s = outputs.AddRangeDels(
          sub_compact->start.has_value() ? &(sub_compact->start.value())
                                         : nullptr,
          sub_compact->end.has_value() ? &(sub_compact->end.value()) : nullptr,
          range_del_out_stats, bottommost_level_, cfd->internal_comparator(),
          earliest_snapshot, next_table_min_key);
    }
    RecordDroppedKeys(range_del_out_stats, &sub_compact->compaction_job_stats);
    TEST_SYNC_POINT("CompactionJob::FinishCompactionOutputFile1");
  }

  const uint64_t current_entries = outputs.NumEntries();

  s = outputs.Finish(s, seqno_time_mapping_,gc_compaction, vertical_compaction);

  //设置oldest ancester time
  if (s.ok()) {
    // With accurate smallest and largest key, we can get a slightly more
    // accurate oldest ancester time.
    // This makes oldest ancester time in manifest more accurate than in
    // table properties. Not sure how to resolve it.
    if (index_meta->smallest.size() > 0 && index_meta->largest.size() > 0) {
      uint64_t refined_oldest_ancester_time;
      Slice new_smallest = index_meta->smallest.user_key();
      Slice new_largest = index_meta->largest.user_key();
      if (!new_largest.empty() && !new_smallest.empty()) {
        refined_oldest_ancester_time =
            sub_compact->compaction->MinInputFileOldestAncesterTime(
                &(index_meta->smallest), &(index_meta->largest));
        if (refined_oldest_ancester_time !=
            std::numeric_limits<uint64_t>::max()) {
          index_meta->oldest_ancester_time = refined_oldest_ancester_time;
        }
      }
    }
  }

  // Finish and check for file errors
  IOStatus io_s;
  if(gc_compaction || vertical_compaction)
    io_s = outputs.WriterSyncClose(s, db_options_.clock, stats_,
                                          db_options_.use_fsync);

  if (s.ok() && io_s.ok()) {
    file_checksum = index_meta->file_checksum;
    file_checksum_func_name = index_meta->file_checksum_func_name;
  }

  if (s.ok()) {
    s = io_s;
  }
  if (sub_compact->io_status.ok()) {
    sub_compact->io_status = io_s;
    // Since this error is really a copy of the
    // "normal" status, it does not also need to be checked
    sub_compact->io_status.PermitUncheckedError();
  }

  TableProperties tp;
  if (s.ok()) {
    tp = outputs.GetTableProperties();
  }

  if (s.ok() && current_entries == 0 && tp.num_range_deletions == 0) {
    // If there is nothing to output, no necessary to generate a sst file.
    // This happens when the output level is bottom level, at the same time
    // the sub_compact output nothing.
    std::string index_name =
        MakeIndexFileName(index_meta->fd.GetNumber());

    // TODO(AR) it is not clear if there are any larger implications if
    // DeleteFile fails here
    Status ds = env_->DeleteFile(index_name);
    if (!ds.ok()) {
      ROCKS_LOG_WARN(
          db_options_.info_log,
          "[%s] [JOB %d] Unable to remove SST file for table #%" PRIu64
          " at bottom level%s",
          cfd->GetName().c_str(), job_id_, output_number,
          index_meta->marked_for_compaction ? " (need compaction)" : "");
    }

    // Also need to remove the file from outputs, or it will be added to the
    // VersionEdit.
    outputs.RemoveLastOutput();
    index_meta = nullptr;
  }

  if (s.ok() && (current_entries > 0 || tp.num_range_deletions > 0)) {
    // Output to event logger and fire events.
    outputs.UpdateTableProperties();
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[%s] [JOB %d] Generated table #%" PRIu64 ": %" PRIu64
                   " keys, %" PRIu64 " bytes%s, temperature: %s",
                   cfd->GetName().c_str(), job_id_, output_number,
                   current_entries, index_meta->fd.file_size,
                   index_meta->marked_for_compaction ? " (need compaction)" : "",
                   temperature_to_string[index_meta->temperature].c_str());
  }
  std::string index_name;
  FileDescriptor output_fd;
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;
  Status status_for_listener = s;
  if (index_meta != nullptr) {
    index_name = GetTableFileName(index_meta->fd.GetNumber());
    output_fd = index_meta->fd;
    oldest_blob_file_number = index_meta->oldest_blob_file_number;
  } else {
    index_name = "(nil)";
    if (s.ok()) {
      status_for_listener = Status::Aborted("Empty SST file not kept");
    }
  }
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(), index_name,
      job_id_, output_fd, oldest_blob_file_number, tp,
      TableFileCreationReason::kCompaction, status_for_listener, file_checksum,
      file_checksum_func_name);

#ifndef ROCKSDB_LITE
  // Report new file to SstFileManagerImpl
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
  if (sfm && index_meta != nullptr && index_meta->fd.GetPathId() == 0) {
    Status add_s = sfm->OnAddFile(index_name);
    if (!add_s.ok() && s.ok()) {
      s = add_s;
    }
    if (sfm->IsMaxAllowedSpaceReached()) {
      // TODO(ajkr): should we return OK() if max space was reached by the final
      // compaction output file (similarly to how flush works when full)?
      s = Status::SpaceLimit("Max allowed space was reached");
      TEST_SYNC_POINT(
          "CompactionJob::FinishCompactionOutputFile:MaxAllowedSpaceReached");
      InstrumentedMutexLock l(db_mutex_);
      db_error_handler_->SetBGError(s, BackgroundErrorReason::kCompaction);
    }
  }
#endif

  outputs.ResetBuilder();
  return s;
}

Status CompactionJob::InstallCompactionResults(
    const MutableCFOptions& mutable_cf_options) {
  assert(compact_);

  db_mutex_->AssertHeld();

  auto* compaction = compact_->compaction;
  assert(compaction);

  {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    if (compaction_stats_.has_penultimate_level_output) {
      ROCKS_LOG_BUFFER(
          log_buffer_,
          "[%s] [JOB %d] Compacted %s => output_to_penultimate_level: %" PRIu64
          " bytes + last: %" PRIu64 " bytes. Total: %" PRIu64 " bytes",
          compaction->column_family_data()->GetName().c_str(), job_id_,
          compaction->InputLevelSummary(&inputs_summary),
          compaction_stats_.penultimate_level_stats.bytes_written,
          compaction_stats_.stats.bytes_written,
          compaction_stats_.TotalBytesWritten());
    } else {
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] [JOB %d] Compacted %s => %" PRIu64 " bytes",
                       compaction->column_family_data()->GetName().c_str(),
                       job_id_, compaction->InputLevelSummary(&inputs_summary),
                       compaction_stats_.TotalBytesWritten());
    }
  }

  VersionEdit* const edit = compaction->edit();
  assert(edit);

  // Add compaction inputs
  compaction->AddInputDeletions(edit);

  std::unordered_map<uint64_t, BlobGarbageMeter::BlobStats> blob_total_garbage;

  for (const auto& sub_compact : compact_->sub_compact_states) {
    sub_compact.AddOutputsEdit(edit);

    for (const auto& blob : sub_compact.Current().GetBlobFileAdditions()) {
      edit->AddBlobFile(blob);
    }

    if (sub_compact.Current().GetBlobGarbageMeter()) {
      const auto& flows = sub_compact.Current().GetBlobGarbageMeter()->flows();

      for (const auto& pair : flows) {
        const uint64_t blob_file_number = pair.first;
        const BlobGarbageMeter::BlobInOutFlow& flow = pair.second;

        assert(flow.IsValid());
        if (flow.HasGarbage()) {
          blob_total_garbage[blob_file_number].Add(flow.GetGarbageCount(),
                                                   flow.GetGarbageBytes());
        }
      }
    }
  }

  for (const auto& pair : blob_total_garbage) {
    const uint64_t blob_file_number = pair.first;
    const BlobGarbageMeter::BlobStats& stats = pair.second;

    edit->AddBlobFileGarbage(blob_file_number, stats.GetCount(),
                             stats.GetBytes());
  }

  if (compaction->compaction_reason() == CompactionReason::kLevelMaxLevelSize &&
      compaction->immutable_options()->compaction_pri == kRoundRobin) {
    int start_level = compaction->start_level();
    if (start_level > 0) {
      auto vstorage = compaction->input_version()->storage_info();
      edit->AddCompactCursor(start_level,
                             vstorage->GetNextCompactCursor(
                                 start_level, compaction->num_input_files(0)));
    }
  }

  return versions_->LogAndApply(compaction->column_family_data(),
                                mutable_cf_options, edit, db_mutex_,
                                db_directory_);
}

void CompactionJob::RecordCompactionIOStats() {
  RecordTick(stats_, COMPACT_READ_BYTES, IOSTATS(bytes_read));
  RecordTick(stats_, COMPACT_WRITE_BYTES, IOSTATS(bytes_written));
  CompactionReason compaction_reason =
      compact_->compaction->compaction_reason();
  if (compaction_reason == CompactionReason::kFilesMarkedForCompaction) {
    RecordTick(stats_, COMPACT_READ_BYTES_MARKED, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_MARKED, IOSTATS(bytes_written));
  } else if (compaction_reason == CompactionReason::kPeriodicCompaction) {
    RecordTick(stats_, COMPACT_READ_BYTES_PERIODIC, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_PERIODIC, IOSTATS(bytes_written));
  } else if (compaction_reason == CompactionReason::kTtl) {
    RecordTick(stats_, COMPACT_READ_BYTES_TTL, IOSTATS(bytes_read));
    RecordTick(stats_, COMPACT_WRITE_BYTES_TTL, IOSTATS(bytes_written));
  }
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_READ, IOSTATS(bytes_read));
  IOSTATS_RESET(bytes_read);
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::COMPACTION_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

Status CompactionJob::OpenCompactionOutputFile(SubcompactionState* sub_compact,
                                               CompactionOutputs& outputs) {
  assert(sub_compact != nullptr);

  // 获取本次compaction是否为value compaction(是的话就要去生成table file了)
  //WQTODO 未来也许可以作为CompactionJob的成员变量，避免每次都要去获取
  bool gc_compaction = sub_compact->compaction->is_gc_compaction();
  bool vertical_compaction = sub_compact->compaction->is_vertical_compaction();
  // no need to lock because VersionSet::next_file_number_ is atomic
  uint64_t index_number = versions_->NewFileNumber();
  std::string index_name = MakeIndexFileName(sub_compact->compaction->immutable_options()->db_nvm_dir,index_number);
  std::string table_name;
  uint64_t table_number;
  if(gc_compaction || vertical_compaction){
    table_number = versions_->NewFileNumber();
    table_name = GetTableFileName(table_number); // 这里得到的是table file的文件路径，index file固定在db_path[1]中
  }
  // Fire events.
  ColumnFamilyData* cfd = sub_compact->compaction->column_family_data();
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      cfd->ioptions()->listeners, dbname_, cfd->GetName(), index_name, job_id_,
      TableFileCreationReason::kCompaction);
#endif  // !ROCKSDB_LITE
  // Make the output file
  std::unique_ptr<FSWritableFile> writable_file;
#ifndef NDEBUG
  bool syncpoint_arg = file_options_.use_direct_writes;
  TEST_SYNC_POINT_CALLBACK("CompactionJob::OpenCompactionOutputFile",
                           &syncpoint_arg);
#endif

  // Pass temperature of botommost files to FileSystem.
  FileOptions fo_copy = file_options_;
  Temperature temperature = sub_compact->compaction->output_temperature();
  if (temperature == Temperature::kUnknown && bottommost_level_ &&
      !sub_compact->IsCurrentPenultimateLevel()) {
    temperature =
        sub_compact->compaction->mutable_cf_options()->bottommost_temperature;
  }
  fo_copy.temperature = temperature;

  Status s;
  IOStatus io_s;
  if(gc_compaction || vertical_compaction){
    io_s = NewWritableFile(fs_.get(), table_name, &writable_file, fo_copy);
    s = io_s;
  }
  if (sub_compact->io_status.ok()) {
    sub_compact->io_status = io_s;
    // Since this error is really a copy of the io_s that is checked below as s,
    // it does not also need to be checked.
    sub_compact->io_status.PermitUncheckedError();
  }
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "[%s] [JOB %d] OpenCompactionOutputFiles for table #%" PRIu64
        " fails at NewWritableFile with status %s",
        sub_compact->compaction->column_family_data()->GetName().c_str(),
        job_id_, index_name, s.ToString().c_str());
    LogFlush(db_options_.info_log);
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        event_logger_, cfd->ioptions()->listeners, dbname_, cfd->GetName(),
        index_name, job_id_, FileDescriptor(), kInvalidBlobFileNumber,
        TableProperties(), TableFileCreationReason::kCompaction, s,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName);
    return s;
  }

  // Try to figure out the output file's oldest ancester time.
  int64_t temp_current_time = 0;
  auto get_time_status = db_options_.clock->GetCurrentTime(&temp_current_time);
  // Safe to proceed even if GetCurrentTime fails. So, log and proceed.
  if (!get_time_status.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Failed to get current time. Status: %s",
                   get_time_status.ToString().c_str());
  }
  uint64_t current_time = static_cast<uint64_t>(temp_current_time);
  InternalKey tmp_start, tmp_end;
  if (sub_compact->start.has_value()) {
    tmp_start.SetMinPossibleForUserKey(sub_compact->start.value());
  }
  if (sub_compact->end.has_value()) {
    tmp_end.SetMinPossibleForUserKey(sub_compact->end.value());
  }
  uint64_t oldest_ancester_time =
      sub_compact->compaction->MinInputFileOldestAncesterTime(
          sub_compact->start.has_value() ? &tmp_start : nullptr,
          sub_compact->end.has_value() ? &tmp_end : nullptr);
  if (oldest_ancester_time == std::numeric_limits<uint64_t>::max()) {
    oldest_ancester_time = current_time;
  }

  // Initialize a SubcompactionState::Output and add it to sub_compact->outputs
  {
    // 默认添加index file的FileMetaData信息
    FileMetaData index_meta;
    // 默认选择file path为1
    index_meta.fd = FileDescriptor(index_number, 1, 0);
    index_meta.oldest_ancester_time = oldest_ancester_time;
    index_meta.file_creation_time = current_time;
    index_meta.temperature = temperature;
    assert(!db_id_.empty());
    assert(!db_session_id_.empty());
    s = GetSstInternalUniqueId(db_id_, db_session_id_, index_meta.fd.GetNumber(),
                               &index_meta.unique_id);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "[%s] [JOB %d] file #%" PRIu64
                      " failed to generate unique id: %s.",
                      cfd->GetName().c_str(), job_id_, index_meta.fd.GetNumber(),
                      s.ToString().c_str());
      return s;
    }
    outputs.AddOutput(std::move(index_meta), cfd->internal_comparator(),
                      sub_compact->compaction->mutable_cf_options()
                          ->check_flush_compaction_key_order,
                      paranoid_file_checks_);
    // 只有当gc_compaction或者vertical_compaction为true时，才会添加table file的FileMetaData信息
    if(gc_compaction || vertical_compaction){
      FileMetaData table_meta;
      // 默认选择pathid 为0
      table_meta.fd = FileDescriptor(table_number, 0, 0);
      table_meta.oldest_ancester_time = oldest_ancester_time;
      table_meta.file_creation_time = current_time;
      table_meta.temperature = temperature;
      assert(!db_id_.empty());
      assert(!db_session_id_.empty());
      s = GetSstInternalUniqueId(db_id_, db_session_id_, table_meta.fd.GetNumber(),
                                &table_meta.unique_id);
      if (!s.ok()) {
        ROCKS_LOG_ERROR(db_options_.info_log,
                        "[%s] [JOB %d] file #%" PRIu64
                        " failed to generate unique id: %s.",
                        cfd->GetName().c_str(), job_id_, table_meta.fd.GetNumber(),
                        s.ToString().c_str());
        return s;
      }
      outputs.AddTableOutput(std::move(table_meta), cfd->internal_comparator(),
                        sub_compact->compaction->mutable_cf_options()
                            ->check_flush_compaction_key_order,
                        paranoid_file_checks_);
    }
    
  }
  FileTypeSet tmp_set;
  if(gc_compaction || vertical_compaction){
    writable_file->SetIOPriority(GetRateLimiterPriority());
    writable_file->SetWriteLifeTimeHint(write_hint_);
    tmp_set = db_options_.checksum_handoff_file_types;
    writable_file->SetPreallocationBlockSize(static_cast<size_t>(
        sub_compact->compaction->OutputFilePreallocationSize()));
  }
  const auto& listeners =
      sub_compact->compaction->immutable_options()->listeners;
  if(gc_compaction || vertical_compaction){
    outputs.AssignFileWriter(new WritableFileWriter(
        std::move(writable_file), index_name, fo_copy, db_options_.clock, io_tracer_,
        db_options_.stats, listeners, db_options_.file_checksum_gen_factory.get(),
        tmp_set.Contains(FileType::kTableFile), false));
  }
  TableBuilderOptions tboptions(
      *cfd->ioptions(), *(sub_compact->compaction->mutable_cf_options()),
      cfd->internal_comparator(), cfd->int_tbl_prop_collector_factories(),
      sub_compact->compaction->output_compression(),
      sub_compact->compaction->output_compression_opts(), cfd->GetID(),
      cfd->GetName(), sub_compact->compaction->output_level(),
      bottommost_level_, TableFileCreationReason::kCompaction,
      0 /* oldest_key_time */, current_time, db_id_, db_session_id_,
      sub_compact->compaction->max_output_file_size(), index_number);

  std::vector<table_information_collect> sub_run;
  outputs.NewBuilder(tboptions,table_number,index_number,sub_run,gc_compaction,vertical_compaction);

  LogFlush(db_options_.info_log);
  return s;
}

void CompactionJob::CleanupCompaction() {
  for (SubcompactionState& sub_compact : compact_->sub_compact_states) {
    sub_compact.Cleanup(table_cache_.get());
  }
  delete compact_;
  compact_ = nullptr;
}

#ifndef ROCKSDB_LITE
namespace {
void CopyPrefix(const Slice& src, size_t prefix_length, std::string* dst) {
  assert(prefix_length > 0);
  size_t length = src.size() > prefix_length ? prefix_length : src.size();
  dst->assign(src.data(), length);
}
}  // namespace

#endif  // !ROCKSDB_LITE

void CompactionJob::UpdateCompactionStats() {
  assert(compact_);

  Compaction* compaction = compact_->compaction;
  compaction_stats_.stats.num_input_files_in_non_output_levels = 0;
  compaction_stats_.stats.num_input_files_in_output_level = 0;
  for (int input_level = 0;
       input_level < static_cast<int>(compaction->num_input_levels());
       ++input_level) {
    if (compaction->level(input_level) != compaction->output_level()) {
      UpdateCompactionInputStatsHelper(
          &compaction_stats_.stats.num_input_files_in_non_output_levels,
          &compaction_stats_.stats.bytes_read_non_output_levels, input_level);
    } else {
      UpdateCompactionInputStatsHelper(
          &compaction_stats_.stats.num_input_files_in_output_level,
          &compaction_stats_.stats.bytes_read_output_level, input_level);
    }
  }

  assert(compaction_job_stats_);
  compaction_stats_.stats.bytes_read_blob =
      compaction_job_stats_->total_blob_bytes_read;

  compaction_stats_.stats.num_dropped_records =
      compaction_stats_.DroppedRecords();
}

void CompactionJob::UpdateCompactionInputStatsHelper(int* num_files,
                                                     uint64_t* bytes_read,
                                                     int input_level) {
  const Compaction* compaction = compact_->compaction;
  auto num_input_files = compaction->num_input_files(input_level);
  *num_files += static_cast<int>(num_input_files);

  for (size_t i = 0; i < num_input_files; ++i) {
    const auto* file_meta = compaction->input(input_level, i);
    *bytes_read += file_meta->fd.GetFileSize();
    compaction_stats_.stats.num_input_records +=
        static_cast<uint64_t>(file_meta->num_entries);
  }
}

void CompactionJob::UpdateCompactionJobStats(
    const InternalStats::CompactionStats& stats) const {
#ifndef ROCKSDB_LITE
  compaction_job_stats_->elapsed_micros = stats.micros;

  // input information
  compaction_job_stats_->total_input_bytes =
      stats.bytes_read_non_output_levels + stats.bytes_read_output_level;
  compaction_job_stats_->num_input_records = stats.num_input_records;
  compaction_job_stats_->num_input_files =
      stats.num_input_files_in_non_output_levels +
      stats.num_input_files_in_output_level;
  compaction_job_stats_->num_input_files_at_output_level =
      stats.num_input_files_in_output_level;

  // output information
  compaction_job_stats_->total_output_bytes = stats.bytes_written;
  compaction_job_stats_->total_output_bytes_blob = stats.bytes_written_blob;
  compaction_job_stats_->num_output_records = stats.num_output_records;
  compaction_job_stats_->num_output_files = stats.num_output_files;
  compaction_job_stats_->num_output_files_blob = stats.num_output_files_blob;

  if (stats.num_output_files > 0) {
    CopyPrefix(compact_->SmallestUserKey(),
               CompactionJobStats::kMaxPrefixLength,
               &compaction_job_stats_->smallest_output_key_prefix);
    CopyPrefix(compact_->LargestUserKey(), CompactionJobStats::kMaxPrefixLength,
               &compaction_job_stats_->largest_output_key_prefix);
  }
#else
  (void)stats;
#endif  // !ROCKSDB_LITE
}

void CompactionJob::LogCompaction() {
  Compaction* compaction = compact_->compaction;
  ColumnFamilyData* cfd = compaction->column_family_data();

  // Let's check if anything will get logged. Don't prepare all the info if
  // we're not logging
  if (db_options_.info_log_level <= InfoLogLevel::INFO_LEVEL) {
    Compaction::InputLevelSummaryBuffer inputs_summary;
    ROCKS_LOG_INFO(
        db_options_.info_log, "[%s] [JOB %d] Compacting %s, score %.2f",
        cfd->GetName().c_str(), job_id_,
        compaction->InputLevelSummary(&inputs_summary), compaction->score());
    char scratch[2345];
    compaction->Summary(scratch, sizeof(scratch));
    ROCKS_LOG_INFO(db_options_.info_log, "[%s] Compaction start summary: %s\n",
                   cfd->GetName().c_str(), scratch);
    // build event logger report
    auto stream = event_logger_->Log();
    stream << "job" << job_id_ << "event"
           << "compaction_started"
           << "compaction_reason"
           << GetCompactionReasonString(compaction->compaction_reason());
    for (size_t i = 0; i < compaction->num_input_levels(); ++i) {
      stream << ("files_L" + std::to_string(compaction->level(i)));
      stream.StartArray();
      for (auto f : *compaction->inputs(i)) {
        stream << f->fd.GetNumber();
      }
      stream.EndArray();
    }
    stream << "score" << compaction->score() << "input_data_size"
           << compaction->CalculateTotalInputSize();
  }
}

std::string CompactionJob::GetTableFileName(uint64_t file_number) {
  return TableFileName(compact_->compaction->immutable_options()->cf_paths,
                       file_number, compact_->compaction->output_path_id());
}
std::string CompactionJob::GetIndexFileName(uint64_t file_number) {
  return MakeIndexFileName(compact_->compaction->immutable_options()->db_nvm_dir,file_number);
}

Env::IOPriority CompactionJob::GetRateLimiterPriority() {
  if (versions_ && versions_->GetColumnFamilySet() &&
      versions_->GetColumnFamilySet()->write_controller()) {
    WriteController* write_controller =
        versions_->GetColumnFamilySet()->write_controller();
    if (write_controller->NeedsDelay() || write_controller->IsStopped()) {
      return Env::IO_USER;
    }
  }

  return Env::IO_LOW;
}

void CompactionJob::UpdateChildrenRanksForLevel(int level, FileMetaData& file_meta, InternalIterator* father_iter) {
    // 1. 预先获取一些基本变量
    const Version* base_version = compact_->compaction->input_version();
    ColumnFamilyData* cfd = compact_->compaction->column_family_data();
    const std::vector<FileMetaData*>& out_files = base_version->storage_info()->LevelFiles(level+1);
    NvmPartitionIterator* partition_father_it = static_cast<NvmPartitionIterator*>(father_iter);
    const Comparator* user_comparator= cfd->user_comparator();
    
    std::vector<std::pair<InternalIterator*,std::pair<std::string,std::string>>> childrenit_range_keys;
    std::vector<PositionKeyList> children_ranks;
    
    // 2. 遍历out_files，为每一个out_files生成一个iterator
    for (auto& f : out_files) {
      // 4.1.1 如果children file与father file没有overlap，直接跳过
      if(user_comparator->Compare(f->largest.user_key(),file_meta.smallest.user_key()) < 0 ||
        user_comparator->Compare(f->smallest.user_key(),file_meta.largest.user_key()) > 0)
          continue;
      // 4.1.2 构造children file的iterator，添加到children_its中
      children_ranks.emplace_back(file_meta.fd.GetNumber(), f->fd.GetNumber());
      ReadOptions read_options;
      auto& prefix_extractor =
          compact_->compaction->mutable_cf_options()->prefix_extractor;
      childrenit_range_keys.push_back(std::pair<InternalIterator*,std::pair<std::string,std::string>>(cfd->table_cache()->NewIterator(
        read_options, file_options_, cfd->internal_comparator(),
        *f, /*range_del_agg=*/nullptr,
        prefix_extractor,
        /*table_reader_ptr=*/nullptr,
        cfd->internal_stats()->GetFileReadHist(
            compact_->compaction->output_level()),
        TableReaderCaller::kCompactionRefill, /*arena=*/nullptr,
        /*skip_filters=*/false, compact_->compaction->output_level(),
        MaxFileSizeForL0MetaPin(
            *compact_->compaction->mutable_cf_options()),
        /*smallest_compaction_key=*/nullptr,
        /*largest_compaction_key=*/nullptr,
        /*allow_unprepared_value=*/false),std::pair<std::string,std::string>()));
    }

    // 4.2 遍历children_its，为每一个子file生成对应的初始children_rank(即只有first key和last key)
    for(int i = 0; i < childrenit_range_keys.size(); i ++) {
      InternalIterator* children_iter = childrenit_range_keys[i].first;
      // 4.2.1 查询子iterator的first key和last key 得到对应的level3_block
      children_iter->SeekToFirst();
      std::string left_key = children_iter->key().ToString();
      uint32_t level3_left_block = static_cast<NvmPartitionIterator*>(children_iter)->GetKeyPosition(3);
      children_iter->SeekToLast();
      std::string right_key = children_iter->key().ToString();
      uint32_t level3_right_block = static_cast<NvmPartitionIterator*>(children_iter)->GetKeyPosition(3);
      // 4.2.2 在father iterator中查询对应key的position
      partition_father_it->Seek(left_key);
      if (!partition_father_it->Valid())
      {
        partition_father_it->SeekToFirst();
      }
      uint32_t left_pos = partition_father_it->GetKeyPosition(0);

      partition_father_it->Seek(right_key);
      if (!partition_father_it->Valid())
      {
        partition_father_it->SeekToLast();
      }
      uint32_t right_pos = partition_father_it->GetKeyPosition(0);

      childrenit_range_keys[i].second = {left_key, right_key};
      children_ranks[i].AddPosKey(left_key, left_pos, level3_left_block);
      children_ranks[i].AddPosKey(right_key, right_pos, level3_right_block);
    }
    // 4.3 遍历father_it，进行EstimateSeek，同时动态添加children_ranks
    if(childrenit_range_keys.size() != 0) {
      int times = 0;
      for (partition_father_it->SeekToFirst(); partition_father_it->Valid(); partition_father_it->Next()) {
        // 4.3.1 按照estimate_interval选择father_iter中的不同key进行EstimateSeek
        if(times++ % compact_->compaction->immutable_options()->estimate_interval == 0) {
          Slice temp_key = partition_father_it->key();
          // 4.3.2 如果当前key与children_ranks中的key range没有overlap，直接跳过
          if (user_comparator->Compare(temp_key, childrenit_range_keys.front().second.first) < 0 ||
            user_comparator->Compare(temp_key, childrenit_range_keys.back().second.second) > 0)
            continue;
          //4.3.3 二分 寻找childrenit_range_keys中第一个大于等于temp_key的值的largest_key
          int left = 0, right = childrenit_range_keys.size() - 1;
          while (left < right) {
            int mid = (left + right) >> 1;
            if(user_comparator->Compare(childrenit_range_keys[mid].second.second, temp_key) >= 0) {
              right = mid;
            } else {
              left = mid + 1;
            }
          }
          // 此时left位置一定是第一个大于等于temp_key的range
          // 4.3.4 如果temp_key比children_range_keys[left].second.first小 那么直接continue
          if (user_comparator->Compare(temp_key, childrenit_range_keys[left].second.first) < 0)
            continue;
          // 4.3.5 此时，temp_key在指定区间内部，可以进行estimate_seek
          int estimate_pos = children_ranks[left].GetEstimatePos(temp_key.ToString(), partition_father_it->GetKeyPosition(0));
          uint32_t seek_levels = static_cast<NvmPartitionIterator*>(childrenit_range_keys[left].first)->EstimateSeek(temp_key, estimate_pos, LinearDetection);
          if (seek_levels >= compact_->compaction->immutable_options()->estimate_threshold) {
            children_ranks[left].AddPosKey(temp_key.ToString(), partition_father_it->GetKeyPosition(0), static_cast<NvmPartitionIterator*>(childrenit_range_keys[left].first)->GetKeyPosition(3));
          }
        }
      }
    }
    // for(auto& f:out_files){
    //   // 如果l1_file与当前file没有任何overlap 那么无需生成对应children_ranks_
    //   if(comparator->Compare(f->largest.user_key(),file_meta.smallest.user_key()) < 0 ||
    //     comparator->Compare(f->smallest.user_key(),file_meta.largest.user_key()) > 0)
    //       continue;
    //   // Seek userkey还是internalkey
    //   // LAST_VERSION
    //   // if((comparator->Compare(f->smallest.user_key(),file_meta.smallest.user_key())>0) && 
    //   //   (comparator->Compare(f->largest.user_key(),file_meta.largest.user_key())<0) ){
    //   // 为每一个children_file生成一个PositionKeyList
    //   children_ranks.emplace_back(0, 0);
    //   std::unique_ptr<InternalIterator> children_it(cfd->table_cache()->NewIterator(
    //     read_options, file_options_, cfd->internal_comparator(),
    //     *f, /*range_del_agg=*/nullptr,
    //     prefix_extractor,
    //     /*table_reader_ptr=*/nullptr,
    //     cfd->internal_stats()->GetFileReadHist(
    //         compact_->compaction->output_level()),
    //     TableReaderCaller::kCompactionRefill, /*arena=*/nullptr,
    //     /*skip_filters=*/false, compact_->compaction->output_level(),
    //     MaxFileSizeForL0MetaPin(
    //         *compact_->compaction->mutable_cf_options()),
    //     /*smallest_compaction_key=*/nullptr,
    //     /*largest_compaction_key=*/nullptr,
    //     /*allow_unprepared_value=*/false));
    //   NvmPartitionIterator* partition_children_it = static_cast<NvmPartitionIterator*>(children_it.get());
    //   int times = 0;
    //   for(partition_children_it->SeekToFirst(); partition_children_it->Valid(); partition_children_it->Next()) {
    //     // WQTODOIMP 使用一个option决定...
    //     if(times++ % compact_->compaction->immutable_options()->estimate_interval == 0) {
    //       std::string temp_key = partition_children_it->key().ToString();
    //       uint32_t level3_block = partition_children_it->GetKeyPosition(3);

    //       partition_father_it->Seek(temp_key);
    //       if (!partition_father_it->Valid())
    //       {
    //         partition_father_it->SeekToFirst();
    //       }
    //       uint32_t father_pos = partition_father_it->GetKeyPosition(0);
    //       children_ranks.back().AddPosKey(temp_key, father_pos, level3_block);
    //     }
    //     // LAST_VERSION
    //     // children_ranks.push_back({});
    //     // partition_father_it->Seek(f->smallest.Encode());
    //     // children_ranks.back().first = partition_father_it->GetKeyPosition();
    //     // partition_father_it->Seek(f->largest.Encode());
    //     // children_ranks.back().second = partition_father_it->GetKeyPosition();
    //     // LAST_VERSION
    //   }
    //   // 最后再添加last_key
    //   partition_children_it->SeekToLast();
    //   std::string temp_key = partition_children_it->key().ToString();
    //   uint32_t level3_block = partition_children_it->GetKeyPosition(3);

    //   partition_father_it->Seek(temp_key);
    //   if (!partition_father_it->Valid()) {
    //     partition_father_it->SeekToFirst();
    //   }
    //   uint32_t father_pos = partition_father_it->GetKeyPosition(0);
    //   children_ranks.back().AddPosKey(temp_key, father_pos, level3_block);
    //   // LAST_VERSION
    //   // children_ranks.push_back({});
    //   // partition_father_it->Seek(f->smallest.Encode());
    //   // children_ranks.back().first = partition_father_it->GetKeyPosition();
    //   // partition_father_it->Seek(f->largest.Encode());
    //   // children_ranks.back().second = partition_father_it->GetKeyPosition();
    //   // }
    // }
    for (auto childrenit_range_key : childrenit_range_keys ) {
      delete childrenit_range_key.first;
    }
  file_meta.children_ranks_ = std::move(children_ranks);
}

}  // namespace ROCKSDB_NAMESPACE
