//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_level.h"

#include <string>
#include <utility>
#include <vector>

#include "db/version_edit.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

bool LevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForForcedBlobGC().empty()) {
    return true;
  }
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  for(auto& c:vstorage->GetIndexToScoreTablesSort()){
    if(c.second.first >= 1.0)
      return true;
  }
  for(auto& m: vstorage->GetIndexToVcScore()){
    for(auto& l: m) {
      if(l.second >= 1.0) 
        return true;
    }
  }
  return false;
}

namespace {
// A class to build a leveled compaction step-by-step.
class LevelCompactionBuilder {
 public:
  LevelCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         SequenceNumber earliest_mem_seqno,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableOptions& ioptions,
                         const MutableDBOptions& mutable_db_options)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        earliest_mem_seqno_(earliest_mem_seqno),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        mutable_db_options_(mutable_db_options) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  // func:寻找本次compaction的input files和output files
  void SetupInitialFiles();

  bool SetupInitialFilesForGC();

  // func：针对vertical compaction，选择多个level的需要compact的file
  // 经过本次函数逻辑后，所有vertical compaction涉及到的文件都存储在start_vertical_inputs_中
  bool SetupInitialFilesForVC();

  // If the initial files are from L0 level, pick other L0
  // files if needed.
  bool SetupOtherL0FilesIfNeeded();

  // Compaction with round-robin compaction priority allows more files to be
  // picked to form a large compaction
  void SetupOtherFilesWithRoundRobinExpansion();
  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  // func: 根据start level的input files，获得output level的input files
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // For the specfied level, pick a file that we want to compact.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one for
  // all compaction priorities except round-robin. For round-robin,
  // multiple consecutive files may be put into inputs->files.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  // func: 针对指定的level，选择一个需要compact的file
  // 如果返回false，说明没有需要compact的file;如果返回true，inputs->files.size()会是1，除非是round-robin
  // 如果level是0，而且该level上已经有一个compaction，那么这个函数会返回false
  bool PickFileToCompact();


  // func: 为vertical compaction选择所有input files...
  void FindFilesForVC(std::vector<CompactionInputFiles>& start_vertical_inputs, int level);


  // Return true if a L0 trivial move is picked up.
  bool TryPickL0TrivialMove();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  // Return true if TrivialMove is extended. `start_index` is the index of
  // the intiial file picked, which should already be in `start_level_inputs_`.
  bool TryExtendNonL0TrivialMove(int start_index);

  // Picks a file from level_files to compact.
  // level_files is a vector of (level, file metadata) in ascending order of
  // level. If compact_to_next_level is true, compact the file to the next
  // level, otherwise, compact to the same level as the input file.
  void PickFileToCompact(
      const autovector<std::pair<int, FileMetaData*>>& level_files,
      bool compact_to_next_level);

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  SequenceNumber earliest_mem_seqno_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  bool is_l0_trivial_move_ = false;
  bool is_gc_compaction_ = false;
  // 针对vertical compaction的特别成员变量
  bool is_vertical_compaction_ = false;
  std::vector<CompactionInputFiles> start_vertical_inputs_; // 存储本次vertical compaction涉及的所有Files
  int vertical_start_level_ = -1;
  int vertical_involve_levels_ = 0; //本次vertical compaction涉及从start_level_开始，长度为involve_levels_的所有level
  //
  std::map<uint32_t,std::vector<uint32_t>> index_to_tables_;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableOptions& ioptions_;
  const MutableDBOptions& mutable_db_options_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
};

void LevelCompactionBuilder::PickFileToCompact(
    const autovector<std::pair<int, FileMetaData*>>& level_files,
    bool compact_to_next_level) {
  for (auto& level_file : level_files) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    start_level_ = level_file.first;
    if ((compact_to_next_level &&
         start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      continue;
    }
    if (compact_to_next_level) {
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
    } else {
      output_level_ = start_level_;
    }
    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                   &start_level_inputs_)) {
      return;
    }
  }
  start_level_inputs_.files.clear();
}

bool LevelCompactionBuilder::SetupInitialFilesForGC(){
  // 应当限制一下gc compaction的个数 太多了... 
  if (compaction_picker_->gc_compactions_in_progress()->size() > ioptions_.max_gc_compactions){
    return false;
  }
  start_level_inputs_.clear();
  // 首先按照gc_score排序，选择score最小的table的对应index进行compaction
  // 在这个table的index file中，再选择具有最多引用table的index进行compaction
  // GetIndexToScoreTablesSort已经按照从小到大进行排序
  for(auto& m : vstorage_->GetIndexToScoreTablesSort()){
    double gc_score = m.second.first;
    if(gc_score >= 1){
      uint32_t index_number = m.first;
      std::set<uint32_t> table_numbers = m.second.second;
      FileMetaData* index_meta = vstorage_->GetFileMetaDataByNumber(index_number);
      if(index_meta->being_compacted){
        continue;
      }
      int level = vstorage_->GetFileLocation(index_number).GetLevel();
      start_level_ = level;
      output_level_ = (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      // 增加额外逻辑 如果是gc最后一层 则输出level选择和start level一样
      if(start_level_ == ioptions_.num_levels -1) output_level_ = start_level_;
      start_level_inputs_.level = start_level_;
      start_level_inputs_.files.push_back(index_meta);
      // 下面的逻辑是判断本次compaction是否能完成
  
      // ExpandInputsToCleanCut()负责将当前的inputs扩展到一个clean cut(就是没有完全重叠的key)
      // FilesRangeOverlapWithCompaction()在目前的inputs的range与正在Compaction的range有overlap时返回true
      // 下面表明如果当前start_level_inputs_的clean range与正在Compaction的range有overlap，那么就跳过这次compaction
      if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &start_level_inputs_) ||
          compaction_picker_->FilesRangeOverlapWithCompaction(
              {start_level_inputs_}, output_level_)) {
        start_level_inputs_.clear();
        continue;
      }

      // 下面判断当前start_level_inputs_有范围重叠的out_level_inputs_,其经过clean扩展后是否与正在compaction的range有overlap
      InternalKey smallest, largest;
      compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
      CompactionInputFiles output_level_inputs;
      output_level_inputs.level = output_level_;
      vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                      &output_level_inputs.files);
      if (output_level_inputs.empty()) {

      } else {
        if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                        &output_level_inputs)) {
          start_level_inputs_.clear();
          continue;
        }
      }

    } else {
      // gc_score已经按照从大到小排序，所以一旦某一个gc_score小于1，说明后面的gc_score均小于1
      break;
    }
    // 在针对某一个index的判断中，如果它可以走到这里的逻辑而没有在中间continue，说明选择到了一个合适的compaction，可以返回。
    break;
  }
  if(start_level_inputs_.size()>0){
    is_gc_compaction_ = true;
  }
  return start_level_inputs_.size() > 0;
}

void LevelCompactionBuilder::SetupInitialFiles() {
  // Find the compactions by size on all levels.
  bool skipped_l0_to_base = false;
  //1.从level0开始遍历,寻找compaction的input/output level
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));
    // 如果当前level的score大于1(本质上说明本level的file_size>本level_max_size)说明当前level的文件需要进行compaction
    if (start_level_score_ >= 1) {
      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // If L0->base_level compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        // 如果有l0到l1的compaction正在准备执行,那么就暂时不安排从l1到l2的compaction,否则这会导致l0到l1的compaction饿死
        // 这里的原因很简单...如果l0到l1的compaction想要执行,但是因为l0有其他compaction正在执行，导致它无法执行。
        // 如果此时安排一个l1到l2的compaction，那么它会大量阻塞这个想要执行的l0到l1compaction(因为l0到l1compaction需要保证
        // l1没有overlap的file正在执行的)
        continue;
      }
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      //2.在PickFileToCompact()中选定input/output的files 
      if (PickFileToCompact()) {
        // found the compaction!
        if (start_level_ == 0) {
          // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // L1+ score = `Level files size` / `MaxBytesForLevel`
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        // 没有找到任何compaciton，则清理输入，准备尝试intra0compaction
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          skipped_l0_to_base = true;
          // L0->base_level may be blocked due to ongoing L0->base_level
          // compactions. It may also be blocked by an ongoing compaction from
          // base_level downwards.
          //
          // In these cases, to reduce L0 file count and thus reduce likelihood
          // of write stalls, we can attempt compacting a span of files within
          // L0.
          if (PickIntraL0Compaction()) {
            output_level_ = 0;
            compaction_reason_ = CompactionReason::kLevelL0FilesNum;
            break;
          }
        }
      }
    } else {
      // Compaction scores are sorted in descending order, no further scores
      // will be >= 1.
      // Compaction scores已经被倒序排序,剩余level的score一定<1,可以直接break
      break;
    }
  }
  if (!start_level_inputs_.empty()) {
    return;
  }

  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  parent_index_ = base_index_ = -1;

  //一些奇奇怪怪的compaction。如果本次没有选到合适的compaction文件，那么就进行这些compaction
  //包括不限于TTL periodic blobGc等
  compaction_picker_->PickFilesMarkedForCompaction(
      cf_name_, vstorage_, &start_level_, &output_level_, &start_level_inputs_);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
    return;
  }

  // Bottommost Files Compaction on deleting tombstones
  PickFileToCompact(vstorage_->BottommostFilesMarkedForCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kBottommostFiles;
    return;
  }

  // TTL Compaction
  PickFileToCompact(vstorage_->ExpiredTtlFiles(), true);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kTtl;
    return;
  }

  // Periodic Compaction
  PickFileToCompact(vstorage_->FilesMarkedForPeriodicCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kPeriodicCompaction;
    return;
  }

  // Forced blob garbage collection
  PickFileToCompact(vstorage_->FilesMarkedForForcedBlobGC(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kForcedBlobGC;
    return;
  }
}

bool LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0 && !is_l0_trivial_move_) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_);
  }
  return true;
}

void LevelCompactionBuilder::SetupOtherFilesWithRoundRobinExpansion() {
  // We only expand when the start level is not L0 under round robin
  assert(start_level_ >= 1);

  // For round-robin compaction priority, we have 3 constraints when picking
  // multiple files.
  // Constraint 1: We can only pick consecutive files
  //  -> Constraint 1a: When a file is being compacted (or some input files
  //                    are being compacted after expanding, we cannot
  //                    choose it and have to stop choosing more files
  //  -> Constraint 1b: When we reach the last file (with largest keys), we
  //                    cannot choose more files (the next file will be the
  //                    first one)
  // Constraint 2: We should ensure the total compaction bytes (including the
  //               overlapped files from the next level) is no more than
  //               mutable_cf_options_.max_compaction_bytes
  // Constraint 3: We try our best to pick as many files as possible so that
  //               the post-compaction level size is less than
  //               MaxBytesForLevel(start_level_)
  // Constraint 4: We do not expand if it is possible to apply a trivial move
  // Constraint 5 (TODO): Try to pick minimal files to split into the target
  //               number of subcompactions
  TEST_SYNC_POINT("LevelCompactionPicker::RoundRobin");

  // Only expand the inputs when we have selected a file in start_level_inputs_
  if (start_level_inputs_.size() == 0) return;

  uint64_t start_lvl_bytes_no_compacting = 0;
  uint64_t curr_bytes_to_compact = 0;
  uint64_t start_lvl_max_bytes_to_compact = 0;
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);
  // Constraint 3 (pre-calculate the ideal max bytes to compact)
  for (auto f : level_files) {
    if (!f->being_compacted) {
      start_lvl_bytes_no_compacting += f->compensated_file_size;
    }
  }
  if (start_lvl_bytes_no_compacting >
      vstorage_->MaxBytesForLevel(start_level_)) {
    start_lvl_max_bytes_to_compact = start_lvl_bytes_no_compacting -
                                     vstorage_->MaxBytesForLevel(start_level_);
  }

  size_t start_index = vstorage_->FilesByCompactionPri(start_level_)[0];
  InternalKey smallest, largest;
  // Constraint 4 (No need to check again later)
  compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
  CompactionInputFiles output_level_inputs;
  output_level_inputs.level = output_level_;
  vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                  &output_level_inputs.files);
  if (output_level_inputs.empty()) {
    if (TryExtendNonL0TrivialMove((int)start_index)) {
      return;
    }
  }
  // Constraint 3
  if (start_level_inputs_[0]->compensated_file_size >=
      start_lvl_max_bytes_to_compact) {
    return;
  }
  CompactionInputFiles tmp_start_level_inputs;
  tmp_start_level_inputs = start_level_inputs_;
  // TODO (zichen): Future parallel round-robin may also need to update this
  // Constraint 1b (only expand till the end)
  for (size_t i = start_index + 1; i < level_files.size(); i++) {
    auto* f = level_files[i];
    if (f->being_compacted) {
      // Constraint 1a
      return;
    }

    tmp_start_level_inputs.files.push_back(f);
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &tmp_start_level_inputs) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {tmp_start_level_inputs}, output_level_)) {
      // Constraint 1a
      tmp_start_level_inputs.clear();
      return;
    }

    curr_bytes_to_compact = 0;
    for (auto start_lvl_f : tmp_start_level_inputs.files) {
      curr_bytes_to_compact += start_lvl_f->compensated_file_size;
    }

    // Check whether any output level files are locked
    compaction_picker_->GetRange(tmp_start_level_inputs, &smallest, &largest);
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      // Constraint 1a
      tmp_start_level_inputs.clear();
      return;
    }

    uint64_t start_lvl_curr_bytes_to_compact = curr_bytes_to_compact;
    for (auto output_lvl_f : output_level_inputs.files) {
      curr_bytes_to_compact += output_lvl_f->compensated_file_size;
    }
    if (curr_bytes_to_compact > mutable_cf_options_.max_compaction_bytes) {
      // Constraint 2
      tmp_start_level_inputs.clear();
      return;
    }

    start_level_inputs_.files = tmp_start_level_inputs.files;
    // Constraint 3
    if (start_lvl_curr_bytes_to_compact > start_lvl_max_bytes_to_compact) {
      return;
    }
  }
}

bool LevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    bool round_robin_expanding =
        ioptions_.compaction_pri == kRoundRobin &&
        compaction_reason_ == CompactionReason::kLevelMaxLevelSize;
    if (round_robin_expanding) {
      SetupOtherFilesWithRoundRobinExpansion();
    }
    if (!is_l0_trivial_move_ &&
        !compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_,
            round_robin_expanding)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    if (!is_l0_trivial_move_) {
      // In some edge cases we could pick a compaction that will be compacting
      // a key range that overlap with another running compaction, and both
      // of them have the same output level. This could happen if
      // (1) we are running a non-exclusive manual compaction
      // (2) AddFile ingest a new file into the LSM tree
      // We need to disallow this from happening.
      // 在一些极端情况下,我们选择的一个compaction与另一个正在运行的compaction的key range有overlap
      // 并且它们都有相同的output level.虽然这个大部分情况会在PickFileToCompact的FilesRangeOverlapWithCompaction
      // 中避免，但是会在以下两种情况出现问题：
      // 1.我们正在运行一个非独占的手动compaction
      // 2.AddFile将一个新文件添加到LSM树中
      if (compaction_picker_->FilesRangeOverlapWithCompaction(
              compaction_inputs_, output_level_)) {
        // This compaction output could potentially conflict with the output
        // of a currently running compaction, we cannot run it.
        return false;
      }
      compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                          output_level_inputs_, &grandparents_);
    }
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

Compaction* LevelCompactionBuilder::PickCompaction() {
  // Pick up the first file to start compaction. It may have been extended
  // to a clean cut.
  // index compaction > vertical compaction > garbage cllection compaction
  SetupInitialFiles();
  if(start_level_inputs_.empty()){
    if(SetupInitialFilesForVC()) {
      compaction_inputs_ = start_vertical_inputs_;
    } else if (SetupInitialFilesForGC()) {

    } else 
      return nullptr;  
  }
  // 只有gc compaction和index compaction会走后面的选择逻辑
  if(!is_vertical_compaction_) {
    assert(start_level_ >= 0 && output_level_ >= 0);

    // If it is a L0 -> base level compaction, we need to set up other L0
    // files if needed.
    if (!SetupOtherL0FilesIfNeeded()) {
      return nullptr;
    }

    // Pick files in the output level and expand more files in the start level
    // if needed.
    if (!SetupOtherInputsIfNeeded()) {
      return nullptr;
    }
  }
  // if(!SetupInitialFilesForVC()){
  //   if(!SetupInitialFilesForGc()){
  //     // 如果没有找到适合gc的compaction，那么本次compaction仅仅进行index compaction而不进行gc_compaction
  //     SetupInitialFiles();
  //   }
  //   if (start_level_inputs_.empty()) {
  //     return nullptr;
  //   }
  //   assert(start_level_ >= 0 && output_level_ >= 0);

  //   // If it is a L0 -> base level compaction, we need to set up other L0
  //   // files if needed.
  //   if (!SetupOtherL0FilesIfNeeded()) {
  //     return nullptr;
  //   }

  //   // Pick files in the output level and expand more files in the start level
  //   // if needed.
  //   if (!SetupOtherInputsIfNeeded()) {
  //     return nullptr;
  //   }
  // } else {
  //   // 在vertical compaction的情况下，不需要通过SetupOtherInputsIfNeeded()等逻辑选择output level的所有inputs 而是直接设置为start_vertical_inputs_就好
  //   compaction_inputs_ = start_vertical_inputs_;
  // }
  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);

  return c;
}

Compaction* LevelCompactionBuilder::GetCompaction() {
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(compaction_inputs_), output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_,
                         vstorage_->base_level()),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
      /* trim_ts */ "", start_level_score_, false /* deletion_compaction */,
      /* l0_files_might_overlap */ start_level_ == 0 && !is_l0_trivial_move_,
      compaction_reason_,BlobGarbageCollectionPolicy::kUseDefault,-1,is_gc_compaction_,is_vertical_compaction_,vertical_start_level_);
  // 在这里设置本次compaction的类型，减少对构造函数的侵占
  if(is_gc_compaction_){
    // 如果是gc compaction，那么把本次需要使用real value的index到table的对应放入Compaction中
    c->set_index_to_tables(vstorage_->GetIndexToScoreTablesSort());
  }
  // if(is_vertical_compaction_){
  //   c->set_vertical_involve_levels(vertical_involve_levels_);
  // }

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t LevelCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
  //现在所有的table文件都统一存在原路径下
  //根据SST所在的level划分其所在的文件夹，[0]是root，[1]是NVM
  return 0;

  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/main/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

bool LevelCompactionBuilder::TryPickL0TrivialMove() {
  if (vstorage_->base_level() <= 0) {
    return false;
  }
  if (start_level_ == 0 && mutable_cf_options_.compression_per_level.empty() &&
      !vstorage_->LevelFiles(output_level_).empty() &&
      ioptions_.db_paths.size() <= 1) {
    // Try to pick trivial move from L0 to L1. We start from the oldest
    // file. We keep expanding to newer files if it would form a
    // trivial move.
    // For now we don't support it with
    // mutable_cf_options_.compression_per_level to prevent the logic
    // of determining whether L0 can be trivial moved to the next level.
    // We skip the case where output level is empty, since in this case, at
    // least the oldest file would qualify for trivial move, and this would
    // be a surprising behavior with few benefits.

    // We search from the oldest file from the newest. In theory, there are
    // files in the middle can form trivial move too, but it is probably
    // uncommon and we ignore these cases for simplicity.
    const std::vector<FileMetaData*>& level_files =
        vstorage_->LevelFiles(start_level_);

    InternalKey my_smallest, my_largest;
    for (auto it = level_files.rbegin(); it != level_files.rend(); ++it) {
      CompactionInputFiles output_level_inputs;
      output_level_inputs.level = output_level_;
      FileMetaData* file = *it;
      if (it == level_files.rbegin()) {
        my_smallest = file->smallest;
        my_largest = file->largest;
      } else {
        if (compaction_picker_->icmp()->Compare(file->largest, my_smallest) <
            0) {
          my_smallest = file->smallest;
        } else if (compaction_picker_->icmp()->Compare(file->smallest,
                                                       my_largest) > 0) {
          my_largest = file->largest;
        } else {
          break;
        }
      }
      vstorage_->GetOverlappingInputs(output_level_, &my_smallest, &my_largest,
                                      &output_level_inputs.files);
      if (output_level_inputs.empty()) {
        assert(!file->being_compacted);
        start_level_inputs_.files.push_back(file);
      } else {
        break;
      }
    }
  }

  if (!start_level_inputs_.empty()) {
    // Sort files by key range. Not sure it's 100% necessary but it's cleaner
    // to always keep files sorted by key the key ranges don't overlap.
    std::sort(start_level_inputs_.files.begin(),
              start_level_inputs_.files.end(),
              [icmp = compaction_picker_->icmp()](FileMetaData* f1,
                                                  FileMetaData* f2) -> bool {
                return (icmp->Compare(f1->smallest, f2->smallest) < 0);
              });

    is_l0_trivial_move_ = true;
    return true;
  }
  return false;
}

bool LevelCompactionBuilder::TryExtendNonL0TrivialMove(int start_index) {
  if (start_level_inputs_.size() == 1 &&
      (ioptions_.db_paths.empty() || ioptions_.db_paths.size() == 1) &&
      (mutable_cf_options_.compression_per_level.empty())) {
    // Only file of `index`, and it is likely a trivial move. Try to
    // expand if it is still a trivial move, but not beyond
    // max_compaction_bytes or 4 files, so that we don't create too
    // much compaction pressure for the next level.
    // Ignore if there are more than one DB path, as it would be hard
    // to predict whether it is a trivial move.
    const std::vector<FileMetaData*>& level_files =
        vstorage_->LevelFiles(start_level_);
    const size_t kMaxMultiTrivialMove = 4;
    FileMetaData* initial_file = start_level_inputs_.files[0];
    size_t total_size = initial_file->fd.GetFileSize();
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    for (int i = start_index + 1;
         i < static_cast<int>(level_files.size()) &&
         start_level_inputs_.size() < kMaxMultiTrivialMove;
         i++) {
      FileMetaData* next_file = level_files[i];
      if (next_file->being_compacted) {
        break;
      }
      vstorage_->GetOverlappingInputs(output_level_, &(initial_file->smallest),
                                      &(next_file->largest),
                                      &output_level_inputs.files);
      if (!output_level_inputs.empty()) {
        break;
      }
      if (i < static_cast<int>(level_files.size()) - 1 &&
          compaction_picker_->icmp()->user_comparator()->Compare(
              next_file->largest.user_key(),
              level_files[i + 1]->smallest.user_key()) == 0) {
        // Not a clean up after adding the next file. Skip.
        break;
      }
      total_size += next_file->fd.GetFileSize();
      if (total_size > mutable_cf_options_.max_compaction_bytes) {
        break;
      }
      start_level_inputs_.files.push_back(next_file);
    }
    return start_level_inputs_.size() > 1;
  }
  return false;
}

// 使用dfs查找一个符合要求的file列表
void LevelCompactionBuilder::FindFilesForVC(std::vector<CompactionInputFiles>& start_vertical_inputs, int level){

    // 1.判断退出条件。
    // 当前vertical compaction中的文件在下面的level中没有重叠的文件时 或者 当前level的重叠file的vc_score均小于1时 退出
    // 1.1 获得当前vertical compaction所选文件中的最小值和最大值(实际上就是start level中的最小值和最大值，因为更高的level的文件范围是真包含于start level的 不一定ORZ 因为跳表guard可能新增的问题...)
    const Slice& smallest_user_key = start_vertical_inputs[0].files.front()->smallest.user_key();
    const Slice& largest_user_key = start_vertical_inputs[0].files.back()->largest.user_key();
    // 1.2 具体判断当前[smallest_user_key,largest_user_key]是否在更高level有重叠。如果没有重叠，说明到达本guard的bottom level，直接返回
    if(!vstorage_->RangeMightExistAfterSortedRun(smallest_user_key, largest_user_key,
                                         level-1,-1)) { // 这里为什么要用level-1？因为RangeMightExistAfterSortedRun中的last_level参数表示比last_level更高的level都要被检查 当前函数中，显然要检查本level，所以使用level-1
      return;
    }
    // 2. 在当前level找到所有与start_vertical_inputs中文件范围有重叠的文件
    const std::vector<FileMetaData*>& level_files = vstorage_->LevelFiles(level);    // 当前level的所有文件
    // 注意，这里并不需要它们处于非being_compacted状态，所以不需要FilesRangeOverlapWithCompaction()的参与
    // WQTODOIMP 这里可能有潜在问题，就是可能会选择一些即将被删除的文件，这些文件的引用计数需要改变一下，等本次读完再释放...引用计数这里怎么搞？好麻烦...
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_vertical_inputs, &smallest, &largest, -1);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = level;
    // 2.1 找到这些所有重叠的文件 存入output_level_inputs
    vstorage_->GetOverlappingInputs(level, &smallest, &largest,
                                    &output_level_inputs.files);
    // 注意，不需要为这个file调用ExpandInputsToCleanCut，选择一个cleancut，因为我们的guard策略，导致在同一level中，不会有一个key存在于多个SST中
    // 另外，如果真的存在这样的情况，我们也不能选择cleancut 这会在本level引入更多index file，但是它们在更低level没有对应，这会造成vertical compaction的错误

    // 2.2 判断当前重叠文件的vc_score是否到达1
    // WQTODO 4.20 这里如果比start level的guard范围要大，那么就剔除 这是为了避免出现vertical compaction时较高level的文件范围比start level的guard范围还要大的情况
    for(auto it = output_level_inputs.files.begin(); it != output_level_inputs.files.end(); ) {
      double merge_keys = (*it)->merge_entries_ / (double)(*it)->total_entries_;
      std::cout << " vc nostart score "<< merge_keys << std::endl;  
      if( ioptions_.vc_merge_threshold_nostart  >  merge_keys ){
        start_vertical_inputs.clear();
        std::cout << "give up vc cause merge_threshold" << std::endl;
        return;
      }
      if( ioptions_.user_comparator->CompareWithoutTimestamp((*it)->smallest.user_key(),smallest_user_key) < 0 || ioptions_.user_comparator->CompareWithoutTimestamp((*it)->largest.user_key(),largest_user_key) > 0) {
        it = output_level_inputs.files.erase(it);
      } else {
        ++it;
      }
    }
    // for(auto f : output_level_inputs.files) {
    //   // 本level的重叠文件，一旦有至少一个file的vc_score不达标，则清空输入，取消本次选择
    //   // WQTODO 这里并没有用到之前计算的gc_score 因为那个不支持按照file number索引gc score  未来考虑一下怎么更合理的设计吧......
    //   if( ioptions_.vc_merge_threshold  > f->merge_entries_ / (double)f->total_entries_) {
    //     start_vertical_inputs.clear();
    //     return;
    //   }
    // }
    // 2.2 将这些重叠文件添加到start_vertical_inputs中
    start_vertical_inputs.push_back(output_level_inputs);
    
    // 3. 去下一level找重叠level
    return FindFilesForVC(start_vertical_inputs, ++level);
    // }
}

// 耦合了选择level和选择file的逻辑 因为我们的score不是以level为维度，而是以file为维度
bool LevelCompactionBuilder::SetupInitialFilesForVC() {
  std::cout << "try vc" << std::endl;
  // 如果没有开启vertical compaction 这里直接返回false
  if(!ioptions_.enable_vertical_compaction) {
    return false;
  }
  // 1. 清空vertical_inputs_列表
  start_vertical_inputs_.clear();
  bool find_vc = false;
  // 2. 从vertical_start_level_开始往下选择初始化的level
  for(int i = ioptions_.vertical_start_level; i < ioptions_.num_levels; i ++ ) {
    std::vector<std::pair<uint64_t,double>> level_index_score = vstorage_->GetIndexToVcScore()[i];
    for(auto& index_to_score : level_index_score) {
      const uint64_t& index_number = index_to_score.first;
      const double& vc_score = index_to_score.second; 
      // 2.1 当本file score > 1时，进入vertical compaction的逻辑
      std:: cout <<"index number "<< index_number<< " vc score "<< vc_score << " threshold " << ioptions_.vc_merge_threshold << " vc_merge_threshold_nostart "<<ioptions_.vc_merge_threshold_nostart<<std::endl;
      if(vc_score >= 1) {
        std::cout << "vc score >= 1" << std::endl;
        FileMetaData* index_file = vstorage_->GetFileMetaDataByNumber(index_number);
        // 如果当前index_file处于being_compacted状态，跳过
        if(index_file->being_compacted) continue;

        // 2.1.1 设置vertical compaction的起始文件
        start_vertical_inputs_.clear();
        CompactionInputFiles file;
        file.level = i;
        file.files.push_back(index_file);
        start_vertical_inputs_.push_back(file);
        // 2.1.2 以index_file为起点，往下面的level寻找范围重叠的所有file，并存储到start_vertical_inputs_中
        FindFilesForVC(start_vertical_inputs_, i+1);
        // 2.1.3 如果本次vertical compaction涉及的level过少，则取消进行vertical compaction
        if((int)start_vertical_inputs_.size() < ioptions_.vertical_min_involve_levels) {       // 实际上这里不能用end_level - start_level，必须用start_vertical_inputs_.size() 因为前者不一定代表本次涉及的level个数，毕竟中间的level可能有空文件...
          std::cout << "give up vc cause little levels" << std::endl;
          continue;
        } else {
            std::cout << "findvc" << std::endl;
            find_vc = true;
            start_level_ = i;
            vertical_start_level_ = start_level_;
            vertical_involve_levels_ = start_vertical_inputs_.size();
            // vertical compaction的情况下，文件输出到同一level
            output_level_ = start_level_;
            break;
        }
      }
      // 否则，直接退出本level的寻找 因为index_to_score已经按照score进行了排序 一旦当前score<1,本level的其它file一定不满足条件
      else {
        break;
      }
    }
    if (find_vc) break;
  }
    // const std::vector<FileMetaData*>& level_files = vstorage_->LevelFiles(i);
    // for(auto f : level_files) {
    //   if(f->being_compacted) continue;
    //   // 构造本次vertical compaction的初始file，存入start_vertical_inputs_
    //   int end_level = i+1;
    //   start_vertical_inputs_.clear();
    //   CompactionInputFiles file;
    //   file.level = i;
    //   file.files.push_back(f);
    //   start_vertical_inputs_.push_back(file);
    //   FindFilesForVC(start_vertical_inputs_, end_level);
    //   // 如果选择的level不超过指定level，那么退出，不进行本次vertical compaction的选择。
    //   if(end_level - i +1 <= ioptions_.vertical_min_involve_levels) {
    //     continue;
    //   } else {
    //     find_vc = true;
    //     start_level_ = i;
    //     vertical_start_level_ = start_level_;
    //     vertical_involve_levels_ = end_level -i + 1;
    //     // vertical compaction的情况下，文件输出到同一level...
    //     output_level_ = start_level_;
    //     break;
    //   }
    // }
  // for(auto f:start_files){
  //   FindFilesForVC(start_vertical_inputs_, end_level);
  //   // 如果本次涉及的level层数没有达到最小level要求，则不进行vertical compaction
  //   // 如果本次compaction无法涉及到bottom level，则也不进行vertical compaction
  //   if(end_level -start_level_ +1 <= ioptions_.vertical_min_involve_levels){
  //     continue;
  //   } else {
  //     find_vc = true;
  //     vertical_involve_levels_ = end_level - start_level_ + 1;
  //     break;
  //   }
  // }
  if(find_vc) 
    is_vertical_compaction_ = true;
  else start_vertical_inputs_.clear();
  return find_vc;
}

bool LevelCompactionBuilder::PickFileToCompact() {
  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  // level0的文件是重叠的，所以我们不能同时选择多个compaction。这可以通过查看正在level0上进行的key-ranges来改进。 
  // 所以如果level0有文件正在compaction,那么就禁止level0其他文件进行compaction 
  // Question:为什么？理论上就算有重叠也可以进行多个compaction并发呀? 
  // ANS:不可以...如果并发的话，两个同时进行的l0-l1 compaction可能产生overlap的l1 files.
  // Should we use level0_to_lbase_compactions_in_progress() 
  // instead of level0_compactions_in_progress()? Because a running 
  // intra0compaction should not prevent l0->lbase compaction from 
  // happening. But if such logic is allowed, it should be noted 
  // that an sst in level0 which is older than any ssts that are 
  // running intra0compaction cannot be compactioned to lbase.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }

  start_level_inputs_.clear();
  start_level_inputs_.level = start_level_;

  assert(start_level_ >= 0);

  if (TryPickL0TrivialMove()) {
    return true;
  }

  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);

  // Pick the file with the highest score in this level that is not already
  // being compacted.
  // 存储start_level按照file size从大到小排序的index(这里也可以不是按照file size排，有不同的方式比如轮询)
  const std::vector<int>& file_scores =
      vstorage_->FilesByCompactionPri(start_level_);

  unsigned int cmp_idx;
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
       cmp_idx < file_scores.size(); cmp_idx++) {
    int index = file_scores[cmp_idx];
    auto* f = level_files[index];

    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    // 如果某file正进行compaction,那么就不选这个file作为start_level的input
    // (不过理论上这个只对level1及以上的有效,level0如果有其它file being_compaction,早就在上面return了)
    if (f->being_compacted) {
      // 如果是RoundRobin的策略的话，轮询到的file如果正处在compaction中，那么就会直接返回false，不进行本次compaction的选择。
      if (ioptions_.compaction_pri == kRoundRobin) {
        // TODO(zichen): this file may be involved in one compaction from
        // an upper level, cannot advance the cursor for round-robin policy.
        // Currently, we do not pick any file to compact in this case. We
        // should fix this later to ensure a compaction is picked but the
        // cursor shall not be advanced.
        return false;
      }
      continue;
    }

    start_level_inputs_.files.push_back(f);
    // ExpandInputsToCleanCut()负责将当前的inputs扩展到一个clean cut(就是没有完全重叠的key)
    // FilesRangeOverlapWithCompaction()在目前的inputs的range与正在Compaction的range有overlap时返回true
    // 这种情况下不允许新启一个compaction的.
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_)) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();

      if (ioptions_.compaction_pri == kRoundRobin) {
        return false;
      }
      continue;
    }

    // Now that input level is fully expanded, we check whether any output
    // files are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (output_level_inputs.empty()) {
      if (TryExtendNonL0TrivialMove(index)) {
        break;
      }
    } else {
      if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &output_level_inputs)) {
        start_level_inputs_.clear();
        if (ioptions_.compaction_pri == kRoundRobin) {
          return false;
        }
        continue;
      }
    }

    base_index_ = index;
    break;
  }

  // store where to start the iteration in the next call to PickCompaction
  if (ioptions_.compaction_pri != kRoundRobin) {
    vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);
  }
  return start_level_inputs_.size() > 0;
}

bool LevelCompactionBuilder::PickIntraL0Compaction() {
  start_level_inputs_.clear();
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(0 /* level */);
  if (level_files.size() <
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger + 2) ||
      level_files[0]->being_compacted) {
    // If L0 isn't accumulating much files beyond the regular trigger, don't
    // resort to L0->L0 compaction yet.
    return false;
  }
  return FindIntraL0Compaction(level_files, kMinFilesForIntraL0Compaction,
                               std::numeric_limits<uint64_t>::max(),
                               mutable_cf_options_.max_compaction_bytes,
                               &start_level_inputs_, earliest_mem_seqno_);
}
}  // namespace

Compaction* LevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, SequenceNumber earliest_mem_seqno) {
  LevelCompactionBuilder builder(cf_name, vstorage, earliest_mem_seqno, this,
                                 log_buffer, mutable_cf_options, ioptions_,
                                 mutable_db_options);
  return builder.PickCompaction();
}
}  // namespace ROCKSDB_NAMESPACE
