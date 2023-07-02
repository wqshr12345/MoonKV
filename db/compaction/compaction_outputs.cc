//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_outputs.h"

#include "db/builder.h"

#include "nvm/partition/nvm_partition_builder.h"
namespace ROCKSDB_NAMESPACE {

void CompactionOutputs::NewBuilder(const TableBuilderOptions& tboptions) {
  builder_.reset(NewTableBuilder(tboptions, file_writer_.get()));
}

void CompactionOutputs::NewBuilder(const TableBuilderOptions& tboptions,
                              uint64_t table_file_seq,
                              uint64_t index_file_seq,
                              std::vector<table_information_collect>& sub_run,
                              bool gc_compaction, bool vertical_compaction){
  // 此处使用的builder_不会被verticalcompaction调用
  builder_.reset(NewTableBuilder(tboptions,file_writer_.get(), table_file_seq, index_file_seq, sub_run, gc_compaction, vertical_compaction));
}

Status CompactionOutputs::Finish(const Status& intput_status,
                                 const SeqnoToTimeMapping& seqno_time_mapping,bool gc_compaction, bool vertical_compaction) {
  FileMetaData* index_meta = GetMetaData();
  FileMetaData* table_meta;
  if(gc_compaction || vertical_compaction){
    table_meta = GetTableMetaData();
  }
  assert(index_meta != nullptr);
  Status s = intput_status;
  if (s.ok()) {
    std::string seqno_time_mapping_str;
    seqno_time_mapping.Encode(seqno_time_mapping_str, index_meta->fd.smallest_seqno,
                              index_meta->fd.largest_seqno, index_meta->file_creation_time);
    builder_->SetSeqnoTimeTableProperties(seqno_time_mapping_str,
                                          index_meta->oldest_ancester_time);
    
    // 只有在非value compaction情况下，才需要从version中读取file size信息。
    // if(!is_value_compaction){
    const Version* version = compaction_->input_version();
    std::vector<table_information_collect>* table_information = static_cast<NvmPartitionBuilder*>(builder_.get())->GetInternalTableInform();
    // 将本table的file size信息存入index的元信息中
    for(auto& i : *table_information){
      FileMetaData* temp_table_meta = version->storage_info()->GetTableFileMetaDataByNumber(i.table_file_number);
      // 1. 如果本table信息可以在version中找到，说明非新生成table(在index compaction、gc compaction、vertical compaction中都有可能出现)
      if(temp_table_meta){
        i.table_file_size = temp_table_meta->fd.GetFileSize();
      } else {
        std::cout<<"new talbe"<<table_meta->fd.GetNumber()<<std::endl;
      }
      // 2. 如果本table信息无法在version中找到，说明是新生成table(在gc compaction、vertical compaction中一定出现)
      // 在Finish()逻辑中生成对应的table size(因为需要先执行table builder的finish，才可以得到新生成table的全部size信息)
      // 3. 将本次index的sub_number_to_referencekey信息从index builder中读取并添加
      // WQTODOIMP assert的bug 这里可能添加一个table number的valid_key_num是0 后续可能出现assert检查一致性的问题...
      index_meta->fd.sub_number_to_reference_key.insert({i.table_file_number,i.valid_key_num});
    }
    // bool find = false;
    // for(int i = 0;i<table_information->size();i++) {
    //   if((*table_information)[i].table_file_number == table_meta->fd.GetNumber())
    //     find = true;
    // }
    // if((gc_compaction || vertical_compaction)&&!find) {
    //   assert(false);
    // }
    // }
    // // 增加value compaction的sub信息
    // else{
    //   index_meta->fd.sub_number_to_reference_key.insert({table_meta->fd.GetNumber(),builder_->NumEntries()});
    // }
    s = builder_->Finish();
  } else {
    builder_->Abandon();
  }
  // 默认添加最后一个元素作为rank的最后一个
  // children_ranks_.push_back(builder_->NumEntries());
  Status io_s = builder_->io_status();
  if (s.ok()) {
    s = io_s;
  } else {
    io_s.PermitUncheckedError();
  }
  const uint64_t current_bytes = builder_->FileSize();
  if (s.ok()) {
    index_meta->fd.file_size = current_bytes;
    index_meta->total_entries_ = builder_->NumEntries();
    index_meta->merge_entries_ = builder_->MergeEntries();
    // // 添加子guard们的位置
    // index_meta->children_ranks_ = children_ranks_;
    if(gc_compaction || vertical_compaction){
      table_meta->fd.file_size = builder_->TableFileSize();
      table_meta->total_entries_ = builder_->TableNumEntries(); // Table的total_entries_应该用TableNumEntries()来获取 Table的reference_entries_在后续由version构造
      // table_meta->reference_entries = tp.num_entries;
    }
    index_meta->marked_for_compaction = builder_->NeedCompact();
  }
  current_output().finished = true;
  stats_.bytes_written += current_bytes;
  stats_.num_output_files = outputs_.size();

  return s;
}

IOStatus CompactionOutputs::WriterSyncClose(const Status& input_status,
                                            SystemClock* clock,
                                            Statistics* statistics,
                                            bool use_fsync) {
  IOStatus io_s;
  if (input_status.ok()) {
    StopWatch sw(clock, statistics, COMPACTION_OUTFILE_SYNC_MICROS);
    io_s = file_writer_->Sync(use_fsync);
  }
  if (input_status.ok() && io_s.ok()) {
    io_s = file_writer_->Close();
  }

  if (input_status.ok() && io_s.ok()) {
    FileMetaData* meta = GetMetaData();
    meta->file_checksum = file_writer_->GetFileChecksum();
    meta->file_checksum_func_name = file_writer_->GetFileChecksumFuncName();
  }

  file_writer_.reset();

  return io_s;
}
Status CompactionOutputs::AddToOutput(
    const CompactionIterator& c_iter,
    const CompactionFileOpenFunc& open_file_func,
    const CompactionFileCloseFunc& close_file_func,bool gc_compaction, bool vertical_compaction, bool first_pick_guard) {
  Status s;
  const Slice& key = c_iter.key();

  // 这里的partition不要触发，我们在划分sub compaction的key range的时候实际上已经进行了partition的策略。
  if (!pending_close_ && c_iter.Valid() && partitioner_ && HasBuilder() &&
      partitioner_->ShouldPartition(
          PartitionerRequest(last_key_for_partitioner_, c_iter.user_key(),
                             current_output_file_size_)) == kRequired) {
    pending_close_ = true;
    assert(false);
  }

  // 禁止在中间pending_close_ 我们的table构建必须是整体进行的... 曾经这里出现过bug
  // if (pending_close_) {
  //   s = close_file_func(*this, c_iter.InputStatus(), key);
  //   pending_close_ = false;
  // }
  if (!s.ok()) {
    return s;
  }

  // Open output file if necessary
  if (!HasBuilder()) {
    s = open_file_func(*this);
  }
  if (!s.ok()) {
    return s;
  }

  // 从level0到level1，增加guard
  int entries = NumEntries();
  if(first_pick_guard && entries !=0 && compaction_->start_level() == 0 && compaction_->output_level() == 1 && entries % compaction_->immutable_options()->choose_guard_interval == 0){
	  int random_level = compaction_->immutable_options()->num_levels-1; //初始level是最大level-1
    while(entries % compaction_->immutable_options()->neighbor_guard_ratio == 0 && random_level >=2){
      random_level--;
      entries = entries/compaction_->immutable_options()->neighbor_guard_ratio;
    }



    
	// std::default_random_engine e(time(NULL));
	// std::uniform_int_distribution<int> u(0, 100);
  // 增加guard的逻辑

    // auto rnd = Random::GetTLSInstance();
    // int temp_branching = (Random::kMaxNext + 1) / compaction_->immutable_options()->neighbor_guard_ratio;
    // while (rnd->Next() < temp_branching && random_level >=2){
    //   random_level--;
    // }
    guards_[random_level].push_back(c_iter.user_key().ToString());
  }

  Output& curr = current_output();
  assert(builder_ != nullptr);
  const Slice& value = c_iter.value();
  s = curr.validator.Add(key, value);
  if (!s.ok()) {
    return s;
  }
  // 这里应该用builder_中得到的是否为real value来构造key
  builder_->Add(key, value, c_iter.real_value());

  stats_.num_output_records++;
  current_output_file_size_ = builder_->EstimatedFileSize();

  if (blob_garbage_meter_) {
    s = blob_garbage_meter_->ProcessOutFlow(key, value);
  }

  if (!s.ok()) {
    return s;
  }

  const ParsedInternalKey& ikey = c_iter.ikey();
  s = current_output().meta.UpdateBoundaries(key, value, ikey.sequence,
                                             ikey.type);

  // 如果是value compaction，也要同步更新table meta的上下界
  if(gc_compaction || vertical_compaction){
    s = current_table_output().meta.UpdateBoundaries(key,value,ikey.sequence,ikey.type);
  }

  // Close output file if it is big enough. Two possibilities determine it's
  // time to close it: (1) the current key should be this file's last key, (2)
  // the next key should not be in this file.
  //
  // TODO(aekmekji): determine if file should be closed earlier than this
  // during subcompactions (i.e. if output size, estimated by input size, is
  // going to be 1.2MB and max_output_file_size = 1MB, prefer to have 0.6MB
  // and 0.6MB instead of 1MB and 0.2MB)
  // 这里是根据当前的生成的file-size 根据max_out_put_file_size来切割文件的逻辑 我们也去掉 默认一个sub compaction只生成一个index file和table file(如果有的话)
  // if (compaction_->output_level() != 0 &&
  //     current_output_file_size_ >= compaction_->max_output_file_size()) {
  //   pending_close_ = true;
  // }

  if (partitioner_) {
    last_key_for_partitioner_.assign(c_iter.user_key().data_,
                                     c_iter.user_key().size_);
  }

  return s;
}

Status CompactionOutputs::AddRangeDels(
    const Slice* comp_start, const Slice* comp_end,
    CompactionIterationStats& range_del_out_stats, bool bottommost_level,
    const InternalKeyComparator& icmp, SequenceNumber earliest_snapshot,
    const Slice& next_table_min_key) {
  assert(HasRangeDel());
  FileMetaData& meta = current_output().meta;
  const Comparator* ucmp = icmp.user_comparator();

  Slice lower_bound_guard, upper_bound_guard;
  std::string smallest_user_key;
  const Slice *lower_bound, *upper_bound;
  bool lower_bound_from_sub_compact = false;

  size_t output_size = outputs_.size();
  if (output_size == 1) {
    // For the first output table, include range tombstones before the min
    // key but after the subcompaction boundary.
    lower_bound = comp_start;
    lower_bound_from_sub_compact = true;
  } else if (meta.smallest.size() > 0) {
    // For subsequent output tables, only include range tombstones from min
    // key onwards since the previous file was extended to contain range
    // tombstones falling before min key.
    smallest_user_key = meta.smallest.user_key().ToString(false /*hex*/);
    lower_bound_guard = Slice(smallest_user_key);
    lower_bound = &lower_bound_guard;
  } else {
    lower_bound = nullptr;
  }
  if (!next_table_min_key.empty()) {
    // This may be the last file in the subcompaction in some cases, so we
    // need to compare the end key of subcompaction with the next file start
    // key. When the end key is chosen by the subcompaction, we know that
    // it must be the biggest key in output file. Therefore, it is safe to
    // use the smaller key as the upper bound of the output file, to ensure
    // that there is no overlapping between different output files.
    upper_bound_guard = ExtractUserKey(next_table_min_key);
    if (comp_end != nullptr &&
        ucmp->Compare(upper_bound_guard, *comp_end) >= 0) {
      upper_bound = comp_end;
    } else {
      upper_bound = &upper_bound_guard;
    }
  } else {
    // This is the last file in the subcompaction, so extend until the
    // subcompaction ends.
    upper_bound = comp_end;
  }
  bool has_overlapping_endpoints;
  if (upper_bound != nullptr && meta.largest.size() > 0) {
    has_overlapping_endpoints =
        ucmp->Compare(meta.largest.user_key(), *upper_bound) == 0;
  } else {
    has_overlapping_endpoints = false;
  }

  // The end key of the subcompaction must be bigger or equal to the upper
  // bound. If the end of subcompaction is null or the upper bound is null,
  // it means that this file is the last file in the compaction. So there
  // will be no overlapping between this file and others.
  assert(comp_end == nullptr || upper_bound == nullptr ||
         ucmp->Compare(*upper_bound, *comp_end) <= 0);
  auto it = range_del_agg_->NewIterator(lower_bound, upper_bound,
                                        has_overlapping_endpoints);
  // Position the range tombstone output iterator. There may be tombstone
  // fragments that are entirely out of range, so make sure that we do not
  // include those.
  if (lower_bound != nullptr) {
    it->Seek(*lower_bound);
  } else {
    it->SeekToFirst();
  }
  for (; it->Valid(); it->Next()) {
    auto tombstone = it->Tombstone();
    if (upper_bound != nullptr) {
      int cmp = ucmp->Compare(*upper_bound, tombstone.start_key_);
      if ((has_overlapping_endpoints && cmp < 0) ||
          (!has_overlapping_endpoints && cmp <= 0)) {
        // Tombstones starting after upper_bound only need to be included in
        // the next table. If the current SST ends before upper_bound, i.e.,
        // `has_overlapping_endpoints == false`, we can also skip over range
        // tombstones that start exactly at upper_bound. Such range
        // tombstones will be included in the next file and are not relevant
        // to the point keys or endpoints of the current file.
        break;
      }
    }

    if (bottommost_level && tombstone.seq_ <= earliest_snapshot) {
      // TODO(andrewkr): tombstones that span multiple output files are
      // counted for each compaction output file, so lots of double
      // counting.
      range_del_out_stats.num_range_del_drop_obsolete++;
      range_del_out_stats.num_record_drop_obsolete++;
      continue;
    }

    auto kv = tombstone.Serialize();
    assert(lower_bound == nullptr ||
           ucmp->Compare(*lower_bound, kv.second) < 0);
    // Range tombstone is not supported by output validator yet.
    builder_->Add(kv.first.Encode(), kv.second);
    InternalKey smallest_candidate = std::move(kv.first);
    if (lower_bound != nullptr &&
        ucmp->Compare(smallest_candidate.user_key(), *lower_bound) <= 0) {
      // Pretend the smallest key has the same user key as lower_bound
      // (the max key in the previous table or subcompaction) in order for
      // files to appear key-space partitioned.
      //
      // When lower_bound is chosen by a subcompaction, we know that
      // subcompactions over smaller keys cannot contain any keys at
      // lower_bound. We also know that smaller subcompactions exist,
      // because otherwise the subcompaction woud be unbounded on the left.
      // As a result, we know that no other files on the output level will
      // contain actual keys at lower_bound (an output file may have a
      // largest key of lower_bound@kMaxSequenceNumber, but this only
      // indicates a large range tombstone was truncated). Therefore, it is
      // safe to use the tombstone's sequence number, to ensure that keys at
      // lower_bound at lower levels are covered by truncated tombstones.
      //
      // If lower_bound was chosen by the smallest data key in the file,
      // choose lowest seqnum so this file's smallest internal key comes
      // after the previous file's largest. The fake seqnum is OK because
      // the read path's file-picking code only considers user key.
      smallest_candidate = InternalKey(
          *lower_bound, lower_bound_from_sub_compact ? tombstone.seq_ : 0,
          kTypeRangeDeletion);
    }
    InternalKey largest_candidate = tombstone.SerializeEndKey();
    if (upper_bound != nullptr &&
        ucmp->Compare(*upper_bound, largest_candidate.user_key()) <= 0) {
      // Pretend the largest key has the same user key as upper_bound (the
      // min key in the following table or subcompaction) in order for files
      // to appear key-space partitioned.
      //
      // Choose highest seqnum so this file's largest internal key comes
      // before the next file's/subcompaction's smallest. The fake seqnum is
      // OK because the read path's file-picking code only considers the
      // user key portion.
      //
      // Note Seek() also creates InternalKey with (user_key,
      // kMaxSequenceNumber), but with kTypeDeletion (0x7) instead of
      // kTypeRangeDeletion (0xF), so the range tombstone comes before the
      // Seek() key in InternalKey's ordering. So Seek() will look in the
      // next file for the user key.
      largest_candidate =
          InternalKey(*upper_bound, kMaxSequenceNumber, kTypeRangeDeletion);
    }
#ifndef NDEBUG
    SequenceNumber smallest_ikey_seqnum = kMaxSequenceNumber;
    if (meta.smallest.size() > 0) {
      smallest_ikey_seqnum = GetInternalKeySeqno(meta.smallest.Encode());
    }
#endif
    meta.UpdateBoundariesForRange(smallest_candidate, largest_candidate,
                                  tombstone.seq_, icmp);
    // The smallest key in a file is used for range tombstone truncation, so
    // it cannot have a seqnum of 0 (unless the smallest data key in a file
    // has a seqnum of 0). Otherwise, the truncated tombstone may expose
    // deleted keys at lower levels.
    assert(smallest_ikey_seqnum == 0 ||
           ExtractInternalKeyFooter(meta.smallest.Encode()) !=
               PackSequenceAndType(0, kTypeRangeDeletion));
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
