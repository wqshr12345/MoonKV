//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/get_context.h"

#include "db/blob//blob_fetcher.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/read_callback.h"
#include "monitoring/file_read_sample.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "nvm/index/btree_iterator.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {

namespace {

void appendToReplayLog(std::string* replay_log, ValueType type, Slice value) {
#ifndef ROCKSDB_LITE
  if (replay_log) {
    if (replay_log->empty()) {
      // Optimization: in the common case of only one operation in the
      // log, we allocate the exact amount of space needed.
      replay_log->reserve(1 + VarintLength(value.size()) + value.size());
    }
    replay_log->push_back(type);
    PutLengthPrefixedSlice(replay_log, value);
  }
#else
  (void)replay_log;
  (void)type;
  (void)value;
#endif  // ROCKSDB_LITE
}

}  // namespace

GetContext::GetContext(const Comparator* ucmp,
                       const MergeOperator* merge_operator, Logger* logger,
                       Statistics* statistics, GetState init_state,
                       const Slice& user_key, PinnableSlice* pinnable_val,
                       std::string* timestamp, bool* value_found,
                       MergeContext* merge_context, bool do_merge,
                       SequenceNumber* _max_covering_tombstone_seq,
                       SystemClock* clock, SequenceNumber* seq,
                       PinnedIteratorsManager* _pinned_iters_mgr,
                       ReadCallback* callback, bool* is_blob_index,
                       uint64_t tracing_get_id, BlobFetcher* blob_fetcher,
                       EstimateSeekTable* estimate_seek_table)
    : ucmp_(ucmp),
      merge_operator_(merge_operator),
      logger_(logger),
      statistics_(statistics),
      state_(init_state),
      user_key_(user_key),
      pinnable_val_(pinnable_val),
      timestamp_(timestamp),
      value_found_(value_found),
      merge_context_(merge_context),
      max_covering_tombstone_seq_(_max_covering_tombstone_seq),
      clock_(clock),
      seq_(seq),
      replay_log_(nullptr),
      pinned_iters_mgr_(_pinned_iters_mgr),
      callback_(callback),
      do_merge_(do_merge),
      is_blob_index_(is_blob_index),
      tracing_get_id_(tracing_get_id),
      blob_fetcher_(blob_fetcher),
      estimate_seek_table_(estimate_seek_table){
  if (seq_) {
    *seq_ = kMaxSequenceNumber;
  }
  sample_ = should_sample_file_read();
}

GetContext::GetContext(
    const Comparator* ucmp, const MergeOperator* merge_operator, Logger* logger,
    Statistics* statistics, GetState init_state, const Slice& user_key,
    PinnableSlice* pinnable_val, bool* value_found, MergeContext* merge_context,
    bool do_merge, SequenceNumber* _max_covering_tombstone_seq,
    SystemClock* clock, SequenceNumber* seq,
    PinnedIteratorsManager* _pinned_iters_mgr, ReadCallback* callback,
    bool* is_blob_index, uint64_t tracing_get_id, BlobFetcher* blob_fetcher,EstimateSeekTable* estimate_seek_table)
    : GetContext(ucmp, merge_operator, logger, statistics, init_state, user_key,
                 pinnable_val, nullptr, value_found, merge_context, do_merge,
                 _max_covering_tombstone_seq, clock, seq, _pinned_iters_mgr,
                 callback, is_blob_index, tracing_get_id, blob_fetcher, estimate_seek_table) {}

GetContext::~GetContext() {
  if(btree_iter_ != nullptr) {
    delete btree_iter_;
  }
}
// Called from TableCache::Get and Table::Get when file/block in which
// key may exist are not there in TableCache/BlockCache respectively. In this
// case we can't guarantee that key does not exist and are not permitted to do
// IO to be certain.Set the status=kFound and value_found=false to let the
// caller know that key may exist but is not there in memory
void GetContext::MarkKeyMayExist() {
  state_ = kFound;
  if (value_found_ != nullptr) {
    *value_found_ = false;
  }
}

void GetContext::SaveValue(const Slice& value, SequenceNumber /*seq*/) {
  assert(state_ == kNotFound);
  appendToReplayLog(replay_log_, kTypeValue, value);

  state_ = kFound;
  if (LIKELY(pinnable_val_ != nullptr)) {
    pinnable_val_->PinSelf(value);
  }
}

void GetContext::ReportCounters() {
  if (get_context_stats_.num_cache_hit > 0) {
    RecordTick(statistics_, BLOCK_CACHE_HIT, get_context_stats_.num_cache_hit);
  }
  if (get_context_stats_.num_cache_index_hit > 0) {
    RecordTick(statistics_, BLOCK_CACHE_INDEX_HIT,
               get_context_stats_.num_cache_index_hit);
  }
  if (get_context_stats_.num_cache_data_hit > 0) {
    RecordTick(statistics_, BLOCK_CACHE_DATA_HIT,
               get_context_stats_.num_cache_data_hit);
  }
  if (get_context_stats_.num_cache_filter_hit > 0) {
    RecordTick(statistics_, BLOCK_CACHE_FILTER_HIT,
               get_context_stats_.num_cache_filter_hit);
  }
  if (get_context_stats_.num_cache_compression_dict_hit > 0) {
    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_HIT,
               get_context_stats_.num_cache_compression_dict_hit);
  }
  if (get_context_stats_.num_cache_index_miss > 0) {
    RecordTick(statistics_, BLOCK_CACHE_INDEX_MISS,
               get_context_stats_.num_cache_index_miss);
  }
  if (get_context_stats_.num_cache_filter_miss > 0) {
    RecordTick(statistics_, BLOCK_CACHE_FILTER_MISS,
               get_context_stats_.num_cache_filter_miss);
  }
  if (get_context_stats_.num_cache_data_miss > 0) {
    RecordTick(statistics_, BLOCK_CACHE_DATA_MISS,
               get_context_stats_.num_cache_data_miss);
  }
  if (get_context_stats_.num_cache_compression_dict_miss > 0) {
    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_MISS,
               get_context_stats_.num_cache_compression_dict_miss);
  }
  if (get_context_stats_.num_cache_bytes_read > 0) {
    RecordTick(statistics_, BLOCK_CACHE_BYTES_READ,
               get_context_stats_.num_cache_bytes_read);
  }
  if (get_context_stats_.num_cache_miss > 0) {
    RecordTick(statistics_, BLOCK_CACHE_MISS,
               get_context_stats_.num_cache_miss);
  }
  if (get_context_stats_.num_cache_add > 0) {
    RecordTick(statistics_, BLOCK_CACHE_ADD, get_context_stats_.num_cache_add);
  }
  if (get_context_stats_.num_cache_add_redundant > 0) {
    RecordTick(statistics_, BLOCK_CACHE_ADD_REDUNDANT,
               get_context_stats_.num_cache_add_redundant);
  }
  if (get_context_stats_.num_cache_bytes_write > 0) {
    RecordTick(statistics_, BLOCK_CACHE_BYTES_WRITE,
               get_context_stats_.num_cache_bytes_write);
  }
  if (get_context_stats_.num_cache_index_add > 0) {
    RecordTick(statistics_, BLOCK_CACHE_INDEX_ADD,
               get_context_stats_.num_cache_index_add);
  }
  if (get_context_stats_.num_cache_index_add_redundant > 0) {
    RecordTick(statistics_, BLOCK_CACHE_INDEX_ADD_REDUNDANT,
               get_context_stats_.num_cache_index_add_redundant);
  }
  if (get_context_stats_.num_cache_index_bytes_insert > 0) {
    RecordTick(statistics_, BLOCK_CACHE_INDEX_BYTES_INSERT,
               get_context_stats_.num_cache_index_bytes_insert);
  }
  if (get_context_stats_.num_cache_data_add > 0) {
    RecordTick(statistics_, BLOCK_CACHE_DATA_ADD,
               get_context_stats_.num_cache_data_add);
  }
  if (get_context_stats_.num_cache_data_add_redundant > 0) {
    RecordTick(statistics_, BLOCK_CACHE_DATA_ADD_REDUNDANT,
               get_context_stats_.num_cache_data_add_redundant);
  }
  if (get_context_stats_.num_cache_data_bytes_insert > 0) {
    RecordTick(statistics_, BLOCK_CACHE_DATA_BYTES_INSERT,
               get_context_stats_.num_cache_data_bytes_insert);
  }
  if (get_context_stats_.num_cache_filter_add > 0) {
    RecordTick(statistics_, BLOCK_CACHE_FILTER_ADD,
               get_context_stats_.num_cache_filter_add);
  }
  if (get_context_stats_.num_cache_filter_add_redundant > 0) {
    RecordTick(statistics_, BLOCK_CACHE_FILTER_ADD_REDUNDANT,
               get_context_stats_.num_cache_filter_add_redundant);
  }
  if (get_context_stats_.num_cache_filter_bytes_insert > 0) {
    RecordTick(statistics_, BLOCK_CACHE_FILTER_BYTES_INSERT,
               get_context_stats_.num_cache_filter_bytes_insert);
  }
  if (get_context_stats_.num_cache_compression_dict_add > 0) {
    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_ADD,
               get_context_stats_.num_cache_compression_dict_add);
  }
  if (get_context_stats_.num_cache_compression_dict_add_redundant > 0) {
    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT,
               get_context_stats_.num_cache_compression_dict_add_redundant);
  }
  if (get_context_stats_.num_cache_compression_dict_bytes_insert > 0) {
    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT,
               get_context_stats_.num_cache_compression_dict_bytes_insert);
  }
  if (get_context_stats_.blocks_no_estimate > 0) {
    RecordTick(statistics_, BLOCKS_NO_ESTIMATE,
               get_context_stats_.blocks_no_estimate);
  }
  if (get_context_stats_.blocks_with_estimate > 0) {
    RecordTick(statistics_, BLOCKS_WITH_ESTIMATE,
               get_context_stats_.blocks_with_estimate);
  }
  if (get_context_stats_.num_search_keys > 0) {
    RecordTick(statistics_, NUM_SEARCH_KEYS,
               get_context_stats_.num_search_keys);
  }  
  if (get_context_stats_.num_search_files > 0) {
    RecordTick(statistics_, NUM_SEARCH_FILES,
               get_context_stats_.num_search_files);
  }  
  if (get_context_stats_.num_not_found > 0) {
    RecordTick(statistics_, NUM_NOT_FOUND,
               get_context_stats_.num_not_found);
  }  
  if (get_context_stats_.num_found > 0) {
    RecordTick(statistics_, NUM_FOUND,
               get_context_stats_.num_found);
  }  
  if (get_context_stats_.num_merge > 0) {
    RecordTick(statistics_, NUM_MERGE,
               get_context_stats_.num_merge);
  }
  if (get_context_stats_.num_seek_merge_key_times +
          get_context_stats_.num_seek_full_key_times > 1) {
    RecordTick(statistics_, NUM_SEEK_MERGE_KEY_TIMES,
               1);
    RecordTick(statistics_, NUM_SEEK_MERGE_KEYS_INCLUDE_RELATED_FULL_KEYS,
               get_context_stats_.num_seek_merge_key_times + get_context_stats_.num_seek_full_key_times);
  }
  if (get_context_stats_.num_seek_merge_key_times +
          get_context_stats_.num_seek_full_key_times == 1) {
    RecordTick(statistics_, NUM_SEEK_FULL_KEY_TIMES,
               1);
  }
  if (get_context_stats_.num_seek_levels > 0) {
    RecordTick(statistics_, NUM_SEEK_LEVELS,
               get_context_stats_.num_seek_levels);
  }
  if (get_context_stats_.num_seek_times > 0) {
    RecordTick(statistics_, NUM_SEEK_TIMES,
               get_context_stats_.num_seek_times);
  }
}

bool GetContext::SaveValue(const ParsedInternalKey& parsed_key,
                           const Slice& value, bool* matched,
                           Cleanable* value_pinner) {
  assert(matched);
  assert((state_ != kMerge && parsed_key.type != kTypeMerge) ||
         merge_context_ != nullptr);
  if (ucmp_->EqualWithoutTimestamp(parsed_key.user_key, user_key_)) {
    *matched = true;
    // If the value is not in the snapshot, skip it
    if (!CheckCallback(parsed_key.sequence)) {
      return true;  // to continue to the next seq
    }

    appendToReplayLog(replay_log_, parsed_key.type, value);

    if (seq_ != nullptr) {
      // Set the sequence number if it is uninitialized
      if (*seq_ == kMaxSequenceNumber) {
        *seq_ = parsed_key.sequence;
      }
    }

    auto type = parsed_key.type;
    // Key matches. Process it
    if ((type == kTypeValue || type == kTypeMerge || type == kTypeBlobIndex ||
         type == kTypeWideColumnEntity) &&
        max_covering_tombstone_seq_ != nullptr &&
        *max_covering_tombstone_seq_ > parsed_key.sequence) {
      type = kTypeRangeDeletion;
    }
    switch (type) {
      case kTypeValue:
        get_context_stats_.num_seek_full_key_times += 1;
        if (level_ != 0)
          get_context_stats_.num_seek_key_times_nol0 += 1;
      case kTypeBlobIndex:
      case kTypeWideColumnEntity:
        assert(state_ == kNotFound || state_ == kMerge);
        if (type == kTypeBlobIndex) {
          if (is_blob_index_ == nullptr) {
            // Blob value not supported. Stop.
            state_ = kUnexpectedBlobIndex;
            return false;
          }
        } else if (type == kTypeWideColumnEntity) {
          // TODO: support wide-column entities
          state_ = kUnexpectedWideColumnEntity;
          return false;
        }

        if (is_blob_index_ != nullptr) {
          *is_blob_index_ = (type == kTypeBlobIndex);
        }

        if (kNotFound == state_) {
          state_ = kFound;
          if (do_merge_) {
            if (LIKELY(pinnable_val_ != nullptr)) {
              if (LIKELY(value_pinner != nullptr)) {
                // If the backing resources for the value are provided, pin them
                pinnable_val_->PinSlice(value, value_pinner);
              } else {
                TEST_SYNC_POINT_CALLBACK("GetContext::SaveValue::PinSelf",
                                         this);
                // Otherwise copy the value
                pinnable_val_->PinSelf(value);
              }
            }
          } else {
            // It means this function is called as part of DB GetMergeOperands
            // API and the current value should be part of
            // merge_context_->operand_list
            if (type == kTypeBlobIndex) {
              PinnableSlice pin_val;
              if (GetBlobValue(value, &pin_val) == false) {
                return false;
              }
              Slice blob_value(pin_val);
              push_operand(blob_value, nullptr);
            } else if (type == kTypeWideColumnEntity) {
              // TODO: support wide-column entities
              state_ = kUnexpectedWideColumnEntity;
              return false;
            } else {
              assert(type == kTypeValue);
              push_operand(value, value_pinner);
            }
          }
        } else if (kMerge == state_) {
          assert(merge_operator_ != nullptr);
          if (type == kTypeBlobIndex) {
            PinnableSlice pin_val;
            if (GetBlobValue(value, &pin_val) == false) {
              return false;
            }
            Slice blob_value(pin_val);
            state_ = kFound;
            if (do_merge_) {
              Merge(&blob_value);
            } else {
              // It means this function is called as part of DB GetMergeOperands
              // API and the current value should be part of
              // merge_context_->operand_list
              push_operand(blob_value, nullptr);
            }
          } else if (type == kTypeWideColumnEntity) {
            // TODO: support wide-column entities
            state_ = kUnexpectedWideColumnEntity;
            return false;
          } else {
            assert(type == kTypeValue);

            state_ = kFound;
            if (do_merge_) {
              Merge(&value);
            } else {
              // It means this function is called as part of DB GetMergeOperands
              // API and the current value should be part of
              // merge_context_->operand_list
              push_operand(value, value_pinner);
            }
          }
        }
        if (state_ == kFound) {
          size_t ts_sz = ucmp_->timestamp_size();
          if (ts_sz > 0 && timestamp_ != nullptr) {
            Slice ts = ExtractTimestampFromUserKey(parsed_key.user_key, ts_sz);
            timestamp_->assign(ts.data(), ts.size());
          }
        }
        return false;

      case kTypeDeletion:
      case kTypeDeletionWithTimestamp:
      case kTypeSingleDeletion:
      case kTypeRangeDeletion:
        // TODO(noetzli): Verify correctness once merge of single-deletes
        // is supported
        assert(state_ == kNotFound || state_ == kMerge);
        if (kNotFound == state_) {
          state_ = kDeleted;
          size_t ts_sz = ucmp_->timestamp_size();
          if (ts_sz > 0 && timestamp_ != nullptr) {
            Slice ts = ExtractTimestampFromUserKey(parsed_key.user_key, ts_sz);
            timestamp_->assign(ts.data(), ts.size());
          }
        } else if (kMerge == state_) {
          state_ = kFound;
          Merge(nullptr);
          // If do_merge_ = false then the current value shouldn't be part of
          // merge_context_->operand_list
        }
        return false;

      case kTypeMerge:
        get_context_stats_.num_seek_merge_key_times += 1;
        if (level_ != 0)
          get_context_stats_.num_seek_key_times_nol0 += 1;
        assert(state_ == kNotFound || state_ == kMerge);
        state_ = kMerge;
        // value_pinner is not set from plain_table_reader.cc for example.
        push_operand(value, value_pinner);
        if (do_merge_ && merge_operator_ != nullptr &&
            merge_operator_->ShouldMerge(
                merge_context_->GetOperandsDirectionBackward())) {
          state_ = kFound;
          Merge(nullptr);
          return false;
        }
        return true;

      default:
        assert(false);
        break;
    }
  }

  // state_ could be Corrupt, merge or notfound
  return false;
}

void GetContext::Merge(const Slice* value) {
  if (LIKELY(pinnable_val_ != nullptr)) {
    if (do_merge_) {
      Status merge_status = MergeHelper::TimedFullMerge(
          merge_operator_, user_key_, value, merge_context_->GetOperands(),
          pinnable_val_->GetSelf(), logger_, statistics_, clock_);
      pinnable_val_->PinSelf();
      if (!merge_status.ok()) {
        state_ = kCorrupt;
      }
    }
  }
}

bool GetContext::GetBlobValue(const Slice& blob_index,
                              PinnableSlice* blob_value) {
  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr uint64_t* bytes_read = nullptr;

  Status status = blob_fetcher_->FetchBlob(
      user_key_, blob_index, prefetch_buffer, blob_value, bytes_read);
  if (!status.ok()) {
    if (status.IsIncomplete()) {
      // FIXME: this code is not covered by unit tests
      MarkKeyMayExist();
      return false;
    }
    state_ = kCorrupt;
    return false;
  }
  *is_blob_index_ = false;
  return true;
}

void GetContext::push_operand(const Slice& value, Cleanable* value_pinner) {
  if (pinned_iters_mgr() && pinned_iters_mgr()->PinningEnabled() &&
      value_pinner != nullptr) {
    value_pinner->DelegateCleanupsTo(pinned_iters_mgr());
    merge_context_->PushOperand(value, true /*value_pinned*/);
  } else {
    merge_context_->PushOperand(value, false);
  }
}

void replayGetContextLog(const Slice& replay_log, const Slice& user_key,
                         GetContext* get_context, Cleanable* value_pinner) {
#ifndef ROCKSDB_LITE
  Slice s = replay_log;
  while (s.size()) {
    auto type = static_cast<ValueType>(*s.data());
    s.remove_prefix(1);
    Slice value;
    bool ret = GetLengthPrefixedSlice(&s, &value);
    assert(ret);
    (void)ret;

    bool dont_care __attribute__((__unused__));
    // Since SequenceNumber is not stored and unknown, we will use
    // kMaxSequenceNumber.
    get_context->SaveValue(
        ParsedInternalKey(user_key, kMaxSequenceNumber, type), value,
        &dont_care, value_pinner);
  }
#else   // ROCKSDB_LITE
  (void)replay_log;
  (void)user_key;
  (void)get_context;
  (void)value_pinner;
  assert(false);
#endif  // ROCKSDB_LITE
}

uint32_t GetContext::EstimateSeekIfNeed(const Slice& key,int btree_levels, bool estimate_seek, int estimate_threshold, SystemClock* clock, Statistics* stats) {
  uint32_t seek_levels = 0;
  uint64_t elapsed = 0;
  const Defer cleanup([btree_levels, &seek_levels, clock, stats, &elapsed]() {
    switch (btree_levels)
        {
        case 0:
          break;
        case 1:
          stats->reportTimeToHistogram(DB_INDEX_LEVEL1_SEEK_BLOCKS, seek_levels);
          stats->reportTimeToHistogram(DB_INDEX_LEVEL1_GET, elapsed);
          break;
        case 2:
          stats->reportTimeToHistogram(DB_INDEX_LEVEL2_SEEK_BLOCKS, seek_levels);
          stats->reportTimeToHistogram(DB_INDEX_LEVEL2_GET, elapsed);
          break;
        case 3:
          stats->reportTimeToHistogram(DB_INDEX_LEVEL3_SEEK_BLOCKS, seek_levels);
          stats->reportTimeToHistogram(DB_INDEX_LEVEL3_GET, elapsed);
          break;
        case 4:
          stats->reportTimeToHistogram(DB_INDEX_LEVEL4_SEEK_BLOCKS, seek_levels);
          stats->reportTimeToHistogram(DB_INDEX_LEVEL4_GET, elapsed);
          break;
        case 5: 
          stats->reportTimeToHistogram(DB_INDEX_LEVEL5_SEEK_BLOCKS, seek_levels);
          stats->reportTimeToHistogram(DB_INDEX_LEVEL5_GET, elapsed);
          break;  
        case 6:
          stats->reportTimeToHistogram(DB_INDEX_LEVEL6_SEEK_BLOCKS, seek_levels);
          stats->reportTimeToHistogram(DB_INDEX_LEVEL6_GET, elapsed);
          break;
        case 7:
          stats->reportTimeToHistogram(DB_INDEX_LEVEL7_SEEK_BLOCKS, seek_levels);
          stats->reportTimeToHistogram(DB_INDEX_LEVEL7_GET, elapsed);
          break;
        case 8:
          stats->reportTimeToHistogram(DB_INDEX_LEVEL8_SEEK_BLOCKS, seek_levels);
          stats->reportTimeToHistogram(DB_INDEX_LEVEL8_GET, elapsed);
          break;
        default:
          stats->reportTimeToHistogram(DB_INDEX_LEVEL9_AND_UP_SEEK_BLOCKS, seek_levels);
          stats->reportTimeToHistogram(DB_INDEX_LEVEL9_AND_UP_GET, elapsed);

          break;
        }
        switch (seek_levels) {
          case 0:
            break;
          case 1:
            stats->reportTimeToHistogram(DB_INDEX_SEEK1_GET, elapsed);
            break;
          case 2:
            stats->reportTimeToHistogram(DB_INDEX_SEEK2_GET, elapsed);
            break;
          case 3:
            stats->reportTimeToHistogram(DB_INDEX_SEEK3_GET, elapsed);
            break;
          case 4:
            stats->reportTimeToHistogram(DB_INDEX_SEEK4_GET, elapsed);
            break;
          case 5:
            stats->reportTimeToHistogram(DB_INDEX_SEEK5_GET, elapsed);
            break;
          case 6:
            stats->reportTimeToHistogram(DB_INDEX_SEEK6_GET, elapsed);
            break;
          case 7:
            stats->reportTimeToHistogram(DB_INDEX_SEEK7_GET, elapsed);
            break;
          case 8:
            stats->reportTimeToHistogram(DB_INDEX_SEEK8_GET, elapsed);
            break;

          default:
            stats->reportTimeToHistogram(DB_INDEX_SEEK9_AND_UP_GET, elapsed);
            break;
        }
  });
  // 1. 没有开启estimate_seek 直接调用Seek接口
  if (!estimate_seek) {
    Histograms temp = DB_INDEX_UNES_GET;
    if(level_ == 0 || level_ == 1)
      temp = DB_INDEX_UNES_PARTIAL_GET;
    StopWatch sw(clock, stats, temp, &elapsed);
    btree_iter_->Seek(key);
    // 清理上一次查询到的iterator
    if(last_btree_iter_ != nullptr) {
      delete last_btree_iter_;
      last_btree_iter_ = nullptr;
    }
    seek_levels = btree_levels;
    return btree_levels; // 不开启estimate_seek时，查询的blocks即为btree_levels
  }
  std::pair<uint64_t, uint64_t> seek_pair = {last_file_number_, file_number_};
  // 2. 判断当前的{last_file_number, file_number}是否在EstimateSeekTable中存在
  // 2.1 不存在对应关系
  if(!estimate_seek_table_->IsKeyExist(seek_pair)) {
    // 2.1.1 对l0的第一个文件的查询，暂时无{last_file_number_, file_number_}的对应关系
    if(last_file_number_ == -1) {
      StopWatch* sw = new StopWatch(clock, stats, DB_INDEX_ENES_NOES_GET, &elapsed);
      btree_iter_->Seek(key);
      delete sw;
      last_btree_posiiton_ = btree_iter_->GetKeyPosition(0);
      seek_levels = btree_levels;
      return btree_levels;
    }
    // 2.2.2 针对{last_file_number_, file_number}的第一次查询，对应关系尚未被添加
    else {
      StopWatch sw(clock, stats, DB_INDEX_ADD_KEY_POS_LIST);
      // 查询child iter中的start key和end_key及其level3_block, 准备添加到map中
      btree_iter_->SeekToFirst();
      std::string left_key = btree_iter_->key().ToString();
      uint32_t level3_left_block = btree_iter_->GetKeyPosition(3);

      btree_iter_->SeekToLast();
      std::string right_key = btree_iter_->key().ToString();
      uint32_t level3_right_block = btree_iter_->GetKeyPosition(3);

      // 查询father iter中的left_key和right_key的position, 准备添加到map中
      last_btree_iter_->Seek(left_key);
      if (!last_btree_iter_->Valid())
      {
        last_btree_iter_->SeekToFirst();
      }
      uint32_t left_pos = last_btree_iter_->GetKeyPosition(0);

      last_btree_iter_->Seek(right_key);
      if (!last_btree_iter_->Valid())
      {
        last_btree_iter_->SeekToLast();
      }
      uint32_t right_pos = last_btree_iter_->GetKeyPosition(0);

      // 构造left key和right key的KeyPosition信息
      KeyPosition left_key_pos(left_key, left_pos, level3_left_block);
      KeyPosition right_key_pos(right_key, right_pos, level3_right_block);

      assert(last_file_number_ != -1 && file_number_ != -1);
      // note: 如果这里有多线程同时构造针对一个pair {last_file_number_, file_number_}的map关系，那么只有第一个会构造成功，后续都会构造失败，这是由folly的ConcurrentHashmap的insert方法保证的
      estimate_seek_table_->ConstructKeyPosList(seek_pair, left_key_pos, right_key_pos);
    }
  }
  // 2.2 存在对应关系(就算不存在 也在上面的ConstructKeyPosList中构造完成)
  StopWatch *sw0 = new StopWatch(clock, stats, DB_INDEX_KEY_POS_LIST_GET);
  KeyPosList* key_pos_list = estimate_seek_table_->GetKeyPosList(seek_pair);
  delete sw0;
  assert(last_btree_posiiton_ != -1);
  StopWatch *sw1 = new StopWatch(clock, stats, DB_INDEX_KEY_POS_GET);
  int estimate_position = key_pos_list->GetKeyPos(key.ToString());
  delete sw1;
  Histograms temp = DB_INDEX_ENES_WITHES_GET;
  if(level_ == 0 || level_ == 1) 
    temp = DB_INDEX_ENES_WITHES_PARTIAL_GET;
  StopWatch* sw2 = new StopWatch(clock, stats, temp, &elapsed);
  seek_levels = btree_iter_->EstimateSeek(key, estimate_position, LinearDetection);
  delete sw2;
  if (seek_levels >= estimate_threshold) {
    StopWatch sw3(clock, stats, DB_INDEX_ADD_KEY_POS);
    // 这里的AddKeyPos的并发安全由atomic_bool+一写多读安全的skiplist保证
    key_pos_list->AddKeyPos(key.ToString(), last_btree_posiiton_, btree_iter_->GetKeyPosition(3));
  }
  if(last_btree_iter_ != nullptr) {
    delete last_btree_iter_;  
    last_btree_iter_ = nullptr;
  }
  // 更新position信息和统计信息
  last_btree_posiiton_ = btree_iter_->GetKeyPosition(0);
  get_context_stats_.blocks_with_estimate += seek_levels;
  get_context_stats_.blocks_no_estimate += btree_levels;
  return seek_levels;
}

// uint32_t GetContext::EstimateSeekIfNeed(const Slice& key,int btree_levels, bool estimate_seek, int estimate_threshold) {
//   if (!estimate_seek) {
//     btree_iter_->Seek(key);
//     if(last_btree_iter_ != nullptr) {
//       delete last_btree_iter_;
//       last_btree_iter_ = nullptr;
//     }
//     return btree_levels; // 不开启estimate_seek时，查询的blocks即为btree_levels
//   }
//   // 1.先根据last_file_number和file_number找父子的对应关系
//   auto it = estimate_seek_table_->find(std::pair<uint64_t, uint64_t>(last_file_number_, file_number_));
//   // 2.如果map中没有对应关系 有两种可能 
//   // a. 上一个table是-1 说明是第一次查询 无对应关系
//   // b. 这是第一次father到child的查询 map中还没添加对应关系 需要添加
//   if(it == estimate_seek_table_->end()) {
//     // 2.1 本次是第一次查询 无法进行estimate seek，什么也不做 直接返回
//     if(last_file_number_ == -1) {
//       get_context_stats_.estimate_search_miss += 1;
//       btree_iter_->Seek(key);
//       last_btree_posiiton_ = btree_iter_->GetKeyPosition(0);
//       return btree_levels;
//     } 
//     // 2.2 本次是第一次father到child的查询 需要添加对应关系
//     else {
//       // 为第一次father到child的查询构造KeyList
//       PositionKeyList key_list(last_file_number_, file_number_);

//       // 查询child iter中的[start, end] key 准备添加到map中
//       btree_iter_->SeekToFirst();
//       std::string left_key = btree_iter_->key().ToString();
//       uint32_t level3_left_block = btree_iter_->GetKeyPosition(3);

//       btree_iter_->SeekToLast();
//       std::string right_key = btree_iter_->key().ToString();
//       uint32_t level3_right_block = btree_iter_->GetKeyPosition(3);

//       // 在father iter中查询对应key的position
//       last_btree_iter_->Seek(left_key);
//       if (!last_btree_iter_->Valid())
//       {
//         last_btree_iter_->SeekToFirst();
//       }
//       uint32_t left_pos = last_btree_iter_->GetKeyPosition(0);

//       last_btree_iter_->Seek(right_key);
//       if (!last_btree_iter_->Valid())
//       {
//         last_btree_iter_->SeekToLast();
//       }
//       uint32_t right_pos = last_btree_iter_->GetKeyPosition(0);

//       key_list.AddPosKey(left_key, left_pos, level3_left_block);
//       key_list.AddPosKey(right_key, right_pos, level3_right_block);

//       // 针对第一次father->child的查询 插入数据
//       assert(last_file_number_ != -1 && file_number_ != -1);
//       // insert()方法，如果key对应元素存在，那么不会被添加。这就保证了针对同一个<last_file_number_,file_numer>,第一个key_list被添加进去之后，后续都不会再重复添加key_list，也不会出现覆盖情况。
//       // 所以我们也不需要考虑原子更新key_list的情况，只需要考虑针对同一个key_list如何做到并发安全(再用一层并发安全skiplist...)
//       estimate_seek_table_->insert({{last_file_number_, file_number_}, key_list});
//       // 插入最新的key_list之后，需要更新iter，以便在下面的代码中使用
//     }


//   }
//   // 3. 如果map中有对应关系 说明之前有过类似的查询，那么可以直接使用对应关系
//   else {
//     // do nothing
//   }
//   // 4. 直接找到对应关系 开查!
//   // 如果返回引用 那么后续需要做AddPosKey操作，这个操作是可能并发的，所以要求key_list是并发安全的
//   // 如果返回拷贝 那么后续只需要重新把key_list放回map里 因为map是并发安全的，所以没有core dump问题
//   // 但返回拷贝有个缺点 同一时刻可能多个线程同时查到了这个key_list 针对它各自添加了一个PosKey 但最后都会被一份覆盖掉...
//   // 所以最好还是返回引用 然后用并发安全的key_list
//   it = estimate_seek_table_->find(std::pair<uint64_t, uint64_t>(last_file_number_, file_number_));
//   PositionKeyList key_list = it->second;
//   // 4.1 找到当前key通过key_list预估的位置

//   assert(last_btree_posiiton_ != -1);
//   int estimate_position = key_list.GetEstimatePos(key.ToString(), last_btree_posiiton_);
//   uint32_t seek_levels = btree_iter_->EstimateSeek(key, estimate_position, LinearDetection);
//   last_btree_posiiton_ = btree_iter_->GetKeyPosition(0);
//   if (seek_levels >= estimate_threshold) {
//     key_list.AddPosKey(key.ToString(), last_btree_posiiton_, btree_iter_->GetKeyPosition(3));
//   }
//   get_context_stats_.estimate_search_hit += 1;
//   // 返回拷贝需要再加上下面这一行
//   // it->second = key_list;
//   if(last_btree_iter_ != nullptr) {
//     delete last_btree_iter_;  
//     last_btree_iter_ = nullptr;
//   }
//   get_context_stats_.blocks_with_estimate += seek_levels;
//   get_context_stats_.blocks_no_estimate += btree_levels;
//   return seek_levels;
// }

}  // namespace ROCKSDB_NAMESPACE
