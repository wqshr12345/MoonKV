//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>

#include "db/read_callback.h"
#include "rocksdb/types.h"
#include "nvm/index/position_key_list.h"
#include "util/estimate_seek_table.h"
namespace ROCKSDB_NAMESPACE {
class BlobFetcher;
class Comparator;
class Logger;
class MergeContext;
class MergeOperator;
class PinnedIteratorsManager;
class Statistics;
class SystemClock;
struct ParsedInternalKey;
class BtreeIterator;

// Data structure for accumulating statistics during a point lookup. At the
// end of the point lookup, the corresponding ticker stats are updated. This
// avoids the overhead of frequent ticker stats updates
struct GetContextStats {
  uint64_t num_cache_hit = 0;
  uint64_t num_cache_index_hit = 0;
  uint64_t num_cache_data_hit = 0;
  uint64_t num_cache_filter_hit = 0;
  uint64_t num_cache_compression_dict_hit = 0;
  uint64_t num_cache_index_miss = 0;
  uint64_t num_cache_filter_miss = 0;
  uint64_t num_cache_data_miss = 0;
  uint64_t num_cache_compression_dict_miss = 0;
  uint64_t num_cache_bytes_read = 0;
  uint64_t num_cache_miss = 0;
  uint64_t num_cache_add = 0;
  uint64_t num_cache_add_redundant = 0;
  uint64_t num_cache_bytes_write = 0;
  uint64_t num_cache_index_add = 0;
  uint64_t num_cache_index_add_redundant = 0;
  uint64_t num_cache_index_bytes_insert = 0;
  uint64_t num_cache_data_add = 0;
  uint64_t num_cache_data_add_redundant = 0;
  uint64_t num_cache_data_bytes_insert = 0;
  uint64_t num_cache_filter_add = 0;
  uint64_t num_cache_filter_add_redundant = 0;
  uint64_t num_cache_filter_bytes_insert = 0;
  uint64_t num_cache_compression_dict_add = 0;
  uint64_t num_cache_compression_dict_add_redundant = 0;
  uint64_t num_cache_compression_dict_bytes_insert = 0;
  uint64_t blocks_no_estimate = 0; // 如果没有EstimateSearch，统计本次查询index的block理论数目
  uint64_t blocks_with_estimate = 0; // 有EstimateSearch的情况下，统计本次查询index的block实际数目
  uint64_t num_search_keys = 0; // 统计到目前为止的所有查询key的个数
  uint64_t num_search_files = 0; // 统计到目前为止的所有查询过的file的个数(不包括查询内存的imm和mem的次数)
  uint64_t num_not_found = 0; // 统计到目前为止的not found类型的查询的个数(这些是贯穿所有level的完全无效的查询)
  uint64_t num_found = 0; // 统计到目前为止的found类型的查询的个数(这些查询不一定贯穿所有level)
  uint64_t num_merge = 0; // 统计到目前为止的merge类型的查询的个数(这些查询也一定贯穿所有level)
  // MultiGet stats.
  uint64_t num_filter_read = 0;
  uint64_t num_index_read = 0;
  uint64_t num_sst_read = 0;
  uint64_t num_seek_merge_key_times = 0;  // 统计该次查询操作查找到的merge key数量
  uint64_t num_seek_full_key_times = 0;   // 统计该次查询操作查找到的full key数量
  uint64_t num_seek_key_times_nol0 = 0; // 统计本次查询操作查到的key数量 排除l0
  uint64_t num_seek_levels = 0; // 统计本次查询一共经过了多少level
  uint64_t num_seek_times = 0; // 统计本次查询实际查询了多少次sst
};

// A class to hold context about a point lookup, such as pointer to value
// slice, key, merge context etc, as well as the current state of the
// lookup. Any user using GetContext to track the lookup result must call
// SaveValue() whenever the internal key is found. This can happen
// repeatedly in case of merge operands. In case the key may exist with
// high probability, but IO is required to confirm and the user doesn't allow
// it, MarkKeyMayExist() must be called instead of SaveValue().
class GetContext {
 public:
  // Current state of the point lookup. All except kNotFound and kMerge are
  // terminal states
  enum GetState {
    kNotFound,
    kFound,
    kDeleted,
    kCorrupt,
    kMerge,  // saver contains the current merge result (the operands)
    kUnexpectedBlobIndex,
    // TODO: remove once wide-column entities are supported by Get/MultiGet
    kUnexpectedWideColumnEntity,
  };
  GetContextStats get_context_stats_;

  // Constructor
  // @param value Holds the value corresponding to user_key. If its nullptr
  //              then return all merge operands corresponding to user_key
  //              via merge_context
  // @param value_found If non-nullptr, set to false if key may be present
  //                    but we can't be certain because we cannot do IO
  // @param max_covering_tombstone_seq Pointer to highest sequence number of
  //                    range deletion covering the key. When an internal key
  //                    is found with smaller sequence number, the lookup
  //                    terminates
  // @param seq If non-nullptr, the sequence number of the found key will be
  //            saved here
  // @param callback Pointer to ReadCallback to perform additional checks
  //                 for visibility of a key
  // @param is_blob_index If non-nullptr, will be used to indicate if a found
  //                      key is of type blob index
  // @param do_merge True if value associated with user_key has to be returned
  // and false if all the merge operands associated with user_key has to be
  // returned. Id do_merge=false then all the merge operands are stored in
  // merge_context and they are never merged. The value pointer is untouched.
  GetContext(const Comparator* ucmp, const MergeOperator* merge_operator,
             Logger* logger, Statistics* statistics, GetState init_state,
             const Slice& user_key, PinnableSlice* value, bool* value_found,
             MergeContext* merge_context, bool do_merge,
             SequenceNumber* max_covering_tombstone_seq, SystemClock* clock,
             SequenceNumber* seq = nullptr,
             PinnedIteratorsManager* _pinned_iters_mgr = nullptr,
             ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
             uint64_t tracing_get_id = 0, BlobFetcher* blob_fetcher = nullptr, EstimateSeekTable* estimate_table = nullptr);
  GetContext(const Comparator* ucmp, const MergeOperator* merge_operator,
             Logger* logger, Statistics* statistics, GetState init_state,
             const Slice& user_key, PinnableSlice* value,
             std::string* timestamp, bool* value_found,
             MergeContext* merge_context, bool do_merge,
             SequenceNumber* max_covering_tombstone_seq, SystemClock* clock,
             SequenceNumber* seq = nullptr,
             PinnedIteratorsManager* _pinned_iters_mgr = nullptr,
             ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
             uint64_t tracing_get_id = 0, BlobFetcher* blob_fetcher = nullptr, EstimateSeekTable* estimate_table = nullptr);

  GetContext() = delete;

  // 在一个GetContext上下文中 前面查询的文件的iterator可能会在后面的查询中被用到(比如第一次构造PositionKeyList时需要用last_btree_iter_查询left_key和right_key) 所以它需要延时delete
  // 但是本次查询中的最后一个btree_iter无法被删除 需要在GetContext中进行delete
  ~GetContext();

  // This can be called to indicate that a key may be present, but cannot be
  // confirmed due to IO not allowed
  void MarkKeyMayExist();

  // Records this key, value, and any meta-data (such as sequence number and
  // state) into this GetContext.
  //
  // If the parsed_key matches the user key that we are looking for, sets
  // matched to true.
  //
  // Returns True if more keys need to be read (due to merges) or
  //         False if the complete value has been found.
  // SaveValue() 表示在当前level能否找到数据
  // 如果在当前level找到了kTypeValue/kTypeDelete，或者什么都没找到，那么SaveValue返回false
  // 如果在当前level找到了kTypeMerge，表明还需要在当前level继续查找，此时SaveValue返回true
  // 也就是说 SaveValue()的返回值仅仅决定是否会在一个level内部继续查；决定要不要在不同level继续查是更外层的FilePicker...那里有switch的return和break逻辑...
  bool SaveValue(const ParsedInternalKey& parsed_key, const Slice& value,
                 bool* matched, Cleanable* value_pinner = nullptr);

  // Simplified version of the previous function. Should only be used when we
  // know that the operation is a Put.
  void SaveValue(const Slice& value, SequenceNumber seq);

  GetState State() const { return state_; }

  SequenceNumber* max_covering_tombstone_seq() {
    return max_covering_tombstone_seq_;
  }

  PinnedIteratorsManager* pinned_iters_mgr() { return pinned_iters_mgr_; }

  // If a non-null string is passed, all the SaveValue calls will be
  // logged into the string. The operations can then be replayed on
  // another GetContext with replayGetContextLog.
  void SetReplayLog(std::string* replay_log) { replay_log_ = replay_log; }

  // Do we need to fetch the SequenceNumber for this key?
  bool NeedToReadSequence() const { return (seq_ != nullptr); }

  bool sample() const { return sample_; }

  bool CheckCallback(SequenceNumber seq) {
    if (callback_) {
      return callback_->IsVisible(seq);
    }
    return true;
  }

  void ReportCounters();

  bool has_callback() const { return callback_ != nullptr; }

  uint64_t get_tracing_get_id() const { return tracing_get_id_; }

  void push_operand(const Slice& value, Cleanable* value_pinner);

  void set_children_ranks(std::vector<PositionKeyList> children_ranks){
    children_ranks_ = children_ranks;
  }

  const std::vector<PositionKeyList>& get_children_ranks(){
    return children_ranks_;
  }

  void set_last_children_ranks(){
    last_children_ranks_ = children_ranks_;
  }

  const std::vector<PositionKeyList>& get_last_children_ranks(){
    return last_children_ranks_;
  }

  // 判断当前得到的Position是否适用 在以下几种情况下，father预估的position无法用于child
  // 1. l0内部
  // 2. 查询过程跳过了一个level
  // 3. 过期的children_rank 此情况通常发生在l0 compaction到l1，但同时有一个新的l0被flush。新l0的children_ranks信息是旧的
  // bool is_match() {
  //   for(auto i : last_children_ranks_) {
  //     if(i.parent() != last_file_number_) return false;
  //     if(i.child() == file_number_) return true;
  //   }
  //   return false;
  // }

  void set_estimate_position(int estimate_position){
    estimate_position_ = estimate_position;
  }

  int get_estimate_position(){
    return estimate_position_;
  }

  void set_level(int level){
    level_ = level;
  }

  int get_level(){
    return level_;
  }

  void set_last_level(){
    last_level_ = level_;
  }

  int get_last_level(){
    return last_level_;
  }

  void set_file_number(int file_number) {
    file_number_ = file_number;
  }

  int get_file_number() {
    return file_number_;
  }

  void set_last_file_number() {
    last_file_number_ = file_number_;
  }

  int get_last_file_number() {
    return last_file_number_;
  }

  void set_fake_estimate_position(int fake_estimate_position){
    fake_estimate_position_ = fake_estimate_position;
  }

  int get_fake_estimate_position(){
    return fake_estimate_position_;
  }

  void set_estimate_search_table(EstimateSeekTable* estimate_search_table) {
    estimate_seek_table_ = estimate_search_table;
  }

  EstimateSeekTable* get_estimate_search_table() {
    return estimate_seek_table_;
  }

  void set_btree_iter(BtreeIterator* btree_iter) {
    last_btree_iter_ = btree_iter_;
    btree_iter_ = btree_iter;
  }

  BtreeIterator* get_btree_iter() {
    return btree_iter_;
  }

  // 如果开启estimate_seek，本方法会使用统计信息调用EstimateSeek，并返回查询涉及的block；否则，本方法直接调用Seek，返回查询的files
  // 经过本方法后，btree_iter_指向key的位置
  uint32_t EstimateSeekIfNeed(const Slice& key,int btree_levels, bool estimate_seek, int estimate_threshold,SystemClock* clock, Statistics* stats);

  // 
  // void 

  void set_total_entries(int total_entries){
    total_entries_ = total_entries;
  }

  int get_total_entries(){
    return total_entries_;
  }

 private:
  void Merge(const Slice* value);
  bool GetBlobValue(const Slice& blob_index, PinnableSlice* blob_value);

  const Comparator* ucmp_;
  const MergeOperator* merge_operator_;
  // the merge operations encountered;
  Logger* logger_;
  Statistics* statistics_;

  GetState state_;
  Slice user_key_;
  PinnableSlice* pinnable_val_;
  std::string* timestamp_;
  bool* value_found_;  // Is value set correctly? Used by KeyMayExist
  MergeContext* merge_context_;
  SequenceNumber* max_covering_tombstone_seq_;
  SystemClock* clock_;
  // If a key is found, seq_ will be set to the SequenceNumber of most recent
  // write to the key or kMaxSequenceNumber if unknown
  SequenceNumber* seq_;
  std::string* replay_log_;
  // Used to temporarily pin blocks when state_ == GetContext::kMerge
  PinnedIteratorsManager* pinned_iters_mgr_;
  ReadCallback* callback_;
  bool sample_;
  // Value is true if it's called as part of DB Get API and false if it's
  // called as part of DB GetMergeOperands API. When it's false merge operators
  // are never merged.
  bool do_merge_;
  bool* is_blob_index_;
  // Used for block cache tracing only. A tracing get id uniquely identifies a
  // Get or a MultiGet.
  const uint64_t tracing_get_id_;
  BlobFetcher* blob_fetcher_;
  // 设置默认值 表示第一次从get中取得值
  int estimate_position_ = -1;
  std::vector<PositionKeyList> children_ranks_;
  std::vector<PositionKeyList> last_children_ranks_;
  int level_ = 0; // 表示本次查询的level
  int last_level_ = 0; // 表示上次查询的level 使用该参数主要是为了区分本次查询文件是否是level0的最后一次查询
  int file_number_ = -1;
  int last_file_number_ = -1;
  int fake_estimate_position_ = -1; //仅在l0->l0的查询中有效
  int total_entries_ = 0; // 表示本次查询文件的entries个数
  // folly::ConcurrentHashMap<std::pair<uint64_t, uint64_t>, PositionKeyList, pair_hash>* estimate_seek_table_ = nullptr;
  EstimateSeekTable* estimate_seek_table_ = nullptr;
  BtreeIterator* btree_iter_ = nullptr; // 表示本次查询需要用到的iter
  BtreeIterator* last_btree_iter_ = nullptr;
  int last_btree_posiiton_ = -1;

};

// Call this to replay a log and bring the get_context up to date. The replay
// log must have been created by another GetContext object, whose replay log
// must have been set by calling GetContext::SetReplayLog().
void replayGetContextLog(const Slice& replay_log, const Slice& user_key,
                         GetContext* get_context,
                         Cleanable* value_pinner = nullptr);

}  // namespace ROCKSDB_NAMESPACE
