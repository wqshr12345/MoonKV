#include "nvm/table/nvm_data_block_builder.h"

#include <assert.h>
#include <algorithm>
#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "table/block_based/data_block_footer.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

NvmDataBlockBuilder::NvmDataBlockBuilder(
    // int block_restart_interval, bool use_delta_encoding,
    //bool use_value_delta_encoding
    )
    // BlockBasedTableOptions::DataBlockIndexType index_type,
    // double data_block_hash_table_util_ratio)
    : //block_restart_interval_(block_restart_interval),
      // use_delta_encoding_(use_delta_encoding),
      // use_value_delta_encoding_(use_value_delta_encoding),
    //   restarts_(1, 0),  // First restart point is at offset 0
    //   counter_(0),
      finished_(false) {
//   switch (index_type) {
//     case BlockBasedTableOptions::kDataBlockBinarySearch:
//       break;
//     case BlockBasedTableOptions::kDataBlockBinaryAndHash:
//       data_block_hash_index_builder_.Initialize(
//           data_block_hash_table_util_ratio);
//       break;
//     default:
//       assert(0);
//   }
  // assert(block_restart_interval_ >= 1);
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
}

void NvmDataBlockBuilder::Reset() {
  buffer_.clear();
//   restarts_.resize(1);  // First restart point is at offset 0
  // assert(restarts_[0] == 0);
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
//   counter_ = 0;
  finished_ = false;
  // last_key_.clear();
//   if (data_block_hash_index_builder_.Valid()) {
//     data_block_hash_index_builder_.Reset();
//   }
#ifndef NDEBUG
  add_with_last_key_called_ = false;
#endif
}

void NvmDataBlockBuilder::SwapAndReset(std::string& buffer) {
  std::swap(buffer_, buffer);
  Reset();
}

size_t NvmDataBlockBuilder::EstimateSizeAfterValue(const Slice& value) const {
  size_t estimate = CurrentSizeEstimate();
  // Note: this is an imprecise estimate as it accounts for the whole key size
  // instead of non-shared key size.
  // 估计value+value length 所占据的总长度
  estimate += VarintLength(value.size());
  estimate += value.size();
  // estimate += key.size();
  // // In value delta encoding we estimate the value delta size as half the full
  // // value size since only the size field of block handle is encoded.
  // estimate +=
  //     !use_value_delta_encoding_ || (counter_ >= block_restart_interval_)
  //         ? value.size()
  //         : value.size() / 2;

  // if (counter_ >= block_restart_interval_) {
  //   estimate += sizeof(uint32_t);  // a new restart entry.
  // }

  // estimate += sizeof(int32_t);  // varint for shared prefix length.
  // // Note: this is an imprecise estimate as we will have to encoded size, one
  // // for shared key and one for non-shared key.
  // estimate += VarintLength(key.size());  // varint for key length.
  // if (!use_value_delta_encoding_ || (counter_ >= block_restart_interval_)) {
  //   estimate += VarintLength(value.size());  // varint for value length.
  // }

  return estimate;
}

Slice NvmDataBlockBuilder::Finish() {
  // Append restart array
  // for (size_t i = 0; i < restarts_.size(); i++) {
  //   PutFixed32(&buffer_, restarts_[i]);
  // }

  // uint32_t num_restarts = static_cast<uint32_t>(restarts_.size());
  // BlockBasedTableOptions::DataBlockIndexType index_type =
  //     BlockBasedTableOptions::kDataBlockBinarySearch;
//   if (data_block_hash_index_builder_.Valid() &&
//       CurrentSizeEstimate() <= kMaxBlockSizeSupportedByHashIndex) {
//     data_block_hash_index_builder_.Finish(buffer_);
//     index_type = BlockBasedTableOptions::kDataBlockBinaryAndHash;
//   }

  // footer is a packed format of data_block_index_type and num_restarts
  // uint32_t block_footer = PackIndexTypeAndNumRestarts(index_type, num_restarts);

  // PutFixed32(&buffer_, block_footer);
  finished_ = true;
  return Slice(buffer_);
}

uint64_t NvmDataBlockBuilder::AddValue(const Slice& value){
  // 实现起来好像就这么简单...?直接把value加入buffer就好了。也合乎常理，毕竟share key、unshared key、key len、value len都没有了。
  assert(!finished_);
  const size_t buffer_size = buffer_.size();
  PutVarint32(&buffer_, static_cast<uint32_t>(value.size()));
  buffer_.append(value.data(), value.size());
  uint64_t offset = buffer_.size() - buffer_size;
  estimate_ += offset;
  return offset;
}

// Nouse: 这是原有为index block设计的方法，在这里无用
// void NvmDataBlockBuilder::Add(const Slice& key, const Slice& value,
//                        const Slice* const delta_value) {
//   // Ensure no unsafe mixing of Add and AddWithLastKey
//   assert(!add_with_last_key_called_);

//   AddWithLastKeyImpl(key, value, last_key_, delta_value, buffer_.size());
//   if (use_delta_encoding_) {
//     // Update state
//     // We used to just copy the changed data, but it appears to be
//     // faster to just copy the whole thing.
//     last_key_.assign(key.data(), key.size());
//   }
// }

// Nouse: 这是原有为data block设计的方法，
// void NvmDataBlockBuilder::AddWithLastKey(const Slice& key, const Slice& value,
//                                   const Slice& last_key_param,
//                                   const Slice* const delta_value) {
//   // Ensure no unsafe mixing of Add and AddWithLastKey
//   assert(last_key_.empty());
// #ifndef NDEBUG
//   add_with_last_key_called_ = false;
// #endif

//   // Here we make sure to use an empty `last_key` on first call after creation
//   // or Reset. This is more convenient for the caller and we can be more
//   // clever inside NvmDataBlockBuilder. On this hot code path, we want to avoid
//   // conditional jumps like `buffer_.empty() ? ... : ...` so we can use a
//   // fast min operation instead, with an assertion to be sure our logic is
//   // sound.
//   size_t buffer_size = buffer_.size();
//   size_t last_key_size = last_key_param.size();
//   assert(buffer_size == 0 || buffer_size >= last_key_size);

//   Slice last_key(last_key_param.data(), std::min(buffer_size, last_key_size));

//   AddWithLastKeyImpl(key, value, last_key, delta_value, buffer_size);
// }

// inline void NvmDataBlockBuilder::AddWithLastKeyImpl(const Slice& key,
//                                              const Slice& value,
//                                              const Slice& last_key,
//                                              const Slice* const delta_value,
//                                              size_t buffer_size) {
//   assert(!finished_);
// //   assert(counter_ <= block_restart_interval_);
//   assert(!use_value_delta_encoding_ || delta_value);
//   size_t shared = 0;  // number of bytes shared with prev key
//   if (counter_ >= block_restart_interval_) {
//     // Restart compression
//     restarts_.push_back(static_cast<uint32_t>(buffer_size));
//     estimate_ += sizeof(uint32_t);
//     counter_ = 0;
//   } else if (use_delta_encoding_) {
//     // See how much sharing to do with previous string
//     shared = key.difference_offset(last_key);
//   }

//   const size_t non_shared = key.size() - shared;

//   if (use_value_delta_encoding_) {
//     // Add "<shared><non_shared>" to buffer_
//     PutVarint32Varint32(&buffer_, static_cast<uint32_t>(shared),
//                         static_cast<uint32_t>(non_shared));
//   } else {
//     // Add "<shared><non_shared><value_size>" to buffer_
//     PutVarint32Varint32Varint32(&buffer_, static_cast<uint32_t>(shared),
//                                 static_cast<uint32_t>(non_shared),
//                                 static_cast<uint32_t>(value.size()));
//   }

//   // Add string delta to buffer_ followed by value
//   buffer_.append(key.data() + shared, non_shared);
//   // Use value delta encoding only when the key has shared bytes. This would
//   // simplify the decoding, where it can figure which decoding to use simply by
//   // looking at the shared bytes size.
//   if (shared != 0 && use_value_delta_encoding_) {
//     buffer_.append(delta_value->data(), delta_value->size());
//   } else {
//     buffer_.append(value.data(), value.size());
//   }

//   if (data_block_hash_index_builder_.Valid()) {
//     data_block_hash_index_builder_.Add(ExtractUserKey(key),
//                                        restarts_.size() - 1);
//   }

//   counter_++;
//   estimate_ += buffer_.size() - buffer_size;
// }

}  // namespace ROCKSDB_NAMESPACE
