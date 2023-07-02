
#ifndef NVM_DATA_BLOCK_BUILDER_H_
#define NVM_DATA_BLOCK_BUILDER_H_
#include "table/block_based/block_builder.h"


namespace ROCKSDB_NAMESPACE {

// func: 本class类似BlockBuilder(这是在RocksDB中构造DataBlock和IndexBlock的类)，以用来在nvm_rocksdb中构造DataBlock。最大区别在于，我们的DataBlock中没有key的存在。这样，自然也没有前缀key的优化，那么也没有存restart point的必要了。

// note: 为什么不直接继承自BlockBuilder？而是要写一个功能类似的类？主要有以下两个原因考虑：
// 1.(effective c++ 36)最大原因是我想要重写BlockBuilder中的function，但是它提供的function并不是virtual的。这就导致，我在NvmDataBlockBuilder中重写函数之后，
// 实际一个BlockBuilder对象调用的还是BlockBuilder中的函数，而不是我重写的函数。本质上这是因为这是在静态编译期间确定的函数地址。想要改变这种情况，最好的方式就是
// 将BlockBuilder中的函数改为virtual，以动态确定调用函数，但这样对原有代码侵入太大。所以干脆重写一个类似的类好了
// 2.NvmDataBlockBuilder中的函数接口参数以及成员变量和BlockBuilder中有一定区别，比如NvmDataBlockBuilder中的Add没有key，比如构造函数中没有DataBlockIndexType
// 等这就导致两个类之间的"is-a"语义没有那么强烈，用继承也就"师出无名"
class NvmDataBlockBuilder{
   public:
  NvmDataBlockBuilder(const NvmDataBlockBuilder&) = delete;
  void operator=(const NvmDataBlockBuilder&) = delete;

  explicit NvmDataBlockBuilder();//int block_restart_interval,
                        // bool use_delta_encoding = true,
                        // bool use_value_delta_encoding = false);
                        // BlockBasedTableOptions::DataBlockIndexType index_type =
                        //     BlockBasedTableOptions::kDataBlockBinarySearch,
                        // double data_block_hash_table_util_ratio = 0.75);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // Swap the contents in BlockBuilder with buffer, then reset the BlockBuilder.
  void SwapAndReset(std::string& buffer);

  // func: 本函数用来将value添加到内存中，并返回添加该value之前的offset
  uint64_t AddValue(const Slice& value);

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // DO NOT mix with AddWithLastKey() between Resets. For efficiency, use
  // AddWithLastKey() in contexts where previous added key is already known
  // and delta encoding might be used.
//   void Add(const Slice& key, const Slice& value,
//            const Slice* const delta_value = nullptr);

  // A faster version of Add() if the previous key is already known for all
  // Add()s.
  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // REQUIRES: if AddWithLastKey has been called since last Reset(), last_key
  // is the key from most recent AddWithLastKey. (For convenience, last_key
  // is ignored on first call after creation or Reset().)
  // DO NOT mix with Add() between Resets.
//   void AddWithLastKey(const Slice& key, const Slice& value,
//                       const Slice& last_key,
//                       const Slice* const delta_value = nullptr);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  // func: 完成对一个block的写入，RocksDB中设置data block中的restart point等。但是我们的实现中不需要这样，仅需简单把buffer返回即可。
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  inline size_t CurrentSizeEstimate() const {
    return estimate_;
    // return estimate_ + (data_block_hash_index_builder_.Valid()
    //                         ? data_block_hash_index_builder_.EstimateSize()
    //                         : 0);
  }

  // Returns an estimated block size after appending key and value.
  // func: 用于在FlushBlockPolicy中判断加入下一个value后是否需要flush
  size_t EstimateSizeAfterValue(const Slice& value) const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  // inline void AddWithLastKeyImpl(const Slice& key, const Slice& value,
  //                                const Slice& last_key,
  //                                const Slice* const delta_value,
  //                                size_t buffer_size);

  // const int block_restart_interval_;
  // TODO(myabandeh): put it into a separate IndexBlockBuilder
//   const bool use_delta_encoding_;
  // Refer to BlockIter::DecodeCurrentValue for format of delta encoded values
  // const bool use_value_delta_encoding_;

  std::string buffer_;              // Destination buffer
//   std::vector<uint32_t> restarts_;  // Restart points
  size_t estimate_;
//   int counter_;    // Number of entries emitted since restart
  bool finished_;  // Has Finish() been called?
//   std::string last_key_;
//   DataBlockHashIndexBuilder data_block_hash_index_builder_;
#ifndef NDEBUG
  bool add_with_last_key_called_ = false;
#endif
};
}
#endif

