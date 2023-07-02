#pragma once

#include "rocksdb/options.h"
#include "nvm/partition/nvm_partition_reader.h"
#include "nvm/index/btree_iterator.h"
#include "nvm/index/btree_iterator_portion.h"
namespace ROCKSDB_NAMESPACE{

// 本class作为NvmPartition的iterator，最大的作用就是封装一个BtreeIterator
class NvmPartitionIterator : public InternalIterator{
 public:
  NvmPartitionIterator(NvmPartition* nvm_partition, const ReadOptions& read_options,const InternalKeyComparator& icomp):nvm_partition_(nvm_partition) {
    // 无需用到PortionIterator了...因为不管是value compaction还是vertical compaction，都需要所有key的参与，只不过是一部分key不需要real value，一部分key需要real value.
      btree_iter_ = new BtreeIterator(nvm_partition_->nvm_btree_, read_options, icomp);
  };

  // 此处的析构函数应该释放btree_iter，因为NvmPartitionIter是基于BtreeIter的。
  ~NvmPartitionIterator() {
    delete btree_iter_;
  };

  bool Valid() const override{
    return btree_iter_->Valid();
  };
  void Seek(const Slice& target) override{
    return btree_iter_->Seek(target);
  };
  uint32_t GetKeyPosition() const {
    return btree_iter_->GetKeyPosition(0);
  };

  uint32_t GetKeyPosition(int n) const {
    return btree_iter_->GetKeyPosition(n);
  };

  double GetKeyPosition(const uint32_t low_bound, const uint32_t up_bound,uint32_t position = UINT32_MAX) const{
    return btree_iter_->GetKeyPosition(low_bound,up_bound,position);
  }

  uint32_t EstimateSeek(const Slice& target, const double estimate_position, const EstimateSearchMode mode = EstimateSearchMode::BackTrack){
    return btree_iter_->EstimateSeek(target,estimate_position,mode);
  }  
  
  void Next() override{
    btree_iter_->Next();
    // 每次Next()之后，都计算一下当前是否为real_value_ 进而得到一个准确的value(不管是vaddr还是real value)
  };
  void Prev() override{
    btree_iter_->Prev();
  };
  void SeekToFirst() override{
    btree_iter_->SeekToFirst();
  };
  void SeekToLast() override{
    btree_iter_->SeekToLast();
  };
  void SeekForPrev(const Slice& target) override{
    btree_iter_->SeekForPrev(target);
  };
  Status status() const override{
    return btree_iter_->status();
  };
  Slice key() const override{
    return btree_iter_->key();
  };
  Slice user_key() const override{
    return btree_iter_->user_key();
  };
  Slice value() const override{
      assert(false);
      // tables_应该用set
      // WQTODO 4.6
      // 1.value compaction选择的bug 在这里重新写完 然后builder那里也写完 done 这里要重新审视一下value()接口 vertical compaction的时候，针对有merge的key的put也需要返回real value
      // 2.vertical compaction写完 开始的level设置为一个可变参数  done
      // 3.value compaction的选择逻辑那里 把耦合在一起的分开 一个是过期key触发 一个是引用table个数触发 前者为了减少空间放大 后者为了范围查询(and空间放大) gcscore计算done pick compaction  done
      // 4.现在iterator需要传入for_read_ 参数，标识本次iterator是否用来读取。同时需要修改全链路(flush和compaction)的iterator和add和build的相关逻辑。
      // 5.修改value compaction的为gc compaction... 重构整个链路上gc compaction和vertical compaction... 重新审视...
      // 6.把所有的gc compaction vertical compaction和iterator耦合的逻辑，全部放到CompactionIterator里面... 然后删掉原来放在iterator里面的value compaction和vertical compaction done
      // WQTODO 4.9
      // 1.gc compaction加大数据量，测试可不可以部分vaddr部分real value
      // 2.开启vertical compaction进行测试
      // 3.read时获取真实value... 传入参数...
      // if(!real_value_) {
      //   return btree_iter_->value();
      // } else {
      //   Slice value;
      //   ReadOptions read_options;
      //   // 我们读取时不校验checksum
      //   read_options.verify_checksums = false;
      //   // 调用Get2接口，将真实的value存储到value对象中
      //   now_table_->Get2(read_options,value_offset_,nullptr,&value,nullptr,false);    
      //   return value;
      // }
  };

  Slice value(bool vertical_merge_type, const std::set<uint32_t>& tables = std::set<uint32_t>()) const override{
    uint32_t table_number = btree_iter_->value_file_seq();
    // 1.如果不是vertical compaction中涉及到的kTypeMerge 或者 不是gc compaction中涉及到需要重写的table 则返回vaddr(这也包括了tables为空的情况)
    if(!vertical_merge_type && tables.find(table_number) == tables.end()) {
      real_value_ = false;
      return btree_iter_->value();
    }
    // 2. 其它情况，是gc compaction中没有涉及到的table，或者是vertical compaction中涉及的kTypeMerge，返回real value
    else {
        real_value_ = true;
        uint32_t value_offset = btree_iter_->value_offset();
        NvmTable* now_table = nvm_partition_->nvm_tables_.find(table_number)->second;
        Slice value;
        ReadOptions read_options;
        // 我们读取时不校验checksum
        read_options.verify_checksums = false;
        // 调用Get2接口，将真实的value存储到value对象中
        now_table->Get2(read_options,value_offset,nullptr,&value,nullptr,false);    
        return value;
    }
  }

  bool real_value() const override {
    return real_value_;
  }

  uint64_t file_number() const override {
    return nvm_partition_->file_number;
  }

  // 计算本次key对应真实value还是vaddr
  // void compute_value() {
  //   if( for_read_ ) return;
  //   // 1. 如果是index compaction，需要返回vaddr，real_value_为false
  //   // 2. 如果是value compaction，但不是被选中的index file(即table数组为空)，也直接返回vaddr，real_value_为false
  //   if( (!value_compaction_ && !vertical_compaction_) ||(value_compaction_ && tables_.size() == 0)){
  //     real_value_ = false; 
  //   } 
  //   // 3. 如果是value compaction，同时是被选中的index file，则判断当前value是否在被选中的table中
  //   else if (value_compaction_){
  //     table_number_ = btree_iter_->value_file_seq();
  //     // 3.1 value不在table中，本次需要返回vaddr
  //     if(1/*tables_中查不到table_number_*/){//WQTODOIMP
  //       real_value_ = false;
  //     }  
  //     // 3.2 value在table中，需要返回real value，同时计算得到value_offset
  //     else{
  //       real_value_ = true;
  //       value_offset_ = btree_iter_->value_offset();
  //       now_table_ = nvm_partition_->nvm_tables_.find(table_number_)->second;
  //     }
  //   }
  //   // 4. 如果是vertical compaction，那么kTypeMerge一定返回real value，用于CompactionIterator中的合并。而其它类型默认返回vaddr。
  //   else if (vertical_compaction_){
  //     ParsedInternalKey* internal_key;
  //     ParseInternalKey(btree_iter_->key(), internal_key, true);
  //     // 4.1 如果是Merge类型，那么直接返回real value
  //     if( internal_key->type == kTypeMerge) {
  //       real_value_ = true;
  //       table_number_ = btree_iter_->value_file_seq();
  //       value_offset_ = btree_iter_->value_offset();
  //       now_table_ = nvm_partition_->nvm_tables_.find(table_number_)->second;
  //     }
  //     // 否则，返回vaddr
  //     else {
  //       real_value_ = false;
  //     }
  //   }
  // }

  private:
  NvmPartition* nvm_partition_;
  BtreeIterator* btree_iter_;
  // 如果本次iterator是为了read而构造的，则直接返回real value
  bool for_read_ = false; 
  
  std::vector<uint32_t> tables_;
  mutable bool real_value_ = true;
  // // 下面仅在real_value_为true的情况下有效
  // uint32_t table_number_;
  // uint64_t value_offset_;
  // NvmTable* now_table_;
};




}