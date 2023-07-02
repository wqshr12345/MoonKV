#pragma once

#include "node_builder.h"
#include "nvm_btree.h"
#include "rocksdb/slice.h"
#include "table/internal_iterator.h"
#include "table/table_builder.h"

namespace ROCKSDB_NAMESPACE {

class BtreeBuilder {
 public:
  /*
   * 构建Btree时，需要传入其包含的TableFile的信息：
   *    1. TableFile的ID uint32_t
   *    2. TableFile的文件总size
   * 这些信息需要存放到table_information_collect结构体中，其中不需要的字段空着就行
   */
  BtreeBuilder(uint64_t file_seq, const TableBuilderOptions& table_builder_options,
               std::vector<table_information_collect> &sub_run)
      : iter_(nullptr),
        file_seq(file_seq),
        nrun(0),
        nvm_dir(table_builder_options.ioptions.db_nvm_dir),
        builder(BtreeNodeLeaf),
        block_num(0),
        last_key("", 8),
        keys_num_is_less_than_one_nvm_block(false) {}
  BtreeBuilder(InternalIterator *iter, uint64_t file_seq,
               const TableBuilderOptions& table_builder_options,
               std::vector<table_information_collect> &sub_run)
      : iter_(iter),
        file_seq(file_seq),
        nrun(0),
        nvm_dir(table_builder_options.ioptions.db_nvm_dir),
        builder(BtreeNodeLeaf),
        block_num(0),
        last_key("", 8),
        keys_num_is_less_than_one_nvm_block(false) {}

  ~BtreeBuilder() { delete buf; }

  void Add(Slice key, uint32_t table, uint64_t value_offset);
  void Add(Slice key, Slice table_offset);
  void Run();
  void Finish();
  void Format();
  void Flush();
  // 返回当前构造的总kv个数
  uint64_t NumEntries();
  uint64_t FileSize() { return file_size_; }
  // 获取B+tree Index中包含的所有Table文件对应的有效key的数量
  std::map<uint32_t, uint32_t> TableValidKeyNumMap();
  /*
   * 用于在BtreeBuilder外部获取sub_run_inform，然后添加TableFileSize数据
   * 需要在Add结束之后，Format开始之前调用
   */
  std::vector<table_information_collect> *GetInternalTableInform() {
    return &sub_run_inform;
  }

 private:
  void add_internal(Slice key, Slice table_offset);

 private:
  InternalIterator *iter_;
  uint64_t file_seq;

  uint32_t nrun;
  /*
   * 当前B+tree所用到的Table是根据Add操作自动添加的
   * 所以sub_run_inform需要在构建的过程中自动更新
   * 并在Add阶段完成之后，将sub_run_inform返回调用者，添加TableSize信息
   */
  std::vector<table_information_collect> sub_run_inform;
  std::string nvm_dir;

  uint8_t *buf = nullptr;
  uint8_t *offset = nullptr;  //当前添加地址相对于buf的偏移
  uint16_t level =
      0;  //经过run函数之后，最后的结果是包括leaf_node在内的Btree层数
  uint32_t already_writen_block_num =
      0;  //用于记录已经写入的block的数量，这个的作用是在写index_node的时候可以正确计算offset
  uint32_t file_size_ = 0;

  std::list<std::pair<std::string, std::string>> kv_paris;
  std::vector<std::vector<std::pair<Slice, uint32_t>>> index_key;
  std::vector<std::pair<std::pair<Slice, Slice>, uint8_t>>
      block_kv_num;  // first.first是这个leaf_node的prefix，first.second是这个leaf_node的suffix，second是这个leaf_node中包含的key数量
  std::vector<std::vector<std::pair<std::pair<Slice, Slice>, uint8_t>>>
      index_block_kv_num;
  // 拆出来的变量
  node_builder builder;
  uint32_t block_num;
  Slice last_key;
  
  bool keys_num_is_less_than_one_nvm_block;

  // 用于根据file_number查找其在sub_run_sequence中的位置，在index中存的是在数组中的位置
  std::map<uint32_t, uint8_t> sub_run_sequence_map;

  uint8_t get_index_form_sub_run_map(uint32_t table_file_number) {
    auto it = sub_run_sequence_map.find(table_file_number);
    if (it != sub_run_sequence_map.end()) { return it->second; }
    else {
      sub_run_inform.push_back({table_file_number, 0, 0});
      sub_run_sequence_map.insert(
          std::pair<uint32_t, uint8_t>(table_file_number, nrun));
      return nrun++;
    }
  }

  static Slice FindPrefix(const Slice &start, const Slice &end) {
    size_t min_length = std::min(start.size(), end.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           (start[diff_index] == end[diff_index])) {
      diff_index++;
    }
    return {start.data(), diff_index};
  }
};

}  // namespace ROCKSDB_NAMESPACE