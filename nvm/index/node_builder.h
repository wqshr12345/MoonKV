#pragma once

#include "btree_node.h"
#include "nvm_btree.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class node_builder {
 private:
  Slice prefix;  // 用于去掉user_key前面的相同部分
  Slice suffix;  // 用于去掉sequence
                 // number的高位（uint64使用小端序存储，高位被存在后面）
  uint8_t kv_num;
  uint16_t temp_size;
  BtreeNodeType type_;

  void UpdatePrefix(Slice &new_key) {
    size_t min_length = std::min(prefix.size(), new_key.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           (prefix[diff_index] == new_key[diff_index])) {
      diff_index++;
    }
    if (prefix.size() > diff_index) {
      temp_size = temp_size + (prefix.size() - diff_index) * (kv_num - 1);
      prefix = Slice(prefix.data(), diff_index);
    }
  }

  // 更新后缀特有的，需要从最后1byte开始比较
  void UpdateSuffix(Slice &new_key) {
    // 最多去掉sequence number的7byte
    int min_length = (int)suffix.size();
    new_key = Slice(new_key.data() + new_key.size() - min_length, min_length);
    int diff_index = min_length - 1;
    while ((diff_index >= 0) && (suffix[diff_index] == new_key[diff_index])) {
      diff_index--;
    }
    if (diff_index >= 0) {
      temp_size = temp_size + (diff_index + 1) * (kv_num - 1);
      suffix = Slice(suffix.data() + (diff_index + 1),
                     suffix.size() - diff_index - 1);
    }
  }

 public:
  explicit node_builder(BtreeNodeType type)
      : prefix(nullptr),
        suffix(nullptr),
        kv_num(0),
        temp_size(0),
        type_(type) {}

  ~node_builder() = default;

  bool AddKey(Slice internal_key) {
    Slice user_key = ExtractUserKey(internal_key);
    Slice sequence_key = Slice(
        internal_key.data() + internal_key.size() - kNumInternalBytes + 1, 7);
    if (prefix.data() == nullptr) {
      prefix = user_key;
      suffix = sequence_key;
      temp_size = 1 + prefix.size() + 1 + suffix.size() + 2
                  + (type_ == BtreeNodeType::BtreeNodeLeaf ? 8 : 4);
    }
    Slice last_prefix(prefix);
    Slice last_suffix(suffix);
    UpdatePrefix(user_key);
    UpdateSuffix(sequence_key);

    temp_size =
        temp_size + 1 +
        (user_key.size() - prefix.size() + kNumInternalBytes - suffix.size()) +
        (type_ == BtreeNodeType::BtreeNodeLeaf ? (2 + VALUEADDR_OFFSET_SIZE)
                                               : 4);
    // 这里需要非常小心，如果新加入的Key会超出一个block，那么这个时候是需要将prefix更新到原来的状态的
    if (temp_size > NVM_BLOCK_SIZE) {
      prefix = last_prefix;
      suffix = last_suffix;
      return false;
    }
    kv_num++;  // 只有在确认可以正确添加到一个block之后，再对key数量进行增加
    return true;
  }

  uint8_t GetKeyNum() const { return kv_num; }

  Slice GetPrefix() { return prefix; }

  Slice GetSuffix() { return suffix; }

  void Clear(BtreeNodeType type) {
    prefix = Slice(nullptr, 8);
    kv_num = 0;
    temp_size = 0;
    type_ = type;
  }
};

class node_writer {
  typedef std::list<std::pair<std::string, std::string>>::iterator
      data_iterator;
  typedef std::vector<std::pair<Slice, uint32_t>>::iterator index_iterator;

 private:
  Slice prefix_;
  Slice suffix_;
  data_iterator *iter_data;
  index_iterator *iter_index;
  uint16_t kv_num_;
  BtreeNodeType type_;
  uint32_t parent_node_;
  uint32_t btree_kv_num_offset_;
  uint8_t *buf_;
  uint8_t *key_offset;  //真正开始写入key的位置，可以通过计算得到
  uint32_t base_block_offset;  //写入index block的时候，需要的基础偏移

  inline Slice remove_prefix_suffix(std::string &key) {
    return {key.data() + prefix_.size(),
            key.size() - prefix_.size() - suffix_.size()};
  }
  inline Slice remove_prefix_suffix(Slice &key) {
    return {key.data() + prefix_.size(),
            key.size() - prefix_.size() - suffix_.size()};
  }

 public:
  node_writer() = default;

  node_writer(Slice prefix, Slice suffix, data_iterator *iter, uint16_t kv_num,
              BtreeNodeType type, uint32_t parent_node,
              uint32_t btree_kv_num_offset, uint8_t *buf)
      : prefix_(prefix),
        suffix_(suffix),
        iter_data(iter),
        iter_index(nullptr),
        kv_num_(kv_num),
        type_(type),
        parent_node_(parent_node),
        btree_kv_num_offset_(btree_kv_num_offset),
        buf_(buf),
        base_block_offset(0) {
    key_offset =
        buf_ + 1 + prefix_.size() + 1 + suffix_.size() + 2 +
        (type_ == BtreeNodeLeaf ? 8 : 4) +
        kv_num_ * (type_ == BtreeNodeLeaf ? (2 + VALUEADDR_OFFSET_SIZE) : 4);
  }

  node_writer(Slice prefix, Slice suffix, index_iterator *iter, uint16_t kv_num,
              BtreeNodeType type, uint32_t parent_node, uint8_t *buf, uint32_t base_offset)
      : prefix_(prefix),
        suffix_(suffix),
        iter_data(nullptr),
        iter_index(iter),
        kv_num_(kv_num),
        type_(type),
        parent_node_(parent_node),
        buf_(buf),
        base_block_offset(base_offset) {
    key_offset =
        buf + 1 + prefix_.size() + 1 + suffix_.size() + 2 +
        (type_ == BtreeNodeLeaf ? 8 : 4) +
        kv_num_ * (type_ == BtreeNodeLeaf ? (2 + VALUEADDR_OFFSET_SIZE) : 4);
  }

  ~node_writer() = default;

  void Build() {
    // write prefix suffix
    uint8_t *ptr = key_prefix::write_prefix(buf_, prefix_);
    ptr = key_prefix::write_prefix(ptr, suffix_);
    // type total_key
    *(ptr++) = type_;
    *(ptr++) = kv_num_;
    *(uint32_t *)(ptr) = parent_node_;
    ptr += sizeof(uint32_t);
    if (type_ == BtreeNodeLeaf) {
      *(uint32_t *)(ptr) = btree_kv_num_offset_;
      ptr += sizeof(uint32_t);
    }
    // 在offset位置写入key，并同时写偏移地址
    for (int i = 0; i < kv_num_; ++i) {
      *(ptr++) = (key_offset - buf_);
      if (type_ == BtreeNodeLeaf) {
        memcpy(ptr, (**iter_data).second.data(), (**iter_data).second.size());
        ptr = ptr + (**iter_data).second.size();
        key_offset = key_prefix::write_prefix(
            key_offset, remove_prefix_suffix((**iter_data).first));
        (*iter_data)++;
      } else {  // BtreeNodeSub or BtreeNodeRoot
        ptr = keyaddr_and_subnode::write_subnode_rank(
            ptr, (**iter_index).second + base_block_offset);
        key_offset = key_prefix::write_prefix(
            key_offset, remove_prefix_suffix((**iter_index).first));
        (*iter_index)++;
      }
    }
    assert(key_offset <= buf_ + NVM_BLOCK_SIZE);
  }

  uint8_t *BlockAddr() { return buf_; }

  uint8_t *BlankAddr() { return key_offset; }
};

}  // namespace ROCKSDB_NAMESPACE