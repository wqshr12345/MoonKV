#pragma once

#include <string>

#include "db/dbformat.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

enum BtreeNodeType {
  BtreeNodeLeaf = 0x0,
  BtreeNodeSub = 0x1,
  BtreeNodeRoot = 0x2
};

struct keyaddr_and_subnode {
  uint8_t key_addr;
  uint8_t subnode_in_btree[3];  // it's just the number of 256Byte subnode in
                                // btree file, subnode_in_btree * 256byte is the
                                // actual address in btree file.

  uint32_t get_subnode_rank() {
    return subnode_in_btree[2] << 16 | subnode_in_btree[1] << 8 |
           subnode_in_btree[0];
  }

  static uint8_t *write_subnode_rank(uint8_t *ptr, uint32_t num) {
    ptr[0] = (num & 0x000000FF) >> 0;
    ptr[1] = (num & 0x0000FF00) >> 8;
    ptr[2] = (num & 0x00FF0000) >> 16;
    return ptr + 3;
  }
};

const uint8_t VALUEADDR_OFFSET_SIZE =
    4;  // valueaddr中的value_offset单位占用的字节数，这个可以变动
struct valueaddr {
  uint8_t run;  // which run does the kv comes from.
  uint8_t
      value_offset[VALUEADDR_OFFSET_SIZE];  // the offset of the value in run

  uint64_t get_value_offset() {
    uint64_t res = 0;
    for (int i = 0; i < VALUEADDR_OFFSET_SIZE; ++i) {
      res |= value_offset[i] << (8 * i);
    }
    return res;
  }

  valueaddr(uint8_t run, uint64_t offset) : run(run) {
    for (int i = 0; i < VALUEADDR_OFFSET_SIZE; ++i) {
      value_offset[i] = (offset >> (8 * i)) & 0xFFl;
    }
  }

  std::string to_string() {
    uint8_t res[VALUEADDR_OFFSET_SIZE + 1];
    res[0] = run;
    for (int i = 0; i < VALUEADDR_OFFSET_SIZE; ++i) {
      res[i + 1] = value_offset[i];
    }
    return {(char *)res, VALUEADDR_OFFSET_SIZE + 1};
  }

  static uint8_t get_run_from_str(std::string str) {
    auto *ptr = (uint8_t *)str.data();
    return *ptr;
  }

  static uint8_t get_run_from_slice(Slice str) {
    auto *ptr = (uint8_t *)str.data();
    return *ptr;
  }

  static void set_run_in_slice(Slice str, uint8_t run) {
    auto *ptr = (uint8_t *)str.data();
    *ptr = run;
  }
};

struct keyaddr_and_valueaddr {
  uint8_t key_addr;
  valueaddr value_pos;
};

struct key_prefix {
  uint8_t key_len;
  char key[0];

  static std::string get_prefix(void *addr) {
    size_t len = ((key_prefix *)addr)->key_len;
    return std::string(((key_prefix *)addr)->key, len);
  }

  static void *get_prefix(void *addr, std::string &des) {
    size_t len = ((key_prefix *)addr)->key_len;
    des = std::string(((key_prefix *)addr)->key, len);
    return (uint8_t *)addr + len + 1;
  }

  static Slice get_prefix_slice(void *addr) {
    size_t len = ((key_prefix *)addr)->key_len;
    return Slice(((key_prefix *)addr)->key, len);
  }

  static uint16_t size(void *addr) {
    size_t len = ((key_prefix *)addr)->key_len;
    return len + 1;
  }

  static uint8_t *write_prefix(uint8_t *buf, Slice key) {
    *buf = key.size();
    buf++;
    memcpy(buf, key.data(), key.size());
    return buf + key.size();
  }
};

struct internal_btree_node {
  keyaddr_and_subnode key_addr_subnode[0];
}__attribute__ ((packed));

struct leaf_btree_node {
  //所有kv从1开始编号到btree_total_kv，kv_num_offset是前一个leaf的最后一个kv的编号
  uint32_t btree_kv_num_offset;
  keyaddr_and_valueaddr key_addr_value[0];
}__attribute__ ((packed));

struct btree_node {
  uint8_t node_type;
  uint8_t total_kv;
  uint32_t parent_node;
  union Socket {
    internal_btree_node inode;
    leaf_btree_node lnode;
  } socket;
}__attribute__ ((packed));

class node_iterator {
 private:
  void *node_address;
  btree_node *bnode;
  std::string prefix;
  std::string suffix;

  int8_t iter;

  const InternalKeyComparator &icomp_;

  inline int Compare(const Slice &a, const Slice &b) const {
    return icomp_.Compare(a, b);
  }
  inline int UserCompare(const Slice &a, const Slice &b) const {
    return icomp_.user_comparator()->Compare(a, b);
  }

 public:
  explicit node_iterator(void *node_addr, const InternalKeyComparator &icomp);
  ~node_iterator() = default;

  void next() { iter++; }

  void prev() { iter--; }

  bool valid() const { return 0 <= iter && iter < bnode->total_kv; }

  BtreeNodeType node_type() {
    return static_cast<BtreeNodeType>(bnode->node_type);
  }

  /*
   * 为Elastic Search设计的函数，用于检查当前的node是否包含目标key
   * 1.如果包含则直接seek到指定位置，并返回0；
   * 2.如果不包含：seek_key小于最小key则返回-1，seek_key大于最大key则返回1；
   */
  int8_t contain_key(const Slice &key);
  uint32_t get_btree_kv_offset() const;
  uint32_t get_parent_node() const;
  bool is_level_head_node() const;

  void seek(const Slice &key);

  void seek_to_first();
  void seek_to_last();

  std::string peek_key();

  uint32_t peek_subnode();

  valueaddr peek_valueaddr();
  Slice peek_value() const;

  /*
   * the three get_fun return key without prefix and suffix
   */
  Slice get_key(uint8_t n) const;
  Slice get_first_key() const;
  Slice get_last_key() const;

  Slice get_prefix() { return prefix; }
  uint8_t node_total_kv() { return bnode->total_kv; }
};

}  // namespace ROCKSDB_NAMESPACE
