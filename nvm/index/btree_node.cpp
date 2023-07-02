#include "btree_node.h"

namespace ROCKSDB_NAMESPACE {

node_iterator::node_iterator(void *node_addr,
                             const InternalKeyComparator &icomp)
    : node_address(node_addr), iter(0), icomp_(icomp) {
  void *ptr = key_prefix::get_prefix(node_addr, prefix);
  ptr = key_prefix::get_prefix(ptr, suffix);
  bnode = (btree_node *)ptr;
}

Slice node_iterator::get_key(uint8_t n) const {
  assert(n < bnode->total_kv);

  void *partial_key = nullptr;
  if (bnode->node_type == BtreeNodeLeaf) {
    partial_key =
        (uint8_t *)node_address + (bnode->socket.lnode.key_addr_value[n].key_addr);
  } else {  // if (bnode->node_type == BtreeNodeSub || bnode->node_type ==
            // BtreeNodeRoot)
    partial_key =
        (uint8_t *)node_address + (bnode->socket.inode.key_addr_subnode[n].key_addr);
  }

  return key_prefix::get_prefix_slice(partial_key);
}

Slice node_iterator::get_first_key() const {
  void *partial_key = nullptr;
  if (bnode->node_type == BtreeNodeLeaf) {
    partial_key =
        (uint8_t *)node_address + (bnode->socket.lnode.key_addr_value[0].key_addr);
  } else if (bnode->node_type == BtreeNodeSub || bnode->node_type == BtreeNodeRoot) {
    partial_key =
        (uint8_t *)node_address + (bnode->socket.inode.key_addr_subnode[0].key_addr);
  }

  return key_prefix::get_prefix_slice(partial_key);
}

Slice node_iterator::get_last_key() const {
  void *partial_key = nullptr;
  if (bnode->node_type == BtreeNodeLeaf) {
    partial_key = (uint8_t *)node_address +
                  (bnode->socket.lnode.key_addr_value[bnode->total_kv - 1].key_addr);
  } else if (bnode->node_type == BtreeNodeSub || bnode->node_type == BtreeNodeRoot) {
    partial_key =
        (uint8_t *)node_address +
        (bnode->socket.inode.key_addr_subnode[bnode->total_kv - 1].key_addr);
  }

  return key_prefix::get_prefix_slice(partial_key);
}

int8_t node_iterator::contain_key(const Slice &key) {
  // 先通过前缀可以简单判断时候符合，但是不一定准确
  Slice search_key(key.data(), prefix.length());
  int cmp = UserCompare(search_key, prefix);
  if (cmp < 0) {
    return -1;
  } else if (cmp > 0) {
    return 1;
  }

  // 前缀相同的条件下比较剩余的部分了，注意需要用Internal Key的比较函数
  search_key = Slice(key.data() + prefix.length(), key.size() - prefix.length());
  uint8_t l = 0;
  uint8_t r = bnode->total_kv - 1;
  Slice edge_key_slice = get_key(r);
  std::string edge_key;
  edge_key.append(edge_key_slice.data(), edge_key_slice.size());
  edge_key.append(suffix);
  cmp = Compare(search_key, edge_key);
  if (cmp > 0) {
    return 1;
  }

  // 比较block中的第一个key非常特殊，查询key的sequence number是一个MAX值，
  // 直接通过Compare接口进行比较会在user_key相同的时候判定为小于，所以这里用只比较user_key的办法解决
  // 但是这里需要特别注意，user_key小于当前node中的最小key并不一定意味着这个key不包含在当前的这个node中，
  // 当然这个问题是没有办法完全解决的，目前只能规避在当前的node处于B+tree的最左边时，避免向上进行回溯。
  edge_key_slice = get_key(l);
  edge_key.clear();
  edge_key.append(edge_key_slice.data(), edge_key_slice.size());
  edge_key.append(suffix);
  cmp = UserCompare(ExtractUserKey(edge_key), ExtractUserKey(search_key));
  if (cmp > 0) {
    if (is_level_head_node())
    {
      seek_to_first();
      return 0;
    } else {return -1;}
  }

  // 确定在此node内部之后，直接调用seek函数将iter调整到制定位置
  seek(key);
  return 0;
}

uint32_t node_iterator::get_btree_kv_offset() const {
  assert(bnode->node_type == BtreeNodeLeaf);
  return bnode->socket.lnode.btree_kv_num_offset + iter;
}
uint32_t node_iterator::get_parent_node() const {
  return bnode->parent_node & 0x7FFFFFFF;
}
bool node_iterator::is_level_head_node() const {
  return (bnode->parent_node & 0x80000000) == 0x80000000;
}

void node_iterator::seek(const Slice &key) {
  // 这里有一个特殊情况需要注意一下，查询的key有可能是和当前的block中的所有key的prefix不同
  // 因此要先比较prefix部分：  1. 如果search_key_prefix < prefix 直接返回 iter =
  // 0
  //                        2. 如果search_key_prefix > prefix 这是错误的情况
  // 并且这里不能用internal_comparator，要用user_comparator
  {
    Slice search_key(key.data(), prefix.length());
    int cmp = UserCompare(search_key, prefix);
    assert(cmp <= 0);
    if (cmp < 0) {
      iter = 0;
      return;
    }
  }
  // 在btree node中查找key时，需要去掉前缀进行查找
  Slice search_key(key.data() + prefix.length(), key.size() - prefix.length());
  assert(key.size() >= 8);  // key must be internal key
  // Binary search
  uint8_t l = 0;
  uint8_t r = bnode->total_kv;
  std::string mid_key;
  while (l < r) {
    const uint8_t m = (l + r) >> 1;
    Slice mid_key_slice = get_key(m);
    mid_key.clear();
    mid_key.append(mid_key_slice.data(), mid_key_slice.size());
    mid_key.append(suffix);

    const int cmp = Compare(search_key, mid_key);
    if (cmp < 0)  // search-key < [m]
      r = m;
    else if (cmp > 0)  // search-key > [m]
      l = m + 1;
    else
      l = r = m;
  }
  /* 注意两种特殊情况：
   * 如果search_key小于当前node中的最小值，那么iter最后被设置为0
   * 如果search_key大于当前node中的最大值，那么iter最后被设置为total_kv，这时node_iter为invalid状态
   */
  iter = r;
}

void node_iterator::seek_to_first() { iter = 0; }
void node_iterator::seek_to_last() { iter = bnode->total_kv - 1; }

std::string node_iterator::peek_key() {
  assert(valid());

  void *partial_key = nullptr;
  if (bnode->node_type == BtreeNodeLeaf) {
    partial_key =
        (uint8_t *)node_address + (bnode->socket.lnode.key_addr_value[iter].key_addr);
  } else if (bnode->node_type == BtreeNodeSub || bnode->node_type == BtreeNodeRoot) {
    partial_key = (uint8_t *)node_address +
                  (bnode->socket.inode.key_addr_subnode[iter].key_addr);
  }

  return prefix + key_prefix::get_prefix(partial_key) + suffix;
}

uint32_t node_iterator::peek_subnode() {
  assert(valid());
  return bnode->socket.inode.key_addr_subnode[iter].get_subnode_rank();
}

valueaddr node_iterator::peek_valueaddr() {
  assert(valid());
  return bnode->socket.lnode.key_addr_value[iter].value_pos;
}

Slice node_iterator::peek_value() const {
  assert(valid());
  return Slice((char *)&(bnode->socket.lnode.key_addr_value[iter].value_pos),
               (1 + VALUEADDR_OFFSET_SIZE));
}

}  // namespace ROCKSDB_NAMESPACE