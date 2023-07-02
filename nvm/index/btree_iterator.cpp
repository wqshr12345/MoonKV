#include "btree_iterator.h"

#include "btree_node.h"
#include "nvm/index/nvm_prefetch.h"

namespace ROCKSDB_NAMESPACE {

bool BtreeIterator::Valid() const {
  return node_iter.valid() ||
         (0 <= node_ptr && node_ptr < (int32_t)btree->leaf_node_num);
}

void BtreeIterator::Seek(const Slice &target) {
  /*
   * 这里需要再考虑一下，在函数入口处是否需要判断一下target与first_key和last_key的大小
   */
  void *node_addr = btree->index_off;
  uint8_t search_levels = 0;
  while (true) {
    /*
     * 添加CPU的软件预取功能，__builtin_prefetch
     * 目前只在B+tree的搜索过程中使用软件的预取
     * 通过
     */
#ifdef NVM_PREFETCH
    prefetch_range(node_addr, 256);
#endif
    search_levels++;
    new (&node_iter) node_iterator(node_addr, icomp_);
    node_iter.seek(target);
    if (node_iter.valid()) {
      if (node_iter.node_type() == BtreeNodeType::BtreeNodeLeaf) {
        // 已经查到叶子节点，可以退出了
        key_buf_.SetInternalKey(node_iter.peek_key());
        value_buf_.SetValue(node_iter.peek_value());
        break;
      } else {
        // 根节点或者子节点都是继续往下查找
        node_ptr = (int32_t)node_iter.peek_subnode();
        node_addr = btree->get_leaf_node_addr(node_ptr);
      }
    } else {
      /*
       * 在一个btree_node里面查找之后，iter是invalid的情况：
       *      1.
       * 如果当前是根节点，这里一定已经出错了，在根节点中查找返回的是invalid说明target超出了last_key
       *      2. 如果是叶子节点，出现invalid的情况也是错误的
       *      3. 如果是子节点，出现invalid的情况也是错误的
       */
      if (node_iter.node_type() == BtreeNodeType::BtreeNodeRoot) {
        assert(Compare(target, btree->last_key) > 0);
        node_ptr =
            (int32_t)(btree->leaf_node_num);  // make BtreeIterator invalid
      } else if (node_iter.node_type() == BtreeNodeType::BtreeNodeLeaf) {
        assert(Compare(target, node_iter.get_last_key()) > 0);
        node_ptr =
            (int32_t)(btree->leaf_node_num);  // make BtreeIterator invalid
      } else if (node_iter.node_type() == BtreeNodeType::BtreeNodeSub) {
        assert(Compare(target, node_iter.get_last_key()) > 0);
        node_ptr =
            (int32_t)(btree->leaf_node_num);  // make BtreeIterator invalid
      }
      return;
    }
  }
  assert(search_levels == btree->levels);
}

void BtreeIterator::Next() {
  node_iter.next();
  if (!node_iter.valid()) {
    if (++node_ptr >= (int32_t)btree->leaf_node_num) return;

    new (&node_iter) node_iterator(btree->get_leaf_node_addr(node_ptr), icomp_);
  }
  key_buf_.SetInternalKey(node_iter.peek_key());
  value_buf_.SetValue(node_iter.peek_value());
}

void BtreeIterator::Prev() {
  node_iter.prev();
  if (!node_iter.valid()) {
    if (--node_ptr < 0) return;

    new (&node_iter) node_iterator(btree->get_leaf_node_addr(node_ptr), icomp_);
    node_iter.seek_to_last();
  }
  key_buf_.SetInternalKey(node_iter.peek_key());
  value_buf_.SetValue(node_iter.peek_value());
}

Slice BtreeIterator::key() const { return key_buf_.GetInternalKey(); }

Slice BtreeIterator::user_key() const { return key_buf_.GetUserKey(); }

Slice BtreeIterator::value() const { return value_buf_.GetValue(); }

Slice BtreeIterator::native_value() const { return node_iter.peek_value(); }

uint32_t BtreeIterator::value_file_seq() const {
  return value_buf_.GetFileSeq();
}
uint64_t BtreeIterator::value_offset() const {
  return value_buf_.GetValueOffset();
}

uint32_t BtreeIterator::GetKeyPosition(int n) const {
  assert(n <= btree->levels);
  /*
   * 这里分成两种情况：
   *  n = 0 -> 直接获得iter所在的位置的key的position
   *  n = 1 -> 获得iter所在的leaf node的位置
   *  n >= 2 -> 获得父节点的node位置
   */
  switch (n) {
    case 0:
      return node_iter.get_btree_kv_offset();
    case 1:
      return GetNodePtr();
    case 2:
      return node_iter.get_parent_node();
    default:
      auto temp_iter = node_iter;
      for (int i = 2; i < n; ++i) {
        auto temp_addr = btree->get_leaf_node_addr(temp_iter.get_parent_node());
        new (&temp_iter) node_iterator(temp_addr, icomp_);
      }
      return temp_iter.get_parent_node();
  }
}

double BtreeIterator::GetKeyPosition(const uint32_t low_bound,
                                     const uint32_t up_bound, uint32_t position = UINT32_MAX) const {
  if(position == UINT32_MAX) position = node_iter.get_btree_kv_offset();
  assert(position <= up_bound);
  return (double)(position - low_bound) /
         (double)(up_bound - low_bound);
}

int32_t BtreeIterator::GetNodePtr() const { return node_ptr; }
uint32_t BtreeIterator::GetLevels() const {
  return btree->levels;
}

uint32_t BtreeIterator::EstimateSeek(const Slice &target,
                                     const int estimate_block,
                                     const EstimateSearchMode mode) {
  /*
   * Estimate Search的核心逻辑过程
   * 1. 首先根据estimate_position直接推算leaf_node进行第一次查找
   * 2. 如果没有找到进入parent_node进行查找
   */
  if(estimate_block < 0) {
    Seek(target);
    return btree->levels;
  }
  void *node_addr = nullptr;
  uint32_t search_blocks = 0;
  uint8_t mark_last_seek_skip = 0;
  int32_t last_seek_node = 0;
  switch (mode) {
      /* case BackTrackBeginLevel3:
       * 这一特殊的查询模式企图从Higher Level的倒数第三层开始查询
       * 这样的好处来源于经过两成的缩小，在三层的key数量已经小于Lower Level的key数量了
       * 根据实验的经验，在两边的key数量接近时，预测结果比较不稳定，总是出现错位的情况
       * 所以考虑用大数据量去推测小数据量，让命中的可能性更高
       * 这里暂定从倒数第三层开始预测，其实和原来的直接预测leaf node的方法只差别在开始查询的位置
       *
       * 首先找到level3 block的node_ptr
       * 然后再计算预测的位置
       */
    case BackTrack:
      // 回溯查询
      /*
       * 当查询的key处于block之间的间隙时，可能出现在间隙处反复横跳的情况
       * 所以设计mark，专门检测这种特殊情况，可以提前跳出这种循环横跳的情况
       * mark_last_seek_skip:
       *    0: 正常情况，上一次seek之后没有出现水平跳转的情况
       *    1: 上一次是像左侧跳转了，如果这次向右跳转需要提前结束
       *    2: 上一次是像右侧跳转了，如果这次向左跳转需要提前结束
       */
      node_ptr = estimate_block;
      while (search_blocks < btree->levels) {
        node_addr = btree->get_leaf_node_addr(node_ptr);
#ifdef NVM_PREFETCH
        prefetch_range(node_addr, 256);
#endif
        search_blocks++;
        new (&node_iter) node_iterator(node_addr, icomp_);
        int8_t contain = node_iter.contain_key(target);
        if (contain == 0) break;
        else if (contain < 0){
          if (mark_last_seek_skip == 2) {
            node_ptr = last_seek_node + 1;
            node_addr = btree->get_leaf_node_addr(node_ptr);
            search_blocks++;
            new (&node_iter) node_iterator(node_addr, icomp_);
            node_iter.seek(target);
            break;
          }
          /* search_key小于当前这个node的最小值，并且当前这个node不是head node
           * 这里应当想判断同层的前一个node是不是与自己是相同的parent node
           *  1. 如果是->则正常的查询parent node
           *  2. 如果不是->取前一个parent node执行下一次查询
           */
          assert(!node_iter.is_level_head_node());
          auto temp_addr = btree->get_leaf_node_addr(node_ptr-1);
          auto temp_iter = node_iterator(temp_addr, icomp_);
          if (node_iter.get_parent_node() == temp_iter.get_parent_node()) {
            mark_last_seek_skip = 0;
            last_seek_node = node_ptr;
            node_ptr = (int32_t)node_iter.get_parent_node();
          } else {
            mark_last_seek_skip = 1;
            last_seek_node = node_ptr;
            node_ptr = (int32_t)temp_iter.get_parent_node();
          }
        } else if (contain > 0) {
          if (mark_last_seek_skip == 1) {
            node_ptr = last_seek_node;
            node_addr = btree->get_leaf_node_addr(node_ptr);
            new (&node_iter) node_iterator(node_addr, icomp_);
            node_iter.seek(target);
            break;
          }
          /* 这种情况刚好和 contain < 0 是倒过来的，不同点是B+tree是以后边界来构建的，】
           * 所以不存在查询的key值大于某层的最后一个node的最大key值的情况
           */
          auto temp_addr = btree->get_leaf_node_addr(node_ptr+1);
          auto temp_iter = node_iterator(temp_addr, icomp_);
          if (node_iter.get_parent_node() == temp_iter.get_parent_node()) {
            mark_last_seek_skip = 0;
            last_seek_node = node_ptr;
            node_ptr = (int32_t)node_iter.get_parent_node();
          } else {
            mark_last_seek_skip = 2;
            last_seek_node = node_ptr;
            node_ptr = (int32_t)temp_iter.get_parent_node();
          }
        }
      }
      // 找到包含target_key的block之后可能不是leaf_node所以需要在次查询
      if (node_iter.node_type() != BtreeNodeLeaf) {
        node_ptr = (int32_t)node_iter.peek_subnode();
        node_addr = btree->get_leaf_node_addr(node_ptr);
        while (true) {
#ifdef NVM_PREFETCH
          prefetch_range(node_addr, 256);
#endif
          search_blocks++;
          new (&node_iter) node_iterator(node_addr, icomp_);
          node_iter.seek(target);
          if (node_iter.valid()) {
            if (node_iter.node_type() == BtreeNodeType::BtreeNodeLeaf) {
              // 已经查到叶子节点，可以退出了
              key_buf_.SetInternalKey(node_iter.peek_key());
              value_buf_.SetValue(node_iter.peek_value());
              break;
            } else {
              // 根节点或者子节点都是继续往下查找
              node_ptr = (int32_t)node_iter.peek_subnode();
              node_addr = btree->get_leaf_node_addr(node_ptr);
            }
          } else {
            /*
             * 在一个btree_node里面查找之后，iter是invalid的情况：
             *      1. 如果当前是根节点，这里一定已经出错了，在根节点中查找返回的是invalid说明target超出了last_key
             *      2. 如果是叶子节点，出现invalid的情况也是错误的
             *      3. 如果是子节点，出现invalid的情况也是错误的
             */
            if (node_iter.node_type() == BtreeNodeType::BtreeNodeRoot) {
              assert(Compare(target, btree->last_key) > 0);
              node_ptr =
                  (int32_t)(btree->leaf_node_num);  // make BtreeIterator invalid
            } else if (node_iter.node_type() == BtreeNodeType::BtreeNodeLeaf) {
              assert(Compare(target, node_iter.get_last_key()) > 0);
              node_ptr =
                  (int32_t)(btree->leaf_node_num);  // make BtreeIterator invalid
            } else if (node_iter.node_type() == BtreeNodeType::BtreeNodeSub) {
              assert(Compare(target, node_iter.get_last_key()) > 0);
              node_ptr =
                  (int32_t)(btree->leaf_node_num);  // make BtreeIterator invalid
            }
          }
        }
      } else {
        // 已经查到叶子节点，可以退出了
        key_buf_.SetInternalKey(node_iter.peek_key());
        value_buf_.SetValue(node_iter.peek_value());
        break;
      }
      break;

    case LinearDetection:
      // 线性探测
      node_ptr = (int32_t)estimate_block;
      int32_t last_node = node_ptr;
      /*
       * 第一个循环只是在当前所在的层线性探测，我们需要在这层找到结果之后进入到下一层开始查找
       */
      while (true) {
        node_addr = btree->get_leaf_node_addr(node_ptr);
#ifdef NVM_PREFETCH
        prefetch_range(node_addr, 256);
#endif
        search_blocks++;
        new (&node_iter) node_iterator(node_addr, icomp_);
        int8_t res = node_iter.contain_key(target);
        if (res == 0) {
          break;
        } else {
          node_ptr += res;
          if (node_ptr == last_node) {
            // 出现循环的时候是进入了node的间隙处，所以循环了，应该取较大的node返回
            node_addr = btree->get_leaf_node_addr(node_ptr + (res < 0? 1 : 0));
#ifdef NVM_PREFETCH
            prefetch_range(node_addr, 256);
#endif
            new (&node_iter) node_iterator(node_addr, icomp_);
            node_iter.seek(target);
            break;
          } else {
            last_node = node_ptr - res;
          }
        }
      }

      // 找到包含target_key的block之后可能不是leaf_node所以需要在次查询
      if (node_iter.node_type() != BtreeNodeLeaf) {
        node_ptr = (int32_t)node_iter.peek_subnode();
        node_addr = btree->get_leaf_node_addr(node_ptr);
        while (true) {
#ifdef NVM_PREFETCH
          prefetch_range(node_addr, 256);
#endif
          search_blocks++;
          new (&node_iter) node_iterator(node_addr, icomp_);
          node_iter.seek(target);
          if (node_iter.valid()) {
            if (node_iter.node_type() == BtreeNodeType::BtreeNodeLeaf) {
              // 已经查到叶子节点，可以退出了
              key_buf_.SetInternalKey(node_iter.peek_key());
              value_buf_.SetValue(node_iter.peek_value());
              break;
            } else {
              // 根节点或者子节点都是继续往下查找
              node_ptr = (int32_t)node_iter.peek_subnode();
              node_addr = btree->get_leaf_node_addr(node_ptr);
            }
          } else {
            /*
             * 在一个btree_node里面查找之后，iter是invalid的情况：
             *      1. 如果当前是根节点，这里一定已经出错了，在根节点中查找返回的是invalid说明target超出了last_key
             *      2. 如果是叶子节点，出现invalid的情况也是错误的
             *      3. 如果是子节点，出现invalid的情况也是错误的
             */
            if (node_iter.node_type() == BtreeNodeType::BtreeNodeRoot) {
              assert(Compare(target, btree->last_key) > 0);
              node_ptr =
                  (int32_t)(btree->leaf_node_num);  // make BtreeIterator invalid
            } else if (node_iter.node_type() == BtreeNodeType::BtreeNodeLeaf) {
              assert(Compare(target, node_iter.get_last_key()) > 0);
              node_ptr =
                  (int32_t)(btree->leaf_node_num);  // make BtreeIterator invalid
            } else if (node_iter.node_type() == BtreeNodeType::BtreeNodeSub) {
              assert(Compare(target, node_iter.get_last_key()) > 0);
              node_ptr =
                  (int32_t)(btree->leaf_node_num);  // make BtreeIterator invalid
            }
          }
        }
      } else {
        // 已经查到叶子节点，可以退出了
        key_buf_.SetInternalKey(node_iter.peek_key());
        value_buf_.SetValue(node_iter.peek_value());
        break;
      }
      break;
  }
  return search_blocks;
}


void BtreeIterator::SeekToFirst() {
  node_ptr = 0;
  new (&node_iter) node_iterator(btree->get_leaf_node_addr(node_ptr), icomp_);
  key_buf_.SetInternalKey(node_iter.peek_key());
  value_buf_.SetValue(node_iter.peek_value());
}

void BtreeIterator::SeekToLast() {
  node_ptr = (int32_t)btree->leaf_node_num - 1;
  new (&node_iter) node_iterator(btree->get_leaf_node_addr(node_ptr), icomp_);
  node_iter.seek_to_last();
  key_buf_.SetInternalKey(node_iter.peek_key());
  value_buf_.SetValue(node_iter.peek_value());
}

void BtreeIterator::SeekForPrev(const Slice &target) {}
Status BtreeIterator::status() const { return Status(); }

}  // namespace ROCKSDB_NAMESPACE