#include "btree_builder.h"

#include "btree_node.h"
#include "db/dbformat.h"
#include "nvm_btree.h"
#include "libpmem.h"
#include "file/filename.h"

namespace ROCKSDB_NAMESPACE {

void BtreeBuilder::Run() {
  // 这个循环完全可以通过Add()函数手动完成
  while (iter_->Valid()) {
    // 这里添加了跳过相同user_key的逻辑，在使用Merge操作指令时需要变更这里的逻辑
    if (ExtractUserKey(last_key).compare(iter_->user_key()) == 0) continue;
    kv_paris.emplace_back(std::string(iter_->key().data(), iter_->key().size()),
                          iter_->value().ToString());

    // 更新
    if (!builder.AddKey(kv_paris.back().first)) {
      block_kv_num.emplace_back(
          std::pair<Slice, Slice>{builder.GetPrefix(), builder.GetSuffix()},
          builder.GetKeyNum());

      builder.Clear(BtreeNodeLeaf);
      builder.AddKey(kv_paris.back().first);

      if (index_key.empty()) index_key.emplace_back();
      index_key[0].emplace_back(last_key, block_num++);
    }
    last_key = kv_paris.back().first;
    iter_->Next();
  }
  Finish();
  Format();
  Flush();
}

void BtreeBuilder::Add(Slice key, uint32_t table, uint64_t value_offset) {
  // 根据传入的table_number从map中查出对应的vector中的对应index值
  uint8_t table_index = get_index_form_sub_run_map(table);

  valueaddr v(table_index, value_offset);
  std::string v_str = v.to_string();
  add_internal(key, Slice(v_str));
}

void BtreeBuilder::Add(Slice key, Slice table_offset) {
  // 这里传入的table_offset是一个特殊的value格式，一般来自于BtreeIterator::value接口的直接输出
  uint8_t table_index = get_index_form_sub_run_map(*(uint32_t *)table_offset.data());

  char value_buf[VALUEADDR_OFFSET_SIZE + 1];
  *(uint8_t *)value_buf = table_index;
  memcpy(value_buf + sizeof(uint8_t), table_offset.data() + sizeof(uint32_t),
         VALUEADDR_OFFSET_SIZE);

  add_internal(key, Slice(value_buf, VALUEADDR_OFFSET_SIZE + 1));
}

void BtreeBuilder::add_internal(Slice key, Slice table_offset) {
  // 关于跳过相同user_key的逻辑，以及使用Merge操作指令时需要的变更在外部处理好了，这里不用管

  // 统计每个索引的Table包含的有效key的数量
  sub_run_inform[valueaddr::get_run_from_slice(table_offset)]
      .valid_key_num += 1;
  kv_paris.emplace_back(std::string(key.data(), key.size()),
                        std::string(table_offset.data(), table_offset.size()));

  // 更新
  if (!builder.AddKey(kv_paris.back().first)) {
    block_kv_num.emplace_back(
        std::pair<Slice, Slice>{builder.GetPrefix(), builder.GetSuffix()},
        builder.GetKeyNum());

    builder.Clear(BtreeNodeLeaf);
    builder.AddKey(kv_paris.back().first);

    if (index_key.empty()) index_key.emplace_back();
    index_key[0].emplace_back(last_key, block_num++);
  }
  last_key = kv_paris.back().first;
}

void BtreeBuilder::Finish() {
  // 把添加KV的循环中还没有添加为一个block的数据添加为一个新的block，这部分的逻辑是一定需要执行的
  {
    block_kv_num.emplace_back(
        std::pair<Slice, Slice>{builder.GetPrefix(), builder.GetSuffix()},
        builder.GetKeyNum());
    // 感觉这里不需要添加index_key为空的判断，data应该一定不止一个block的数量
    if (index_key.empty()) {
      keys_num_is_less_than_one_nvm_block = true;
      return;
    } else {
      index_key[0].emplace_back(last_key, block_num);
    }
  }

  while (index_key[level].size() > 1) {
    builder.Clear(BtreeNodeSub);
    uint32_t block_num_ = 0;

    for (int i = 0; i < (int)index_key[level].size(); ++i) {
      // 构建index的时候不需要去重逻辑，直接添加新的元素
      if (!builder.AddKey(index_key[level][i].first)) {
        if (index_block_kv_num.size() == level)
          index_block_kv_num.emplace_back();
        index_block_kv_num[level].emplace_back(
            std::pair<Slice, Slice>{builder.GetPrefix(), builder.GetSuffix()},
            builder.GetKeyNum());

        builder.Clear(BtreeNodeSub);
        builder.AddKey(index_key[level][i].first);

        if (index_key.size() == level + 1) index_key.emplace_back();
        index_key[level + 1].emplace_back(index_key[level][i - 1].first,
                                          block_num_++);
      }
    }
    {
      if (index_block_kv_num.size() == level) index_block_kv_num.emplace_back();
      index_block_kv_num[level].emplace_back(
          std::pair<Slice, Slice>{builder.GetPrefix(), builder.GetSuffix()},
          builder.GetKeyNum());
      if (index_key.size() == level + 1) index_key.emplace_back();
      index_key[level + 1].emplace_back(index_key[level].back().first,
                                        block_num_);
    }
    level++;
  }
}

void BtreeBuilder::Format() {

  uint32_t block_num_ = block_kv_num.size();
  for (const auto &item : index_block_kv_num) block_num_ += item.size();
  // 这里在计算空间的时候多注意预留一部分，以防意外
  uint32_t file_size = block_num_ * NVM_BLOCK_SIZE + sizeof(btree_file_meta)
                       + sub_run_inform.size() * sizeof(uint32_t) * 4 + NVM_BLOCK_SIZE;

  buf = new uint8_t[file_size];
  offset = buf;

  // 先写leaf_node
  uint32_t btree_kv_offset = 0;
  auto iter_data = kv_paris.begin();
  node_writer writer;
  if (keys_num_is_less_than_one_nvm_block) {
    /* 这里是当前Btree包含的所有key不足一个NVM Block的情况，
     * 这时候不再有root node和internal node，只有一个leaf node
     */
    new (&writer) node_writer(
        block_kv_num[0].first.first, block_kv_num[0].first.second,
        &iter_data, block_kv_num[0].second,BtreeNodeLeaf,
        0,btree_kv_offset, offset);
    writer.Build();
    offset += NVM_BLOCK_SIZE;
    assert(offset < buf + file_size);
  } else {
    /*
     * 辅助生成parent node number的几个变量
     * 因为这里在生成leaf node的时候还没有构建index的node，其实是不能直接得到parent node的
     * 所以这里是通过index_block_kv_num[0]中保存的最后一层index的block信息来推算的parent node位置
     */
    uint32_t parent_node = block_kv_num.size();
    auto parent_node_iter = index_block_kv_num[0].begin();
    uint8_t parent_keys = parent_node_iter->second;
    bool is_level_head_node = true;
    for (const auto &block : block_kv_num) {
      if (parent_keys == 0) {
        parent_node_iter++;
        parent_keys = parent_node_iter->second;
        parent_node++;
      }
      new (&writer) node_writer(
          block.first.first, block.first.second, &iter_data, block.second,
          BtreeNodeLeaf,
          is_level_head_node ? parent_node | 0x80000000 : parent_node,
          btree_kv_offset, offset);
      writer.Build();
      offset += NVM_BLOCK_SIZE;
      // 统计当前的这个leaf_node所包含的key在当前btree中所占的位置
      btree_kv_offset += block.second;
      parent_keys--;
      is_level_head_node = false;
      assert(offset < buf + file_size);
    }

    // 然后从低到高写index层的node
    uint8_t *last_node_blank_address;
    for (int i = 0; i < level; ++i) {
      auto iter_index = index_key[i].begin();
      // parent node只在非root节点中生效
      if (i < level - 1) {
        parent_node +=
            1;  // 这里是紧接着上一次，直接+1讲位置调整到新一层的block位置即可
        parent_node_iter = index_block_kv_num[i + 1].begin();
        parent_keys = parent_node_iter->second;
      }
      is_level_head_node = true;
      for (const auto &block : index_block_kv_num[i]) {
        if (parent_keys == 0 && i < level - 1) {
          parent_node_iter++;
          parent_keys = parent_node_iter->second;
          parent_node++;
        }
        // 如果 i == level - 1，说明是最后一个block，这个应该是根节点
        new (&writer) node_writer(
            block.first.first, block.first.second, &iter_index, block.second,
            i == level - 1 ? BtreeNodeRoot : BtreeNodeSub,
            is_level_head_node ? parent_node | 0x80000000 : parent_node, offset,
            already_writen_block_num);
        writer.Build();
        offset += NVM_BLOCK_SIZE;
        parent_keys--;
        is_level_head_node = false;
      }
      if (i == 0)
        already_writen_block_num = block_kv_num.size();
      else
        already_writen_block_num += index_block_kv_num[i - 1].size();
    }
  }

  // 接着写sub_runs的文件编号，数组的形式
  offset = writer.BlankAddr();
  uint8_t *sub_run_seq_array_address = offset;
  for (const auto &num : sub_run_inform) {
    *(uint32_t *)offset = num.table_file_number;
    offset += sizeof(uint32_t);
  }

  uint8_t *table_valid_key_num_array_address = offset;
  for (const auto &num : sub_run_inform) {
    *(uint32_t *)offset = num.valid_key_num;
    offset += sizeof(uint32_t);
  }

  uint8_t *table_total_size_array_address = offset;
  for (const auto &num : sub_run_inform) {
    *(uint32_t *)offset = num.table_file_size;
    offset += sizeof(uint32_t);
  }

  // 然后写first_key last_key
  uint8_t *small_and_large_key_address = offset;
  offset = key_prefix::write_prefix(offset, kv_paris.front().first);
  offset = key_prefix::write_prefix(offset, kv_paris.back().first);

  // 最后写meta数据段，紧接着root_block后面的空白字段写
  auto *meta = (btree_file_meta *)offset;
  meta->nrun = nrun;
  meta->sub_run_off = sub_run_seq_array_address - buf;
  meta->table_valid_key_num_off = table_valid_key_num_array_address - buf;
  meta->table_total_size_off = table_total_size_array_address - buf;
  meta->first_last_key_offset = small_and_large_key_address - buf;
  meta->seq = file_seq;
  meta->total_kv = kv_paris.size();
  meta->index_off = writer.BlockAddr() - buf;
  meta->leaf_node_num = block_kv_num.size();
  meta->levels = level + 1;
  meta->file_size = offset - buf + sizeof(btree_file_meta);
  meta->magic = BTREE_FILE_MAGIC_NUMBER;

  file_size_ = meta->file_size;
}

void BtreeBuilder::Flush() {

  //todoopt.文件路径可以用option里面新增一个字段,或者就暂时干脆写死好了...
  std::string file_name = MakeIndexFileName(nvm_dir,file_seq);
  // if (nvm_dir[nvm_dir.length()-1] != '/')
  //   file_name = nvm_dir + "/" + std::to_string(file_seq) + ".index";
  // else
  //   file_name = nvm_dir + std::to_string(file_seq) + ".index";

  size_t mapped_len;
  int is_pmem;
  uint8_t* pmemaddr;  //映射NVM内存的起始地址

  if ((pmemaddr = (uint8_t *)pmem_map_file(file_name.c_str(), file_size_, PMEM_FILE_CREATE|PMEM_FILE_EXCL,
                                           0666, &mapped_len, &is_pmem)) == nullptr) {
    std::cout<<file_seq<<".table"<<std::endl;
    perror("pmem_map_file");
    assert(pmemaddr != nullptr);
    exit(1);
  }

  if (is_pmem) {
    pmem_memcpy_persist(pmemaddr, buf, file_size_);
  } else {
    memcpy(pmemaddr, buf, file_size_);
    pmem_msync(pmemaddr, file_size_);
    // std::cout << "SSD" << std::endl;
  }
  pmem_unmap(pmemaddr, mapped_len);
}

uint64_t BtreeBuilder::NumEntries() { return kv_paris.size(); }

std::map<uint32_t, uint32_t> BtreeBuilder::TableValidKeyNumMap() {
  std::map<uint32_t, uint32_t> res_map;
  for (const auto &item : sub_run_inform) {
    res_map.emplace(std::pair<uint32_t, uint32_t>(item.table_file_number,
                                                  item.valid_key_num));
  }
  return res_map;
}


}  // namespace ROCKSDB_NAMESPACE