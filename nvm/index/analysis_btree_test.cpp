//
// Created by Lluvia on 2022/12/18.
//
#include "btree_builder.h"
#include "btree_iterator.h"
#include "gtest/gtest.h"
#include "port/stack_trace.h"
#include "rocksdb/slice.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <clipp.h>
#include "btree_node.h"
#include "file/filename.h"


std::string dir = "";
uint64_t file_seq = 0;

namespace ROCKSDB_NAMESPACE {


class BtreeAnalysis
{
 private:
  void* pmemaddr;
  btree_file_meta* meta;

  void* index_off;
  uint32_t leaf_node_num;

  std::string first_key;
  std::string last_key;

  std::vector<uint32_t> sub_runs;
  std::vector<uint32_t> table_valid_key_num;
  std::vector<uint32_t> table_total_size;

  InternalKeyComparator icomp_;

  void* get_leaf_node_addr(uint32_t n) {
    return (uint8_t*)pmemaddr + (n * NVM_BLOCK_SIZE);
  }

 public:
  BtreeAnalysis(std::string dir, uint32_t seq) : icomp_(BytewiseComparator()) {
    size_t mapped_len;

    std::string file_name = MakeIndexFileName(dir,seq);
    int fd;
    if ( (fd = open(file_name.c_str(), O_RDWR|O_CREAT, S_IRWXU)) < 0){
      printf("open file wrong!");
      exit(1);
    }

    struct stat file_stat;
    if ( fstat( fd, &file_stat) < 0 )
    {
      printf(" fstat wrong");
      exit(1);
    }

    if( ( pmemaddr = mmap(NULL, file_stat.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0 )) == MAP_FAILED)
    {
      printf("mmap wrong");
      exit(0);
    }

    mapped_len = file_stat.st_size;

    meta = (btree_file_meta *)((uint8_t *)pmemaddr + mapped_len -
                               sizeof(btree_file_meta));

    index_off = (uint8_t *)pmemaddr + meta->index_off;
    leaf_node_num = meta->leaf_node_num;

    void *first_last_key = (uint8_t *)pmemaddr + meta->first_last_key_offset;
    first_last_key = key_prefix::get_prefix(first_last_key, first_key);
    first_last_key = key_prefix::get_prefix(first_last_key, last_key);

    // std::cout << "first_key = ";
    // uint8_t *p = (uint8_t *)first_key.data();
    // for (size_t i = 0; i < first_key.size(); i++) {
    //   std::cout << (int)(p[i]) << " ";
    // }
    // std::cout << std::endl << "last_key = ";
    // p = (uint8_t *)last_key.data();
    // for (size_t i = 0; i < last_key.size(); i++) {
    //   std::cout << (int)(p[i]) << " ";
    // }
    // std::cout << std::endl;

    auto *sub_runs_addr = (uint32_t *)((uint8_t *)pmemaddr + meta->sub_run_off);
    for (uint32_t i = 0; i < meta->nrun; ++i) {
      sub_runs.push_back(sub_runs_addr[i]);
    }

    auto *table_valid_key_addr =
        (uint32_t *)((uint8_t *)pmemaddr + meta->table_valid_key_num_off);
    for (uint32_t i = 0; i < meta->nrun; ++i) {
      table_valid_key_num.push_back(table_valid_key_addr[i]);
    }

    auto *table_total_size_addr =
        (uint32_t *)((uint8_t *)pmemaddr + meta->table_total_size_off);
    for (uint32_t i = 0; i < meta->nrun; ++i) {
      table_total_size.push_back(table_total_size_addr[i]);
    }
  };
  ~BtreeAnalysis() = default;

  void ListKeyValue() {
    list_node(index_off);
  }

  void list_node(void *node_addr) {
    node_iterator node_iter(node_addr, icomp_);
    while (node_iter.valid()) {
      std::string key = node_iter.peek_key();
      std::cout << "key = " << key << " ";
      // std::cout << "key = ";
      // uint8_t *p = (uint8_t *)key.data();
      // for (size_t i = 0; i < key.size(); i++) {
      //   std::cout << (int)(p[i]) << " ";
      // }

      if (node_iter.node_type() != BtreeNodeLeaf)
      {
        std::cout << "subnode = " << node_iter.peek_subnode() << std::endl;

        uint32_t node_ptr = (int32_t)node_iter.peek_subnode();
        void *child_node_addr = get_leaf_node_addr(node_ptr);
        list_node(child_node_addr);
      } else {
        std::cout << "value~~" << std::endl;
        // std::cout << "value = " << std::string(node_iter.peek_value().data(),node_iter.peek_value().size()) << std::endl;
      }
      node_iter.next();
    }
  }

  void Check() { check_btree(index_off, last_key, UINT32_MAX); }

  void check_btree(void *node_addr, std::string up_bound, uint32_t parent_node) {
    node_iterator node_iter(node_addr, icomp_);
    if (parent_node != UINT32_MAX)
    {
      ASSERT_EQ(parent_node, node_iter.get_parent_node());
    }
    int i = 0;
    while (node_iter.valid()) {
      ASSERT_LE(string_compare(node_iter.peek_key(), up_bound), 0);

      if (node_iter.node_type() != BtreeNodeLeaf)
      {
        uint32_t node_ptr = (int32_t)node_iter.peek_subnode();
        void *child_node_addr = get_leaf_node_addr(node_ptr);

        uint32_t this_node = ((uint8_t*)node_addr - (uint8_t*)pmemaddr) / NVM_BLOCK_SIZE;
        check_btree(child_node_addr, node_iter.peek_key(), this_node);
      }
      node_iter.next();
      i++;
    }
    int j = strncmp(node_iter.get_prefix().data(), up_bound.data(), node_iter.get_prefix().size());
    ASSERT_EQ(j, 0);
  }

  void SeekAllKeys() {
    ReadOptions rop;
    DBOptions dbop;
    ColumnFamilyOptions cfop;
    ImmutableOptions iop(dbop, cfop);
    iop.db_nvm_dir = "/ssd/dbtest/nvm";
    InternalKeyComparator icomp(BytewiseComparator());
    NvmBtree nbtree(33, rop, iop, icomp);
    BtreeIterator* iter = dynamic_cast<BtreeIterator*>(nbtree.NewIterator(
        rop, nullptr, nullptr, true, TableReaderCaller::kUserGet, 0, false));

    BtreeIterator* seek = static_cast<BtreeIterator*>(nbtree.NewIterator(
        rop, nullptr, nullptr, true, TableReaderCaller::kUserGet, 0, false));

    std::cout << "find keys        ";
    for (int i = 0; iter->Valid(); ++i) {
      seek->Seek(iter->key());
      ASSERT_EQ(strncmp(iter->key().data(), seek->key().data(), iter->key().size()), 0);
      ASSERT_EQ(strncmp(iter->value().data(), seek->value().data(), iter->value().size()), 0);

      seek->EstimateSeek(iter->key(), 0, BackTrack);
      ASSERT_EQ(strncmp(iter->key().data(), seek->key().data(), iter->key().size()), 0);
      ASSERT_EQ(strncmp(iter->value().data(), seek->value().data(), iter->value().size()), 0);

      seek->EstimateSeek(iter->key(), 1, BackTrack);
      ASSERT_EQ(strncmp(iter->key().data(), seek->key().data(), iter->key().size()), 0);
      ASSERT_EQ(strncmp(iter->value().data(), seek->value().data(), iter->value().size()), 0);
      std::cout << "\b\b\b\b\b\b\b" << std::setw(7) << std::setfill('0') << i;
      iter->Next();
    }
  }

  void Check_Btree_KV_Num_Offset() {
    void *node_addr = nullptr;
    uint32_t kvs = 0;
    for (uint32_t i = 0; i < leaf_node_num; i++)
    {
      node_addr = get_leaf_node_addr(i);
      node_iterator node_iter(node_addr, icomp_);

      ASSERT_EQ(node_iter.get_btree_kv_offset(), kvs);
      kvs += node_iter.node_total_kv();
    }
  }

  int string_compare(Slice a, Slice b) {
    return icomp_.Compare(a, b);
  }
};



TEST(AnalysisBtreeTest, AnalysisBtreeTest_iterator) {
  std::string dir = "/ssd/dbtest/nvm/000033.index";

  BtreeAnalysis btree(dir, 33);

  btree.ListKeyValue();
  btree.Check();
  btree.Check_Btree_KV_Num_Offset();
  btree.SeekAllKeys();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  auto cli = (
      clipp::value("DB dir", dir),
      clipp::option("-n") & clipp::value("file sequence", file_seq)
  );
  if(!parse(argc, argv, cli)) std::cout << make_man_page(cli, argv[0]);

  return RUN_ALL_TESTS();
}
