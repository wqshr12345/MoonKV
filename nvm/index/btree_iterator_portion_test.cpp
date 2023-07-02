//
// Created by Lluvia on 2022/12/18.
//

#include "btree_iterator_portion.h"

#include "btree_builder.h"
#include "btree_iterator.h"
#include "gtest/gtest.h"
#include "port/stack_trace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class KeyBuilder {
 private:
  uint32_t max_num;
  uint32_t ptr;

 public:
  KeyBuilder(uint32_t num) : max_num(num), ptr(0) {}
  std::string GetKey() {
    auto num_str = std::to_string(ptr++);
    auto pedding = std::string(10 - num_str.length(), '0');
    return "user_" + pedding + num_str + "internal";
  }
  bool Valid() const { return ptr < max_num; }
};

TEST(BtreeIteratorPortionTest, BBtreeIteratorPortionTest_BuildAndIterator) {
  // 1.构造Btree
  std::vector<table_information_collect> runs_seq = {
      {0, 0, 12312}, {1, 0, 4645634}, {2, 0, 56545}, {3, 0, 9567467}};
  BtreeBuilder builder(1, runs_seq);

  KeyBuilder key_builder(100000);
  uint32_t num = 0;
  while (key_builder.Valid()) {
    builder.Add(rocksdb::Slice(key_builder.GetKey()), num % 4, num);
    num++;
  }
  builder.Finish();
  builder.Format();
  builder.Flush();
  // 2.读取Btree
  ReadOptions rop;
  InternalKeyComparator icomp(BytewiseComparator());
  NvmBtree nbtree(1, rop, icomp);
  BtreeIterator* iter = dynamic_cast<BtreeIterator*>(nbtree.NewIterator(
      rop, nullptr, nullptr, true, TableReaderCaller::kUserGet, 0, false));
  int i = 0;
  while (iter->Valid()) {
    uint32_t file_seq = iter->value_file_seq();
    uint64_t offset = iter->value_offset();
    //    std::cout<<"true offset = "<<values[i]<<",real offset =
    //    "<<offset<<std::endl;
    ASSERT_EQ(i % 4, file_seq);
    ASSERT_EQ(i, offset);
    i++;
    iter->Next();
  }
}

TEST(BtreeIteratorPortionTest, BtreeIteratorPortionTest_portion_iterator) {
  std::vector<uint32_t> runs_seq = {1, 2};
  ReadOptions rop;
  InternalKeyComparator icomp(BytewiseComparator());
  NvmBtree nbtree(1, rop, icomp);
  auto iter = dynamic_cast<BtreePortionIterator*>(nbtree.NewPortionIterator(
      rop, nullptr, nullptr, true, TableReaderCaller::kUserGet, 0, false,
      runs_seq));

  // 正向
  int i = 0;
  while (iter->Valid()) {
    uint32_t file_seq = iter->value_file_seq();
    uint64_t offset = iter->value_offset();
    //    std::cout<<"true offset = "<<values[i]<<",real offset =
    //    "<<offset<<std::endl;
    ASSERT_EQ(runs_seq[i % (runs_seq.size())], file_seq);
    ASSERT_EQ(((i / (runs_seq.size()) * 4) + runs_seq[i % (runs_seq.size())]),
              offset);
    i++;
    iter->Next();
  }

  // 反向
  i--;
  iter->SeekToLast();
  while (iter->Valid()) {
    uint32_t file_seq = iter->value_file_seq();
    uint64_t offset = iter->value_offset();
    //    std::cout<<"true offset = "<<values[i]<<",real offset =
    //    "<<offset<<std::endl;
    ASSERT_EQ(runs_seq[i % (runs_seq.size())], file_seq);
    ASSERT_EQ(((i / (runs_seq.size()) * 4) + runs_seq[i % (runs_seq.size())]),
              offset);
    i--;
    iter->Prev();
  }
}

TEST(BtreeIteratorPortionTest, BtreeIteratorPortionTest_iterator) {
  ReadOptions rop;
  InternalKeyComparator icomp(BytewiseComparator());
  NvmBtree nbtree(1, rop, icomp);
  BtreeIterator* iter = dynamic_cast<BtreeIterator*>(nbtree.NewIterator(
      rop, nullptr, nullptr, true, TableReaderCaller::kUserGet, 0, false));

  for (int i = 0; iter->Valid(); ++i) {
    std::string ukey =
        std::string(iter->user_key().data() + 5, iter->user_key().size() - 5);
    int num = std::stoi(ukey);
    //    std::cout<<"true key_num = "<<i<<",real key_num = "<<num<<std::endl;
    ASSERT_EQ(i, num);
    iter->Next();
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
