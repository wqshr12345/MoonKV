//
// Created by Lluvia on 2022/12/18.
//

#include "btree_builder.h"

#include "gtest/gtest.h"
#include "port/stack_trace.h"
#include "rocksdb/slice.h"
#include "btree_iterator.h"

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
TEST(BtreeBuildTest,BtreeBuilderTest_BuildAndRead){
  // 1.构造Btree
  std::vector<table_information_collect> runs_seq = {};
  ImmutableOptions ioptions;
  ioptions.db_nvm_dir = "/home/wq/ssd4/nvm";
  MutableCFOptions moptions;
  InternalKeyComparator internal_comparator;
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  CompressionType compression_type;
  CompressionOptions compression_opts;
  uint32_t column_family_id;
  const std::string column_family_name;
  int level;

  TableBuilderOptions tbo(ioptions, moptions, internal_comparator,
                          &int_tbl_prop_collector_factories, compression_type, compression_opts,
                          column_family_id, column_family_name, level);

  BtreeBuilder builder(2, tbo, runs_seq);

  KeyBuilder key_builder(100000);
  std::vector<std::string> keys(100001,"");
  std::vector<uint64_t> values(100001,0);
  srand(time(0));
  for(int i = 1;i<=10;i++){
    std::ostringstream ostr;
    ostr << std::setfill('0') << std::setw(5) << i;
    std::string key = ostr.str();
    // std::string key = key_builder.GetKey();
    InternalKey ik(key, 0, kTypeValue);
    // keys[num] = ik.user_key().ToString();
    keys[i] = ik.Encode().ToString();
    int value = rand() % 10000;
    values[i] = value;
    builder.Add(ik.Encode(), 1, value);
  }
  builder.Finish();
  builder.Format();
  builder.Flush();
  // 2.读取Btree
  ReadOptions rop;
  InternalKeyComparator icomp(BytewiseComparator());
  NvmBtree nbtree(2, rop, ioptions, icomp);
  BtreeIterator* iter = dynamic_cast<BtreeIterator*>(nbtree.NewIterator(rop, nullptr, nullptr, true,
                                 TableReaderCaller::kUserGet, 0, false));

  for (int i = 1; i <= 10; ++i) {
    std::cout<<"key "<<keys[i]<<std::endl;
    iter->Seek(keys[i]);
    // ASSERT_TRUE(iter->Valid());
    uint32_t file_seq = iter->value_file_seq();
    uint64_t offset = iter->value_offset();
    std::cout<<"true offset = "<<values[i]<<",real offset = "<<offset<<std::endl;
    // ASSERT_EQ(values[i],offset);
  }
}

/*
TEST(BtreeBuilderTest, BtreeBuilderTest_UpdatePrefix) {
  node_builder builder(BtreeNodeLeaf);
  builder.AddKey("00000001i4ternal");
  builder.AddKey("00000002i7ternal");
  std::string res_p =
      std::string(builder.GetPrefix().data(), builder.GetPrefix().size());
  ASSERT_STREQ(res_p.c_str(), "0000000");
  std::string res_f =
      std::string(builder.GetSuffix().data(), builder.GetSuffix().size());
  ASSERT_STREQ(res_f.c_str(), "ternal");

  builder.AddKey("00005332i7ter6al");
  res_p = std::string(builder.GetPrefix().data(), builder.GetPrefix().size());
  ASSERT_STREQ(res_p.c_str(), "0000");
  res_f = std::string(builder.GetSuffix().data(), builder.GetSuffix().size());
  ASSERT_STREQ(res_f.c_str(), "al");

  builder.AddKey("00i7ter6a8");
  res_p = std::string(builder.GetPrefix().data(), builder.GetPrefix().size());
  ASSERT_STREQ(res_p.c_str(), "00");
  ASSERT_EQ(builder.GetSuffix().size(), 0);
}

TEST(BtreeBuilderTest, BtreeBuilderTest_build) {
  std::vector<uint32_t> runs_seq = {1, 2, 3};
  BtreeBuilder builder(1, runs_seq);

  KeyBuilder key_builder(100000);
  uint32_t num = 0;
  while (key_builder.Valid()) {
    builder.Add(rocksdb::Slice(key_builder.GetKey()), 1, num++);
  }
  builder.Finish();
  builder.Format();
  builder.Flush();
}

TEST(BtreeBuilderTest, BtreeBuilderTest_iterator) {
  ReadOptions rop;
  InternalKeyComparator icomp(BytewiseComparator());
  NvmBtree nbtree(1, rop, ImmutableOptions(), icomp);
  auto iter = nbtree.NewIterator(rop, nullptr, nullptr, true,
                                 TableReaderCaller::kUserGet, 0, false);

  for (int i = 0; i < 100000; ++i) {
    ASSERT_TRUE(iter->Valid());
    std::string ukey =
        std::string(iter->user_key().data() + 5, iter->user_key().size() - 5);
    int num = std::stoi(ukey);
    ASSERT_EQ(i, num);
    iter->Next();
  }
}

TEST(BtreeBuilderTest, BtreeBuilderTest_seek) {
  ReadOptions rop;
  InternalKeyComparator icomp(BytewiseComparator());
  NvmBtree nbtree(1, rop, ImmutableOptions(), icomp);
  auto iter = nbtree.NewIterator(rop, nullptr, nullptr, true,
                                 TableReaderCaller::kUserGet, 0, false);

  KeyBuilder key_builder(100000);
  uint32_t i = 0;
  while (key_builder.Valid()) {
    iter->Seek(rocksdb::Slice(key_builder.GetKey()));
    ASSERT_TRUE(iter->Valid());
    std::string ukey =
        std::string(iter->user_key().data() + 5, iter->user_key().size() - 5);
    int num = std::stoi(ukey);
    ASSERT_EQ(i, num);
    i++;
  }
}

*/

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
