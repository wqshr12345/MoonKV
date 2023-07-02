//
// Created by Lluvia on 2022/12/18.
//

#include "gtest/gtest.h"
#include "port/stack_trace.h"
#include "nvm/table/nvm_table_builder.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "nvm/table/nvm_table_factory.cc"

namespace ROCKSDB_NAMESPACE {
std::string strRand(int length) {			// length: 产生字符串的长度
    char tmp;							// tmp: 暂存一个随机数
    std::string buffer;						// buffer: 保存返回值
    
    // 下面这两行比较重要:
    std::random_device rd;					// 产生一个 std::random_device 对象 rd
    std::default_random_engine random(rd());	// 用 rd 初始化一个随机数发生器 random
    
    for (int i = 0; i < length; i++) {
        tmp = random() % 36;	// 随机一个小于 36 的整数，0-9、A-Z 共 36 种字符
        if (tmp < 10) {			// 如果随机数小于 10，变换成一个阿拉伯数字的 ASCII
            tmp += '0';
        } else {				// 否则，变换成一个大写字母的 ASCII
            tmp -= 10;
            tmp += 'A';
        }
        buffer += tmp;
    }
    return buffer;
}

TEST(NvmTableTest, NvmTableTest_AddTable) {
  const int numbers = 100000;
  // 因为BlockBasedTableOptions这个class和很多其它函数有耦合，所以我们的NvmTable也直接使用这个Option。
  // 用作NvmTable的Option
  BlockBasedTableOptions bbto;
  bbto.block_align = true;
  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "" /* don't care */, FileOptions()));
  
  // 用作NvmTableBuilder的Option
  Options options;
  options.compression = kNoCompression;
  // 通过确定table_factory中的TableBuilder子类对象确定我们的tableBuilder是什么。
  options.table_factory.reset(NewNvmTableFactory(bbto));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  std::string column_family_name;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, ikc,
                          &int_tbl_prop_collector_factories, kNoCompression,
                          CompressionOptions(), TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
                          column_family_name, -1),
      file_writer.get()));
  // 存储value的offset，用来在后面TableReader中测试
  std::vector<uint64_t> value_offsets(numbers,0);
  std::vector<std::string> values(numbers,"");
  for (int i = 0; i < numbers; ++i) {
    int length = rand()%100;
    std::string value = strRand(length);
    values[i] = value;
    uint64_t value_offset = builder->Add2(value);
    value_offsets[i] = value_offset;
    std::cout<<"value = "<<value<<", value_offset = "<<value_offset<<std::endl;//key = "<<key<<", 
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(file_writer->Flush());
  ASSERT_EQ(builder->FileSize(),sink->contents().size());
  std::unique_ptr<FSRandomAccessFile> source(
      new test::StringSource(sink->contents(), 73342, false));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(source), "test"));
  // Helper function to get version, global_seqno, global_seqno_offset
  std::function<void()> VerifyBlockAlignment = [&]() {
    std::unique_ptr<TableProperties> props;
    ASSERT_OK(ReadTableProperties(file_reader.get(), sink->contents().size(),
                                  kBlockBasedTableMagicNumber, ioptions,
                                  &props));

    uint64_t data_block_size = props->data_size / props->num_data_blocks;
    ASSERT_EQ(data_block_size, 4096);
    ASSERT_EQ(props->data_size, data_block_size * props->num_data_blocks);
  };

  VerifyBlockAlignment();

  // // The below block of code verifies that we can read back the keys. Set
  // // block_align to false when creating the reader to ensure we can flip between
  // // the two modes without any issues
  std::unique_ptr<TableReader> table_reader;
  bbto.block_align = false;
  Options options2;
  options2.table_factory.reset(NewNvmTableFactory(bbto));
  ImmutableOptions ioptions2(options2);
  const MutableCFOptions moptions2(options2);
  std::unique_ptr<InternalKeyComparator> plain_internal_comparator;
  plain_internal_comparator.reset(
            new test::PlainInternalKeyComparator(options2.comparator));
  ioptions.table_factory->NewTableReader(
      TableReaderOptions(ioptions2, moptions2.prefix_extractor, EnvOptions(),
                         *plain_internal_comparator),
      std::move(file_reader), sink->contents().size(), &table_reader);

  ReadOptions read_options;
  // 读取时不应该检验checksum 因为如果要检验这个，我们就需要为每个data block维护具体的size，这是我们不希望的
  // 所以，我们不得不去掉checksum的检验 我认为这个无伤大雅...
  read_options.verify_checksums = false;

  for (int i = 0;i<numbers;i++) {
    Slice get_value;
    GetContext get_context(options2.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, "", nullptr, nullptr,
                           nullptr, true, nullptr, nullptr, nullptr, nullptr,
                           nullptr, nullptr, /*tracing_get_id=*/i);
    table_reader->Get2(read_options,value_offsets[i],
                     &get_context,&get_value,
                     moptions.prefix_extractor.get());
    std::cout<<"get_value = "<<get_value.ToString()<<", value = "<<values[i]<<std::endl;
    ASSERT_EQ(get_value.ToString(),values[i]);
  }
  table_reader.reset();
}
}

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}