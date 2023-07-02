//
// Created by Lluvia on 2022/12/18.
//

#include "gtest/gtest.h"
#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "nvm/partition/nvm_partition_factory.cc"
#include "nvm/table/nvm_table_factory.cc"
#include "env/fs_posix.cc"
#include "db/table_cache.h"
#include "cache/lru_cache.h"
namespace ROCKSDB_NAMESPACE {
extern IOStatus NewWritableFile(FileSystem* fs, const std::string& fname,
                         std::unique_ptr<FSWritableFile>* result,
                         const FileOptions& options);
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
TEST(NvmPartitionTest, NvmPartitionTest_AddAndReadTable) {
  const int numbers = 100000;
  // 因为BlockBasedTableOptions这个class和很多其它函数有耦合，所以我们的NvmTable也直接使用这个Option。
  // 用作NvmTable的Option
  BlockBasedTableOptions bbto;
  // 因为RocksDB有一个option叫做block_align。它没有默认开启这个，就导致不是页对齐的。不管是压缩还是没压缩，它都默认没有使用block_align，必须在这里手动开启
  bbto.block_align = true;

  // 1.构造输入fname
  std::vector<DbPath> paths;
  // 这里一定要注意，随时修改文件夹，同时文件夹不存在要自己创建
  std::string path = "/home/wq/ssd4";
  // std::string nvm_path = "/home/wq/nvm";
  DbPath db_path(path,1000000000);
  // DbPath db_path2(nvm_path,100000000);
  paths.push_back(db_path);
  // paths.push_back(db_path2);
  // 此处测试使用的file_seq是1，所以后续builder那里要用1
  std::string fname = path+"/000001.sst";
  std::unique_ptr<FSWritableFile> file;
  FileSystem* fs = new PosixFileSystem();
  IOStatus io_s = NewWritableFile(fs, fname, &file, FileOptions());

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
          std::move(file), fname, FileOptions()));
  
  // 用作TableBuilder的Option
  Options options;
  options.compression = kNoCompression;
  options.cf_paths = paths;

  // 通过确定table_factory中的TableBuilder子类对象确定我们的tableBuilder是什么。
  options.table_factory.reset(NewNvmPartitionFactory(bbto));
  options.second_table_factory.reset(NewNvmTableFactory(bbto));
  ImmutableOptions ioptions(options);
  ioptions.db_nvm_dir = "/home/wq/ssd4/nvm";
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  IntTblPropCollectorFactories int_tbl_prop_collector_factories;
  std::string column_family_name;
  // WQTODO 可能有内存泄漏？以后再说吧
  NvmPartitionFactory *nvm_partition_factory =dynamic_cast<NvmPartitionFactory *>(options.table_factory.get());
  // 这里的sub_rum代表的是本次构造btree-index的时候，其所使用的旧的table的seq
  // WQTODO 这里需要注意一个很关键的地方，就是sub_run参数怎么得到？
  // 直接在TableBuilder的时候不一定能拿到。比如
  // 1.有value compaction的时候，index上全是新生成的table，这个table的bumber在构造NvmPartitionBuilder的时候并不知道...
  // 2.compaction的时候，怎么提前知道table有哪些？
  // 这个参数也许可以不作为TableBuilder的输入参数
  std::vector<table_information_collect> sub_run{{1, 0, 0}};
  std::unique_ptr<TableBuilder> builder(nvm_partition_factory->NewTableBuilder(
      TableBuilderOptions(
          ioptions, moptions, ikc, &int_tbl_prop_collector_factories,
          kNoCompression, CompressionOptions(),
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
          column_family_name, -1),
      file_writer.get(), 1, 2, sub_run, true));
  // 存储本次Add的key值，用于后续测试Reader
  std::vector<std::string> keys(numbers,"");
  std::vector<std::string> user_keys(numbers,"");
  // 存储本次Add的value值，用于在后续测试
  std::vector<std::string> values(numbers,"");
  KeyBuilder key_builder(numbers);
  for (int i = 0; i < numbers; ++i) {
    // 生成不重复key值 有两种选择：可以选择注释的三行中的生成方式，也可以选择key_builder方式。
    // std::ostringstream ostr;
    // ostr << std::setfill('0') << std::setw(5) << i;
    // std::string key = ostr.str();
    std::string key = key_builder.GetKey();
    user_keys[i] = key;
    // 生成随机的value值
    int length = rand()%100;
    std::string value = strRand(length);
    values[i] = value;
    // 将随机生成的key和value添加到builder中
    InternalKey ik(key, 0, kTypeValue);
    keys[i] = ik.Encode().ToString();
    builder->Add(keys[i],value);
    std::cout<<"key = "<<key<<", value = "<<value<<std::endl;//key = "<<key<<", 
  }
  ASSERT_OK(builder->Finish());
  // 目前的file_size就是代表构造的table的file_size
  uint64_t file_size = builder->FileSize();
  std::cout<<"file size = "<<file_size<<std::endl;
  ASSERT_OK(file_writer->Flush());

  std::unique_ptr<FSRandomAccessFile> access_file;
  fs->NewRandomAccessFile(fname,FileOptions(),&access_file,nullptr);

  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(access_file),fname));

  // The below block of code verifies that we can read back the keys. Set
  // block_align to false when creating the reader to ensure we can flip between
  // the two modes without any issues
  std::unique_ptr<TableReader> table_reader;
  bbto.block_align = false;
  options.cf_paths = paths;
  // options.table_factory.reset(NewNvmPartitionFactory(bbto));
  // WQTODO 现在好多option里面都有second_table_factory... 看看吧，到时候整合一下
  // WQTODO 为什么用两个LRUCache就会内存不够？
  // options.second_table_factory.reset(NewNvmTableFactory(bbto));
  ImmutableOptions ioptions2(options);
  ioptions2.db_nvm_dir = "/home/wq/ssd4/nvm";
  const MutableCFOptions moptions2(options);
  FileOptions file_options;
  LRUCache cache(1 << 20,2,false,0);
  TableCache table_cache(ioptions2,&file_options,&cache,nullptr,nullptr,"");
  ReadOptions read_options;
  // 读取时不应该检验checksum 因为如果要检验这个，我们就需要为每个data block维护具体的size(直白一点，要为每个data block的checksum维护一个offset，不然不知道checksum在哪)，这是我们不希望的
  // 所以，我们不得不去掉checksum的检验 我认为这个无伤大雅...
  read_options.verify_checksums = false;
  std::unique_ptr<InternalKeyComparator> plain_internal_comparator;
  plain_internal_comparator.reset(
            new test::PlainInternalKeyComparator(options.comparator));

  nvm_partition_factory->NewTableReader2(read_options,
      TableReaderOptions(ioptions2, moptions2.prefix_extractor, EnvOptions(),
                         *plain_internal_comparator),
      std::move(file_reader),&table_reader, &table_cache,true);
  NvmPartition* partition_reader =dynamic_cast<NvmPartition*>(table_reader.get());
  InternalKeyComparator internal_key_compacrator;
  for (int i = 0;i< numbers;i++) {
    std::cout<<"user_key = "<<user_keys[i]<<std::endl;
    PinnableSlice value;
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, user_keys[i], &value, nullptr,
                           nullptr, true, nullptr, nullptr, nullptr, nullptr,
                           nullptr, nullptr, /*tracing_get_id=*/i);
    table_reader->Get(read_options,keys[i],
                     &get_context,nullptr);
    std::cout<<"get_value = "<<value.ToString()<<", value = "<<values[i]<<std::endl;
    ASSERT_EQ(value.ToString(),values[i]);
  }
  
  
  //以下是iter的测试

  InternalIterator* nvm_partition_iter = partition_reader->NewIterator(read_options,nullptr,nullptr,false,TableReaderCaller::kUserIterator,
      0, false, true);
  int i = 0;
  while(nvm_partition_iter->Valid()){
    std::cout<<"user_key = "<<user_keys[i]<<"get_key = "<<nvm_partition_iter->key().ToString()<<std::endl;
    std::cout<<"user_value = "<<values[i]<<"get_value = "<<nvm_partition_iter->value().ToString()<<std::endl;
    ASSERT_EQ(nvm_partition_iter->value(),values[i]);
    i++;
    nvm_partition_iter->Next();
  }
  // 为什么需要这一步？为了让tableReader在cache前析构，这样其析构函数中就可以调用table_cache的cache_的release方法了...
  // WQTODO 未来整个大项目，这里怎么办.......
  table_reader.reset();
  }
  }

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}