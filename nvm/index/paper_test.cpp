//
// Created by Lluvia on 2023/5/6.
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
#include <dirent.h>
#include <vector>
#include <random>
#include "btree_node.h"
#include "clipp.h"

std::string dir = "";
uint64_t file_num = 0;
uint64_t file_seq = 0;
uint64_t for_times = 0;

namespace ROCKSDB_NAMESPACE {


class NumberRandom {
  std::default_random_engine e;
  std::uniform_int_distribution<int> u;

 public:
  NumberRandom() : u(0, 1000000) {
    e.seed(time(nullptr));
  }

  int GetRandom(int max) {
    return u(e) % max;
  }
};

class TimeStatistic {
  uint64_t sum;
  uint64_t begin;
  uint64_t count;
  std::shared_ptr<SystemClock> clock;

 public:
  TimeStatistic() : sum(0), begin(0), count(0), clock(SystemClock::Default()) {}
  ~TimeStatistic() = default;

  void Begin() {
    begin = clock->NowNanos();
  }

  void End() {
    sum += clock->NowNanos() - begin;
    count++;
    begin = 0;
  }

  double Result() const {
    return count == 0? 0 : double(sum) / double(count);
  }

  std::string ToString() const {
    return " SUM: " + std::to_string(sum) +
           " COUNT: " +  std::to_string(count) +
           " AVG: " + std::to_string(Result());
  }
};

void getFiles(std::string path, std::vector<uint64_t>& file_seq)
{
  std::vector<std::string> filenames;
  DIR *pDir;
  struct dirent* ptr;
  if(!(pDir = opendir(path.c_str()))){
    std::cout<<"Folder doesn't Exist!"<<std::endl;
    return;
  }
  while((ptr = readdir(pDir))!=0) {
    if (strcmp(ptr->d_name, ".") != 0 && strcmp(ptr->d_name, "..") != 0){
      filenames.push_back(path + "/" + ptr->d_name);
    }
  }
  closedir(pDir);

  // 将filenames转成file_seq
  for (const auto &item : filenames) {
    std::string file_seq_str = item.substr(dir.size()+1, 6);
    file_seq.push_back(std::stoul(file_seq_str));
  }
}

BtreeIterator* OpenBtreeIndex(std::string dir, int file_seq) {
  static ReadOptions rop;
  static DBOptions dbop;
  static ColumnFamilyOptions cfop;
  static ImmutableOptions iop(dbop, cfop);
  iop.db_nvm_dir = dir;
  static InternalKeyComparator icomp(BytewiseComparator());
  NvmBtree *nbtree = new NvmBtree(file_seq, rop, iop, icomp);
  BtreeIterator* iter = static_cast<BtreeIterator*>(nbtree->NewIterator(
      rop, nullptr, nullptr, true, TableReaderCaller::kUserGet, 0, false));
  return iter;
}

void ExecOneSeek(std::vector<std::string> &read_key, BtreeIterator* seek, TimeStatistic &ts) {
  static NumberRandom rand;
  // 先取出key，再统计时长
  Slice key = read_key[rand.GetRandom(read_key.size())];

  ts.Begin();
  seek->Seek(key);
  ts.End();
}

TEST(AnalysisBtreeTest, AnalysisBtreeTest_iterator) {
  // 1. 先解析出传入参数的文件

  std::vector<uint64_t>* file_list;
  if (file_seq == 0) {
    std::vector<uint64_t> file_list_all;
    // 获取该路径下的所有文件
    getFiles(dir, file_list_all);
    file_list = new std::vector<uint64_t>(file_list_all.begin(), file_list_all.begin() +
                                                                     (int)(file_num < file_list_all.size()? file_num: file_list_all.size()));
  } else {
    file_list = new std::vector<uint64_t>();
    file_list->push_back(file_seq);
  }

  for(auto seq : *file_list) {
    std::cout << seq << std::endl;
  }

  // 2. 打开b+tree index文件返回iterator
  std::vector<BtreeIterator*> iters;
  std::vector<BtreeIterator*> seeks;
  for (const auto &item : *file_list) {
    iters.push_back(OpenBtreeIndex("/data/read", (int)item));
    seeks.push_back(OpenBtreeIndex(dir, (int)item));
  }

  // 将所有的key读出，存储在vector中，方便后面的随机查询操作
  std::vector<std::vector<std::string>> read_key(file_list->size());
  for (int i = 0; i < file_list->size(); ++i) {
    while (iters[i]->Valid()) {
      read_key[i].push_back(iters[i]->key().ToString());
    }
  }

  // 3. 顺序查询所有的key，统计平均时间
  std::vector<TimeStatistic> ts(file_list->size());

  int count = for_times;
  while (count != 0) {
    count--;
    for (int i = 0; i < file_list->size(); ++i) {
      ExecOneSeek(read_key[i], seeks[i], ts[i]);
    }
  }

  // 4. 随机查询一些key，统计平均时间

  // 5. 打印结果
  //  for (int i = 0; i < file_list->size(); ++i) {
  //    std::cout << " File: " << std::to_string((*file_list)[i])
  //              << " Levels: " << std::to_string(seeks[i]->GetLevels())
  //              << ts[i].ToString() << std::endl;
  //  }

  struct result {
    int level;
    int count;
    double time_sum;
  };

  std::vector<result> result_map(10);
  for (int i = 0; i < result_map.size(); ++i) {
    result_map[i].level = i;
  }

  for (int i = 0; i < file_list->size(); ++i) {
    result_map[seeks[i]->GetLevels()].count++;
    result_map[seeks[i]->GetLevels()].time_sum += ts[i].Result();
  }

  for (int i = 0; i < result_map.size(); ++i) {
    if (result_map[i].count != 0) {
      std::cout << "Levels: " << std::to_string(result_map[i].level)
                << " COUNT: " +  std::to_string(result_map[i].count)
                << " AVG: " + std::to_string(result_map[i].time_sum / result_map[i].count) << std::endl;
    }
  }

}


}



int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  auto cli = (
      clipp::value("DB dir", dir),
      clipp::option("-n") & clipp::value("file numbers", file_num),
      clipp::option("-file") & clipp::value("file sequence", file_seq),
      clipp::option("-for") & clipp::value("for times", for_times)
  );
  if(!parse(argc, argv, cli)) std::cout << make_man_page(cli, argv[0]);

  return RUN_ALL_TESTS();
}
