//
// Created by Lluvia on 2023/5/6.
//
#include "btree_builder.h"
#include "btree_iterator.h"
#include "gtest/gtest.h"
#include "port/stack_trace.h"
#include "rocksdb/slice.h"
#include <unistd.h>
#include <dirent.h>
#include <vector>
#include <random>
#include <cmath>
#include <algorithm>
#include "btree_node.h"
#include "position_key_list.h"
#include "clipp.h"

std::string dir = "";
int root_file;
std::vector<int> child_files;
std::string special_search_key = "";

namespace ROCKSDB_NAMESPACE {


class NumberRandom {
  std::default_random_engine e;
  std::uniform_int_distribution<int> u;

 public:
  NumberRandom() : u(0, 1000000000) {
    e.seed(time(nullptr));
  }

  int GetRandom(int max) {
    return u(e) % max;
  }
};


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

class SeekStatistic {
  uint32_t levels;
  uint32_t seek_sum;
  uint32_t seek_count;
  std::vector<uint32_t> excel;

 public:
  explicit SeekStatistic(uint32_t levels) : levels(levels), seek_sum(0), seek_count(0) {
    for (int i = 0; i < 500; ++i) {
      excel.push_back(0);
    }
  }

  void Add(uint32_t n) {
    seek_sum += n;
    seek_count++;
    excel[n]++;
  }

  uint32_t GetLevels() const {
    return levels;
  };

  double AverageSeekLevels() const {
    return (double)seek_sum / (double)seek_count;
  }

  std::string ExcelResult() {
    std::string result = "分布情况：\n";
    for (int i = 1; i < excel.size(); ++i) {
      if (excel[i] != 0)
      {
        result.append("\tl"+std::to_string(i)+"->"+std::to_string(excel[i])+"\n");
      }
    }
    return result;
  }
};

// TEST(EstimateSeekTest, EstimateSeekTest_2) {
//   // 1. 打开b+tree index文件
//   BtreeIterator* root = OpenBtreeIndex(dir, root_file);
//   uint32_t pos_begin, pos_end;
//   int32_t block_begin, block_end;

//   root->SeekToFirst();
//   pos_begin = root->GetKeyPosition();
//   block_begin = root->GetNodePtr();

//   root->SeekToLast();
//   pos_end = root->GetKeyPosition();
//   block_end = root->GetNodePtr();

//   root->SeekToFirst();
//   while (root->Valid()) {
//     double estimate_pos = root->GetKeyPosition(pos_begin, pos_end, root->GetKeyPosition());
//     uint32_t estimate_block = uint32_t(estimate_pos * block_end);
//     if (estimate_block != root->GetNodePtr()) {
//       std::cout << "Real:" << root->GetNodePtr() << " Estimate:" << estimate_block << std::endl;
//     }
//     root->Next();
//   }
// }


TEST(EstimateSeekTest, EstimateSeekTest_iterator) {
  // 1. 打开b+tree index文件
  BtreeIterator* root = OpenBtreeIndex(dir, root_file);
  std::vector<BtreeIterator*> child;
  child.reserve(child_files.size());
  for (const auto &item : child_files) {
    child.push_back(OpenBtreeIndex(dir, (int)item));
  }
  // 增加临时测试
  int times = child[1]->EstimateSeek("user16977777002128759094", 50017, LinearDetection);
  int real_block = child[1]->GetKeyPosition(3);

  std ::cout << "times "<< times << "real blocks" << real_block << std::endl;

  //   times = child[0]->EstimateSeek("user16581709563861968841", 0, LinearDetection);
  // real_block = child[0]->GetKeyPosition(3);

  // std ::cout << "real blocks" << real_block << std::endl;
  std::vector<std::string> keys {"user1652571064775785055100000000", "user1658170956386196884100000000", "user1764142420659973042300000000"};
  for (int i = 0; i < keys.size(); i++) {
  times = child[1]->EstimateSeek(keys[i], 5000, LinearDetection);
  real_block = child[1]->GetKeyPosition(3);

  std ::cout << "real blocks " << real_block << std::endl;

  }


  times = root->EstimateSeek("user1658170956386196884100000000", 0, LinearDetection);
  int father_position = root->GetKeyPosition(0);

  std ::cout << "father position" << father_position << std::endl;

  // times = child[3]->EstimateSeek("user16581709563861968841", 0, LinearDetection);
  // real_block = child[3]->GetKeyPosition(3);

  std ::cout << "real blocks" << real_block << std::endl;

  times = child[2]->EstimateSeek("user6499663864101946726", 48753, LinearDetection);
  real_block = child[2]->GetKeyPosition(3);

  std ::cout << "times "<< times << "real blocks" << real_block << std::endl;

  times = child[3]->EstimateSeek("user9337525935813647613", 34252, LinearDetection);
  real_block = child[3]->GetKeyPosition(3);

  std ::cout << "times "<< times << "real blocks" << real_block << std::endl;

  // std ::cout << "times "<< child[2]->EstimateSeek("user6499663864101946726", 48753, LinearDetection) << std::endl;
  // std ::cout << "times "<< child[3]->EstimateSeek("user9337525935813647613", 34252, LinearDetection) << std::endl;

  // 2. 先找到上下层文件之间的映射关系，核心是找到下层文件的边界值对应上层文件的NUMBER
  std::vector<std::pair<std::string, std::string>> edge_keys;
  std::vector<std::pair<uint32_t, uint32_t>> edge_pos;
  std::vector<SeekStatistic> stat;
  std::vector<PositionKeyList> pos_list;

  for (const auto &item : child) {
    item->SeekToFirst();
    std::string left_key = item->key().ToString();
    uint32_t level3_left_block = item->GetKeyPosition(3);
    item->SeekToLast();
    std::string right_key = item->key().ToString();
    uint32_t level3_right_block = item->GetKeyPosition(3);

    root->Seek(left_key);
    if (!root->Valid())
    {
      root->SeekToFirst();
    }
    uint32_t left_pos = root->GetKeyPosition(0);

    root->Seek(right_key);
    if (!root->Valid())
    {
      root->SeekToLast();
    }
    uint32_t right_pos = root->GetKeyPosition(0);

    edge_keys.emplace_back(left_key, right_key);
    edge_pos.emplace_back(left_pos, right_pos);

    stat.emplace_back(item->GetLevels());
    pos_list.emplace_back(0,0);
    pos_list.back().AddPosKey(left_key, left_pos, level3_left_block);
    pos_list.back().AddPosKey(right_key, right_pos, level3_right_block);
  }

  std::cout << "准备阶段结束" << std::endl;

  // 3. 执行查询的操作，这里先设计成遍历查询所有key的办法
  InternalKeyComparator icomp(BytewiseComparator());
  std::vector<std::string> read_key;
  root->SeekToFirst();
  while (root->Valid()) {
    read_key.push_back(root->key().ToString());
    root->Next();
  }
  NumberRandom rand;

  // 先随机做一些查询，预热数据
  root->SeekToFirst();
  for (int i = 0; i < 50000; ++i) {
    // 先取出key，再统计时长
    int p = rand.GetRandom(read_key.size());
    root->Seek(read_key[p]);
    Slice search_key = root->key();
    for (int i = 0; i < edge_keys.size(); ++i) {
      if (icomp.Compare(edge_keys[i].first, search_key) <= 0 &&
          icomp.Compare(search_key, edge_keys[i].second) <= 0) {
        int estimate_pos2 = pos_list[i].GetEstimatePos(search_key.ToString(), root->GetKeyPosition(0));

        uint32_t seek_level2 = child[i]->EstimateSeek(search_key, estimate_pos2, LinearDetection);
        if (seek_level2 >= 5) {
          pos_list[i].AddPosKey(search_key.ToString(), root->GetKeyPosition(0), child[i]->GetKeyPosition(3));
        }
        break;
      }
    }
  }


  // 执行正常的查询操作
  root->SeekToFirst();
  while (root->Valid()) {
    if (special_search_key.size() >0)
    {
      root->Seek(special_search_key);
    }
    Slice search_key = root->key();
    for (int i = 0; i < edge_keys.size(); ++i) {
      if (icomp.Compare(edge_keys[i].first, search_key) <= 0 &&
          icomp.Compare(search_key, edge_keys[i].second) <= 0) {
        int estimate_pos2 = pos_list[i].GetEstimatePos(search_key.ToString(), root->GetKeyPosition(0));
        // double estimate_pos = root->GetKeyPosition(edge_pos[i].first, edge_pos[i].second, root->GetKeyPosition());

        // uint32_t seek_level = child[i]->EstimateSeek(search_key, estimate_pos, LinearDetection);
        uint32_t seek_level2 = child[i]->EstimateSeek(search_key, estimate_pos2, LinearDetection);
        if (seek_level2 >= 7) {
          // pos_list[i].AddPosKey(search_key.ToString(), root->GetKeyPosition(), child[i]->GetKeyPosition(3));
        }
        /* 统计预测值和实际block的偏差：
         * 这里需要注意，按照estimate中的计算结果来计算预测值，需要取PosKeySkipList的最左侧和最右侧的值来计算
         */
        // int tmp = child[i]->GetKeyPosition(3) - (int)estimate_pos2;
        // if (tmp >= 0) {
        //   stat[i].Add(tmp);
        // } else {
        //   stat[i].Add(-tmp);
        // }
        stat[i].Add(seek_level2);
        break;
      }
    }
    root->Next();
  }

  // 4. 打印结果
  for (int i = 0; i < stat.size(); ++i) {
    std::cout << "File:" << child_files[i] << " Level:" << stat[i].GetLevels()
              << " SeekLevel: " << stat[i].AverageSeekLevels() << std::endl;

    std::cout << stat[i].ExcelResult() << std::endl;
  }

  for (int i = 0; i < pos_list.size(); i++)
  {
    std::cout << "Position List Size: " << i << " -> " << pos_list[i].Size()
              << std::endl;

    for (int j = 0; j < pos_list[i].Size() && j < 100; j++)
    {
      std::cout << "LowerLevelPos:" << pos_list[i][j].LowerLevelPos
                << "HigherLevelPos:" << pos_list[i][j].HigherLevelPos
                << "\tKey:" << pos_list[i][j].PositionKey << std::endl;
    }
  }
}


}



int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  auto cli = (
      clipp::value("DB dir", dir),
      clipp::option("-rfs") & clipp::value("root file", root_file),
      clipp::required("-cfs") & clipp::values("child files", child_files),
      clipp::option("-ssk") & clipp::value("special search key", special_search_key)
  );
  if(!parse(argc, argv, cli)) std::cout << make_man_page(cli, argv[0]);

  return RUN_ALL_TESTS();
}
