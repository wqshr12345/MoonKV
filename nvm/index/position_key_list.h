#pragma once

#include <iostream>
#include <string>
#include <vector>
#include "folly/concurrency/ConcurrentHashMap.h"

namespace ROCKSDB_NAMESPACE {


struct PositionInfo {
  uint32_t LowerLevelPos;
  uint32_t HigherLevelPos;
  std::string PositionKey;

  bool operator < (const PositionInfo &pos_info) const
  {
    return (LowerLevelPos < pos_info.LowerLevelPos) ||
           (LowerLevelPos == pos_info.LowerLevelPos && PositionKey.compare(pos_info.PositionKey) < 0);
  }

  PositionInfo(std::string &str, uint32_t llp, uint32_t hlp)
      : LowerLevelPos(llp), HigherLevelPos(hlp), PositionKey(str) {}
};


class PositionKeyList {
  typedef std::vector<PositionInfo> PosKeyList;

  PosKeyList list_;
  uint32_t parent_file;
  uint32_t child_file;

 public:
  PositionInfo& front() {
    return list_.front();
  }

  PositionInfo& back() {
    return list_.back();
  }

  uint32_t parent() {
    return parent_file;
  }

  uint32_t child() {
    return child_file;
  }
  
  PositionInfo & operator[](int i)
  {
    return list_[i];
  }

  PositionKeyList(uint32_t pfs, uint32_t cfs)
      : parent_file(pfs), child_file(cfs) {
  }

  uint32_t Size() { return list_.size(); }

  void Clear() { list_.clear(); }

  void AddPosKey(std::string key, uint32_t llp, uint32_t hlp) {
    auto n = std::lower_bound(list_.begin(), list_.end(), PositionInfo{key, llp, hlp});
    if ((n != list_.end()) && ((*n).LowerLevelPos == llp)) {
      if((*n).PositionKey.compare(key) < 0){
        (*n).PositionKey = key;
      }
    } else if ((n != list_.end()) && ((*n).HigherLevelPos == hlp)) {
      if ((*n).LowerLevelPos < llp) {
        (*n).LowerLevelPos = llp;
        (*n).PositionKey = key;
      }
    }else {
      list_.insert(n, PositionInfo{key, llp, hlp});
    }
  }

  int GetEstimatePos(std::string key, uint32_t llp) {
    auto next = std::lower_bound(list_.begin(), list_.end(), PositionInfo{key, llp, 0});
    if (next == list_.begin()) return 0;
    auto prev = next - 1;

    double low_level_pos = (double)(llp - prev->LowerLevelPos) / (double)(next->LowerLevelPos - prev->LowerLevelPos);
    uint32_t high_level_estimate_pos = uint32_t(low_level_pos * (next->HigherLevelPos - prev->HigherLevelPos)) + prev->HigherLevelPos;
    if (high_level_estimate_pos > next->HigherLevelPos) {
      high_level_estimate_pos = next->HigherLevelPos;
    }
    return high_level_estimate_pos;
  }

};






}