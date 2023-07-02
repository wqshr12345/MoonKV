//
//  discrete_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DISCRETE_GENERATOR_H_
#define YCSB_C_DISCRETE_GENERATOR_H_

#include "generator.h"

#include <atomic>
#include <cassert>
#include <vector>
#include "utils.h"

namespace ycsbc {

template <typename Value>
class DiscreteGenerator : public Generator<Value> {
 public:
  DiscreteGenerator() : sum_(0) { }
  DiscreteGenerator(int total_number) : sum_(0) { }
  void AddValue(Value value, double weight);

  Value Next();
  Value GetValueFromOps(double ops_percent);
  Value Last() { return last_; }

 private:
  std::vector<std::pair<Value, double>> values_;
  std::vector<std::pair<Value, double>> values_total_double_; // 上面这个vector的double的累加值 
  int total_numbers_;
  double sum_;
  std::atomic<Value> last_;
};

template <typename Value>
inline void DiscreteGenerator<Value>::AddValue(Value value, double weight) {
  if (values_.empty()) {
    last_ = value;
  }
  values_.push_back(std::make_pair(value, weight));
  values_total_double_.push_back(std::make_pair(value,sum_+weight));
  sum_ += weight;
}

template <typename Value>
inline Value DiscreteGenerator<Value>::Next() {
  double chooser = utils::ThreadLocalRandomDouble();

  for (auto p = values_.cbegin(); p != values_.cend(); ++p) {
    if (chooser < p->second / sum_) {
      return last_ = p->first;
    }
    chooser -= p->second / sum_;
  }

  assert(false);
  return last_;
}

template <typename Value>
inline Value DiscreteGenerator<Value>::GetValueFromOps(double ops_percent) {
  thread_local int temp_index_ = 0; // 只有在第一次被访问的时候记为0 和函数内的static是同样的行为
  assert(ops_percent<=1);
  while(ops_percent > values_total_double_[temp_index_].second / sum_) {
    temp_index_ ++;
  }
  return values_total_double_[temp_index_].first;
}

} // ycsbc

#endif // YCSB_C_DISCRETE_GENERATOR_H_
