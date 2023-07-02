//
// Created by Lluvia on 2023/2/13.
//

#include <algorithm>
#include <memory>
#include <string>
#include <iostream>

#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/merge_operators.h"
#include "rocksdb/slice.h"
#include "util/coding.h"

using namespace ROCKSDB_NAMESPACE;

namespace {

class YCSBUpdateMergeOperator : public rocksdb::AssociativeMergeOperator {
 private:
  struct Field {
    std::string name;
    std::string value;
  };

  static void DeserializeRow(std::vector<Field> &values, const char *p,
                             const char *lim) {
    // std::cout<<"okk"<<std::endl;
    while (p != lim) {
      assert(p < lim);
      uint32_t len = *reinterpret_cast<const uint32_t *>(p);
      p += sizeof(uint32_t);
      std::string field(p, static_cast<const size_t>(len));
      p += len;
      len = *reinterpret_cast<const uint32_t *>(p);
      p += sizeof(uint32_t);
      std::string value(p, static_cast<const size_t>(len));
      p += len;
      values.push_back({field, value});
    }
  }

  static void DeserializeRow(std::vector<Field> &values,
                             const std::string &data) {
    const char *p = data.data();
    const char *lim = p + data.size();
    DeserializeRow(values, p, lim);
  }

  static void SerializeRow(const std::vector<Field> &values,
                           std::string &data) {
    for (const Field &field : values) {
      uint32_t len = field.name.size();
      data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
      data.append(field.name.data(), field.name.size());
      len = field.value.size();
      data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
      data.append(field.value.data(), field.value.size());
    }
  }

 public:
  bool Merge(const Slice &key, const Slice *existing_value, const Slice &value,
             std::string *new_value, Logger *logger) const override {
    if (!existing_value) {
      // No existing_value. Set *new_value = value
      new_value->assign(value.data(), value.size());
      return true;
    }

    // 暂时有反序列化失败的问题...
    std::vector<Field> values;
    const char *p = existing_value->data();
    const char *lim = p + existing_value->size();
    DeserializeRow(values, p, lim);

    std::vector<Field> new_values;
    p = value.data();
    lim = p + value.size();
    DeserializeRow(new_values, p, lim);

    for (Field &new_field : new_values) {
      bool found = false;
      for (Field &field : values) {
        if (field.name == new_field.name) {
          found = true;
          field.value = new_field.value;
          break;
        }
      }
      if (!found) {
        values.push_back(new_field);
      }
    }
    // *new_value = value.ToString();
    SerializeRow(values, *new_value);
    return true;
  }

  static const char *kClassName() { return "YCSBUpdateMergeOperator"; }
  static const char *kNickName() { return "ycsbupdate"; }
  const char *Name() const override { return kClassName(); }
};

}  // namespace

namespace ROCKSDB_NAMESPACE {

std::shared_ptr<MergeOperator> MergeOperators::CreateYCSBUpdateMergeOperator() {
  return std::make_shared<YCSBUpdateMergeOperator>();
}

}  // namespace ROCKSDB_NAMESPACE
