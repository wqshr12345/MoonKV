//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "rocksdb_db.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "core/properties.h"
#include "core/utils.h"

#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/write_batch.h>
#ifdef matrixkv
#include <rocksdb/table.h>
#include <rocksdb/merge_operators.h>
#endif
#include <iostream>

#define USE_MERGEUPDATE
namespace {
  const std::string PROP_NAME = "rocksdb.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_FORMAT = "rocksdb.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_MERGEUPDATE = "rocksdb.mergeupdate";
  const std::string PROP_MERGEUPDATE_DEFAULT = "false";

  const std::string PROP_DESTROY = "rocksdb.destroy";
  const std::string PROP_DESTROY_DEFAULT = "false";

  const std::string PROP_COMPRESSION = "rocksdb.compression";
  const std::string PROP_COMPRESSION_DEFAULT = "no";

  const std::string PROP_MAX_BG_JOBS = "rocksdb.max_background_jobs";
  const std::string PROP_MAX_BG_JOBS_DEFAULT = "0";

  const std::string PROP_TARGET_FILE_SIZE_BASE = "rocksdb.target_file_size_base";
  const std::string PROP_TARGET_FILE_SIZE_BASE_DEFAULT = "0";

  const std::string PROP_TARGET_FILE_SIZE_MULT = "rocksdb.target_file_size_multiplier";
  const std::string PROP_TARGET_FILE_SIZE_MULT_DEFAULT = "0";

  const std::string PROP_MAX_BYTES_FOR_LEVEL_BASE = "rocksdb.max_bytes_for_level_base";
  const std::string PROP_MAX_BYTES_FOR_LEVEL_BASE_DEFAULT = "0";

  const std::string PROP_WRITE_BUFFER_SIZE = "rocksdb.write_buffer_size";
  const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "0";

  const std::string PROP_MAX_WRITE_BUFFER = "rocksdb.max_write_buffer_number";
  const std::string PROP_MAX_WRITE_BUFFER_DEFAULT = "0";

  const std::string PROP_COMPACTION_PRI = "rocksdb.compaction_pri";
  const std::string PROP_COMPACTION_PRI_DEFAULT = "-1";

  const std::string PROP_MAX_OPEN_FILES = "rocksdb.max_open_files";
  const std::string PROP_MAX_OPEN_FILES_DEFAULT = "-1";

  const std::string PROP_L0_COMPACTION_TRIGGER = "rocksdb.level0_file_num_compaction_trigger";
  const std::string PROP_L0_COMPACTION_TRIGGER_DEFAULT = "0";

  const std::string PROP_L0_SLOWDOWN_TRIGGER = "rocksdb.level0_slowdown_writes_trigger";
  const std::string PROP_L0_SLOWDOWN_TRIGGER_DEFAULT = "0";

  const std::string PROP_L0_STOP_TRIGGER = "rocksdb.level0_stop_writes_trigger";
  const std::string PROP_L0_STOP_TRIGGER_DEFAULT = "0";

  const std::string PROP_USE_DIRECT_WRITE = "rocksdb.use_direct_io_for_flush_compaction";
  const std::string PROP_USE_DIRECT_WRITE_DEFAULT = "false";

  const std::string PROP_USE_DIRECT_READ = "rocksdb.use_direct_reads";
  const std::string PROP_USE_DIRECT_READ_DEFAULT = "false";

  const std::string PROP_REPORT_BG_IO_STATS = "rocksdb.report_bg_io_stats";
  const std::string PROP_REPORT_BG_IO_STATS_DEFAULT = "false";

  const std::string PROP_USE_MMAP_WRITE = "rocksdb.allow_mmap_writes";
  const std::string PROP_USE_MMAP_WRITE_DEFAULT = "false";

  const std::string PROP_USE_MMAP_READ = "rocksdb.allow_mmap_reads";
  const std::string PROP_USE_MMAP_READ_DEFAULT = "false";

  const std::string PROP_CACHE_SIZE = "rocksdb.cache_size";
  const std::string PROP_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_COMPRESSED_CACHE_SIZE = "rocksdb.compressed_cache_size";
  const std::string PROP_COMPRESSED_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_BLOB_CACHE_SIZE = "rocksdb.blob_cache_size";
  const std::string PROP_BLOB_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_USE_PMEM_MAP = "rocksdb.use_pmem_map";
  const std::string PROP_USE_PMEM_MAP_DEFAULT = "false";

  const std::string PROP_BLOOM_BITS = "rocksdb.bloom_bits";
  const std::string PROP_BLOOM_BITS_DEFAULT = "0";

  const std::string PROP_INCREASE_PARALLELISM = "rocksdb.increase_parallelism";
  const std::string PROP_INCREASE_PARALLELISM_DEFAULT = "false";

  const std::string PROP_OPTIMIZE_LEVELCOMP = "rocksdb.optimize_level_style_compaction";
  const std::string PROP_OPTIMIZE_LEVELCOMP_DEFAULT = "false";

  const std::string PROP_OPTIONS_FILE = "rocksdb.optionsfile";
  const std::string PROP_OPTIONS_FILE_DEFAULT = "";

  const std::string PROP_ENV_URI = "rocksdb.env_uri";
  const std::string PROP_ENV_URI_DEFAULT = "";

  const std::string PROP_FS_URI = "rocksdb.fs_uri";
  const std::string PROP_FS_URI_DEFAULT = "";

  const std::string PROP_COMPACTION_STYLE = "rocksdb.compaction_style";
  const std::string PROP_COMPACTION_STYLE_DEFAULT = "";

  // 为nvm_rocksdb的额外option增加一些配置
  const std::string PROP_DB_NVM_DIR = "rocksdb.db_nvm_dir";
  const std::string PROP_DB_NVM_DIR_DEFAULT = "";

  const std::string PROP_USE_DB_STATS = "rocksdb.use_db_stats";
  const std::string PROP_USE_DB_STATS_DEFAULT = "false";

  const std::string PROP_MERGE_OPERATOR = "rocksdb.merge_operator";
  const std::string PROP_MERGE_OPERATOR_DEFAULT = "";

  const std::string PROP_BLOCK_ALIGN = "rocksdb.block_align";
  const std::string PROP_BLOCK_ALIGN_DEFAULT = "false";

  const std::string PROP_VERIFY_CHECKSUM = "rocksdb.verify_checksum";
  const std::string PROP_VERIFY_CHECKSUM_DEFAULT = "true";

  const std::string PROP_MAX_SUCCESSIVE_MERGES = "rocksdb.max_successive_merges";
  const std::string PROP_MAX_SUCCESSIVE_MERGES_DEFAULT = "0";

  const std::string PROP_MAX_GC_COMPACTIONS = "rocksdb.max_gc_compactions";
  const std::string PROP_MAX_GC_COMPACTIONS_DEFAULT = "5";

  const std::string PROP_GC_MULTIPLE = "rocksdb.gc_multiple";
  const std::string PROP_GC_MULTIPLE_DEFAULT = "1.0";

  const std::string PROP_GC_GARBAGE_THRESHOLD = "rocksdb.gc_garbage_threshold";
  const std::string PROP_GC_GARBAGE_THRESHOLD_DEFAULT = "0.3";

  const std::string PROP_GC_RANGE_TABLES_THRESHOLD = "rocksdb.gc_range_tables_threshold";
  const std::string PROP_GC_RANGE_TABLES_THRESHOLD_DEFAULT = "10";

  const std::string PROP_PICK_TABLE_THRESHOLD = "rocksdb.pick_table_threshold";
  const std::string PROP_PICK_TABLE_THRESHOLD_DEFAULT = "0.70";

  const std::string PROP_PICK_TABLE_MAX_MULTIPLE = "rocksdb.pick_table_max_multiple";
  const std::string PROP_PICK_TABLE_MAX_MULTIPLE_DEFAULT = "2.0";

  const std::string PROP_ENABLE_VERTICAL_COMPACTION = "rocksdb.enable_vertical_compaction";
  const std::string PROP_ENABLE_VERTICAL_COMPACTION_DEFAULT = "true";

  const std::string PROP_VC_MERGE_THRESHOLD = "rocksdb.vc_merge_threshold";
  const std::string PROP_VC_MERGE_THRESHOLD_DEFAULT = "0.5";

  const std::string PROP_VC_MERGE_THRESHOLD_NOSTART = "rocksdb.vc_merge_threshold_nostart";
  const std::string PROP_VC_MERGE_THRESHOLD_NOSTART_DEFAULT = "0.1";

  const std::string PROP_VERTICAL_START_LEVEL = "rocksdb.vertical_start_level";
  const std::string PROP_VERTICAL_START_LEVEL_DEFAULT = "2";

  const std::string PROP_VERTICAL_MIN_INVOLVE_LEVELS = "rocksdb.vertical_min_involve_levels";
  const std::string PROP_VERTICAL_MIN_INVOLVE_LEVELS_DEFAULT = "2";

  const std::string PROP_PICK_INDEX_MIN_TABLE_NUMBERS = "rocksdb.pick_index_min_table_numbers";
  const std::string PROP_PICK_INDEX_MIN_TABLE_NUMBERS_DEFAULT = "3";

  const std::string PROP_CHOOSE_GUARD_INTERVAL = "rocksdb.choose_guard_interval";
  const std::string PROP_CHOOSE_GUARD_INTERVAL_DEFAULT = "2000";

  const std::string PROP_SUBCOMPACTIONS = "rocksdb.subcompactions";
  const std::string PROP_SUBCOMPACTIONS_DEFAULT = "1";

  const std::string PROP_STATS_DUMP_PERIOD_SEC = "rocksdb.stats_dump_period_sec";
  const std::string PROP_STATS_DUMP_PERIOD_SEC_DEFAULT = "600";

  const std::string PROP_NEIGHBOR_GUARD_RATIO = "rocksdb.neighbor_guard_ratio";
  const std::string PROP_NEIGHBOR_GUARD_RATIO_DEFAULT = "3";

  const std::string PROP_ENABLE_ESTIMATE_SEEK = "rocksdb.enable_estimate_seek";
  const std::string PROP_ENABLE_ESTIMATE_SEEK_DEFAULT = "true";

  const std::string PROP_ESTIMATE_INTERVAL = "rocksdb.estimate_interval";
  const std::string PROP_ESTIMATE_INTERVAL_DEFAULT = "50";

  // 为blobdb增加的一些额外配置
  const std::string PROP_ENABLE_BLOB_FILES = "rocksdb.enable_blob_files";
  const std::string PROP_ENABLE_BLOB_FILES_DEFAULT = "false";

  // const std::string PROP_MIN_BLOB_SIZE = "rocksdb.min_blob_size";
  // const std::string PROP_MIN_BLOB_SIZE_DEFAULT = "0";

  const std::string PROP_ENABLE_BLOB_GARBAGE_COLLECTION = "rocksdb.enable_blob_garbage_collection";
  const std::string PROP_ENABLE_BLOB_GARBAGE_COLLECTION_DEFAULT = "false";


  static std::shared_ptr<rocksdb::Env> env_guard;
  static std::shared_ptr<rocksdb::Cache> block_cache;
  static std::shared_ptr<rocksdb::Cache> block_cache_compressed;
  static std::shared_ptr<rocksdb::Cache> blob_cache;
} // anonymous

namespace ycsbc {
rocksdb::DB *RocksdbDB::db_ = nullptr;
int RocksdbDB::ref_cnt_ = 0;
std::mutex RocksdbDB::mu_;

void RocksdbDB::Init() {
// merge operator disabled by default due to link error
// #ifdef USE_MERGEUPDATE
  // class YCSBUpdateMerge : public rocksdb::AssociativeMergeOperator {
  //  public:
  //   virtual bool Merge(const rocksdb::rocksdb::Slice &key, const rocksdb::rocksdb::Slice *existing_value,
  //                      const rocksdb::rocksdb::Slice &value, std::string *new_value,
  //                      rocksdb::rocksdb::Logger *logger) const override {
  //     assert(existing_value);

  //     std::vector<Field> values;
  //     const char *p = existing_value->data();
  //     const char *lim = p + existing_value->size();
  //     DeserializeRow(values, p, lim);

  //     std::vector<Field> new_values;
  //     p = value.data();
  //     lim = p + value.size();
  //     DeserializeRow(new_values, p, lim);

  //     for (Field &new_field : new_values) {
  //       bool found = false;
  //       for (Field &field : values) {
  //         if (field.name == new_field.name) {
  //           found = true;
  //           field.value = new_field.value;
  //           break;
  //         }
  //       }
  //       if (!found) {
  //         values.push_back(new_field);
  //       }
  //     }

  //     SerializeRow(values, *new_value);
  //     return true;
  //   }

  //   virtual const char *Name() const override {
  //     return "YCSBUpdateMerge";
  //   }
  // };
// #endif
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  const std::string format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  if (format == "single") {
    format_ = kSingleRow;
    method_read_ = &RocksdbDB::ReadSingle;
    method_scan_ = &RocksdbDB::ScanSingle;
    method_update_ = &RocksdbDB::UpdateSingle;
    method_insert_ = &RocksdbDB::InsertSingle;
    method_delete_ = &RocksdbDB::DeleteSingle;
    method_merge_ = &RocksdbDB::MergeSingle;
// #ifdef USE_MERGEUPDATE
//     if (props.GetProperty(PROP_MERGEUPDATE, PROP_MERGEUPDATE_DEFAULT) == "true") {
//       method_update_ = &RocksdbDB::MergeSingle;
//     }
// #endif
  } else {
    throw utils::Exception("unknown format");
  }
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  ref_cnt_++;
  if (db_) {
    return;
  }

  const std::string &db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("RocksDB db path is missing");
  }

  rocksdb::Options opt;
  opt.create_if_missing = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
  GetOptions(props, &opt, &cf_descs);
// #ifdef USE_MERGEUPDATE
//   opt.merge_operator.reset(new YCSBUpdateMerge);
// #endif

  rocksdb::Status s;
  if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
    s = rocksdb::DestroyDB(db_path, opt);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB DestroyDB: ") + s.ToString());
    }
  }
  if (cf_descs.empty()) {
    s = rocksdb::DB::Open(opt, db_path, &db_);
  } else {
    s = rocksdb::DB::Open(opt, db_path, cf_descs, &cf_handles, &db_);
  }
  // 修复内存不释放bug
  for (auto& c : cf_handles) {
    delete c;
  }
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Open: ") + s.ToString());
  }
}

void RocksdbDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  delete db_;
}

void RocksdbDB::GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                           std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs) {
  std::string env_uri = props.GetProperty(PROP_ENV_URI, PROP_ENV_URI_DEFAULT);
  std::string fs_uri = props.GetProperty(PROP_FS_URI, PROP_FS_URI_DEFAULT);
  rocksdb::Env* env =  rocksdb::Env::Default();;
  #ifndef matrixkv
  if (!env_uri.empty() || !fs_uri.empty()) {
    rocksdb::Status s = rocksdb::Env::CreateFromUri(rocksdb::ConfigOptions(),
                                                    env_uri, fs_uri, &env, &env_guard);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB CreateFromUri: ") + s.ToString());
    }
    opt->env = env;
  }
  #endif

  const std::string options_file = props.GetProperty(PROP_OPTIONS_FILE, PROP_OPTIONS_FILE_DEFAULT);
  if (options_file != "") {
    rocksdb::Status s = rocksdb::LoadOptionsFromFile(options_file, env, opt, cf_descs);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB LoadOptionsFromFile: ") + s.ToString());
    }
  } else {
    const std::string compression_type = props.GetProperty(PROP_COMPRESSION,
                                                           PROP_COMPRESSION_DEFAULT);
    if (compression_type == "no") {
      opt->compression = rocksdb::kNoCompression;
    } else if (compression_type == "snappy") {
      opt->compression = rocksdb::kSnappyCompression;
    } else if (compression_type == "zlib") {
      opt->compression = rocksdb::kZlibCompression;
    } else if (compression_type == "bzip2") {
      opt->compression = rocksdb::kBZip2Compression;
    } else if (compression_type == "lz4") {
      opt->compression = rocksdb::kLZ4Compression;
    } else if (compression_type == "lz4hc") {
      opt->compression = rocksdb::kLZ4HCCompression;
    } else if (compression_type == "xpress") {
      opt->compression = rocksdb::kXpressCompression;
    } else if (compression_type == "zstd") {
      opt->compression = rocksdb::kZSTD;
    } else {
      throw utils::Exception("Unknown compression type");
    }
    if(props.GetProperty(PROP_USE_DB_STATS, PROP_USE_DB_STATS_DEFAULT) == "true") {
      opt->statistics = rocksdb::CreateDBStatistics();
    }
    #ifndef matrixkv
    rocksdb::ConfigOptions config_options;
    config_options.ignore_unsupported_options = false;

    if(props.GetProperty(PROP_MERGE_OPERATOR, PROP_MERGE_OPERATOR_DEFAULT) != "") {
      rocksdb::MergeOperator::CreateFromString(config_options, props.GetProperty(PROP_MERGE_OPERATOR, PROP_MERGE_OPERATOR_DEFAULT), &opt->merge_operator);
    }
    #else
    if(props.GetProperty(PROP_MERGE_OPERATOR, PROP_MERGE_OPERATOR_DEFAULT) != "") {
      opt->merge_operator = rocksdb::MergeOperators::CreateFromStringId(props.GetProperty(PROP_MERGE_OPERATOR, PROP_MERGE_OPERATOR_DEFAULT));
    }
    #endif
    int val = std::stoi(props.GetProperty(PROP_MAX_BG_JOBS, PROP_MAX_BG_JOBS_DEFAULT));
    if (val != 0) {
      opt->max_background_jobs = val;
    }
    val = std::stoi(props.GetProperty(PROP_TARGET_FILE_SIZE_BASE, PROP_TARGET_FILE_SIZE_BASE_DEFAULT));
    if (val != 0) {
      opt->target_file_size_base = val;
    }
    val = std::stoi(props.GetProperty(PROP_TARGET_FILE_SIZE_MULT, PROP_TARGET_FILE_SIZE_MULT_DEFAULT));
    if (val != 0) {
      opt->target_file_size_multiplier = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_BYTES_FOR_LEVEL_BASE, PROP_MAX_BYTES_FOR_LEVEL_BASE_DEFAULT));
    if (val != 0) {
      opt->max_bytes_for_level_base = val;
    }
    val = std::stoi(props.GetProperty(PROP_WRITE_BUFFER_SIZE, PROP_WRITE_BUFFER_SIZE_DEFAULT));
    if (val != 0) {
      opt->write_buffer_size = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_WRITE_BUFFER, PROP_MAX_WRITE_BUFFER_DEFAULT));
    if (val != 0) {
      opt->max_write_buffer_number = val;
    }
    val = std::stoi(props.GetProperty(PROP_COMPACTION_PRI, PROP_COMPACTION_PRI_DEFAULT));
    if (val != -1) {
      opt->compaction_pri = static_cast<rocksdb::CompactionPri>(val);
    }
    val = std::stoi(props.GetProperty(PROP_MAX_OPEN_FILES, PROP_MAX_OPEN_FILES_DEFAULT));
    if (val != 0) {
      opt->max_open_files = val;
    }

    val = std::stoi(props.GetProperty(PROP_L0_COMPACTION_TRIGGER, PROP_L0_COMPACTION_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_file_num_compaction_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_L0_SLOWDOWN_TRIGGER, PROP_L0_SLOWDOWN_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_slowdown_writes_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_L0_STOP_TRIGGER, PROP_L0_STOP_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_stop_writes_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_SUBCOMPACTIONS, PROP_SUBCOMPACTIONS_DEFAULT));
    if (val != 0) {
      opt->max_subcompactions = val;
    }
    val = std::stoi(props.GetProperty(PROP_STATS_DUMP_PERIOD_SEC, PROP_STATS_DUMP_PERIOD_SEC_DEFAULT));
    if (val != 0) {
      opt->stats_dump_period_sec = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_SUCCESSIVE_MERGES, PROP_MAX_SUCCESSIVE_MERGES_DEFAULT));
    if (val != 0) {
      opt->max_successive_merges = val;
    }
    #ifdef BIND_TITANDB
    std::cout << "DiffKV!" << std::endl;
    opt->disable_background_gc = false;
    opt->target_file_size_base = 16<<20;
    opt->min_gc_batch_size = 32<<20;
    opt->max_gc_batch_size = 64<<20;
    val = std::stoi(props.GetProperty(PROP_MAX_SORTED_RUNS, PROP_MAX_SORTED_RUNS_DEFAULT));
    if (val != 0) {
      opt->max_sorted_runs = val;
    }
    opt->level_merge = true;
    opt->range_merge = true;
    opt->lazy_merge = true;
    opt->max_background_gc = 8; 
    opt->block_write_size = 0;
    opt->blob_file_discardable_ratio = 0.3;
    if(options.level_merge) {
      options.base_level_for_dynamic_level_bytes = 4;
      options.level_compaction_dynamic_level_bytes = true;
    }
    opt->intra_compact_small_l0 = false;
    opt->sep_before_flush = true;
    opt->max_background_jobs =8;
    opt->disable_auto_compactions = false;
    opt->mid_blob_size = 8192;
    opt->min_blob_size = 128;
        
    #endif
    #ifdef nvm_rocksdb
    std::cout<<"NVM!"<<std::endl;
    opt->db_nvm_dir = props.GetProperty(PROP_DB_NVM_DIR, PROP_DB_NVM_DIR_DEFAULT);
    std::cout<<opt->db_nvm_dir;
    double dou_val = std::stod(props.GetProperty(PROP_MAX_GC_COMPACTIONS, PROP_MAX_GC_COMPACTIONS_DEFAULT));
    if (val != 0) {
      opt->max_gc_compactions = val;
    }
    dou_val = std::stod(props.GetProperty(PROP_GC_MULTIPLE, PROP_GC_MULTIPLE_DEFAULT));
    if (dou_val != 0) {
      opt->gc_multiple = dou_val;
    }
    dou_val = std::stod(props.GetProperty(PROP_GC_GARBAGE_THRESHOLD, PROP_GC_GARBAGE_THRESHOLD_DEFAULT));
    if (dou_val != 0) {
      opt->gc_garbage_threshold = dou_val;
    }
    val = std::stoi(props.GetProperty(PROP_GC_RANGE_TABLES_THRESHOLD, PROP_GC_RANGE_TABLES_THRESHOLD_DEFAULT));
    if (val != 0) {
      opt->gc_range_tables_threshold = val;
    }
    dou_val = std::stod(props.GetProperty(PROP_PICK_TABLE_THRESHOLD, PROP_PICK_TABLE_THRESHOLD_DEFAULT));
    if (dou_val != 0) {
      opt->pick_table_threshold = dou_val;
    }
    dou_val = std::stod(props.GetProperty(PROP_PICK_TABLE_MAX_MULTIPLE, PROP_PICK_TABLE_MAX_MULTIPLE_DEFAULT));
    if (dou_val != 0) {
      opt->pick_table_max_multiple = dou_val;
    }
    dou_val = std::stod(props.GetProperty(PROP_VC_MERGE_THRESHOLD, PROP_VC_MERGE_THRESHOLD_DEFAULT));
    if (dou_val != 0) {
      opt->vc_merge_threshold = dou_val;
    }
    dou_val = std::stod(props.GetProperty(PROP_VC_MERGE_THRESHOLD_NOSTART, PROP_VC_MERGE_THRESHOLD_NOSTART_DEFAULT));
      opt->vc_merge_threshold_nostart = dou_val;
    val = std::stoi(props.GetProperty(PROP_VERTICAL_START_LEVEL, PROP_VERTICAL_START_LEVEL_DEFAULT));
    if (val != 0) {
      opt->vertical_start_level = val;
    }
    val = std::stoi(props.GetProperty(PROP_VERTICAL_MIN_INVOLVE_LEVELS, PROP_VERTICAL_MIN_INVOLVE_LEVELS_DEFAULT));
    if (val != 0) {
      opt->vertical_min_involve_levels = val;
    }
    val = std::stoul(props.GetProperty(PROP_PICK_INDEX_MIN_TABLE_NUMBERS, PROP_PICK_INDEX_MIN_TABLE_NUMBERS_DEFAULT));
    if (val != 0) {
      opt->pick_index_min_table_numbers = val;
    }
    val = std::stoul(props.GetProperty(PROP_CHOOSE_GUARD_INTERVAL, PROP_CHOOSE_GUARD_INTERVAL_DEFAULT));
    if (val != 0) {
      opt->choose_guard_interval = val;
    }
    val = std::stoul(props.GetProperty(PROP_NEIGHBOR_GUARD_RATIO, PROP_NEIGHBOR_GUARD_RATIO_DEFAULT));
    if (val != 0) {
      opt->neighbor_guard_ratio = val;
    }
    if(props.GetProperty(PROP_ENABLE_ESTIMATE_SEEK, PROP_ENABLE_ESTIMATE_SEEK_DEFAULT) == "true") {
      opt->enable_estimate_seek = true;
    }
    else {
      opt->enable_estimate_seek = false;
    }
    val = std::stoul(props.GetProperty(PROP_ESTIMATE_INTERVAL, PROP_ESTIMATE_INTERVAL_DEFAULT));
    opt->estimate_interval = val;
    if(props.GetProperty(PROP_ENABLE_VERTICAL_COMPACTION, PROP_ENABLE_VERTICAL_COMPACTION_DEFAULT) == "true") {
      opt->enable_vertical_compaction = true;
    }
    else {
      opt->enable_vertical_compaction = false;
    }
    std:: cout << "enable_vertical_compaction" << opt->enable_vertical_compaction << std::endl;
    #endif
    #ifdef matrixkv
    auto nvm_setup = new rocksdb::NvmSetup();
    nvm_setup->use_nvm_module = true;
    nvm_setup->pmem_path = "/data/matrixkv";
    // nvm_setup->Level0_column_compaction_trigger_size =  256 * 1024 * 1024;
    // nvm_setup->Level0_column_compaction_slowdown_size = 1.25 * 1024 * 1024 * 1024 ;  //2G slowdown
    // nvm_setup->Level0_column_compaction_stop_size = 2.25 * 1024 * 1024 * 1024;   //8G stop
    opt->nvm_setup.reset(nvm_setup);
    #endif

    if (props.GetProperty(PROP_USE_DIRECT_WRITE, PROP_USE_DIRECT_WRITE_DEFAULT) == "true") {
      opt->use_direct_io_for_flush_and_compaction = true;
    }
    if (props.GetProperty(PROP_USE_DIRECT_READ, PROP_USE_DIRECT_READ_DEFAULT) == "true") {
      opt->use_direct_reads = true;
    }
    if (props.GetProperty(PROP_REPORT_BG_IO_STATS, PROP_REPORT_BG_IO_STATS_DEFAULT) == "true") {
      opt->report_bg_io_stats = true;
    }    
    if (props.GetProperty(PROP_USE_MMAP_WRITE, PROP_USE_MMAP_WRITE_DEFAULT) == "true") {
      opt->allow_mmap_writes = true;
    }
    if (props.GetProperty(PROP_USE_MMAP_READ, PROP_USE_MMAP_READ_DEFAULT) == "true") {
      opt->allow_mmap_reads = true;
    }

    rocksdb::BlockBasedTableOptions table_options;
    size_t cache_size = std::stoul(props.GetProperty(PROP_CACHE_SIZE, PROP_CACHE_SIZE_DEFAULT));
    if (cache_size >= 0) {
      block_cache = rocksdb::NewLRUCache(cache_size);
      table_options.block_cache = block_cache;
    }
    if (props.GetProperty(PROP_BLOCK_ALIGN, PROP_BLOCK_ALIGN_DEFAULT) == "true") {
      table_options.block_align = true;
    }
    size_t compressed_cache_size = std::stoul(props.GetProperty(PROP_COMPRESSED_CACHE_SIZE,
                                                                PROP_COMPRESSED_CACHE_SIZE_DEFAULT));
    if (compressed_cache_size > 0) {
      block_cache_compressed = rocksdb::NewLRUCache(cache_size);
      table_options.block_cache_compressed = rocksdb::NewLRUCache(compressed_cache_size);
    }
    #ifndef matrixkv
    size_t blob_cache_size = std::stoul(props.GetProperty(PROP_BLOB_CACHE_SIZE,
                                                                PROP_BLOB_CACHE_SIZE_DEFAULT));
    if (blob_cache_size > 0) {
      blob_cache = rocksdb::NewLRUCache(blob_cache_size);
      opt->blob_cache = blob_cache;
    }
    #ifndef nvm_rocksdb
    if (props.GetProperty(PROP_USE_PMEM_MAP, PROP_USE_PMEM_MAP_DEFAULT) == "true") {
      opt->use_pmem_map = true;
    } else {
      opt->use_pmem_map = false;
    }
    #endif
    #endif
    if(props.GetProperty(PROP_COMPACTION_STYLE, PROP_COMPACTION_STYLE_DEFAULT) == "leveled") {
      opt->compaction_style = rocksdb::kCompactionStyleLevel;
    } else if (props.GetProperty(PROP_COMPACTION_STYLE, PROP_COMPACTION_STYLE_DEFAULT) == "universal") {
      opt->compaction_style = rocksdb::kCompactionStyleUniversal;
    }
    
    int bloom_bits = std::stoul(props.GetProperty(PROP_BLOOM_BITS, PROP_BLOOM_BITS_DEFAULT));
    if (bloom_bits > 0) {
      table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloom_bits));
    }
    #ifdef nvm_rocksdb
    opt->table_factory.reset(rocksdb::NewNvmPartitionFactory(table_options));
    opt->second_table_factory.reset(rocksdb::NewNvmTableFactory(table_options));
    #else
    opt->table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    #endif

    if (props.GetProperty(PROP_INCREASE_PARALLELISM, PROP_INCREASE_PARALLELISM_DEFAULT) == "true") {
      opt->IncreaseParallelism();
    }
    if (props.GetProperty(PROP_OPTIMIZE_LEVELCOMP, PROP_OPTIMIZE_LEVELCOMP_DEFAULT) == "true") {
      opt->OptimizeLevelStyleCompaction();
    }
    #ifndef matrixkv
    if (props.GetProperty(PROP_ENABLE_BLOB_FILES, PROP_ENABLE_BLOB_FILES_DEFAULT) == "true") {
      opt->enable_blob_files = true;
    }
    if (props.GetProperty(PROP_ENABLE_BLOB_GARBAGE_COLLECTION, PROP_ENABLE_BLOB_GARBAGE_COLLECTION_DEFAULT) == "true") {
      opt->enable_blob_garbage_collection = true;
    }
    #endif
  }
}

void RocksdbDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void RocksdbDB::DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                     const std::vector<std::string> &fields) {
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      values.push_back({field, value});
      filter_iter++;
    }
  }
  assert(values.size() == fields.size());
}

void RocksdbDB::DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                     const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRowFilter(values, p, lim, fields);
}

void RocksdbDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
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

void RocksdbDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}

DB::Status RocksdbDB::ReadSingle(const std::string &table, const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  std::string data;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(false, true), key, &data); // 默认不检验checksum
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Get: ") + s.ToString());
  }
  if (fields != nullptr) {
    DeserializeRowFilter(result, data, *fields);
  } else {
    DeserializeRow(result, data);
    // 下面的assert应该去掉
    // IMP 因为在Merge负载下，如果此前有一个key——Key001没有被插入，而是直接以(Key001, value)的形式Merge到RocksDB中，此时filed并不是10个，而是1个..因为Merge()方法中只更新了一个field...
    // assert(result.size() == static_cast<size_t>(fieldcount_));
  }
  return kOK;
}

DB::Status RocksdbDB::ScanSingle(const std::string &table, const std::string &key, int len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {
  rocksdb::Iterator *db_iter = db_->NewIterator(rocksdb::ReadOptions(false, true));
  db_iter->Seek(key);
  for (int i = 0; db_iter->Valid() && i < len; i++) {
    std::string data = db_iter->value().ToString();
    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, data, *fields);
    } else {
      DeserializeRow(values, data);
      assert(values.size() == static_cast<size_t>(fieldcount_));
    }
    db_iter->Next();
  }
  delete db_iter;
  return kOK;
}

DB::Status RocksdbDB::UpdateSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  std::string data;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(false, true), key, &data);
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Get: ") + s.ToString());
  }
  std::vector<Field> current_values;
  DeserializeRow(current_values, data);
  assert(current_values.size() == static_cast<size_t>(fieldcount_));
  for (Field &new_field : values) {
    bool found __attribute__((unused)) = false;
    for (Field &cur_field : current_values) {
      if (cur_field.name == new_field.name) {
        found = true;
        cur_field.value = new_field.value;
        break;
      }
    }
    assert(found);
  }
  rocksdb::WriteOptions wopt;

  data.clear();
  SerializeRow(current_values, data);
  s = db_->Put(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Put: ") + s.ToString());
  }
  return kOK;
}

DB::Status RocksdbDB::MergeSingle(const std::string &table, const std::string &key,
                                  std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
  rocksdb::WriteOptions wopt;
  rocksdb::Status s = db_->Merge(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Merge: ") + s.ToString());
  }
  return kOK;
}

DB::Status RocksdbDB::InsertSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
  rocksdb::WriteOptions wopt;
  // std::cout<< data.size() <<std::endl;
  rocksdb::Status s = db_->Put(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Put: ") + s.ToString());
  }
  return kOK;
}

DB::Status RocksdbDB::DeleteSingle(const std::string &table, const std::string &key) {
  rocksdb::WriteOptions wopt;
  rocksdb::Status s = db_->Delete(wopt, key);
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Delete: ") + s.ToString());
  }
  return kOK;
}

DB *NewRocksdbDB() {
  return new RocksdbDB;
}

const bool registered = DBFactory::RegisterDB("rocksdb", NewRocksdbDB);

} // ycsbc
