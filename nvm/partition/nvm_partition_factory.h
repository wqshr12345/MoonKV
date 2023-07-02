#pragma once
#include <stdint.h>

#include <memory>
#include <string>

#include "cache/cache_reservation_manager.h"
#include "port/port.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/table.h"
#include "db/version_set.h"
namespace ROCKSDB_NAMESPACE {

class NvmPartitionFactory : public TableFactory {
public:
    explicit NvmPartitionFactory(
        const BlockBasedTableOptions& table_options = BlockBasedTableOptions());

    ~NvmPartitionFactory() {}

    static const char* kClassName() { return kNvmTableName(); }

     const char* Name() const override { return kNvmPartitionName(); }

    // 为了能够使用TableFactory中的另一个NewTableReader的重载版本
    using TableFactory::NewTableReader;
    Status NewTableReader(
        const ReadOptions& ro, const TableReaderOptions& table_reader_options,
        std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
        std::unique_ptr<TableReader>* table_reader,
        bool prefetch_index_and_filter_in_cache = true) const override{ 
            return Status::OK(); 
    };
    Status NewTableReader2(
        const ReadOptions& ro, const TableReaderOptions& table_reader_options,
        std::unique_ptr<RandomAccessFileReader>&& file,
        std::unique_ptr<TableReader>* table_reader,TableCache* table_cache,
        bool prefetch_index_and_filter_in_cache = true) const;
    
    TableBuilder* NewTableBuilder(
        const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file) const override {return nullptr;};

    // 非重载函数 增加了额外的table_file_seq,index_file_seq,sub_run参数 
    // 为什么需要增加这几个参数？
    // 1.table_file_seq 为新生成的table file的file_seq
    // 这个参数理论上可以从file中得到，但是WritableFileWriter并没有
    // 实现返回file_seq的接口，只有返回file_name的接口，所以必须单独传入一个参数
    // 2.index_file_seq 为新生成的index的file_seq 用于在TableBuilder中构造index
    // file 3.sub_run 构造index file的参数 WQTODO
    // 貌似不需要在这里传入？和zhenghong再讨论一下
    //
    //使用时需要强制类型转换
    // WQTODO 这里可能需要考虑改造为虚函数...
    TableBuilder* NewTableBuilder(
        const TableBuilderOptions& table_builder_options,
        WritableFileWriter* file, uint64_t table_file_seq,
        uint64_t index_file_seq,
        std::vector<table_information_collect>& sub_run,
        bool gc_compaction, bool vertical_compaction) const override;

   protected:
    //func: 初始化bbto的函数
    void InitializeOptions();

private:
    BlockBasedTableOptions table_options_;
    std::shared_ptr<CacheReservationManager> table_reader_cache_res_mgr_;
};

}