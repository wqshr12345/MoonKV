#include "nvm_btree.h"

#include "btree_iterator.h"
#include "btree_iterator_portion.h"
#include "memory/arena.h"
#include "libpmem.h"
#include "file/filename.h"

namespace ROCKSDB_NAMESPACE {

extern uint8_t simulate_file[1024 * 1024 * 1024];
extern uint32_t simulate_file_size;

NvmBtree::NvmBtree(uint32_t seq, const ReadOptions &read_options,
                   const ImmutableOptions& ioptions,
                   const InternalKeyComparator &internal_key_comparator)
    : read_options_(read_options),
      ioptions_(ioptions),
      icomp_(internal_key_comparator),
      user_comparator_(icomp_.user_comparator()) {

  size_t mapped_len;
  int is_pmem;
  std::string file_name = MakeIndexFileName(ioptions.db_nvm_dir,seq);
  // if (ioptions.db_nvm_dir[ioptions.db_nvm_dir.length()-1] != '/')
  //   file_name = ioptions.db_nvm_dir + "/" + std::to_string(seq) + ".index";
  // else
  //   file_name = ioptions.db_nvm_dir + std::to_string(seq) + ".index";
  if ((pmemaddr = (char *)pmem_map_file(file_name.c_str(), 0, 0,
                                        0666, &mapped_len, &is_pmem)) == NULL) {
    std::cout<<seq<<".table"<<std::endl;
    perror("pmem_map_file");
    assert(pmemaddr != nullptr);
    exit(1);
  }
  file_size_ = mapped_len;
  meta = (btree_file_meta *)((uint8_t *)pmemaddr + mapped_len -
                             sizeof(btree_file_meta));

  index_off = (uint8_t *)pmemaddr + meta->index_off;
  leaf_node_num = meta->leaf_node_num;
  levels = meta->levels;

  void *first_last_key = (uint8_t *)pmemaddr + meta->first_last_key_offset;
  first_last_key = key_prefix::get_prefix(first_last_key, first_key);
  first_last_key = key_prefix::get_prefix(first_last_key, last_key);

  auto *sub_runs_addr = (uint32_t *)((uint8_t *)pmemaddr + meta->sub_run_off);
  for (uint32_t i = 0; i < meta->nrun; ++i) {
    sub_runs.push_back(sub_runs_addr[i]);
  }

  auto *table_valid_key_addr =
      (uint32_t *)((uint8_t *)pmemaddr + meta->table_valid_key_num_off);
  for (uint32_t i = 0; i < meta->nrun; ++i) {
    table_valid_key_num.push_back(table_valid_key_addr[i]);
  }

  auto *table_total_size_addr =
      (uint32_t *)((uint8_t *)pmemaddr + meta->table_total_size_off);
  for (uint32_t i = 0; i < meta->nrun; ++i) {
    table_total_size.push_back(table_total_size_addr[i]);
  }
}
NvmBtree::~NvmBtree() {
    pmem_unmap(pmemaddr, file_size_);
 }

InternalIterator *NvmBtree::NewIterator(const ReadOptions &read_options,
                                        const SliceTransform *prefix_extractor,
                                        Arena *arena, bool skip_filters,
                                        TableReaderCaller caller,
                                        size_t compaction_readahead_size,
                                        bool allow_unprepared_value) {
  if (arena == nullptr) {
    return new BtreeIterator(this, read_options, icomp_);
  } else {
    auto *mem = arena->AllocateAligned(sizeof(BtreeIterator));
    return new (mem) BtreeIterator(this, read_options, icomp_);
  }
}

InternalIterator *NvmBtree::NewPortionIterator(
    const ReadOptions &read_options, const SliceTransform *prefix_extractor,
    Arena *arena, bool skip_filters, TableReaderCaller caller,
    size_t compaction_readahead_size, bool allow_unprepared_value,
    std::vector<uint32_t> &select_tables) {
  if (arena == nullptr) {
    return new BtreePortionIterator(this, select_tables, read_options, icomp_);
  } else {
    auto *mem = arena->AllocateAligned(sizeof(BtreePortionIterator));
    return new (mem)
        BtreePortionIterator(this, select_tables, read_options, icomp_);
  }
}

uint64_t NvmBtree::ApproximateOffsetOf(const Slice &key,
                                       TableReaderCaller caller) {
  return 0;
}
uint64_t NvmBtree::ApproximateSize(const Slice &start, const Slice &end,
                                   TableReaderCaller caller) {
  return 0;
}
void NvmBtree::SetupForCompaction() {}
std::shared_ptr<const TableProperties> NvmBtree::GetTableProperties() const {
  return std::shared_ptr<const TableProperties>();
}
size_t NvmBtree::ApproximateMemoryUsage() const { return 0; }
Status NvmBtree::Get(const ReadOptions &readOptions, const Slice &key,
                     GetContext *get_context,
                     const SliceTransform *prefix_extractor,
                     bool skip_filters, const ImmutableOptions& ioptions) {
  return Status();
}
Status NvmBtree::Get2(const ReadOptions &readOptions, const uint64_t &offset,
                      GetContext *get_context, Slice *value,
                      const SliceTransform *prefix_extractor,
                      bool skip_filters) {
  return Status();
}

std::vector<table_information_collect> NvmBtree::GetTableInformation() {
  std::vector<table_information_collect> table_res;
  table_res.reserve(sub_runs.size());
  for (uint i = 0; i < sub_runs.size(); ++i) {
    table_res.push_back(table_information_collect{
        sub_runs[i], table_valid_key_num[i], table_total_size[i]});
  }
  return table_res;
}

}  // namespace ROCKSDB_NAMESPACE