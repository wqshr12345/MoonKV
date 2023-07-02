#pragma once
#include "pair_hash.h"
#include "folly/concurrency/ConcurrentHashMap.h"
#ifdef USE_CSL
// #include "folly/ConcurrentSkipList.h"
// #include "memtable/inlineskiplist.h"
#include "memtable/skiplist.h"
#include "memory/arena.h"
#else
#include <set>
#endif

// 关于使用并发安全有序容器的设计.
// 1.传统std::map/std::set:显然不合适。底层基于红黑树，插入会造成节点旋转，多线程插入100%会出现一些预期外的core dump。如果要保证正确性，可以在其基础上加读写锁，但显然性能会差特别多。
// 2.folly::ConcurrentSkipList:比较合适的，然而它的iterator只支持lower_bound且不支持--，所以不满足需求。
// 3.RocksDB的InlineSkipList:非常合适的，使用了CAS，保证了多写多读情况下的正确性，且iterator支持++和--。但它提供的接口没有实现template key，而是默认const char* key。不管是修改代码适配template还是把KeyPosition编码为char*，工程量都很大，且后者会有编解码开销。
// 4.RocksDB的SkipList:只保证了一写多读的正确性，多写多读的情况未知。如果多写多读场景仅仅会造成数据丢失而不会造成正确性影响，那么也是符合要求的；如果多写多读会造成正确性影响或者core dump，那么就不符合要求。
// 最终决定基于RocksDB的SkipList，再加一个atomic bool，保证同一时刻只有一个写线程。如果已经有线程在写，可以采取自旋策略或者直接break。目前倾向于直接break的实现，自旋会比较影响性能。

namespace ROCKSDB_NAMESPACE {

struct KeyPosition {
    std::string key_;
    uint32_t low_level_pos_;
    uint32_t high_level_pos_;
    KeyPosition() {}
    KeyPosition(std::string key, uint32_t llp, uint32_t hlp)
        : key_(key), low_level_pos_(llp), high_level_pos_(hlp) {}
    bool operator< (const KeyPosition &it) const {
        return key_.compare(it.key_) < 0;
    }
};

struct KeyPositionComparator {
  int operator()(const KeyPosition& a, const KeyPosition& b) const {
    if (a.key_.compare(b.key_) < 0) {
      return -1;
    } else if (a.key_.compare(b.key_) > 0) {
      return +1;
    } else {
      return 0;
    }
  }
};

class KeyPosList {
    #ifdef USE_CSL
    // using SkipList = folly::ConcurrentSkipList<KeyPosition>;
    // using SkipList = InlineSkipList<>;
    using KPSkipList = SkipList<KeyPosition, KeyPositionComparator>;
    KPSkipList key_pos_list_;
    std::atomic<bool> is_writing_;
    #else
    using SkipList = std::set<KeyPosition>;
    SkipList key_pos_list_;
    #endif

  public:
    // KeyPosList(int height): key_pos_list_(height){}
    #ifdef USE_CSL

    KeyPosList(KeyPositionComparator cmp, Arena* arena, int32_t max_height, const KeyPosition& key_pos1, const KeyPosition& key_pos2): key_pos_list_(cmp, arena, max_height), is_writing_(false) { //
        key_pos_list_.Insert(key_pos1);
        key_pos_list_.Insert(key_pos2);
        // SkipList::Accessor accessor(&key_pos_list_);
        // accessor.insert(key_pos1);
        // accessor.insert(key_pos2);
    }
    #else 
    KeyPosList() {}
    KeyPosList(int height, const KeyPosition& key_pos1, const KeyPosition& key_pos2) {
        key_pos_list_.insert(key_pos1);
        key_pos_list_.insert(key_pos2);
    }
    #endif
    void AddKeyPos(const std::string& key, uint32_t llp, uint32_t hlp) {
        #ifdef USE_CSL
        bool expected = is_writing_.load();
        if(!expected && is_writing_.compare_exchange_weak(expected, true))
            key_pos_list_.Insert(KeyPosition(key, llp, hlp));
        is_writing_.store(false);
        // SkipList::Accessor accessor(&key_pos_list_);
        // accessor.insert(KeyPosition(key, llp, hlp));
        #else
        key_pos_list_.insert(KeyPosition(key, llp, hlp));
        #endif
    }
    uint32_t GetKeyPos(std::string key) {
        #ifdef USE_CSL
        // SkipList::Accessor accessor(&key_pos_list_);
        // SkipList::Skipper skipper(&key_pos_list_);
        // skipper.accessor();
        // auto it = accessor.lower_bound(KeyPosition{key, 1, 1});
        // if( it == accessor.begin()) {
        //     return it->high_level_pos_;
        // }
        KPSkipList::Iterator iter(&key_pos_list_);
        // 因为KeyPosition只基于key排序，所以这里的查找只要key是有效的即可
        iter.SeekForPrev(KeyPosition{key, 1, 1});
        const uint32_t left_llp = iter.key().low_level_pos_, left_hlp = iter.key().high_level_pos_;
        iter.Next();
        if(!iter.Valid()) {
            return left_hlp;   
        }
        const uint32_t right_llp = iter.key().low_level_pos_, right_hlp = iter.key().high_level_pos_;
        #else
        auto it = key_pos_list_.lower_bound(KeyPosition{key, 1, 1});
        if(it == key_pos_list_.begin()) {
            return it->high_level_pos_;
        }
        const uint32_t right_llp = it->low_level_pos_, right_hlp = it->high_level_pos_;
        it--;
        // 这里的right的llp和hlp是过时的也无所谓。并不会造成正确性问题。
        const uint32_t left_llp = it->low_level_pos_, left_hlp = it->high_level_pos_;
        #endif

        double llp = left_llp; // 如果left_llp == right_llp 那么预测的当前key的llp即为left_llp
        if(right_llp != left_llp) {
         llp = (double)(llp - left_llp) / (double)(right_llp - left_llp);
        }
       
        uint32_t estimate_pos = uint32_t(llp * (right_hlp - left_hlp)) + right_hlp;
        if (estimate_pos > right_hlp)
            estimate_pos = right_hlp;
        return estimate_pos;
    }
};


class EstimateSeekTable {
    folly::ConcurrentHashMap<std::pair<uint64_t, uint64_t>, KeyPosList*, pair_hash, pair_equal> estimate_table;
    const int32_t skip_list_height = 7;
    KeyPositionComparator cmp;
    Arena arena;

  public:
    
    // 此处不需要任何同步保证 因为就算多个线程同时判断KeyList，也只有一个KeyPosList被添加(得益于并发安全的insert)
    bool IsKeyExist(const std::pair<uint64_t, uint64_t>& seek_pair) {
        return estimate_table.find(seek_pair) != estimate_table.end();
    }

    // func：KeyPosList第一次被构造时调用
    void ConstructKeyPosList(const std::pair<uint64_t, uint64_t>& seek_pair, const KeyPosition& key_pos1, const KeyPosition& key_pos2) {
        // 再次确认是否有构造 尽可能减少这里KeyPosList的重复构造次数
        if(IsKeyExist(seek_pair))
            return;
        KeyPosList* key_pos_list = new KeyPosList(cmp, &arena, skip_list_height, key_pos1, key_pos2);
        // 并发安全的insert方法，保证只有第一个被添加的key_pos_list会一直驻留在estimate_table中
        auto it = estimate_table.insert(seek_pair, key_pos_list);
        if(!it.second) {
            delete key_pos_list;
        }
    }

    KeyPosList* GetKeyPosList(const std::pair<uint64_t, uint64_t>& seek_pair) {
        auto it = estimate_table.find(seek_pair);
        if (it == estimate_table.end()) return nullptr;
        return it->second;
    }

    // 多线程删除不安全
    void DestroyKeyPosList(const std::pair<uint64_t, uint64_t>& seek_pair) {
        auto it = estimate_table.find(seek_pair);
        estimate_table.erase(it);
        delete it->second;
    }
    // 多线程删除安全
    // 使用assign_if_equal 原子地将seek_pair对应的value更新为nullptr。只有第一个成功更新为nullptr地才可以delete it->second。
    void DestroyKeyPosList(uint64_t father_number) {
        for(auto it = estimate_table.begin(); it != estimate_table.end(); ++it) {
            if(it->first.first == father_number || it->first.second == father_number) {
                auto it2 = estimate_table.assign_if_equal(it->first, it->second, nullptr);
                // 此时，说明是第一次被删除，可以安全地delete it->second
                if(it2.hasValue())
                    delete it->second;
                // 不管此时key对应的value是pointe人还是nullptr，都可以erase掉这个元素，因为内存已经被释放完毕
                estimate_table.erase(it->first);
            }
        }
    }


};

} // namespace ROCKSDB_NAMESPACE