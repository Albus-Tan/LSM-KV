#pragma once

#include "kvstore_api.h"
#include "MemTables.h"
#include "SSTables.h"
#include "constant.h"
#include <vector>
#include <queue>
#include <algorithm>
#include <string>

// 用于解决 bad_alloc 问题，通过在过程中先写一部分到硬盘上，减少内存压力的方式
// 该值定义了在 result 积累到多少时尝试进行一批 SSTable 的写
// 300
#define KV_NUM_TO_WRITE_IN_COMPACTION 500

// 自定义 compaction 中要使用的堆的比较函数
// heap 会首先按照 first.first 来进行排序，之后按照 first.second 排序
// 其中 first.first 越小越先输出（key小的在前输出）
// 当  first.first 一样时候， second越大越先输出（时间戳大的先输出）
// heap 出来的结果存入 list
// 之后遍历 list，遇到重复的 key 只保留第一个即可
struct cmpHeap{
    bool operator() ( std::pair<std::pair<uint64_t, std::string>, uint64_t> &a, std::pair<std::pair<uint64_t, std::string>, uint64_t> &b ){
        if( a.first.first == b.first.first ) {
            return a.second < b.second;
        }
        return a.first.first > b.first.first;
    }
};


// 自定义 scan 中要使用的堆的比较函数
// heap 会首先按照 first.first 来进行排序，之后按照 first.second 排序
// 其中 first.first 越小越先输出（key小的在前输出）
// 当  first.first 一样时候， second越小越先输出（level 小的优先级更高，先输出）
// heap 出来的结果存入 list
// 之后遍历 list，遇到重复的 key 只保留第一个即可
struct cmpScanHeap{
    bool operator() ( std::pair<std::pair<uint64_t, std::string>, uint64_t> &a, std::pair<std::pair<uint64_t, std::string>, uint64_t> &b ){
        if( a.first.first == b.first.first ) {
            return a.second > b.second;
        }
        return a.first.first > b.first.first;
    }
};

// 自定义 list unique 的比较函数，按照 key 比较（不会比较 value）是否相等
// 遍历 list，遇到重复的 key 只保留第一个
inline bool cmpList( std::pair<uint64_t, std::string> &a, std::pair<uint64_t, std::string> &b )
{
    return a.first == b.first;
}

// 自定义每一层 cache vector<SSTables*> 的比较函数，按照 minKey 比较大小
// 遍历这一层 vector<SSTables*>，并按照 minKey 从小到大重新在这一层中排列
inline bool cmpSSTableMinKey( SSTables* &a, SSTables* &b )
{
    return a->getMinKey() < b->getMinKey();
}

// 自定义每一层 cache vector<SSTables*> 的比较函数，按照 timeStamp 比较大小
// 遍历这一层 vector<SSTables*>，并按照 timeStamp 从小到大重新在这一层中排列
inline bool cmpSSTableTimeStamp( SSTables* &a, SSTables* &b )
{
    return a->getTimeStamp() < b->getTimeStamp();
}

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    MemTables* memTable = nullptr;
    // 使用 cache[i][j] 表示第 i 层第 j 个文件，第0层越后面文件越新，之后层越后面索引越大
    std::vector<std::vector<SSTables*>> cache;

    std::string dir;
    uint64_t nextTimeStamp = 1;
    uint64_t maxLevel = 0;
    void checkCompaction();  // 每次新增 SSTable 都调用检查一次
    void compaction(uint64_t level, unsigned int moreNum);
    void writeAllListToSSTables(std::list<std::pair<uint64_t, std::string> > &allList, const uint64_t &timeStamp, const uint64_t &level);
    void tryWriteSomeListToSSTables(std::list<std::pair<uint64_t, std::string> > &allList, const uint64_t &timeStamp, const uint64_t &level, std::vector<SSTables *> &nextLevelTempCache);

    // 确定文件名并赋值到此处，不包含.sst
    // 文件命名格式 timeStamp minKey-maxKey pairsNum
    std::string generateFileName(uint64_t timeStamp, uint64_t minKey, uint64_t maxKey, uint64_t numKey) {
        return std::to_string(timeStamp)+" "+std::to_string(minKey)+"-"+std::to_string(maxKey)+" "+ std::to_string(numKey);
    }
    // 返回两数最大值
    uint64_t getMax(uint64_t a, uint64_t b){
        return ( a > b ) ? a : b;
    }

    void clearAllCacheAndFiles();
    bool rebuildCacheFromDir();
    void convertMemToSS();

public:

    KVStore(const std::string &dir);
    ~KVStore();

	void put(uint64_t key, const std::string &s) override;
	std::string get(uint64_t key) override;
	bool del(uint64_t key) override;
	void reset() override;
	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &allList) override;
};
