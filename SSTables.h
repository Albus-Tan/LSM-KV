//
// Created by ENVY on 2022/3/9.
//

#ifndef LSM_KV_SSTABLES_H
#define LSM_KV_SSTABLES_H

#include "utils.h"
#include <list>
#include <cstdint>
#include "constant.h"
#include <fstream>
#include "BloomFilters.h"

struct Header {
    uint64_t timeStamp;
    uint64_t pairsNum;
    uint64_t minKey;
    uint64_t maxKey;
};

struct Index {
    uint64_t key;
    uint32_t offset;
};

class SSTables {
public:
    SSTables(const std::string dir, const std::string fileName);
    SSTables(const std::string dir, std::list<std::pair<uint64_t, std::string> > &list, uint64_t &minKey, uint64_t &maxKey, uint64_t &numKey, const uint64_t &timeStamp, const std::string fileName);
    ~SSTables(){ if(bloomFilter) delete bloomFilter; index.clear(); };
    void writeSSTable(std::list<std::pair<uint64_t, std::string> > &list);
    void readSSTable();

    std::string get(uint64_t key, const std::string &filePath);
    void readAllIndexAndData(std::list<std::pair<uint64_t, std::string> > &all);
    void readAllIndexAndDataWithTimeStamp(std::list<std::pair<std::pair<uint64_t, std::string>, uint64_t> > &all);

    void readIndexAndDataForScan(std::list<std::pair<uint64_t, std::string> > &all, const uint64_t & key1, const uint64_t & key2);

    uint64_t getTimeStamp(){return header.timeStamp;};
    uint64_t getMinKey(){return header.minKey;};
    uint64_t getMaxKey(){return header.maxKey;};

    // 在构造函数中确定文件名并赋值到此处，不包含.sst
    // 文件命名格式 timeStamp minKey-maxKey pairsNum
    std::string fileName = "";

private:
    std::string dir;
    Header header;
    BloomFilters* bloomFilter = nullptr;
    std::vector<Index> index;

    void writeHeader(std::ofstream &ostrm);
    void writeBloomFilter(std::ofstream &ostrm);
    void writeIndexAndData(std::ofstream &ostrm, std::list<std::pair<uint64_t, std::string> > &list);

    void readHeader(std::ifstream &istrm);
    void readBloomFilter(std::ifstream &istrm);
    void readAllIndex(std::ifstream &istrm);

    std::string getData(const uint32_t &posDataStart, uint32_t posDataEnd, const std::string &filePath);

};


#endif //LSM_KV_SSTABLES_H
