//
// Created by ENVY on 2022/3/9.
//

#ifndef LSM_KV_MEMTABLES_H
#define LSM_KV_MEMTABLES_H

#include "kvstore_api.h"
#include "SkipLists.h"
#include "constant.h"

#include <cassert>

class MemTables : public KVStoreAPI {
private:
    SkipLists* skipList = nullptr;
public:
    MemTables(const std::string &dir): KVStoreAPI(dir){
        skipList = new SkipLists(dir);
    };

    ~MemTables(){
        delete skipList;
    };

    uint64_t getSize() {return skipList->getSize(); };
    void setSize(uint64_t _size) {skipList->setSize(_size); };
    void getAll(std::list<std::pair<uint64_t, std::string> > &all, uint64_t &minKey, uint64_t &maxKey, uint64_t &numKey);
    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};



#endif //LSM_KV_MEMTABLES_H
