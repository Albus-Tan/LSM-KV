//
// Created by ENVY on 2022/3/9.
//

#include "MemTables.h"

void MemTables::put(uint64_t key, const std::string &s)
{
    skipList->put(key, s);
}

std::string MemTables::get(uint64_t key)
{
    return skipList->get(key);
}

bool MemTables::del(uint64_t key)
{
    return skipList->del(key);
}

void MemTables::reset()
{
    // reset skipList
    skipList->reset();
}

void MemTables::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    skipList->scan(key1,key2,list);
}

void MemTables::getAll(std::list<std::pair<uint64_t, std::string> > &all, uint64_t &minKey, uint64_t &maxKey, uint64_t &numKey)
{
    skipList->getAll(all, minKey, maxKey, numKey);
}