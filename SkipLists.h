//
// Created by ENVY on 2022/3/9.
//

#ifndef LSM_KV_SKIPLISTS_H
#define LSM_KV_SKIPLISTS_H

#include <vector>
#include <climits>
#include <time.h>
#include <cstdint>
#include <iostream>
#include "kvstore_api.h"
#include "constant.h"

#define MAX_LEVEL 8

enum SKNodeType
{
    HEAD = 1,
    NORMAL,
    NIL
};

struct SKNode
{
    uint64_t key;
    std::string val;
    SKNodeType type;
    std::vector<SKNode *> forwards;
    SKNode(uint64_t _key, std::string _val, SKNodeType _type)
            : key(_key), val(_val), type(_type)
    {
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            forwards.push_back(nullptr);
        }
    }
};

class SkipLists : public KVStoreAPI {
private:
    SKNode *head;
    SKNode *NIL;
    unsigned long long s = 1;
    double my_rand();
    int randomLevel();
    uint64_t size = INIT_BYTES_SIZE;

public:
    SkipLists(const std::string &dir): KVStoreAPI(dir)
    {
        head = new SKNode(0, "", SKNodeType::HEAD);
        NIL = new SKNode(INT_MAX, "", SKNodeType::NIL);
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            head->forwards[i] = NIL;
        }
    }
    uint64_t getSize() {return size; };
    void setSize(uint64_t _size) {size = _size; };
    void put(uint64_t key, const std::string &value);
    std::string get(uint64_t key);
    bool del(uint64_t key);
    void scan(uint64_t key_start, uint64_t key_end, std::list<std::pair<uint64_t, std::string> > &list);
    void reset();
    void display();
    void getAll(std::list<std::pair<uint64_t, std::string> > &all, uint64_t &minKey, uint64_t &maxKey, uint64_t &numKey);
    ~SkipLists()
    {
        SKNode *n1 = head;
        SKNode *n2;
        while (n1)
        {
            n2 = n1->forwards[0];
            delete n1;
            n1 = n2;
        }
    }
};


#endif //LSM_KV_SKIPLISTS_H
