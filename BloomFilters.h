//
// Created by ENVY on 2022/3/11.
//

#ifndef LSM_KV_BLOOMFILTERS_H
#define LSM_KV_BLOOMFILTERS_H

#include <vector>
#include <cstdint>
#include "MurmurHash3.h"

// BF 假设输入元素个数已经确定为 n，每次插入一个元素会计算其 k 个哈希函数的哈希值，并将哈希数组对应的位置置为 1
class BloomFilters {
public:
    BloomFilters(const uint64_t &numKeys) : n(numKeys){
        while (bitSet.size() < m) bitSet.push_back(false);
    }
    void set(const uint64_t &key){
        doHash(key);
        for(unsigned int i = 0; i < k; ++i) {
            // std::cout << "set data: " << data << "  hash" << i+1 << ": " << hash_val << std::endl;
            bitSet[hash[i] % m] = true;
        }
    }
    bool find(const uint64_t & key){
        bool is_one = true;
        doHash(key);
        for(unsigned int i = 0; i < k; ++i){
            is_one = is_one && bitSet[hash[i] % m];
            // std::cout << "find data: " << data << "  hash" << i+1 << ": " << hash_val << std::endl;
        }
        return is_one;
    }
    uint64_t getSize(){return m;}
    bool getBit(const uint64_t & index){
        if(index < 0 || index >= m) throw "ERROR BloomFilter getBit index out of range";
        return bitSet[index];
    }
    void setBit(const uint64_t & index, bool bitVal){
        if(index < 0 || index >= m) throw "ERROR BloomFilter getBit index out of range";
        bitSet[index] = bitVal;
    }
    ~BloomFilters(){
        bitSet.clear();
    }
private:
    const uint64_t n = 0;  // 插入集合的元素个数 n
    // const uint64_t M_FRAC_N = 6; // m/n
    const uint64_t m = 10240 * 8;  // 用大小为 m 的哈希数组存储哈希值是否已被插入（ BloomFilter 位数组 bitset 的长度）
    static const unsigned int k = 4;  // 取哈希值范围为 [0, m-1] 的 k 个哈希函数
    std::vector<bool> bitSet;
    uint32_t hash[k] = {0};
    void doHash(const uint64_t & key){
        hash[k] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    }
};


#endif //LSM_KV_BLOOMFILTERS_H
