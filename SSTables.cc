//
// Created by ENVY on 2022/3/9.
//

#include "SSTables.h"

#include <iostream>
#include <cassert>

SSTables::SSTables(const std::string dir, std::list<std::pair<uint64_t, std::string> > &allList, uint64_t &minKey, uint64_t &maxKey, uint64_t &numKey, const uint64_t &timeStamp, const std::string fileName){
    this->fileName = fileName;
    this->dir = dir;
    header.minKey = minKey;
    header.maxKey = maxKey;
    header.pairsNum = numKey;
    header.timeStamp = timeStamp;
    // std::cout << "header.pairsNum: " << header.pairsNum << std::endl;
    bloomFilter = new BloomFilters(header.pairsNum);
    auto it= allList.begin();
    while(it != allList.end())
    {
        bloomFilter->set(it->first);
        ++it;
    }

    // Index is written in writeIndexAndData
    writeSSTable(dir, allList);
}

SSTables::SSTables(const std::string dir, const std::string fileName)
{
    this->dir = dir;
    this->fileName = fileName;

    //TODO: may have bugs here, fileName including .sst??
    //  also readSSTable's read index and data is really old version

    readSSTable(dir, fileName);
}

void SSTables::writeSSTable(const std::string dir, std::list<std::pair<uint64_t, std::string> > &allList)
{
    if(!utils::dirExists(dir)) {
        utils::mkdir(dir.c_str());
    }
    std::string filepath = dir + "/" + this->fileName + ".sst";
    std::ofstream ostrm(filepath, std::ios::binary);

    // Header
    writeHeader(ostrm);

    // BloomFilter
    writeBloomFilter(ostrm);

    // Index and Data
    writeIndexAndData(ostrm, allList);

    ostrm.close();
}

void SSTables::readSSTable(const std::string dir, const std::string fileName)
{
    if(!utils::dirExists(dir)) {
        utils::mkdir(dir.c_str());
    }
    std::string filepath = dir + "/" + fileName;
    std::ifstream istrm(filepath, std::ios::binary);

    // Header
    readHeader(istrm);
    // BloomFilter
    readBloomFilter(istrm);
    // Index and Data
    readIndexAndData(istrm);

    istrm.close();
}

void SSTables::writeHeader(std::ofstream &ostrm)
{
    // Header
    ostrm.write(reinterpret_cast<char*>(&header.timeStamp), sizeof(header.timeStamp));
    ostrm.write(reinterpret_cast<char*>(&header.pairsNum), sizeof(header.pairsNum));
    ostrm.write(reinterpret_cast<char*>(&header.minKey), sizeof(header.minKey));
    ostrm.write(reinterpret_cast<char*>(&header.maxKey), sizeof(header.maxKey));

    /*std::cout << "WRITE header.timeStamp " << header.timeStamp
              << " header.pairsNum " << header.pairsNum
              << " header.minKey " << header.minKey
              << " header.maxKey " << header.maxKey << std::endl;*/
}

void SSTables::writeBloomFilter(std::ofstream &ostrm)
{
    if(bloomFilter == nullptr) throw("ERROR null bloomFilter !");
    uint64_t size = bloomFilter->getSize();
    if(size > (BITS_IN_BYTE * BF_BYTES_SIZE)) {
        std::cout << "bloomFilter->getSize(): " << size << std::endl;
        throw("ERROR bloomFilter size larger than BF_BYTES_SIZE !");
    }
    uint64_t times = size / BITS_IN_BYTE;
    int restBitNum = size % BITS_IN_BYTE;
    uint64_t index = 0;
    bool val[BITS_IN_BYTE] = {false};
    char packedVal = 0;
    while(index < times){
        for(int i = 0; i < BITS_IN_BYTE; ++i){
            val[i] = bloomFilter->getBit((index * BITS_IN_BYTE) + i);
            // std::cout << "bloomFilter index: " << ((index * BITS_IN_BYTE) + i) << " bitVal: " << val[i] << std::endl;
        }
        packedVal = PACK_BYTE(val);
        ostrm.write((char*)&packedVal, 1);
        ++index;
    }
    for(int i = 0; i < BITS_IN_BYTE; ++i){
        if(i >= restBitNum) val[i] = 0;
        else val[i] = bloomFilter->getBit((index * BITS_IN_BYTE) + i);
        // std::cout << "bloomFilter index: " << ((index * BITS_IN_BYTE) + i) << " bitVal: " << val[i] << std::endl;
    }
    packedVal = PACK_BYTE(val);
    ostrm.write((char*)&packedVal, 1);
}

void SSTables::writeIndexAndData(std::ofstream &ostrm, std::list<std::pair<uint64_t, std::string> > &allList)
{
    // 索引区总长度计算
    uint64_t indexLength = (KEY_BYTES_SIZE + OFFSET_BYTES_SIZE) * header.pairsNum;

    // 索引区和数据区开头位置计算
    uint64_t posIndex = INIT_BYTES_SIZE;
    uint64_t posData = indexLength + INIT_BYTES_SIZE;

    std::pair<uint64_t, std::string> tmp;
    uint64_t tmpKey;
    char* tmpVal;
    uint32_t tmpValSize;
    uint32_t offset;  // 数据起始位置与文件开头 ios::beg 的距离
    // 写入索引区和数据区
    while (!allList.empty()){

        tmp = allList.front();
        tmpKey = tmp.first;
        tmpValSize = tmp.second.length();
        tmpVal = new char [tmpValSize + 1];
        strcpy(tmpVal, tmp.second.c_str());
        allList.pop_front();
        ostrm.seekp(posIndex, std::ios::beg);
        // 写入 Key
        ostrm.write(reinterpret_cast<char*>(&tmpKey), KEY_BYTES_SIZE);
        // 写入 Offset
        offset = posData;
        ostrm.write(reinterpret_cast<char*>(&(offset)), OFFSET_BYTES_SIZE);
        posIndex += (KEY_BYTES_SIZE + OFFSET_BYTES_SIZE);
        // add in index
        Index tmp;
        tmp.key = tmpKey;
        tmp.offset = offset;
        index.push_back(tmp);
        ostrm.seekp(posData, std::ios::beg);
        // 写入 Value
        ostrm.write(reinterpret_cast<char*>(tmpVal), tmpValSize);
        posData += tmpValSize;
        // std::cout << "WRITE   KEY: " << tmpKey << " offset: " << offset << " tmpVal: " << tmpVal << std::endl;
        delete tmpVal;
    }
}

void SSTables::readHeader(std::ifstream &istrm)
{
    istrm.read(reinterpret_cast<char*>(&header.timeStamp), sizeof(header.timeStamp));
    istrm.read(reinterpret_cast<char*>(&header.pairsNum), sizeof(header.pairsNum));
    istrm.read(reinterpret_cast<char*>(&header.minKey), sizeof(header.minKey));
    istrm.read(reinterpret_cast<char*>(&header.maxKey), sizeof(header.maxKey));

    // if(header.timeStamp != 0) std::cout << "READ header.timeStamp " << header.timeStamp << std::endl;
    /*std::cout << "READ header.timeStamp " << header.timeStamp
              << " header.pairsNum " << header.pairsNum
              << " header.minKey " << header.minKey
              << " header.maxKey " << header.maxKey << std::endl;*/
}
void SSTables::readBloomFilter(std::ifstream &istrm)
{
    if(bloomFilter != nullptr) delete bloomFilter;
    bloomFilter = new BloomFilters(header.pairsNum);
    uint64_t size = bloomFilter->getSize();

    uint64_t times = size / BITS_IN_BYTE;
   int restBitNum = size % BITS_IN_BYTE;
    uint64_t index = 0;
    bool val[BITS_IN_BYTE] = {false};
    char packedVal = 0;
    while(index < times){
        istrm.read((char*)&packedVal, 1);
        for(int i = 0; i < BITS_IN_BYTE; ++i){
            val[i] = GET_BIT(i,packedVal);
            bloomFilter->setBit(((index * BITS_IN_BYTE) + i), val[i]);
            // std::cout << "bloomFilter index: " << ((index * BITS_IN_BYTE) + i) << " bitVal: " << val[i] << std::endl;
        }
        ++index;
    }
    istrm.read((char*)&packedVal, 1);
    for(int i = 0; i < BITS_IN_BYTE; ++i){
        if(i >= restBitNum) { break; }
        else {
            val[i] = GET_BIT(i,packedVal);
            bloomFilter->setBit(((index * BITS_IN_BYTE) + i), val[i]);
            // std::cout << "bloomFilter index: " << ((index * BITS_IN_BYTE) + i) << " bitVal: " << val[i] << std::endl;
        }
    }
}
void SSTables::readIndexAndData(std::ifstream &istrm)
{
    // TODO

    // 索引区和数据区开头位置计算
    uint64_t posIndex = INIT_BYTES_SIZE;
    uint32_t posDataStart = posIndex;  // 数据起始位置与文件开头 ios::beg 的距离
    uint32_t posDataEnd = posIndex;  // 数据终止位置与文件开头 ios::beg 的距离
    // std::pair<uint64_t, std::string> tmp;
    uint64_t tmpKey;
    // 读出索引区和数据区
    istrm.seekg(posIndex, std::ios::beg);
    // 读出 Key
    istrm.read(reinterpret_cast<char*>(&tmpKey), KEY_BYTES_SIZE);
    // 读出 Offset
    istrm.read(reinterpret_cast<char*>(&(posDataStart)), OFFSET_BYTES_SIZE);
    istrm.seekg((posIndex + OFFSET_BYTES_SIZE + 2 * KEY_BYTES_SIZE), std::ios::beg);
    istrm.read(reinterpret_cast<char*>(&(posDataEnd)), OFFSET_BYTES_SIZE);

    istrm.seekg(posDataStart, std::ios::beg);

    /*std::cout << posDataEnd << std::endl;
    std::cout << posDataStart << std::endl;
    std::cout << posDataEnd - posDataStart + 1;*/
    // 读出 Value
    char* tmpVal = new char [posDataEnd - posDataStart + 1];
    istrm.read((char*)tmpVal, posDataEnd - posDataStart);

    tmpVal[posDataEnd - posDataStart] = '\0';
    std::string val = tmpVal;
    delete [] tmpVal;
    // std::cout << "READ   KEY: " << tmpKey << " posStart: " << posDataStart << " posEnd: " << posDataEnd << " val: " << val  << std::endl;
}

std::string SSTables::get(uint64_t key, const std::string &filePath)
{
    // 检查 key 是否在上下界范围内
    if(key > header.maxKey || key < header.minKey) return "";

    // 用 Bloom Filter 快速判断 SSTable 中是否存在该 key
    if(!bloomFilter->find(key)) return "";

    // 二分查找
    size_t left = 0;
    size_t right = index.size() - 1;
    size_t mid;
    while(left <= right)
    {
        mid = (left + right) / 2;
        if(index[mid].key == key)  // found
        {
            // std::cout << "check in sstable, FOUND key : " << key << std::endl;

            if(mid != index.size() - 1) // not the last data
            return getData(index[mid].offset, index[mid+1].offset, filePath);
            else return getData(index[mid].offset, 0, filePath);
        }
        if(index[mid].key < key)
        {
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }
    return "";  // not found
}

std::string SSTables::getData(const uint32_t &posDataStart, uint32_t posDataEnd, const std::string &filePath)
{
    std::ifstream istrm(filePath, std::ios::binary);
    if(!istrm) throw("file not exist!");
    if(posDataEnd == 0) {  // last val
        istrm.seekg(0, std::ios::end);
        posDataEnd = istrm.tellg();
    }
    istrm.seekg(posDataStart, std::ios::beg);
    // 读出 Value
    char* tmpVal = new char [posDataEnd - posDataStart + 1];
    istrm.read((char*)tmpVal, posDataEnd - posDataStart);
    tmpVal[posDataEnd - posDataStart] = '\0';
    std::string val = tmpVal;
    delete [] tmpVal;
    istrm.close();
    return val;
}

void SSTables::readAllIndexAndData(std::list<std::pair<uint64_t, std::string> > &all) {

    if(!utils::dirExists(this->dir)) {
        utils::mkdir((this->dir).c_str());
    }
    std::string filepath = (this->dir) + "/" + (this->fileName) + ".sst";
    std::ifstream istrm(filepath, std::ios::binary);

    // 索引区和数据区开头位置计算
    uint64_t posIndex = INIT_BYTES_SIZE;
    uint32_t posDataStart = posIndex;  // 数据起始位置与文件开头 ios::beg 的距离
    uint32_t posDataEnd = posIndex;  // 数据终止位置与文件开头 ios::beg 的距离
    int readed = 0;
    uint64_t tmpKey;

    // 读出索引区和数据区
    while(readed < (this->header.pairsNum)){

        istrm.seekg(posIndex, std::ios::beg);
        // 读出 Key
        istrm.read(reinterpret_cast<char*>(&tmpKey), KEY_BYTES_SIZE);
        // 读出 Offset
        istrm.read(reinterpret_cast<char*>(&(posDataStart)), OFFSET_BYTES_SIZE);
        posIndex += OFFSET_BYTES_SIZE + KEY_BYTES_SIZE;
        if(readed != this->header.pairsNum - 1){
            istrm.seekg((posIndex + KEY_BYTES_SIZE), std::ios::beg);
            istrm.read(reinterpret_cast<char*>(&(posDataEnd)), OFFSET_BYTES_SIZE);
        } else {  // last one
            istrm.seekg(0, std::ios::end);
            posDataEnd = istrm.tellg();
        }

        istrm.seekg(posDataStart, std::ios::beg);
        // 读出 Value
        char* tmpVal = new char [posDataEnd - posDataStart + 1];
        istrm.read((char*)tmpVal, posDataEnd - posDataStart);
        tmpVal[posDataEnd - posDataStart] = '\0';
        std::string val = tmpVal;
        delete [] tmpVal;

        std::pair<uint64_t, std::string> tmp(tmpKey, val);
        all.push_back(tmp);

        ++readed;
    }
    istrm.close();
}

// 其中最外层 pair 的 second 的 uint64_t 放当前 table 的时间戳
void SSTables::readAllIndexAndDataWithTimeStamp(std::list<std::pair<std::pair<uint64_t, std::string>, uint64_t> > &all)
{
    uint64_t timeStamp = this->header.timeStamp;
    if(!utils::dirExists(this->dir)) {
        utils::mkdir((this->dir).c_str());
    }
    std::string filepath = (this->dir) + "/" + (this->fileName) + ".sst";
    std::ifstream istrm(filepath, std::ios::binary);

    // 索引区和数据区开头位置计算
    uint64_t posIndex = INIT_BYTES_SIZE;
    uint32_t posDataStart = posIndex;  // 数据起始位置与文件开头 ios::beg 的距离
    uint32_t posDataEnd = posIndex;  // 数据终止位置与文件开头 ios::beg 的距离
    int readed = 0;
    uint64_t tmpKey;

    // 读出索引区和数据区
    while(readed < (this->header.pairsNum)){

        istrm.seekg(posIndex, std::ios::beg);
        // 读出 Key
        istrm.read(reinterpret_cast<char*>(&tmpKey), KEY_BYTES_SIZE);
        // 读出 Offset
        istrm.read(reinterpret_cast<char*>(&(posDataStart)), OFFSET_BYTES_SIZE);
        posIndex += OFFSET_BYTES_SIZE + KEY_BYTES_SIZE;
        if(readed != this->header.pairsNum - 1){
            istrm.seekg((posIndex + KEY_BYTES_SIZE), std::ios::beg);
            istrm.read(reinterpret_cast<char*>(&(posDataEnd)), OFFSET_BYTES_SIZE);
        } else {  // last one
            istrm.seekg(0, std::ios::end);
            posDataEnd = istrm.tellg();
        }

        istrm.seekg(posDataStart, std::ios::beg);
        // 读出 Value
        char* tmpVal = new char [posDataEnd - posDataStart + 1];
        istrm.read((char*)tmpVal, posDataEnd - posDataStart);
        tmpVal[posDataEnd - posDataStart] = '\0';
        std::string val = tmpVal;
        delete [] tmpVal;

        std::pair<uint64_t, std::string> tmp(tmpKey, val);
        all.emplace_back(tmp, timeStamp);

        ++readed;
    }
    istrm.close();
}

// 返回的 all 包括 key1 与 key2 （如果这两个 key 出现在文件中）
void SSTables::readIndexAndDataForScan(std::list<std::pair<uint64_t, std::string>> &all, const uint64_t &key1,const uint64_t &key2)
{
    // 实际有在当前文件中出现的起始 key
    uint64_t startKey;
    uint64_t startKeyIndex;
    // 检查 key1 是否在上下界范围内
    if(key1 > header.maxKey) throw("ERROR  readIndexAndDataForScan key1 bigger than maxKey");
    if(key1 < header.minKey)
    {
        startKey = header.minKey;
        startKeyIndex = 0;
    }
    else{
        // 二分查找 key1
        size_t left = 0;
        size_t right = index.size() - 1;
        size_t mid;
        while(left <= right)
        {
            mid = (left + right) / 2;
            if(index[mid].key == key1)  // found
            {
                startKey = key1;
                startKeyIndex = mid;
                break;
            }
            if(index[mid].key < key1)
            {
                left = mid + 1;
            }
            else
            {
                right = mid - 1;
            }
        }
        if(left > right){  // not found
            if(left >= index.size()) throw("ERROR  readIndexAndDataForScan left bigger than size");
            startKey = index[left].key;
            startKeyIndex = left;
        }
    }

    if(!utils::dirExists(this->dir)) {
        utils::mkdir((this->dir).c_str());
    }
    std::string filepath = (this->dir) + "/" + (this->fileName) + ".sst";
    std::ifstream istrm(filepath, std::ios::binary);

    // 从 startKey 开始读
    // 索引区和数据区开头位置计算
    uint64_t posIndex = INIT_BYTES_SIZE + startKeyIndex * (OFFSET_BYTES_SIZE + KEY_BYTES_SIZE);
    uint32_t posDataStart = posIndex;  // 数据起始位置与文件开头 ios::beg 的距离
    uint32_t posDataEnd = posIndex;  // 数据终止位置与文件开头 ios::beg 的距离

    uint64_t tmpKey = startKey;

    // 读出索引区和数据区
    // 注意判定是否是最后一个
    bool readAll = false;
    while(tmpKey < key2 && !readAll){

        istrm.seekg(posIndex, std::ios::beg);
        // 读出 Key
        istrm.read(reinterpret_cast<char*>(&tmpKey), KEY_BYTES_SIZE);
        // 读到比上界大的
        if(tmpKey > key2) break;

        // 读出 Offset
        istrm.read(reinterpret_cast<char*>(&(posDataStart)), OFFSET_BYTES_SIZE);
        posIndex += OFFSET_BYTES_SIZE + KEY_BYTES_SIZE;

        // 注意判定是否是最后一个
        if(tmpKey != this->header.maxKey){
            istrm.seekg((posIndex + KEY_BYTES_SIZE), std::ios::beg);
            istrm.read(reinterpret_cast<char*>(&(posDataEnd)), OFFSET_BYTES_SIZE);
        } else {  // last one
            istrm.seekg(0, std::ios::end);
            posDataEnd = istrm.tellg();
            readAll = true;
        }

        istrm.seekg(posDataStart, std::ios::beg);
        // 读出 Value
        char* tmpVal = new char [posDataEnd - posDataStart + 1];
        istrm.read((char*)tmpVal, posDataEnd - posDataStart);
        tmpVal[posDataEnd - posDataStart] = '\0';
        std::string val = tmpVal;
        delete [] tmpVal;

        all.emplace_back(tmpKey, val);

    }
    istrm.close();
}

