#include "kvstore.h"

KVStore::KVStore(const std::string &_dir): KVStoreAPI(_dir)
{
    if(!utils::dirExists(_dir)) utils::mkdir(_dir.c_str());
    dir = _dir;
    memTable = new MemTables(dir);

    // now only pushback level 0
    std::vector<SSTables*> level0;
    cache.push_back(level0);

    /*// reload cache
    cache.clear();
    std::vector<std::string> fileNames;
    std::string fileName;
    uint64_t maxLevel = 0;
    uint64_t level = 0;
    while(level <= maxLevel){
        std::string level_str = "/level-" + std::to_string(level);
        int fileNum = utils::scanDir(dir + level_str, fileNames);
        while(fileNum > 0){
            fileName = fileNames.back();
            fileNames.pop_back();
            SSTables ssTable(dir + level_str, fileName);
            cache[level].push_back(ssTable);
            --fileNum;
        }
        ++level;
    }*/
}

KVStore::~KVStore()
{
    for(auto it = cache.begin(); it != cache.end(); ++it){
        for (auto _it = (*it).begin(); _it != (*it).end(); ++_it) {
            delete *_it;
        }
        (*it).clear();
    }
    cache.clear();
    delete memTable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    // val string too large
    if(INIT_BYTES_SIZE + s.length() + KEY_BYTES_SIZE + OFFSET_BYTES_SIZE > MAX_BYTES_SIZE) assert(0);
    memTable->setSize(memTable->getSize() + s.length() + KEY_BYTES_SIZE + OFFSET_BYTES_SIZE);
    if(memTable->getSize() <= MAX_BYTES_SIZE){
        memTable->put(key, s);
    } else {
        // convert to SSTable
        std::list<std::pair<uint64_t, std::string> > all;
        uint64_t numKey = 0;
        uint64_t minKey = 0;
        uint64_t maxKey = 0;
        memTable->getAll(all, minKey, maxKey, numKey);
        assert(numKey != 0);
        uint64_t level = 0;
        std::string level_str = "/level-" + std::to_string(level);
        // 确定文件名并赋值到此处，不包含.sst
        // 文件命名格式 timeStamp minKey-maxKey pairsNum
        std::string currentFileName = generateFileName(nextTimeStamp, minKey, maxKey, numKey);
        SSTables* ssTable = new SSTables(dir + level_str, all, minKey, maxKey, numKey, nextTimeStamp, currentFileName);
        ++nextTimeStamp;

        cache[0].push_back(ssTable);
        checkCompaction();

        memTable->reset();
        memTable->setSize(memTable->getSize() + s.length() + KEY_BYTES_SIZE + OFFSET_BYTES_SIZE);
        memTable->put(key, s);
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string memTableResult = memTable->get(key);
    if(memTableResult == "~DELETED~") return "";
    if(memTableResult != "") return memTableResult;

    std::string ssTableResult;
    // Search by level
    std::string fileName;
    std::string filePathName;
    uint64_t level = 0;
    while(level <= this->maxLevel){
        std::string level_str = "/level-" + std::to_string(level);
        /*int fileNum = utils::scanDir(dir + level_str, fileNames);*/
        // 注意 level 0 要从最新的 SSTable 开始检查
        uint64_t tableNum = cache[level].size();
        // level0 之中下标越大，越新，应当先检查
        // TODO: 其余level中按照下标大小排序的，按照此修改检查顺序
        while(tableNum > 0){
            fileName = cache[level][tableNum - 1]->fileName +".sst";
            filePathName = dir + level_str + "/" + fileName;
            ssTableResult = (cache[level][tableNum - 1])->get(key, filePathName);
            if(ssTableResult == "~DELETED~") return "";
            if(ssTableResult != "") return ssTableResult;
            --tableNum;
        }
        ++level;
    }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    //TODO: 添加delete之后出现bug 最后生成的一个文件（可能包含全是delete）为0kb 考虑是不是memtable中delete键值都不需要特殊处理？
    if(memTable->del(key)) return true;
    std::string res = get(key);
    if(res == "" || res == "~DELETED~") return false;
    else{
        put(key,"~DELETED~");
        return true;
    }
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    memTable->reset();
    clearAllCacheAndFiles();
    this->nextTimeStamp = 1;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    // 取出 memTable 中在 scan 范围内的
    std::list<std::pair<uint64_t, std::string> > listMem;
    memTable->scan(key1,key2,listMem);
    uint64_t maxLevel = this->maxLevel;

    // 按照自定义函数，heap 会首先按照 first.first 来进行排序，之后按照 first.second 排序
    // 其中 first.first 越小越先输出（key小的在前输出）
    // 当  first.first 一样时候， second越大越先输出（时间戳大的先输出）
    // heap 出来的结果存入 list
    // 之后遍历 list，遇到重复的 key 只保留第一个即可
    std::priority_queue<std::pair<std::pair<uint64_t, std::string>,uint64_t>, std::vector<std::pair<std::pair<uint64_t, std::string>,uint64_t> >, cmpScanHeap> heap;
    // second 对应数字表示来源（下面 vector 中访问对应 list 的下标），方便堆顶 pop 出元素之后知道再新增哪个

    // 考虑到除了 level 0 每一个 level 内文件的 key 在 cache 中按下标都是顺序排列
    // 遍历每一层
    std::vector<std::list<std::pair<uint64_t, std::string> > > listsAllLevels;
    //  注意！！考虑到优先级，下标为 0 位置（优先级最高）应当放置 listMem，原本第零层的紧随其后
    //  （！！且因为索引区间有交叉，需要按照时间戳顺序，时间戳大在前），之后再是剩余的 level 由小到大
    //  这样下标越小优先级越高

    // 用于记录从 level1 开始，每一个 level 读到了哪个文件，应当读到哪个文件的下标，
    // 这样等上一个文件读入排序完再进行下一个，防止内存中文件过多（可能相同的 key 很多导致内存炸掉）
    // 其中 first 表示当前读到的 index （初始为 startIndex），second 表示
    std::vector<std::pair<uint64_t, uint64_t> > nextAndEndFileIndexInLevels;
    nextAndEndFileIndexInLevels.emplace_back(0,0);  // level 0 填充位置

    // 进堆
    if(!listMem.empty()){
        heap.emplace(listMem.front(), 0);
        listMem.pop_front();
    }
    // 下标为 0 位置（优先级最高）应当放置 listMem
    listsAllLevels.push_back(std::move(listMem));

    // 之后放置原本第零层的文件对应 list （！！且因为索引区间有交叉，需要按照时间戳顺序，时间戳大在前）
    unsigned int fileNumLevel0 = 0;  // 记录第零层有交集的文件数
    auto lvl0_size = cache[0].size();
    for(auto j = 0; j < lvl0_size; ++j){
        if(!((cache[0][lvl0_size-j-1]->getMinKey() > key2)||(cache[0][lvl0_size-j-1]->getMaxKey() < key1))){  // 判定不是没有交集

            // 将该文件与 scan 范围有交集的部分读入相应 listLevel
            std::list<std::pair<uint64_t, std::string> > listLevel;
            cache[0][lvl0_size-j-1]->readIndexAndDataForScan(listLevel, key1, key2);

            // 进堆
            if(!listLevel.empty()){
                heap.emplace(listLevel.front(), fileNumLevel0);
                listLevel.pop_front();
            }

            // 将相应 list 放入索引
            listsAllLevels.push_back(std::move(listLevel));

            // 增加 level0 文件数
            ++fileNumLevel0;
        }
    }

    for(uint64_t level = 1; level <= maxLevel; ++level){

        // 寻找该层所有与 scan 范围有交集的文件并将交集部分读入相应 listLevel
        std::list<std::pair<uint64_t, std::string> > listLevel;

        // 在 level 层中找到与 scan 区间有交集的所有 SSTable 文件下标（有序无重复，顺序查找）
        uint64_t maxIndex = cache[level].size() - 1;
        uint64_t now = 0;
        uint64_t startIndex = -1, endIndex = -1;
        uint64_t nowMinKey, nowMaxKey;
        bool findTableInLevel = false;
        bool findStartIndex = false;
        bool findEndIndex = false;
        if(!cache[level].empty()){
            while(now <= maxIndex)
            {
                nowMinKey = cache[level][now]->getMinKey();
                nowMaxKey = cache[level][now]->getMaxKey();
                if(!findStartIndex && key1 <= nowMaxKey)
                {
                    if(now == 0 && key2 < nowMinKey){
                        findTableInLevel = false;
                        break;
                    }
                    findTableInLevel = true;
                    findStartIndex = true;
                    startIndex = now;
                }
                if(key2 < nowMinKey)
                {
                    findTableInLevel = true;
                    findEndIndex = true;
                    endIndex = now - 1;
                    break;
                }
                ++now;
            }
            if(!findEndIndex){
                endIndex = maxIndex;
            }
            if(findTableInLevel && endIndex < startIndex) findTableInLevel = false;
        }

        if(findTableInLevel){
            (cache[level][startIndex])->readIndexAndDataForScan(listLevel, key1, key2);
            if(!listLevel.empty()){
                heap.emplace(listLevel.front(), level + fileNumLevel0);
                listLevel.pop_front();
            }
        }

        // 无论有没有内容，将相应 list 放入索引
        listsAllLevels.push_back(std::move(listLevel));
        nextAndEndFileIndexInLevels.emplace_back(startIndex + 1, endIndex);
    }

    // 对所有 lists 做最堆排序
    uint64_t lastKey = -1;  // TODO: FIRST Key can not be max uint64_t
    while(!heap.empty()){
        auto top = heap.top();
        heap.pop();
        if(lastKey != top.first.first){
            list.push_back(top.first);
            lastKey = top.first.first;
        }
        auto lvlIndex = top.second;
        if(!listsAllLevels[lvlIndex].empty()){
            heap.emplace(listsAllLevels[lvlIndex].front(), lvlIndex);
            listsAllLevels[lvlIndex].pop_front();
        } else {
            if(lvlIndex <= fileNumLevel0) continue;
            auto nextAndEndFileIndex = nextAndEndFileIndexInLevels[lvlIndex - fileNumLevel0];
            if(nextAndEndFileIndex.first <= nextAndEndFileIndex.second){
                // 读入下一个 table
                std::list<std::pair<uint64_t, std::string> > listLevel;
                (cache[lvlIndex - fileNumLevel0][nextAndEndFileIndex.first])->readIndexAndDataForScan(listLevel, key1, key2);
                heap.emplace(listLevel.front(), lvlIndex);
                listLevel.pop_front();
                listsAllLevels[lvlIndex] = std::move(listLevel);
                nextAndEndFileIndexInLevels[lvlIndex - fileNumLevel0].first = nextAndEndFileIndex.first + 1;
            }
        }
    }

    // 注意 scan 如果发现多个相同的 key 以层数最小的为准！
    // 遍历 list 去除重复 key，遇到重复的 key 只保留第一个
    // 注意判定相等时候只比较 key，需要自定义比较函数
    // auto _it = unique(list.begin(), list.end(), cmpList);
    // list.erase(_it, list.end());

    // 此时list为最终结果
}

// 参数level为当前文件数达到阈值的层号
void KVStore::compaction(uint64_t level,  unsigned int moreNum)
{
    // TODO: compaction 中可能出现问题
    //  最后一层出现范围重复的情况
    //  且尚未实现最后一层中 DELETED 键值删除

    // 当前最大层数文件数达到阈值
    if(this->maxLevel == level){
        // 新建一层
        std::vector<SSTables*> newLevel;
        cache.push_back(newLevel);
        this->maxLevel = level + 1;
    }
    if(level == 0){
        /*对于 Level 0 层的合并操作来说，需要将所有的
        Level 0 层中的 SSTable 与 Level 1 层中的部分 SSTable 进行合并，随后将
        产生的新 SSTable 文件写入到 Level 1 层中。*/

        //统计 Level 0 层中所有 SSTable 所覆盖的键的区间（无序，逐个查找）
        uint64_t minKeyLevel = cache[level][0]->getMinKey();
        uint64_t maxKeyLevel = cache[level][0]->getMaxKey();
        uint64_t curMinKey, curMaxKey;
        for(auto it = cache[level].begin(); it != cache[level].end(); ++it){
            curMinKey = (*it)->getMinKey();
            curMaxKey = (*it)->getMaxKey();
            if(curMinKey < minKeyLevel) minKeyLevel = curMinKey;
            if(curMaxKey > maxKeyLevel) maxKeyLevel = curMaxKey;
        }

        //然后在 Level 1 层中找到与此区间有交集的所有 SSTable 文件（有序无重复，顺序查找）
        uint64_t maxIndex = cache[level + 1].size() - 1;
        uint64_t now = 0;
        uint64_t startIndex = -1, endIndex = -1;
        uint64_t nowMinKey, nowMaxKey;
        bool findTableInNextLevel = false;
        bool findStartIndex = false;
        bool findEndIndex = false;
        if(!cache[level + 1].empty()){
            while(now <= maxIndex)
            {
                nowMinKey = cache[level + 1][now]->getMinKey();
                nowMaxKey = cache[level + 1][now]->getMaxKey();
                if(!findStartIndex && minKeyLevel <= nowMaxKey)
                {
                    if(now == 0 && maxKeyLevel < nowMinKey){
                        findTableInNextLevel = false;
                        break;
                    }
                    findTableInNextLevel = true;
                    findStartIndex = true;
                    startIndex = now;
                }
                if(maxKeyLevel < nowMinKey)
                {
                    findTableInNextLevel = true;
                    findEndIndex = true;
                    endIndex = now - 1;
                    break;
                }
                ++now;
            }
            if(!findEndIndex){
                endIndex = maxIndex;
            }
            if(findTableInNextLevel && endIndex < startIndex) findTableInNextLevel = false;
        }

        // 将涉及到的文件读到内存，进行归并排序，并生成SSTable文件写回下一层（Level 1）
        // 其中 level0 中每个文件当做一路，level1整体当做一路
        // level0 发生 compaction 只可能有且仅有有三个文件
        std::list<std::pair<uint64_t, std::string> > level0_list0;
        std::list<std::pair<uint64_t, std::string> > level0_list1;
        std::list<std::pair<uint64_t, std::string> > level0_list2;
        cache[level][0]->readAllIndexAndData(level0_list0);
        cache[level][1]->readAllIndexAndData(level0_list1);
        cache[level][2]->readAllIndexAndData(level0_list2);
        // 读取原来各个 SSTable 的时间戳
        uint64_t level0_timeStamp0 = cache[level][0]->getTimeStamp();
        uint64_t level0_timeStamp1 = cache[level][1]->getTimeStamp();
        uint64_t level0_timeStamp2 = cache[level][2]->getTimeStamp();


        // 建立最小化堆，进行四路（level1 为空时三路）归并
        // 按照自定义函数，heap 会首先按照 first.first 来进行排序，之后按照 first.second 排序
        std::priority_queue<std::pair<std::pair<uint64_t, std::string>,uint64_t>, std::vector<std::pair<std::pair<uint64_t, std::string>,uint64_t> >, cmpHeap> heap;
        // 前一个数字表示来源，方便堆顶pop出元素之后知道再新增哪个
        std::pair<std::pair<uint64_t, std::string>, uint64_t> level0_0(level0_list0.front(), level0_timeStamp0);
        std::pair<std::pair<uint64_t, std::string>, uint64_t> level0_1(level0_list1.front(), level0_timeStamp1);
        std::pair<std::pair<uint64_t, std::string>, uint64_t> level0_2(level0_list2.front(), level0_timeStamp2);
        level0_list0.pop_front();
        level0_list1.pop_front();
        level0_list2.pop_front();
        heap.push(level0_0);
        heap.push(level0_1);
        heap.push(level0_2);
        uint64_t maxTimeStamp = getMax(level0_timeStamp0, getMax(level0_timeStamp1, level0_timeStamp2));
        std::list<std::pair<std::pair<uint64_t, std::string>, uint64_t> > level1_list;
        if(findTableInNextLevel){
            for(auto j = startIndex; j <= endIndex; ++j){
                // 直接把 timeStamp 信息一起弄进去
                (cache[level + 1][j])->readAllIndexAndDataWithTimeStamp(level1_list);
                maxTimeStamp = getMax(maxTimeStamp, (cache[level + 1][j])->getTimeStamp());
            }
            heap.push(level1_list.front());
            level1_list.pop_front();
        }

        std::list<std::pair<uint64_t, std::string> > result;
        // 进行归并直到堆空
        while(!heap.empty()){
            // 在合并时，如果遇到相同键 K 的多条记录，通过比较时间戳来决定键 K 的最新值，时间戳大的记录被保留。
            // heap 会首先按照 first.first 来进行排序，之后按照 first.second 排序
            // 取出堆顶最小的
            auto top = heap.top();
            heap.pop();
            result.push_back(top.first);
            auto topTimeStamp = top.second;
            if(topTimeStamp == level0_timeStamp0){
                if(!level0_list0.empty()){
                    heap.push(std::make_pair(level0_list0.front(), level0_timeStamp0));
                    level0_list0.pop_front();
                }
                continue;
            }
            if(topTimeStamp == level0_timeStamp1){
                if(!level0_list1.empty()){
                    heap.push(std::make_pair(level0_list1.front(), level0_timeStamp1));
                    level0_list1.pop_front();
                }
                continue;
            }
            if(topTimeStamp == level0_timeStamp2){
                if(!level0_list2.empty()){
                    heap.push(std::make_pair(level0_list2.front(), level0_timeStamp2));
                    level0_list2.pop_front();
                }
                continue;
            }
            if(findTableInNextLevel){
                if(!level1_list.empty()){
                    heap.push(level1_list.front());
                    level1_list.pop_front();
                }
            }
        }

        // 遍历 result 去除重复 key
        // 注意判定相等时候只比较 key，需要自定义比较函数
        auto _it = unique(result.begin(), result.end(), cmpList);
        result.erase(_it, result.end());

        std::vector<SSTables*> tempCache;
        auto cacheSize = cache[level + 1].size();
        std::string level_str = "/level-" + std::to_string(level+1);
        if(findTableInNextLevel){

            // 依据已经判断好在下一层 level 中 startIndex, endIndex，将下一层 level 的 cache
            // 分为两个部分，后半部分取出之后再拼凑上去，只留下前半部分
            // 因为文件在 level 中要按照 key 顺序排（与原来已有的契合），而 fileName 和 cache 也与这个有关

            for(auto j = endIndex + 1; j < cacheSize; ++j){
                tempCache.push_back(cache[level + 1][j]);
            }
            for(auto j = endIndex + 1; j < cacheSize; ++j){
                cache[level + 1].pop_back();
            }

            // 删除原来的 SSTable 文件，cache中对应的也要 delete！！！（因为 new 出来）
            for(auto j = startIndex; j <= endIndex; ++j){
                std::string currentFileName = cache[level + 1][j]->fileName + ".sst";
                // delete that file
                if(utils::rmfile((dir+level_str+"/"+currentFileName).c_str())){
                    throw("remove file error in compaction!");
                }
                // delete that cache
                delete (cache[level + 1][j]);
            }

            for(auto j = startIndex; j <= endIndex; ++j){
                cache[level + 1].pop_back();
            }
        }

        // 删除 level0 层文件，清空对应 cache
        level_str = "/level-" + std::to_string(level);
        for(auto j = 0; j <= 2; ++j){
            std::string currentFileName = cache[level][j]->fileName + ".sst";
            // delete that file
            if(utils::rmfile((dir+level_str+"/"+currentFileName).c_str())){
                throw("remove file error in compaction!");
            }
            // delete that cache
            delete (cache[level][j]);
        }
        cache[level].clear();

        // 将结果每 2 MB 分成一个新的 SSTable 文件（最后一个 SSTable 可以不足 2MB），写入到 Level 1 中
        // 在其中操作时 cache 直接 push_back 即可
        // 取最大 maxTimeStamp 作为共同 timeStamp
        writeAllListToSSTables(result, maxTimeStamp, level + 1);

        // 将临时缓存与目前的缓存合并
        if(findTableInNextLevel)
        for(auto j = endIndex + 1; j < cacheSize; ++j){
            cache[level + 1].push_back(tempCache[j-endIndex-1]);
        }
        tempCache.clear();

        // 对下一个 level 层的 cache 进行由小到大重新排序
        std::sort(cache[level + 1].begin(), cache[level + 1].end(), cmpSSTableMinKey);

        return;

    } else {  // 其余level
        // 从 level 的 SSTable 中，优先选择时间戳最小的若干个文件（时间戳相等选择键最小的文件）使得文件数满足层数要求
        // first.first 参数为时间戳，first.second 参数为最小键值，second 参数为对应 level 中的 cache 下标
        std::priority_queue<std::pair<std::pair<uint64_t, uint64_t>, unsigned int>, std::vector<std::pair<std::pair<uint64_t, uint64_t>, unsigned int> >, std::greater< > > heapSelectFiles;
        uint64_t tmpStamp;
        uint64_t tmpMinKey;
        for(unsigned int j = 0; j < cache[level].size(); ++j){
            tmpStamp = cache[level][j]->getTimeStamp();
            tmpMinKey = cache[level][j]->getMinKey();
            heapSelectFiles.push(std::make_pair(std::make_pair(tmpStamp, tmpMinKey), j));
        }
        std::priority_queue<unsigned int, std::vector<unsigned int>, std::greater<> > selectedFilesIndex;
        while (moreNum > 0){
            auto top = heapSelectFiles.top();
            selectedFilesIndex.push(top.second);
            heapSelectFiles.pop();
            --moreNum;
        }
        while(!heapSelectFiles.empty()) heapSelectFiles.pop();

        // 按顺序输出 index
        std::list<unsigned int> fileIndex;
        while(!selectedFilesIndex.empty()){
            auto index = selectedFilesIndex.top();
            fileIndex.push_back(index);
            selectedFilesIndex.pop();
        }

        //计算 Level 层中选出的 SSTable 所覆盖的键的区间（有序，直接找最小和最大）
        uint64_t minKeyLevel = cache[level][(fileIndex.front())]->getMinKey();
        uint64_t maxKeyLevel = cache[level][(fileIndex.back())]->getMaxKey();

        //然后在 next Level 中找到与此区间有交集的所有 SSTable 文件（有序无重复）
        uint64_t maxIndex = cache[level + 1].size() - 1;
        uint64_t now = 0;
        uint64_t startIndex = -1, endIndex = -1;
        uint64_t nowMinKey, nowMaxKey;
        bool findTableInNextLevel = false;
        bool findStartIndex = false;
        bool findEndIndex = false;
        if(!cache[level + 1].empty()){
            while(now <= maxIndex)
            {
                nowMinKey = cache[level + 1][now]->getMinKey();
                nowMaxKey = cache[level + 1][now]->getMaxKey();
                if(!findStartIndex && minKeyLevel <= nowMaxKey)
                {
                    if(now == 0 && maxKeyLevel < nowMinKey){
                        findTableInNextLevel = false;
                        break;
                    }
                    findTableInNextLevel = true;
                    findStartIndex = true;
                    startIndex = now;
                }
                if(maxKeyLevel < nowMinKey)
                {
                    findTableInNextLevel = true;
                    findEndIndex = true;
                    endIndex = now - 1;
                    break;
                }
                ++now;
            }
            if(!findEndIndex){
                endIndex = maxIndex;
            }
            if(findTableInNextLevel && endIndex < startIndex) findTableInNextLevel = false;
        }

        // 如果没有在下一层找到区间相交的
        if(!findTableInNextLevel){

        }

        uint64_t maxTimeStamp = 0;
        // 将涉及到的文件读到内存，进行归并排序，并生成SSTable文件写回下一层
        // level 中所有被选中文件当做一路
        std::list<std::pair<std::pair<uint64_t, std::string>, uint64_t> > level_list;
        auto cacheLevelSize = cache[level].size();
        std::string level_str = "/level-" + std::to_string(level);
        std::vector<SSTables*> newLevelCache;
        for(auto j = 0; j < cacheLevelSize; ++j){
            // 直接把 timeStamp 信息一起弄进去
            if(!fileIndex.empty() && fileIndex.front() == j) {
                fileIndex.pop_front();
                (cache[level][j])->readAllIndexAndDataWithTimeStamp(level_list);
                maxTimeStamp = getMax(maxTimeStamp, (cache[level][j])->getTimeStamp());
                std::string currentFileName = cache[level][j]->fileName + ".sst";
                // 删除文件与对应缓存
                if(utils::rmfile((dir+level_str+"/"+currentFileName).c_str())){
                    throw("remove file error in compaction!");
                }
                delete (cache[level][j]);
            } else {
                newLevelCache.push_back(cache[level][j]);
            }
        }

        if(newLevelCache.size() != (2 << level)){
            throw("newLevelCache size incorrect !");
        }

        cache[level].clear();
        cache[level] = std::move(newLevelCache);

        // next level 中区间相交的文件可能有很多很多，不能同时放在一个 list 中
        std::list<std::pair<std::pair<uint64_t, std::string>, uint64_t> > nextLevel_list;
        uint64_t nextIndex = startIndex + 1;
        if(findTableInNextLevel){
            for(auto j = startIndex; j <= endIndex; ++j){
                // 直接把 timeStamp 信息一起弄进去
                maxTimeStamp = getMax(maxTimeStamp, (cache[level + 1][j])->getTimeStamp());
            }
            (cache[level + 1][startIndex])->readAllIndexAndDataWithTimeStamp(nextLevel_list);
        }

        std::list<std::pair<uint64_t, std::string> > result;
        // 为了缓解内存占用过大的问题，在 result 具备一定规模时尝试写一部分，nextLevelTempCache 暂存这一部分的 cache
        std::vector<SSTables *> nextLevelTempCache;
        // 记录 result 中放了多少 key-val 对，达到一定程度就先写入 SSTable
        uint64_t cnt = 0;
        // 本层挑选出的文件 level_list 与下一层挑选出的文件 nextLevel_list 进行两路归并
        while(!level_list.empty()){
            if(cnt > KV_NUM_TO_WRITE_IN_COMPACTION){
                tryWriteSomeListToSSTables(result, maxTimeStamp, level+1, nextLevelTempCache);
                cnt = 0;
            }
            if(findTableInNextLevel){
                if(nextLevel_list.empty()){
                    if(nextIndex <= endIndex){
                        nextLevel_list.clear();
                        (cache[level + 1][nextIndex])->readAllIndexAndDataWithTimeStamp(nextLevel_list);
                        ++nextIndex;
                    } else {
                        break;
                    }
                }
            } else break;
            auto level_front = level_list.front();
            auto nextLevel_front = nextLevel_list.front();
            if(level_front.first.first == nextLevel_front.first.first){
                if(level_front.second > nextLevel_front.second){
                    result.push_back(level_front.first);
                    ++cnt;
                } else {
                    result.push_back(nextLevel_front.first);
                    ++cnt;
                }
                level_list.pop_front();
                nextLevel_list.pop_front();
                continue;
            }
            if(level_front.first.first > nextLevel_front.first.first){
                result.push_back(nextLevel_front.first);
                ++cnt;
                nextLevel_list.pop_front();
            } else {
                result.push_back(level_front.first);
                ++cnt;
                level_list.pop_front();
            }
        }
        if(level_list.empty()){
            while(!nextLevel_list.empty()){
                auto nextLevel_front = nextLevel_list.front();
                result.push_back(nextLevel_front.first);
                ++cnt;
                nextLevel_list.pop_front();
                if(cnt > KV_NUM_TO_WRITE_IN_COMPACTION){
                    tryWriteSomeListToSSTables(result, maxTimeStamp, level+1, nextLevelTempCache);
                    cnt = 0;
                }
            }
        } else {
            while(!level_list.empty()){
                auto level_front = level_list.front();
                result.push_back(level_front.first);
                ++cnt;
                level_list.pop_front();
            }
        }

        std::vector<SSTables*> tempCache;
        auto cacheSize = cache[level + 1].size();
        level_str = "/level-" + std::to_string(level+1);
        if(findTableInNextLevel){

            // 依据已经判断好在下一层 level 中 startIndex, endIndex，将下一层 level 的 cache
            // 分为两个部分，后半部分取出之后再拼凑上去，只留下前半部分
            // 因为文件在 level 中要按照 key 顺序排（与原来已有的契合），而 fileName 和 cache 也与这个有关

            for(auto j = endIndex + 1; j < cacheSize; ++j){
                tempCache.push_back(cache[level + 1][j]);
            }
            for(auto j = endIndex + 1; j < cacheSize; ++j){
                cache[level + 1].pop_back();
            }

            // 删除原来的 SSTable 文件，cache中对应的也要 delete！！！（因为 new 出来）
            for(auto j = startIndex; j <= endIndex; ++j){
                std::string currentFileName = cache[level + 1][j]->fileName + ".sst";
                // delete that file
                if(utils::rmfile((dir+level_str+"/"+currentFileName).c_str())){
                    throw("remove file error in compaction!");
                }
                // delete that cache
                delete (cache[level + 1][j]);
            }

            for(auto j = startIndex; j <= endIndex; ++j){
                cache[level + 1].pop_back();
            }
        }

        // 将中途为避免占用内存过大事先读走的 nextLevelTempCache 先放入 level+1 cache 中
        for(auto j = 0; j < nextLevelTempCache.size(); ++j){
            cache[level + 1].push_back(nextLevelTempCache[j]);
        }
        nextLevelTempCache.clear();

        // 将结果每 2 MB 分成一个新的 SSTable 文件（最后一个 SSTable 可以不足 2MB），写入到 Level 1 中
        // 在其中操作时 cache 直接 push_back 即可
        // 取最大 maxTimeStamp 作为共同 timeStamp
        writeAllListToSSTables(result, maxTimeStamp, level + 1);

        // 将临时缓存与目前的缓存合并
        if(findTableInNextLevel)
            for(auto j = endIndex + 1; j < cacheSize; ++j){
                cache[level + 1].push_back(tempCache[j-endIndex-1]);
            }
        tempCache.clear();

        // 对下一个 level 层的 cache 进行由小到大重新排序
        std::sort(cache[level + 1].begin(), cache[level + 1].end(), cmpSSTableMinKey);

        return;
    }

}

// 每次新增 SSTable 都调用检查一次
void KVStore::checkCompaction()
{
    uint64_t current = 0;
    uint64_t fileNum = 2;
    while(current <= this->maxLevel){
        auto size = cache[current].size();
        if(size > fileNum)
            compaction(current, size - fileNum);
        else return;
        ++current;
        fileNum *= 2;
    }
}

// 函数将所有 allList 中的 key-value 对写入 SSTables （每达到 2MB 分新文件），并记录对应缓存
// 第一个参数为 allList，第二个参数为临时的指向缓存的指针（new 出来的）
void KVStore::writeAllListToSSTables(std::list<std::pair<uint64_t, std::string> > &allList, const uint64_t &timeStamp, const uint64_t &level)
{
    bool isMaxLvl = (level == this->maxLevel);
    uint64_t size = INIT_BYTES_SIZE;
    std::string level_str = "/level-" + std::to_string(level);
    std::list<std::pair<uint64_t, std::string>> currentList;
    uint64_t numKey = 0;
    while (!allList.empty()){
        auto tmp = allList.front();
        allList.pop_front();
        std::string tmpVal = tmp.second;
        size += (tmpVal.length() + KEY_BYTES_SIZE + OFFSET_BYTES_SIZE);
        if(size <= MAX_BYTES_SIZE){
            if(isMaxLvl)
                if(tmpVal == "~DELETED~"){
                    size -= (tmpVal.length() + KEY_BYTES_SIZE + OFFSET_BYTES_SIZE);
                    continue;
                }
            currentList.push_back(std::move(tmp));
            ++numKey;
        } else {
            // 到 2MB，转化成 SSTable
            // 获取有关信息
            uint64_t minKey = currentList.front().first;
            uint64_t maxKey = currentList.back().first;
            if(maxKey < minKey)
                throw("ERROR   maxKey < minKey in writeListToSSTables");
            std::string currentFileName = generateFileName(timeStamp, minKey, maxKey, numKey);
            // 新建 SSTables 并存入对应缓存
            SSTables* ssTable = new SSTables(dir + level_str, currentList, minKey, maxKey, numKey, timeStamp, currentFileName);
            cache[level].push_back(ssTable);
            // 将刚刚未能转换的插入 currentList
            currentList.clear();
            numKey = 0;
            size = INIT_BYTES_SIZE;
            size += (tmpVal.length() + KEY_BYTES_SIZE + OFFSET_BYTES_SIZE);
            currentList.push_back(tmp);
            ++numKey;
        }
    }
    uint64_t minKey = currentList.front().first;
    uint64_t maxKey = currentList.back().first;
    std::string currentFileName = generateFileName(timeStamp, minKey, maxKey, numKey);
    // 新建 SSTables 并存入对应缓存
    SSTables* ssTable = new SSTables(dir + level_str, currentList, minKey, maxKey, numKey, timeStamp, currentFileName);
    cache[level].push_back(ssTable);
    currentList.clear();
}

void KVStore::clearAllCacheAndFiles() {
    // Clear by level
    std::string fileName;
    uint64_t level = 0;
    while(level <= this->maxLevel){
        std::string level_str = "/level-" + std::to_string(level);
        uint64_t tableNum = cache[level].size();
        while(tableNum > 0){
            fileName = cache[level][tableNum - 1]->fileName +".sst";
            // delete that file
            if(utils::rmfile((dir+level_str+"/"+fileName).c_str())){
                throw("remove file error in reset!");
            }
            // delete that cache
            delete (cache[level][tableNum - 1]);
            --tableNum;
        }
        cache[level].clear();
        if(utils::rmdir((dir+level_str).c_str())){
            if(this->maxLevel != 0) throw("remove level dir error in reset!");
        }
        ++level;
    }
    cache.clear();

    // now only pushback level 0
    std::vector<SSTables*> level0;
    cache.push_back(level0);
    this->maxLevel = 0;
}

// 为了缓解内存占用过大的问题，在 result 具备一定规模时尝试写一部分，nextLevelTempCache 暂存这一部分的 cache
void KVStore::tryWriteSomeListToSSTables(std::list<std::pair<uint64_t, std::string>> &allList, const uint64_t &timeStamp, const uint64_t &level, std::vector<SSTables *> &nextLevelTempCache) {

    bool isMaxLvl = (level == this->maxLevel);
    uint64_t size = INIT_BYTES_SIZE;
    std::string level_str = "/level-" + std::to_string(level);
    std::list<std::pair<uint64_t, std::string> > currentList;
    uint64_t numKey = 0;
    while (!allList.empty()){
        auto tmp = allList.front();
        allList.pop_front();
        std::string tmpVal = tmp.second;
        size += (tmpVal.length() + KEY_BYTES_SIZE + OFFSET_BYTES_SIZE);
        if(size <= MAX_BYTES_SIZE){
            if(isMaxLvl)
                if(tmpVal == "~DELETED~"){
                    size -= (tmpVal.length() + KEY_BYTES_SIZE + OFFSET_BYTES_SIZE);
                    continue;
                }
            currentList.push_back(std::move(tmp));
            ++numKey;
        } else {
            // 到 2MB，转化成 SSTable
            // 获取有关信息
            uint64_t minKey = currentList.front().first;
            uint64_t maxKey = currentList.back().first;
            if(maxKey < minKey)
                throw("ERROR   maxKey < minKey in writeListToSSTables");
            std::string currentFileName = generateFileName(timeStamp, minKey, maxKey, numKey);
            // 新建 SSTables 并存入对应缓存
            SSTables* ssTable = new SSTables(dir + level_str, currentList, minKey, maxKey, numKey, timeStamp, currentFileName);
            nextLevelTempCache.push_back(ssTable);
            // 将刚刚未能转换的插入 currentList
            currentList.clear();
            numKey = 0;
            size = INIT_BYTES_SIZE;
            size += (tmpVal.length() + KEY_BYTES_SIZE + OFFSET_BYTES_SIZE);
            currentList.push_back(tmp);
            ++numKey;
        }
    }
    // 剩下的没有来得及写的再放回去
    allList = std::move(currentList);
}
