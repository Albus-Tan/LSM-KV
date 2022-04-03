//
// Created by ENVY on 2022/3/9.
//

#include <iostream>

#include "SkipLists.h"

double SkipLists::my_rand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

int SkipLists::randomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && my_rand() < 0.5)
    {
        ++result;
    }
    return result;
}

void SkipLists::scan(uint64_t key_start, uint64_t key_end, std::list<std::pair<uint64_t, std::string> > &list)
{
    SKNode* x = head;
    // -- loop invariant: head→key < searchKey
    for (int i = MAX_LEVEL - 1; i >= 0 ; --i){
        while(x->forwards[i]->key < key_start){
            x = x->forwards[i];
        }
    }
    // -- x→key < searchKey ≤ x→forward[1]→key
    x = x->forwards[0];
    if (x->key == key_start) { // find
        if(x->val != "~DELETED~"){
            std::pair<uint64_t, std::string> new_pair(x->key, x->val);
            list.push_back(new_pair);
        }
    }
    while(x->forwards[0]->key <= key_end){
        x = x->forwards[0];
        if(x->val != "~DELETED~"){
            std::pair<uint64_t, std::string> new_pair(x->key, x->val);
            list.push_back(new_pair);
        }
    }
}

void SkipLists::put(uint64_t key, const std::string &value)
{
    SKNode* x = head;
    std::vector<SKNode *> update;

    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        update.push_back(nullptr);
    }

    for (int i = MAX_LEVEL - 1; i >= 0; --i) {
        while (x->forwards[i]->key < key) {
            x = x->forwards[i];
        }
        update[i] = x;
    }

    // -- x→key < searchKey ≤ x→forward[i]→key
    x = x->forwards[0];
    if (x->key == key) {
        // size 的增加由预测时做
        this->setSize(this->getSize() - (x->val).length() - KEY_BYTES_SIZE - OFFSET_BYTES_SIZE);
        x->val = value;
    }
    else{
        int level = randomLevel();
        x = new SKNode(key, value, SKNodeType::NORMAL);
        for(int i = 0; i < level; ++i) {
            x->forwards[i] = update[i]->forwards[i];
            update[i]->forwards[i] = x;
        }
    }
}

std::string SkipLists::get(uint64_t key)
{
    SKNode* x = head;
    // -- loop invariant: head→key < searchKey
    for (int i = MAX_LEVEL - 1; i >= 0 ; --i){
        while(x->forwards[i]->key < key){
            x = x->forwards[i];
        }
    }
    // -- x→key < searchKey ≤ x→forward[1]→key
    x = x->forwards[0];
    if (x->key == key) { // found, return x->value
        return x->val;
    }
    else { // not found, return failure
        return "";
    }
}

bool SkipLists::del(uint64_t key)
{
    std::vector<SKNode *> update;
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        update.push_back(nullptr);
    }
    SKNode* x = head;
    for (int i = MAX_LEVEL - 1; i >= 0; --i){
        while(x->forwards[i]->key < key){
            x = x->forwards[i];
        }
        update[i] = x;
    }
    x = x->forwards[0];
    if(x->key == key){
        if(x->val == "~DELETED~") return false;
        for(int i = 0; i < MAX_LEVEL; ++i){
            if(update[i]->forwards[i] != x) break;
            update[i]->forwards[i] = x->forwards[i];
        }
        this->setSize(this->getSize() - (x->val).length() - KEY_BYTES_SIZE - OFFSET_BYTES_SIZE);
        delete x;
        return true;
    }
    return false;
}

void SkipLists::display()
{
    for (int i = MAX_LEVEL - 1; i >= 0; --i)
    {
        std::cout << "Level " << i + 1 << ":h";
        SKNode *node = head->forwards[i];
        while (node->type != SKNodeType::NIL)
        {
            std::cout << "-->(" << node->key << "," << node->val << ")";
            node = node->forwards[i];
        }

        std::cout << "-->N" << std::endl;
    }
}

void SkipLists::reset()
{
    SKNode *n1 = head->forwards[0];
    SKNode *n2;
    while (n1->type != SKNodeType::NIL)
    {
        n2 = n1->forwards[0];
        delete n1;
        n1 = n2;
    }
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        head->forwards[i] = NIL;
    }
    setSize(INIT_BYTES_SIZE);
}

void SkipLists::getAll(std::list<std::pair<uint64_t, std::string> > &all, uint64_t &minKey, uint64_t &maxKey, uint64_t &numKey)
{
    SKNode *n1 = head->forwards[0];
    SKNode *n2;
    numKey = 0;
    minKey = n1->key;
    while (n1->type != SKNodeType::NIL)
    {
        n2 = n1->forwards[0];
        std::pair<uint64_t, std::string> new_pair(n1->key, n1->val);
        ++numKey;
        all.push_back(new_pair);
        maxKey = n1->key;
        n1 = n2;
    }
}