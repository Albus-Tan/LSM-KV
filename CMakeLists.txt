cmake_minimum_required(VERSION 3.21)
project(lsm-kv)

set(CMAKE_CXX_STANDARD 14)

add_executable(lsm-kv BloomFilters.h SSTables.cc SkipLists.cc MemTables.cc kvstore.cc correctness.cc)