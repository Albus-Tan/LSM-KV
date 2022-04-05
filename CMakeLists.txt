cmake_minimum_required(VERSION 3.21)
project(lsm-kv)

set(CMAKE_CXX_STANDARD 14)

# 修改clion 内存
#[[MATH(EXPR stack_size "4*1024*1024")
set(CMAKE_EXE_LINKER_FLAGS "-Wl,--stack,${stack_size}")]]

add_executable(lsm-kv BloomFilters.h SSTables.cc SkipLists.cc MemTables.cc kvstore.cc correctness.cc)