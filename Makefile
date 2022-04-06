
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: BloomFilters.h SSTables.o SkipLists.o MemTables.o kvstore.o correctness.o

persistence: BloomFilters.h SSTables.o SkipLists.o MemTables.o kvstore.o persistence.o

try:  utils.h try.cpp

clean:
	-rm -f correctness persistence *.o
