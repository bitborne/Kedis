
CC = gcc
FLAGS = -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -luring -ldl
JEMALLOC_FLAGS = -DHAVE_JEMALLOC -ljemalloc
SRCS = kvstore.c src/core/protocol.c src/network/ntyco.c src/network/proactor.c src/engines/kvs_array.c src/engines/kvs_rbtree.c src/engines/kvs_hash.c src/engines/kvs_skiplist.c src/persistence/ksf.c src/persistence/aof.c replication.c src/utils/memory_pool.c
TESTCASE_SRCS = testcase.c
TARGET = kvstore
SUBDIR = ./NtyCo/
TESTCASE = testcase

OBJS = $(SRCS:.c=.o)


all: $(SUBDIR) $(TARGET) $(TESTCASE)

$(SUBDIR): ECHO
	make -C $@

ECHO:
	@echo $(SUBDIR)

$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(FLAGS) $(JEMALLOC_FLAGS)

$(TESTCASE): $(TESTCASE_SRCS)
	$(CC) -o $@ $^

%.o: %.c
	$(CC) $(FLAGS) $(JEMALLOC_FLAGS) -c $^ -o $@

clean:
	rm -rf $(OBJS) $(TARGET) $(TESTCASE)


