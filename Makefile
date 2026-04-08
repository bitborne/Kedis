CC = gcc
CFLAGS = -O2 -Wall
LDFLAGS = -lpthread -luring -ldl -ljemalloc -lrdmacm -libverbs

# 主项目源文件
SRCS = src/core/kvstore.c \
       src/core/protocol.c \
       src/core/config.c \
       src/core/sync_command.c \
       src/core/slave_sync.c \
       src/network/proactor.c \
       src/network/rdma_sync.c \
       src/engines/kvs_array.c \
       src/engines/kvs_rbtree.c \
       src/engines/kvs_hash.c \
       src/engines/kvs_skiplist.c \
       src/persistence/ksf.c \
       src/persistence/ksf_stream.c \
       src/persistence/aof.c \
       src/utils/kmem.c \
       src/utils/kmem_compat.c \
       src/utils/kvs_log.c

OBJS = $(SRCS:.c=.o)

# 测试用例（独立编译）
# TEST_SRCS = tests/testcase.c
# TEST_OBJS = $(TEST_SRCS:.c=.o)

# kmem测试
KMEM_TEST_SRC = tests/test_kmem.c
KMEM_TEST_OBJ = tests/test_kmem.o
KMEM_TEST = tests/test_kmem

TARGET = kvstore
# TESTCASE = tests/testcase

.PHONY: all clean

all: $(TARGET) $(TESTCASE)

# 静态模式规则：.o 挨着 .c
$(OBJS): %.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# $(TEST_OBJS): %.o: %.c
# 	$(CC) $(CFLAGS) -c $< -o $@

# 主程序（根目录）
$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

# 测试程序（独立，不链接主项目）
# $(TESTCASE): $(TEST_OBJS)
# 	$(CC) -o $@ $^

# kmem测试程序
$(KMEM_TEST_OBJ): $(KMEM_TEST_SRC)
	$(CC) $(CFLAGS) -c $< -o $@

$(KMEM_TEST): $(KMEM_TEST_OBJ) src/utils/kmem.o
	$(CC) -o $@ $^ -lpthread

test-kmem: $(KMEM_TEST)
	./$(KMEM_TEST)

clean:
	rm -f $(OBJS) $(TEST_OBJS) $(TARGET) $(TESTCASE)
	rm -f $(KMEM_TEST_OBJ) $(KMEM_TEST)
	rm -f src/utils/kmem.o