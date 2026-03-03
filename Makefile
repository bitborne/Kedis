CC = gcc
CFLAGS = -I./NtyCo/core/ -O2 -Wall
LDFLAGS = -L./NtyCo/ -lntyco -lpthread -luring -ldl -ljemalloc -lrdmacm -libverbs

# 主项目源文件
SRCS = src/core/kvstore.c \
       src/core/protocol.c \
       src/core/config.c \
       src/core/sync_command.c \
       src/network/ntyco.c \
       src/network/proactor.c \
       src/network/rdma_sync.c \
       src/engines/kvs_array.c \
       src/engines/kvs_rbtree.c \
       src/engines/kvs_hash.c \
       src/engines/kvs_skiplist.c \
       src/persistence/ksf.c \
       src/persistence/ksf_stream.c \
       src/persistence/aof.c \
       src/utils/memory_pool.c \
	   src/utils/kvs_log.c

OBJS = $(SRCS:.c=.o)

# 测试用例（独立编译）
# TEST_SRCS = tests/testcase.c
# TEST_OBJS = $(TEST_SRCS:.c=.o)

TARGET = kvstore
# TESTCASE = tests/testcase
SUBDIR = ./NtyCo/

.PHONY: all clean $(SUBDIR)

all: $(SUBDIR) $(TARGET) $(TESTCASE)

$(SUBDIR):
	$(MAKE) -C $@

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

clean:
	rm -f $(OBJS) $(TEST_OBJS) $(TARGET) $(TESTCASE)
	$(MAKE) -C $(SUBDIR) clean 2>/dev/null || true