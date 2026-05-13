#include "../../include/kvstore.h"
#include <liburing.h>

/* ---------------- 从节点连接管理 ---------------- */

struct slave_conn {
	struct conn *nc;
	struct slave_conn *next;
};

static struct slave_conn *g_slaves = NULL;

/* flush_send_queue 定义在 proactor.c，已去掉 static */
extern void flush_send_queue(struct io_uring *ring, struct conn *c);

/* ---------------- 从节点注册 ---------------- */
void slave_register(struct conn *nc)
{
	struct slave_conn *s = kvs_malloc(sizeof(*s));
	if (!s) {
		kvs_logError("[REPL] Failed to alloc slave_conn\n");
		return;
	}
	s->nc = nc;
	s->next = g_slaves;
	g_slaves = s;
	nc->is_slave = 1;
	nc->is_replconf = 1;
	kvs_logInfo("[REPL] Slave registered fd=%d\n", nc->fd);
}

/* ---------------- 清理 dead slave（连接关闭时调用） ---------------- */
void slave_cleanup_dead(void)
{
	struct slave_conn **pp = &g_slaves;
	while (*pp) {
		struct slave_conn *s = *pp;
		if (s->nc->fd < 0 || s->nc->state == ST_CLOSE) {
			*pp = s->next;
			kvs_free(s);
		} else {
			pp = &s->next;
		}
	}
}

/* ---------------- 快速 RESP 序列化：优先使用栈 buffer ---------------- */
static char* encode_resp_fast(int argc, robj *argv, size_t *out_len,
                              char *stack_buf, size_t stack_size)
{
	int n = snprintf(stack_buf, stack_size, "*%d\r\n", argc);
	if (n < 0 || (size_t)n >= stack_size) return NULL;

	for (int i = 0; i < argc; i++) {
		int m = snprintf(stack_buf + n, stack_size - n, "$%zu\r\n", argv[i].len);
		if (m < 0 || (size_t)m >= stack_size - n) return NULL;
		n += m;

		if ((size_t)argv[i].len + 2 > stack_size - n) return NULL;
		memcpy(stack_buf + n, argv[i].ptr, argv[i].len);
		n += argv[i].len;
		memcpy(stack_buf + n, "\r\n", 2);
		n += 2;
	}

	*out_len = n;
	return stack_buf;
}

/* ---------------- 将命令序列化为 RESP 字节（堆分配，大命令回退） ---------------- */
static char* encode_resp_heap(int argc, robj *argv, size_t *out_len)
{
	/* 先计算总长度 */
	size_t total = 0;
	total += 1 + snprintf(NULL, 0, "%d", argc) + 2; /* *<argc>\r\n */
	for (int i = 0; i < argc; i++) {
		total += 1 + snprintf(NULL, 0, "%zu", argv[i].len) + 2; /* $<len>\r\n */
		total += argv[i].len + 2; /* <data>\r\n */
	}

	char *buf = kvs_malloc(total);
	if (!buf) return NULL;

	char *p = buf;
	p += sprintf(p, "*%d\r\n", argc);
	for (int i = 0; i < argc; i++) {
		p += sprintf(p, "$%zu\r\n", argv[i].len);
		memcpy(p, argv[i].ptr, argv[i].len);
		p += argv[i].len;
		memcpy(p, "\r\n", 2);
		p += 2;
	}
	*out_len = p - buf;
	return buf;
}

/* ---------------- 主节点广播写命令给所有从节点 ---------------- */
void repl_propagate(struct io_uring *ring, int argc, robj *argv)
{
	if (!g_slaves) return;	/* 无从节点时避免不必要的序列化开销 */

	size_t len;
	char stack_buf[4096];
	char *cmd = encode_resp_fast(argc, argv, &len, stack_buf, sizeof(stack_buf));
	int cmd_needs_free = 0;
	if (!cmd) {
		cmd = encode_resp_heap(argc, argv, &len);
		if (!cmd) {
			kvs_logError("[REPL] encode_resp_heap failed\n");
			return;
		}
		cmd_needs_free = 1;
	}

	for (struct slave_conn *s = g_slaves; s; s = s->next) {
		struct conn *nc = s->nc;
		if (nc->fd < 0 || nc->state == ST_CLOSE) continue;

		struct repl_cmd *rc = kvs_malloc(sizeof(*rc) + len);
		if (!rc) {
			kvs_logError("[REPL] Failed to alloc repl_cmd\n");
			nc->state = ST_CLOSE;
			continue;
		}
		rc->data = (char *)(rc + 1);
		memcpy(rc->data, cmd, len);
		rc->len = len;
		rc->next = NULL;

		if (nc->repl_tail) {
			nc->repl_tail->next = rc;
		} else {
			nc->repl_head = rc;
		}
		nc->repl_tail = rc;
		nc->repl_total += len;

		flush_send_queue(ring, nc);
	}

	if (cmd_needs_free)
		kvs_free(cmd);
}
