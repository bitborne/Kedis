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

/* ---------------- 将命令序列化为 RESP 字节（堆分配） ---------------- */
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
	size_t len;
	char *cmd = encode_resp_heap(argc, argv, &len);
	if (!cmd) {
		kvs_logError("[REPL] encode_resp_heap failed\n");
		return;
	}

	for (struct slave_conn *s = g_slaves; s; s = s->next) {
		struct conn *nc = s->nc;
		if (nc->fd < 0 || nc->state == ST_CLOSE) continue;

		if (nc->wlen + len <= RESP_BUF_SIZE) {
			/* 小命令：直接塞 wbuf */
			memcpy(nc->wbuf + nc->wlen, cmd, len);
			nc->wlen += len;
		} else {
			/* 大命令：独立分配 buffer，塞满 wbuf 后剩余挂 iov_data */
			char *buf = kvs_malloc(len);
			if (!buf) {
				nc->state = ST_CLOSE;
				continue;
			}
			memcpy(buf, cmd, len);

			size_t space = RESP_BUF_SIZE - nc->wlen;
			if (space > 0) {
				memcpy(nc->wbuf + nc->wlen, buf, space);
				nc->wlen += space;
				nc->iov_data = buf + space;
				nc->iov_len = len - space;
			} else {
				nc->iov_data = buf;
				nc->iov_len = len;
			}
			nc->iov_base = buf;
			nc->iov_needs_free = 1;
		}
		flush_send_queue(ring, nc);
	}

	kvs_free(cmd);
}
