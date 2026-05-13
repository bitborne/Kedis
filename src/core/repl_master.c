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

	char hexbuf[64];
	int hn = 0;
	for (int i = 0; i < 16 && i < (int)len; i++)
		hn += snprintf(hexbuf + hn, sizeof(hexbuf) - hn, "%02x ", (unsigned char)cmd[i]);
	debug("propagate cmd=%s len=%zu hex=%s", argv[0].ptr ? argv[0].ptr : "NULL", len, hexbuf);

	for (struct slave_conn *s = g_slaves; s; s = s->next) {
		struct conn *nc = s->nc;
		if (nc->fd < 0 || nc->state == ST_CLOSE) continue;

		debug("slave fd=%d wlen=%zu iov_len=%zu", nc->fd, nc->wlen, nc->iov_len);

		/* 若已有 pending iov 且无 inflight send，先发出去，
		 * 减少后续合并拷贝。 */
		if (nc->iov_len > 0 && nc->send_inflight == 0)
			flush_send_queue(ring, nc);

		if (nc->wlen + len <= RESP_BUF_SIZE && nc->iov_len == 0) {
			/* 小命令且 iov 为空：直接塞 wbuf */
			memcpy(nc->wbuf + nc->wlen, cmd, len);
			nc->wlen += len;
			debug("propagate to fd=%d -> wbuf (wlen=%zu)", nc->fd, nc->wlen);
		} else {
			/* wbuf 满或已有 iov：追加到 iov，不再 split 到 wbuf，
			 * 避免 flush_send_queue 只发 wbuf 不发 iov 导致积压。 */
			size_t old_iov = nc->iov_len;

			/* 限制总缓冲区大小，防止从节点跟不上时主节点 OOM */
			if (nc->wlen + old_iov + nc->iov_next_len + len > 8 * 1024 * 1024) {
				kvs_logError("[REPL] Slave buffer overflow fd=%d, closing\n", nc->fd);
				nc->state = ST_CLOSE;
				continue;
			}

			if (old_iov > 0 && nc->send_inflight) {
				/* 当前 iov 正在内核发送中，不能覆盖 iov_data/len/base。
				 * 把新数据暂存到 iov_next，等 OP_SEND 完成后再替换。
				 * iov_next 只保存新命令，不复制旧数据（旧数据已在 iov 中）。 */
				size_t next_len = nc->iov_next_len;
				char *new_next = kvs_malloc(next_len + len);
				if (!new_next) {
					nc->state = ST_CLOSE;
					continue;
				}
				if (next_len > 0) {
					memcpy(new_next, nc->iov_next, next_len);
					kvs_free(nc->iov_next);
				}
				memcpy(new_next + next_len, cmd, len);
				nc->iov_next = new_next;
				nc->iov_next_len = next_len + len;
				debug("propagate to fd=%d -> iov_next (len=%zu)", nc->fd, nc->iov_next_len);
			} else {
				/* 没有 inflight send，可以安全覆盖 iov_data */
				char *old_base = nc->iov_base;
				char *new_base = kvs_malloc(old_iov + len);
				if (!new_base) {
					nc->state = ST_CLOSE;
					continue;
				}
				if (old_iov > 0)
					memcpy(new_base, nc->iov_data, old_iov);
				if (old_base)
					kvs_free(old_base);
				memcpy(new_base + old_iov, cmd, len);

				nc->iov_data = new_base;
				nc->iov_len = old_iov + len;
				nc->iov_base = new_base;
				nc->iov_needs_free = 1;
				debug("propagate to fd=%d -> iov (iov_len=%zu)", nc->fd, nc->iov_len);
			}
		}
		flush_send_queue(ring, nc);
	}

	if (cmd_needs_free)
		kvs_free(cmd);
}
