#include "kvstore.h"
#if ENABLE_HASH

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

kvs_hash_t global_hash;

static int kvs_hash_need_expand(hashtable_t *hash);

#define ROTL32(x, r) ((x << r) | (x >> (32 - r)))

static inline uint32_t fmix32(uint32_t h) {
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;
	return h;
}

static uint32_t murmurhash3_32(const char *key, int len) {
	const uint8_t *data = (const uint8_t *)key;
	const int nblocks = len / 4;

	uint32_t h1 = 0xdeadbeef;
	uint32_t c1 = 0xcc9e2d51;
	uint32_t c2 = 0x1b873593;

	const uint32_t *blocks = (const uint32_t *)data;

	for (int i = 0; i < nblocks; i++) {
		uint32_t k1 = blocks[i];
		k1 *= c1;
		k1 = ROTL32(k1, 15);
		k1 *= c2;
		h1 ^= k1;
		h1 = ROTL32(h1, 13);
		h1 = h1 * 5 + 0xe6546b64;
	}

	const uint8_t *tail = (const uint8_t *)(data + nblocks * 4);
	uint32_t k1 = 0;

	switch (len & 3) {
		case 3: k1 ^= tail[2] << 16;
		case 2: k1 ^= tail[1] << 8;
		case 1: k1 ^= tail[0];
			k1 *= c1;
			k1 = ROTL32(k1, 15);
			k1 *= c2;
			h1 ^= k1;
	}

	h1 ^= len;
	h1 = fmix32(h1);
	return h1;
}

static int _hash(const char *key, int size) {
	if (!key) return -1;
	return murmurhash3_32(key, strlen(key)) % size;
}

hashnode_t *_create_node(const char *key, const char *value) {

	hashnode_t *node = (hashnode_t*)kvs_malloc(sizeof(hashnode_t));
	if (!node) return NULL;

#if HASH_ENABLE_KEY_POINTER
	size_t key_len = strlen(key);
	size_t value_len = strlen(value);

	char *kcopy = kvs_malloc(key_len + 1);
	if (kcopy == NULL) {
		kvs_free(node);
		return NULL;
	}
	memcpy(kcopy, key, key_len + 1);

	node->key = kcopy;

	char *kvalue = kvs_malloc(value_len + 1);
	if (kvalue == NULL) {
		kvs_free(kcopy);
		kvs_free(node);
		return NULL;
	}
	memcpy(kvalue, value, value_len + 1);

	node->value = kvalue;

#else
	strncpy(node->key, key, MAX_KEY_LEN - 1);
	node->key[MAX_KEY_LEN - 1] = '\0';
	strncpy(node->value, value, MAX_VALUE_LEN - 1);
	node->value[MAX_VALUE_LEN - 1] = '\0';
#endif
	node->next = NULL;

	return node;
}

int kvs_hash_create(kvs_hash_t *hash) {

	if (!hash) return -1;

	hash->nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * MAX_TABLE_SIZE);
	if (!hash->nodes) return -1;
	memset(hash->nodes, 0, sizeof(hashnode_t*) * MAX_TABLE_SIZE);

	hash->max_slots = MAX_TABLE_SIZE;
	hash->count = 0;
	hash->capacity = MAX_TABLE_SIZE;
	hash->load_factor = 0.75f;

	hash->rehash_nodes = NULL;
	hash->rehash_slots = 0;
	hash->rehash_index = 0;
	hash->rehash_state = REHASH_STATE_IDLE;

	return 0;
}

void kvs_hash_destroy(kvs_hash_t *hash) {

	if (!hash || !hash->nodes) return;

	for (int i = 0; i < hash->max_slots; i++) {
		hashnode_t *node = hash->nodes[i];
		while (node != NULL) {
			hashnode_t *tmp = node;
			node = node->next;

#if HASH_ENABLE_KEY_POINTER
			if (tmp->key) kvs_free(tmp->key);
			if (tmp->value) kvs_free(tmp->value);
#endif
			kvs_free(tmp);
		}
		hash->nodes[i] = NULL;
	}

	kvs_free(hash->nodes);
	hash->nodes = NULL;
	hash->count = 0;
	hash->max_slots = 0;

	if (hash->rehash_nodes) {
		for (int i = 0; i < hash->rehash_slots; i++) {
			hashnode_t *node = hash->rehash_nodes[i];
			while (node != NULL) {
				hashnode_t *tmp = node;
				node = node->next;

#if HASH_ENABLE_KEY_POINTER
				if (tmp->key) kvs_free(tmp->key);
				if (tmp->value) kvs_free(tmp->value);
#endif
				kvs_free(tmp);
			}
			hash->rehash_nodes[i] = NULL;
		}
		kvs_free(hash->rehash_nodes);
		hash->rehash_nodes = NULL;
	}
	hash->rehash_slots = 0;
	hash->rehash_index = 0;
	hash->rehash_state = REHASH_STATE_IDLE;
}

#define REHASH_STEPS_PER_OP 16

int kvs_hash_set(kvs_hash_t *hash, char *key, char *value) {
	if (!hash || !key || !value) return -1;

	for (int step = 0; step < REHASH_STEPS_PER_OP; step++) {
		if (kvs_hash_step_rehash(hash) == 1) break;
	}

	if (hash->rehash_state == REHASH_STATE_IDLE &&
	    kvs_hash_need_expand(hash)) {
		kvs_hash_start_rehash(hash);
	}

	int idx_main = _hash(key, hash->max_slots);
	int idx_rehash = hash->rehash_nodes ? _hash(key, hash->rehash_slots) : -1;

	hashnode_t *node = NULL;

	if (hash->rehash_nodes && idx_rehash < hash->rehash_index) {
		node = hash->rehash_nodes[idx_rehash];
		while (node != NULL) {
			if (strcmp(node->key, key) == 0) {
				return 1;
			}
			node = node->next;
		}
	}

	node = hash->nodes[idx_main];
	while (node != NULL) {
		if (strcmp(node->key, key) == 0) {
			return 1;
		}
		node = node->next;
	}

	hashnode_t *new_node = _create_node(key, value);
	if (!new_node) return -1;

	if (hash->rehash_nodes && idx_rehash < hash->rehash_index) {
		new_node->next = hash->rehash_nodes[idx_rehash];
		hash->rehash_nodes[idx_rehash] = new_node;
	} else {
		new_node->next = hash->nodes[idx_main];
		hash->nodes[idx_main] = new_node;
	}

	hash->count++;
	return 0;
}

char * kvs_hash_get(kvs_hash_t *hash, char *key) {

	if (!hash || !key) return NULL;

	for (int step = 0; step < REHASH_STEPS_PER_OP; step++) {
		if (kvs_hash_step_rehash(hash) == 1) break;
	}

	int idx_main = _hash(key, hash->max_slots);
	int idx_rehash = hash->rehash_nodes ? _hash(key, hash->rehash_slots) : -1;

	if (hash->rehash_nodes && idx_rehash < hash->rehash_index) {
		hashnode_t *node = hash->rehash_nodes[idx_rehash];
		while (node != NULL) {
			if (strcmp(node->key, key) == 0) {
				return node->value;
			}
			node = node->next;
		}
	}

	hashnode_t *node = hash->nodes[idx_main];
	while (node != NULL) {
		if (strcmp(node->key, key) == 0) {
			return node->value;
		}
		node = node->next;
	}

	return NULL;
}

int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value) {

	if (!hash || !key) return -1;

	for (int step = 0; step < REHASH_STEPS_PER_OP; step++) {
		if (kvs_hash_step_rehash(hash) == 1) break;
	}

	int idx_main = _hash(key, hash->max_slots);
	int idx_rehash = hash->rehash_nodes ? _hash(key, hash->rehash_slots) : -1;

	hashnode_t *node = NULL;

	if (hash->rehash_nodes && idx_rehash < hash->rehash_index) {
		node = hash->rehash_nodes[idx_rehash];
		while (node != NULL) {
			if (strcmp(node->key, key) == 0) {
				break;
			}
			node = node->next;
		}
	}

	if (node == NULL) {
		node = hash->nodes[idx_main];
		while (node != NULL) {
			if (strcmp(node->key, key) == 0) {
				break;
			}
			node = node->next;
		}
	}

	if (node == NULL) {
		return 1;
	}

#if HASH_ENABLE_KEY_POINTER
	kvs_free(node->value);
	size_t value_len = strlen(value);
	char *kvalue = kvs_malloc(value_len + 1);
	if (kvalue == NULL) return -2;
	memcpy(kvalue, value, value_len + 1);
	node->value = kvalue;
#else
	strncpy(node->value, value, MAX_VALUE_LEN - 1);
	node->value[MAX_VALUE_LEN - 1] = '\0';
#endif

	return 0;
}

inline int kvs_hash_count(kvs_hash_t *hash) {
	return hash->count;
}

int kvs_hash_del(kvs_hash_t *hash, char *key) {
	if (!hash || !key) return -2;

	for (int step = 0; step < REHASH_STEPS_PER_OP; step++) {
		if (kvs_hash_step_rehash(hash) == 1) break;
	}

	int idx_main = _hash(key, hash->max_slots);
	int idx_rehash = hash->rehash_nodes ? _hash(key, hash->rehash_slots) : -1;

	hashnode_t *node = NULL;
	hashnode_t *prev = NULL;

	if (hash->rehash_nodes && idx_rehash < hash->rehash_index) {
		node = hash->rehash_nodes[idx_rehash];
		prev = NULL;
		while (node != NULL) {
			if (strcmp(node->key, key) == 0) {
				if (prev == NULL) {
					hash->rehash_nodes[idx_rehash] = node->next;
				} else {
					prev->next = node->next;
				}

#if HASH_ENABLE_KEY_POINTER
				kvs_free(node->key);
				kvs_free(node->value);
#endif
				kvs_free(node);
				hash->count--;

				return 0;
			}
			prev = node;
			node = node->next;
		}
	}

	node = hash->nodes[idx_main];
	prev = NULL;
	while (node != NULL) {
		if (strcmp(node->key, key) == 0) {
			if (prev == NULL) {
				hash->nodes[idx_main] = node->next;
			} else {
				prev->next = node->next;
			}

#if HASH_ENABLE_KEY_POINTER
			kvs_free(node->key);
			kvs_free(node->value);
#endif
			kvs_free(node);
			hash->count--;

			return 0;
		}
		prev = node;
		node = node->next;
	}

	return -1;
}

int kvs_hash_exist(kvs_hash_t *hash, char *key) {

	char *value = kvs_hash_get(hash, key);
	if (value) return 1;

	return 0;
}

static int kvs_hash_next_power_of_2(int size) {
	if (size <= 16) return 16;

	size_t n = (size_t)size;
	n--;
	n |= n >> 1;
	n |= n >> 2;
	n |= n >> 4;
	n |= n >> 8;
	n |= n >> 16;
	n++;

	return (int)n;
}

int kvs_hash_resize(hashtable_t *hash, int new_size) {
	if (!hash || !hash->nodes) return -1;

	if (new_size <= hash->max_slots) {
		fprintf(stderr, "resize error: new_size %d <= current %d\n", new_size, hash->max_slots);
		return -1;
	}

	if (new_size > 1024 * 1024) {
		fprintf(stderr, "resize error: new_size %d exceeds maximum\n", new_size);
		return -1;
	}

	new_size = kvs_hash_next_power_of_2(new_size);

	printf("resizing hash table from %d to %d slots...\n", hash->max_slots, new_size);

	hashnode_t **new_nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * new_size);
	if (!new_nodes) {
		fprintf(stderr, "resize error: failed to allocate memory\n");
		return -1;
	}

	memset(new_nodes, 0, sizeof(hashnode_t*) * new_size);

	int moved_count = 0;
	for (int i = 0; i < hash->max_slots; i++) {
		hashnode_t *node = hash->nodes[i];
		while (node != NULL) {
			hashnode_t *next = node->next;

			int new_idx = _hash(node->key, new_size);

			node->next = new_nodes[new_idx];
			new_nodes[new_idx] = node;

			moved_count++;
			node = next;
		}
	}

	kvs_free(hash->nodes);

	hash->nodes = new_nodes;
	hash->max_slots = new_size;
	hash->capacity = new_size;

	printf("resize complete: moved %d nodes\n", moved_count);
	return 0;
}

int kvs_hash_get_stats(hashtable_t *hash, kvs_hash_stats_t *stats) {
	if (!hash || !stats) return -1;

	stats->count = hash->count;
	stats->max_slots = hash->max_slots;
	stats->capacity = hash->capacity;
	stats->load_factor = hash->count > 0 ? (float)hash->count / hash->max_slots : 0.0f;

	int total_chain_length = 0;
	int empty_slots = 0;

	for (int i = 0; i < hash->max_slots; i++) {
		hashnode_t *node = hash->nodes[i];
		if (node == NULL) {
			empty_slots++;
		} else {
			int chain_len = 0;
			while (node != NULL) {
				chain_len++;
				node = node->next;
			}
			total_chain_length += chain_len;
			if (chain_len > stats->max_chain_length) {
				stats->max_chain_length = chain_len;
			}
		}
	}

	stats->empty_slots = empty_slots;
	stats->avg_chain_length = hash->count > 0 ? (float)total_chain_length / (hash->max_slots - empty_slots) : 0.0f;

	return 0;
}

void kvs_hash_print_stats(hashtable_t *hash) {
	kvs_hash_stats_t stats;
	if (kvs_hash_get_stats(hash, &stats) == 0) {
		printf("\n=== Hash Table Statistics ===\n");
		printf("Count:           %d\n", stats.count);
		printf("Max Slots:       %d\n", stats.max_slots);
		printf("Capacity:        %d\n", stats.capacity);
		printf("Load Factor:     %.2f\n", stats.load_factor);
		printf("Empty Slots:     %d\n", stats.empty_slots);
		printf("Max Chain:       %d\n", stats.max_chain_length);
		printf("Avg Chain:       %.2f\n", stats.avg_chain_length);

		if (hash->rehash_state == REHASH_STATE_ACTIVE) {
			printf("Rehash:          ACTIVE\n");
			printf("  Progress:      %d/%d slots\n",
			       hash->rehash_index, hash->max_slots);
			float progress = (float)hash->rehash_index / hash->max_slots * 100;
			printf("  Percentage:    %.1f%%\n", progress);
		} else {
			printf("Rehash:          IDLE\n");
		}

		printf("============================\n\n");
	}
}

static inline int kvs_hash_need_expand(hashtable_t *hash) {
	if (!hash) return 0;
	return (float)hash->count / hash->max_slots > hash->load_factor;
}

int kvs_hash_start_rehash(hashtable_t *hash) {
	if (!hash || !hash->nodes) return -1;
	if (hash->rehash_state == REHASH_STATE_ACTIVE) {
		return 0;
	}

	int new_size = kvs_hash_next_power_of_2(hash->max_slots * 2);
	if (new_size > 1024 * 1024) {
		return -1;
	}

	printf("[REHASH] Starting rehash from %d to %d slots\n",
	       hash->max_slots, new_size);

	hash->rehash_nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * new_size);
	if (!hash->rehash_nodes) {
		fprintf(stderr, "[REHASH] Failed to allocate memory for new table\n");
		return -1;
	}
	memset(hash->rehash_nodes, 0, sizeof(hashnode_t*) * new_size);

	hash->rehash_slots = new_size;
	hash->rehash_index = 0;
	hash->rehash_state = REHASH_STATE_ACTIVE;

	return 0;
}

int kvs_hash_step_rehash(hashtable_t *hash) {
	if (!hash || hash->rehash_state != REHASH_STATE_ACTIVE) {
		return 0;
	}

	if (hash->rehash_index < hash->max_slots) {
		hashnode_t *node = hash->nodes[hash->rehash_index];
		while (node != NULL) {
			hashnode_t *next = node->next;

			int new_idx = _hash(node->key, hash->rehash_slots);

			node->next = hash->rehash_nodes[new_idx];
			hash->rehash_nodes[new_idx] = node;

			node = next;
		}

		hash->nodes[hash->rehash_index] = NULL;
		hash->rehash_index++;
	}

	if (hash->rehash_index >= hash->max_slots) {
		kvs_hash_finish_rehash(hash);
		return 1;
	}

	return 0;
}

void kvs_hash_finish_rehash(hashtable_t *hash) {
	if (!hash || hash->rehash_state != REHASH_STATE_ACTIVE) return;

	printf("[REHASH] Finishing rehash\n");

	kvs_free(hash->nodes);

	hash->nodes = hash->rehash_nodes;
	hash->max_slots = hash->rehash_slots;
	hash->capacity = hash->rehash_slots;

	hash->rehash_nodes = NULL;
	hash->rehash_slots = 0;
	hash->rehash_index = 0;
	hash->rehash_state = REHASH_STATE_IDLE;

	printf("[REHASH] Rehash completed. New size: %d slots\n", hash->max_slots);
}

inline int kvs_hash_is_rehashing(hashtable_t *hash) {
	if (!hash) return 0;
	return hash->rehash_state == REHASH_STATE_ACTIVE;
}

#endif
