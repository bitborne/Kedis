#include "../../kvstore.h"
#if ENABLE_SKIPLIST

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_LEVEL 6

typedef struct skiplist_node_s {
    char *key;
    char *value;
    struct skiplist_node_s **forward;
} skiplist_node_t;

typedef struct skiplist_s {
    int level;
    skiplist_node_t *header;
} skiplist_t;

typedef struct skiplist_s kvs_skiplist_t;

kvs_skiplist_t global_skiplist;

static skiplist_node_t* skiplist_create_node(int level, const robj *key, const robj *value) {
    // fprintf(stderr, "skp0-->\n");
    if (!value || !key || !key->ptr || !value->ptr) return NULL; 
    // fprintf(stderr, "skp1-->\n");
    skiplist_node_t *newNode = (skiplist_node_t*)kvs_malloc(sizeof(skiplist_node_t));
    if (!newNode) return NULL;
    
    // fprintf(stderr, "skp2-->\n");
    size_t key_len = key->len;
    size_t value_len = value->len;
    
    // fprintf(stderr, "skp3-->\n");
    char *kcopy = kvs_malloc(key_len + 1);
    if (!kcopy) {
      kvs_free(newNode);
      return NULL;
    }
    // fprintf(stderr, "skp4-->\n");
    memcpy(kcopy, key->ptr, key_len + 1);
    
    char *vcopy = kvs_malloc(value_len + 1);
    if (!vcopy) {
      kvs_free(kcopy);
      kvs_free(newNode);
      return NULL;
    }
    // fprintf(stderr, "skp5-->\n");
    memcpy(vcopy, value->ptr, value_len + 1);

    newNode->key = kcopy;
    newNode->value = vcopy;
    newNode->forward = (skiplist_node_t**)kvs_malloc((level + 1) * sizeof(skiplist_node_t*));
    if (!newNode->forward) {
        kvs_free(vcopy);
        kvs_free(kcopy);
        kvs_free(newNode);
        return NULL;
    }

    return newNode;
}

static int skiplist_random_level() {
    int level = 0;
    while (rand() < RAND_MAX / 2 && level < MAX_LEVEL)
        level++;
    return level;
}

int kvs_skiplist_create(kvs_skiplist_t *skiplist) {
    if (!skiplist) return -1;

    srand(time(NULL));

    skiplist->level = 0;

    robj hdr_key = {0};
    hdr_key.len = 0, hdr_key.ptr = "";
    robj hdr_value = {0};
    hdr_value.len = 0, hdr_value.ptr = "";
    skiplist->header = skiplist_create_node(MAX_LEVEL, &hdr_key, &hdr_value);
    if (!skiplist->header) return -1;

    for (int i = 0; i <= MAX_LEVEL; ++i) {
        skiplist->header->forward[i] = NULL;
    }

    return 0;
}

void kvs_skiplist_destroy(kvs_skiplist_t *skiplist) {
    if (!skiplist || !skiplist->header) return;

    skiplist_node_t *current = skiplist->header->forward[0];
    while (current != NULL) {
        skiplist_node_t *next = current->forward[0];
        if (current->key) kvs_free(current->key);
        if (current->value) kvs_free(current->value);
        if (current->forward) kvs_free(current->forward);
        kvs_free(current);
        current = next;
    }

    if (skiplist->header->forward) kvs_free(skiplist->header->forward);
    kvs_free(skiplist->header);
    skiplist->header = NULL;
}

int kvs_skiplist_set(kvs_skiplist_t *skiplist, robj* key, robj* value) {
    if (!skiplist || !key || !value || !key->ptr || !value->ptr) return -1;

    skiplist_node_t *update[MAX_LEVEL + 1];
    skiplist_node_t *current = skiplist->header;

    for (int i = skiplist->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key->ptr) < 0)
            current = current->forward[i];
        update[i] = current;
    }

    current = current->forward[0];

    if (current == NULL || strcmp(current->key, key->ptr) != 0) {
        int level = skiplist_random_level();

        if (level > skiplist->level) {
            for (int i = skiplist->level + 1; i <= level; ++i)
                update[i] = skiplist->header;
            skiplist->level = level;
        }

        skiplist_node_t *newNode = skiplist_create_node(level, key, value);
        if (!newNode) return -1;

        for (int i = 0; i <= level; ++i) {
            newNode->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = newNode;
        }

        return 0;
    } else {
        return 1;
    }
}

char* kvs_skiplist_get(kvs_skiplist_t *skiplist, robj* key) {
    if (!skiplist || !key || !key->ptr) return NULL;

    skiplist_node_t *current = skiplist->header;

    for (int i = skiplist->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key->ptr) < 0)
            current = current->forward[i];
    }

    current = current->forward[0];

    if (current && strcmp(current->key, key->ptr) == 0) {
        return current->value;
    } else {
        return NULL;
    }
}

int kvs_skiplist_del(kvs_skiplist_t *skiplist, robj* key) {
    if (!skiplist || !key || !key->ptr) return -1;

    skiplist_node_t *update[MAX_LEVEL + 1];
    skiplist_node_t *current = skiplist->header;

    for (int i = skiplist->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key->ptr) < 0)
            current = current->forward[i];
        update[i] = current;
    }

    current = current->forward[0];

    if (current && strcmp(current->key, key->ptr) == 0) {
        for (int i = 0; i <= skiplist->level; ++i) {
            if (update[i]->forward[i] != current) break;
            update[i]->forward[i] = current->forward[i];
        }

        while (skiplist->level > 0 && skiplist->header->forward[skiplist->level] == NULL) {
            skiplist->level--;
        }

        if (current->key) kvs_free(current->key);
        if (current->value) kvs_free(current->value);
        if (current->forward) kvs_free(current->forward);
        kvs_free(current);

        return 0;
    } else {
        return 1;
    }
}

int kvs_skiplist_mod(kvs_skiplist_t *skiplist, robj* key, robj* value) {
    if (!skiplist || !key || !value || !key->ptr || !value->ptr) return -1;

    skiplist_node_t *current = skiplist->header;

    for (int i = skiplist->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key->ptr) < 0)
            current = current->forward[i];
    }

    current = current->forward[0];

    if (current && strcmp(current->key, key->ptr) == 0) {
        size_t value_len = value->len;
        char *vcopy = kvs_malloc(value_len + 1);
        if (!vcopy) return -1;

        memcpy(vcopy, value->ptr, value_len + 1);
        kvs_free(current->value);
        current->value = vcopy;

        return 0;
    } else {
        return 1;
    }
}

int kvs_skiplist_exist(kvs_skiplist_t *skiplist, robj* key) {
    if (!skiplist || !key || !key->ptr) return 0;

    skiplist_node_t *current = skiplist->header;

    for (int i = skiplist->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key->ptr) < 0)
            current = current->forward[i];
    }

    current = current->forward[0];

    if (current && strcmp(current->key, key->ptr) == 0) {
        return 1;
    } else {
        return 0;
    }
}

#endif
