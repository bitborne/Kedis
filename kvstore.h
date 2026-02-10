#ifndef __KV_STORE_H__
#define __KV_STORE_H__
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>

#include "include/kvs_constants.h"
#include "include/kvs_hash.h"
#include "include/kvs_rbtree.h"
#include "include/kvs_array.h"
#include "include/kvs_skiplist.h"
#include "include/kvs_aof.h"
#include "include/kvs_ksf.h"
#include "include/kvs_network.h"
#include "include/memory_pool.h"

void* kvs_calloc(size_t num, size_t size);
void *kvs_malloc(size_t size);
void kvs_free(void *ptr);

#endif