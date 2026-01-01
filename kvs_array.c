#include "kvstore.h"
#if ENABLE_ARRAY
#include <math.h>
// 使用 singleton 单例模式
kvs_array_t global_array = {0};

int kvs_array_create(kvs_array_t *inst) {
  
  if (!inst) return -1;
  if (inst->table) {
    printf("kvs_array_create: Table has allocated memory\n");
    return -2;
  }
  inst->table = kvs_calloc(KVS_ARRAY_SIZE, sizeof(kvs_array_item_t));
  for (int i = 0; i < KVS_ARRAY_SIZE; i++) {
    inst->table[i].key = inst->table[i].value = NULL;
  }
  inst->total = 0;
  
  return 0;
}
// 有开就有关
void kvs_array_destroy(kvs_array_t* inst) {

  if (inst == NULL) return;

  if (inst->table) kvs_free(inst->table);
  /* 为什么不释放inst？ *///=> 因为不是在kvs_array_create 里面创建的，我们不管 ==> `开闭原则`

}

char* kvs_array_get(kvs_array_t* inst, char* key) {

  if (inst == NULL || key == NULL) return NULL;
  // printf("-->arr not NULL\n");
  // for (int i = 0; i < inst->total; i++) {
  for (int i = 0; i < KVS_ARRAY_SIZE; i++) {
    if (inst->table[i].key) { // 找到了一个非空位
      if (!strcmp(key, inst->table[i].key)) return inst->table[i].value;
      
    }
  }
  return NULL;
}
/// @brief 
/// @param inst ;
/// @param key ;
/// @param value ;
/// @return < 0, error |  == 0, success | >0, key has existed
int kvs_array_set(kvs_array_t* inst, char* key, char* value) {
  if (key == NULL || value == NULL || inst == NULL) return -1;
  if (inst->total == KVS_ARRAY_SIZE) return -2;

  // 先查找是否 key has existed
  if (kvs_array_get(inst, key)) return 1;  //  > 0 表示 key 已经存在

  char* tmpKey = strdup(key); // strdup 自动 malloc 且自动预留 \0
  char* tmpValue = strdup(value); // 两行替代下面两坨注释 但是它好像是 POXIS API
  // printf("tmpkey = %s, tmpvalue = %s\n", tmpKey, tmpValue); // DEBUG
  // char* tmpKey = kvs_calloc(1, strlen(key) + 1);
  // if (tmpKey == NULL) return -3;
  // strncpy(tmpKey, key, strlen(key));
  
  // char* tmpValue = kvs_calloc(1, strlen(value) + 1);
  // if (tmpValue == NULL) return -4;
  // strncpy(tmpValue, value, strlen(value));

  
  for (int idx = 0; idx < KVS_ARRAY_SIZE; idx++) {
    if (inst->table[idx].key == NULL) {
      inst->table[idx].key = tmpKey;
      inst->table[idx].value = tmpValue;
      inst->total++;
      return 0;
    }
  }
};


/// @brief 
/// @param inst ;
/// @param key ;
/// @return < 0 error | == 0 success | > 0 not exist;
int kvs_array_del(kvs_array_t *inst, char* key) {
  if (inst == NULL || key == NULL) return -1;

  // for (int i = 0; i < inst->total; i++) {
  for (int idx = 0; idx < KVS_ARRAY_SIZE; idx++) {
    if (inst->table[idx].key) {
      if (!strcmp(key, inst->table[idx].key)) {
        kvs_free(inst->table[idx].key); // set的时候calloc, del的时候 free
        inst->table[idx].key = NULL;  // free 后置空
        kvs_free(inst->table[idx].value); 
        inst->table[idx].value = NULL;
        inst->total--;
        return 0;
      }
    }
  }
  return 1;
}

/// @brief 
/// @param inst 
/// @param key 
/// @param value 
/// @return < 0 error | == 0 success  | > 0  not exist
int kvs_array_mod(kvs_array_t *inst, char* key, char* value) {
  if (inst == NULL || key == NULL || value == NULL) return -1;

  int i = 0;
  // for (;i < inst->total; i++) {
  for (; i < KVS_ARRAY_SIZE; i++) {
    if (inst->table[i].key) {
      if (!strcmp(key, inst->table[i].key)) {
        kvs_free(inst->table[i].value);
        inst->table[i].value = NULL;

        char* tmpValue = kvs_calloc(1, strlen(value) + 1);
        if (!tmpValue) return -2;
        strncpy(tmpValue, value, strlen(value));
        inst->table[i].value = tmpValue;

        return 0;

      }
    }
  }

  return i;
}

/// @brief
/// @param inst 
/// @param key 
/// @return 1 Yes | 0 No | -1 error
int kvs_array_exist(kvs_array_t *inst, char* key) { 
  if (inst == NULL || key == NULL ) return -1;
  return (kvs_array_get(inst, key) != NULL);

}
#endif
