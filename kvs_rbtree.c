#include "kvstore.h"
#if ENABLE_RBTREE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RED 1
#define BLACK 2

#define ENABLE_KEY_CHAR 1

#if ENABLE_KEY_CHAR
typedef char* KEY_TYPE;
#else
typedef int KEY_TYPE;
#endif
// 1:49:05
// TODO^ 这里继续完成红黑树的 CRUD 与 SET MOD DEL GET 等的对接
// 以及`不完整类型`的错误？？？

typedef struct _rbtree_node {
  unsigned char color;
  struct _rbtree_node* right;
  struct _rbtree_node* left;
  struct _rbtree_node* parent;
  KEY_TYPE key;
  void* value;
} rbtree_node;

typedef struct _rbtree {
  rbtree_node* root;
  rbtree_node* nil;
} rbtree;

rbtree_node* rbtree_mini(rbtree* T, rbtree_node* x) {
  while (x->left != T->nil) {
    x = x->left;
  }
  return x;
}

rbtree_node* rbtree_maxi(rbtree* T, rbtree_node* x) {
  while (x->right != T->nil) {
    x = x->right;
  }
  return x;
}

rbtree_node* rbtree_successor(rbtree* T, rbtree_node* x) {
  rbtree_node* y = x->parent;

  if (x->right != T->nil) {
    return rbtree_mini(T, x->right);
  }

  while ((y != T->nil) && (x == y->right)) {
    x = y;
    y = y->parent;
  }
  return y;
}

void rbtree_left_rotate(rbtree* T, rbtree_node* x) {
  rbtree_node* y =
      x->right;  // x  --> y  ,  y --> x,   right --> left,  left --> right

  x->right = y->left;       // 1 1
  if (y->left != T->nil) {  // 1 2
    y->left->parent = x;
  }

  y->parent = x->parent;      // 1 3
  if (x->parent == T->nil) {  // 1 4
    T->root = y;
  } else if (x == x->parent->left) {
    x->parent->left = y;
  } else {
    x->parent->right = y;
  }

  y->left = x;    // 1 5
  x->parent = y;  // 1 6
}

void rbtree_right_rotate(rbtree* T, rbtree_node* y) {
  rbtree_node* x = y->left;

  y->left = x->right;
  if (x->right != T->nil) {
    x->right->parent = y;
  }

  x->parent = y->parent;
  if (y->parent == T->nil) {
    T->root = x;
  } else if (y == y->parent->right) {
    y->parent->right = x;
  } else {
    y->parent->left = x;
  }

  x->right = y;
  y->parent = x;
}

void rbtree_insert_fixup(rbtree* T, rbtree_node* z) {
  while (z->parent->color == RED) {  // z ---> RED
    if (z->parent == z->parent->parent->left) {
      rbtree_node* y = z->parent->parent->right;
      if (y->color == RED) {
        z->parent->color = BLACK;
        y->color = BLACK;
        z->parent->parent->color = RED;

        z = z->parent->parent;  // z --> RED
      } else {
        if (z == z->parent->right) {
          z = z->parent;
          rbtree_left_rotate(T, z);
        }

        z->parent->color = BLACK;
        z->parent->parent->color = RED;
        rbtree_right_rotate(T, z->parent->parent);
      }
    } else {
      rbtree_node* y = z->parent->parent->left;
      if (y->color == RED) {
        z->parent->color = BLACK;
        y->color = BLACK;
        z->parent->parent->color = RED;

        z = z->parent->parent;  // z --> RED
      } else {
        if (z == z->parent->left) {
          z = z->parent;
          rbtree_right_rotate(T, z);
        }

        z->parent->color = BLACK;
        z->parent->parent->color = RED;
        rbtree_left_rotate(T, z->parent->parent);
      }
    }
  }

  T->root->color = BLACK;
}

void rbtree_insert(rbtree* T, rbtree_node* z) {
  rbtree_node* y = T->nil;
  rbtree_node* x = T->root;

  while (x != T->nil) {
    y = x;

#if ENABLE_KEY_CHAR

    if (strcmp(z->key, x->key) < 0)
      x = x->left;
    else if (strcmp(z->key, x->key) > 0)
      x = x->right;
    else
      return;

#else

    if (z->key < x->key) {
      x = x->left;
    } else if (z->key > x->key) {
      x = x->right;
    } else {  // Exist
      return;
    }
#endif
  }

  z->parent = y;
  if (y == T->nil) {
    T->root = z;
#if ENABLE_KEY_CHAR
  } else if (strcmp(z->key, y->key) < 0) {
#else
  } else if (z->key < y->key) {
#endif
    y->left = z;
  } else {
    y->right = z;
  }

  z->left = T->nil;
  z->right = T->nil;
  z->color = RED;

  rbtree_insert_fixup(T, z);
}

void rbtree_delete_fixup(rbtree* T, rbtree_node* x) {
  while ((x != T->root) && (x->color == BLACK)) {
    if (x == x->parent->left) {
      rbtree_node* w = x->parent->right;
      if (w->color == RED) {
        w->color = BLACK;
        x->parent->color = RED;

        rbtree_left_rotate(T, x->parent);
        w = x->parent->right;
      }

      if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
        w->color = RED;
        x = x->parent;
      } else {
        if (w->right->color == BLACK) {
          w->left->color = BLACK;
          w->color = RED;
          rbtree_right_rotate(T, w);
          w = x->parent->right;
        }

        w->color = x->parent->color;
        x->parent->color = BLACK;
        w->right->color = BLACK;
        rbtree_left_rotate(T, x->parent);

        x = T->root;
      }

    } else {
      rbtree_node* w = x->parent->left;
      if (w->color == RED) {
        w->color = BLACK;
        x->parent->color = RED;
        rbtree_right_rotate(T, x->parent);
        w = x->parent->left;
      }

      if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
        w->color = RED;
        x = x->parent;
      } else {
        if (w->left->color == BLACK) {
          w->right->color = BLACK;
          w->color = RED;
          rbtree_left_rotate(T, w);
          w = x->parent->left;
        }

        w->color = x->parent->color;
        x->parent->color = BLACK;
        w->left->color = BLACK;
        rbtree_right_rotate(T, x->parent);

        x = T->root;
      }
    }
  }

  x->color = BLACK;
}

rbtree_node* rbtree_delete(rbtree* T, rbtree_node* z) {
  rbtree_node* y = T->nil;
  rbtree_node* x = T->nil;

  if ((z->left == T->nil) || (z->right == T->nil)) {
    y = z;
  } else {
    y = rbtree_successor(T, z);
  }

  if (y->left != T->nil) {
    x = y->left;
  } else if (y->right != T->nil) {
    x = y->right;
  }

  x->parent = y->parent;
  if (y->parent == T->nil) {
    T->root = x;
  } else if (y == y->parent->left) {
    y->parent->left = x;
  } else {
    y->parent->right = x;
  }

  if (y != z) {
#if ENABLE_KEY_CHAR
    void* tmp = z->key;
    z->key = y->key;
    y->key = tmp;

    tmp = z->value;
    z->value = y->value;
    y->value = tmp;

#else
    z->key = y->key;
    z->value = y->value;
#endif
  }

  if (y->color == BLACK) {
    rbtree_delete_fixup(T, x);
  }

  return y;
}

rbtree_node* rbtree_search(rbtree* T, KEY_TYPE key) {
  rbtree_node* node = T->root;
  while (node != T->nil) {
#if ENABLE_KEY_CHAR
    if (strcmp(key, node->key) < 0)
      node = node->left;
    else if (strcmp(key, node->key) > 0)
      node = node->right;
    else
      return node;
#else
    if (key < node->key) {
      node = node->left;
    } else if (key > node->key) {
      node = node->right;
    } else {
      return node;
    }

#endif
  }
  return T->nil;
}

void rbtree_traversal(rbtree* T, rbtree_node* node) {
  if (node != T->nil) {
    rbtree_traversal(T, node->left);

#if ENABLE_KEY_CHAR
    printf("key:%s, value:%s\n", node->key, node->value);
#else
    printf("key:%d, color:%d\n", node->key, node->color);
#endif
    rbtree_traversal(T, node->right);
  }
}


#if 0

int main() {
#if ENABLE_KEY_CHAR

  char* keyArray[10] = {"k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "k10"};
  char* valueArray[10] = {"v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10"};

  rbtree* T = (rbtree*)kvs_malloc(sizeof(rbtree));
  if (T == NULL) {
    printf("malloc failed\n");
    return -1;
  }

  T->nil = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
  T->nil->color = BLACK;
  T->root = T->nil;

  rbtree_node* node = T->nil;
  int i = 0;
  for (i = 0; i < 10; i++) {

    node = (rbtree_node*)kvs_malloc(sizeof (rbtree_node));

    node->key = kvs_calloc(1, strlen(keyArray[i]) + 1);
    strcpy(node->key, keyArray[i]);

    node->value = kvs_calloc(1, strlen(valueArray[i] + 1));
    strcpy(node->value, valueArray[i]);

    rbtree_insert(T, node);
  }

  rbtree_traversal(T, T->root);
  printf("----------------------------------------\n");

  for (i = 0; i < 10; i++) {
    rbtree_node* node = rbtree_search(T, keyArray[i]);
    rbtree_node* cur = rbtree_delete(T, node);
    kvs_free(cur);

    rbtree_traversal(T, T->root);
    printf("----------------------------------------\n");
  }

#else
  int keyrbtree[20] = {24, 25, 13, 35, 23, 26, 67, 47, 38, 98,
                       20, 19, 17, 49, 12, 21, 9,  18, 14, 15};

  rbtree* T = (rbtree*)malloc(sizeof(rbtree));
  if (T == NULL) {
    printf("malloc failed\n");
    return -1;
  }

  T->nil = (rbtree_node*)malloc(sizeof(rbtree_node));
  T->nil->color = BLACK;
  T->root = T->nil;

  rbtree_node* node = T->nil;
  int i = 0;
  for (i = 0; i < 20; i++) {
    node = (rbtree_node*)malloc(sizeof(rbtree_node));
    node->key = keyrbtree[i];
    node->value = NULL;

    rbtree_insert(T, node);
  }

  rbtree_traversal(T, T->root);
  printf("----------------------------------------\n");

  for (i = 0; i < 20; i++) {
    rbtree_node* node = rbtree_search(T, keyrbtree[i]);
    rbtree_node* cur = rbtree_delete(T, node);
    free(cur);

    rbtree_traversal(T, T->root);
    printf("----------------------------------------\n");
  }

#endif
}
#endif

typedef rbtree kvs_rbtree_t;
kvs_rbtree_t global_rbtree;

int kvs_rbtree_create(kvs_rbtree_t* inst) {
  if (inst == NULL) return 1;
  inst->nil = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
  inst->nil->color = BLACK;
  inst->nil->parent = inst->nil;
  inst->nil->left = inst->nil;
  inst->nil->right = inst->nil;
  inst->nil->key = NULL; // 安全初始化
  inst->nil->value = NULL; // 安全初始化
  inst->root = inst->nil;

  return 0;
}

void kvs_rbtree_destroy(kvs_rbtree_t* inst) {
  // 每次找到最小的节点，删除
  if (inst == NULL) return;

  while (inst->root != inst->nil) {
    rbtree_node* mini = rbtree_mini(inst, inst->root);
    rbtree_node* cur = rbtree_delete(inst, mini);
    kvs_free(cur->key);
    kvs_free(cur->value);
    kvs_free(cur);
  }

  kvs_free(inst->nil);
}

char* kvs_rbtree_get(kvs_rbtree_t* inst, char* key) {
  if (inst == NULL || key == NULL) return NULL;
  
  rbtree_node* node = rbtree_search(inst, key);

  if (node == inst->nil) {
    return NULL; // 键不存在，安全返回NULL
  }

  return node->value;
}
/// @brief
/// @param inst ;
/// @param key ;
/// @param value ;
/// @return < 0, error |  == 0, success | >0, key has existed
int kvs_rbtree_set(kvs_rbtree_t* inst, char* key, char* value) {
  if (inst == NULL || key == NULL || value == NULL) return -1;
  
  if (rbtree_search(inst, key) != inst->nil) return 1;

  rbtree_node* node = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
  node->key = kvs_calloc(1, strlen(key) + 1);
  if (!node->key) return -2;
  strcpy(node->key, key);
  
  node->value = kvs_calloc(1, strlen(value) + 1);
  if (!node->value) return -2;
  strcpy(node->value, value);

  rbtree_insert(inst, node);
  return 0;
}

/// @brief
/// @param inst ;
/// @param key ;
/// @return < 0 error | == 0 success | > 0 not exist;
int kvs_rbtree_del(kvs_rbtree_t* inst, char* key) {
  if (inst == NULL || key == NULL) return -1;

  rbtree_node* node = rbtree_search(inst, key);

  if (node == inst->nil) return 1;

  rbtree_node* deleted = rbtree_delete(inst, node);
  kvs_free(deleted->key);
  kvs_free(deleted->value);
  kvs_free(deleted);

  return 0;
}

/// @brief
/// @param inst#
/// @param key
/// @param value
/// @return < 0 error | == 0 success  | > 0  not exist
int kvs_rbtree_mod(kvs_rbtree_t* inst, char* key, char* value) {
  if (inst == NULL || key == NULL || value == NULL) return -1;

  rbtree_node* node = rbtree_search(inst, key);

  if (node == inst->nil) return 1;
  kvs_free(node->value);

  node->value = kvs_calloc(1, strlen(value) + 1);
  if (!node->value) return -2;
  strcpy(node->value, value);
  

  return 0;
}

// 递归遍历红黑树并保存到文件
static void _rbtree_save_recursive(rbtree *T, rbtree_node *node, FILE *file, const char *prefix) {
    if (!T || !node || !file || !prefix || node == T->nil) {
        return;
    }

    _rbtree_save_recursive(T, node->left, file, prefix);

    // 保存当前节点的数据，使用适当的前缀
    if (node->key && node->value) {
        fprintf(file, "%sSET %s %s\n", prefix, (char*)node->key, (char*)node->value);
    }

    _rbtree_save_recursive(T, node->right, file, prefix);
}

// 保存红黑树到快照文件（使用RSET格式）
void kvs_rbtree_save_snapshot(rbtree *T, FILE *file) {
    if (!T || !file || !T->root || !T->nil) return;

    // 确保根节点不是nil节点才开始遍历
    if (T->root != T->nil) {
        _rbtree_save_recursive(T, T->root, file, "R");
    }
}


/// @brief
/// @param inst
/// @param key
/// @return 1 Yes | 0 No | -1 error
int kvs_rbtree_exist(kvs_rbtree_t* inst, char* key) {
  if (inst == NULL || key == NULL) return -1;

  return rbtree_search(inst, key) != inst->nil;

}

#endif