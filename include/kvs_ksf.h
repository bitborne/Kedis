#ifndef __KVS_KSF_H__
#define __KVS_KSF_H__

#include "kvs_constants.h"

// KSF持久化相关函数声明
int ksfSave(const char *filename);  // 保存KSF快照
int ksfSaveBackground(void);  // 后台保存KSF快照
int ksfLoad(const char *filename);  // 加载KSF快照

// 多引擎模式下的KSF引擎保存函数
#if ENABLE_MULTI_ENGINE
int ksfSaveAll(void);  // 保存所有引擎的KSF快照
int ksfLoadAll(void);  // 加载所有引擎的KSF快照
#endif

// mmap 优化版本
int ksfLoadAll_mmap(void);  // 使用 mmap 加载所有 KSF 快照

#endif // __KVS_KSF_H__
