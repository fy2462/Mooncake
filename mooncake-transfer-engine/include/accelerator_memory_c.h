// Copyright 2024 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef ACCELERATOR_MEMORY_C_H
#define ACCELERATOR_MEMORY_C_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MEMORY_POINTER_HOST (0)
#define MEMORY_POINTER_DEVICE (1)

int classifyMemoryPointer(const void *ptr);
int copyMemoryToHost(void *dst, const void *src, size_t length);
int copyMemoryFromHost(void *dst, const void *src, size_t length);

#ifdef __cplusplus
}
#endif

#endif  // ACCELERATOR_MEMORY_C_H
