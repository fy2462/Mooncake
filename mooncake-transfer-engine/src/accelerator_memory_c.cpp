// Copyright 2024 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "accelerator_memory_c.h"

#include <cstring>

#include "cuda_alike.h"
#include "memory_location.h"

using namespace mooncake;

int classifyMemoryPointer(const void *ptr) {
    if (ptr == nullptr) return -1;
    const auto locations =
        getMemoryLocation(const_cast<void *>(ptr), 1, true);
    if (locations.empty()) return -1;
    const auto &location = locations.front().location;
    return location == kWildcardLocation || location.rfind("cpu:", 0) == 0
               ? MEMORY_POINTER_HOST
               : MEMORY_POINTER_DEVICE;
}

int copyMemoryToHost(void *dst, const void *src, size_t length) {
    if ((dst == nullptr || src == nullptr) && length != 0) return -1;
#if defined(USE_HIP)
    return hipMemcpy(dst, src, length, hipMemcpyDefault) == hipSuccess ? 0 : -1;
#elif defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MLU) || \
    defined(USE_MACA) || defined(USE_HYGON) || defined(USE_COREX) || \
    defined(USE_SUNRISE)
    return cudaMemcpy(dst, src, length, cudaMemcpyDefault) == cudaSuccess ? 0
                                                                          : -1;
#else
    std::memcpy(dst, src, length);
    return 0;
#endif
}

int copyMemoryFromHost(void *dst, const void *src, size_t length) {
    return copyMemoryToHost(dst, src, length);
}
