// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef CONTEXT_LOOKUP_H_
#define CONTEXT_LOOKUP_H_

#include <memory>
#include <string_view>
#include <vector>

namespace mooncake {

template <typename Context>
std::shared_ptr<Context> findContextByDeviceName(
    const std::vector<std::shared_ptr<Context>>& contexts,
    std::string_view device_name) {
    for (const auto& context : contexts) {
        if (context && context->deviceName() == device_name) return context;
    }
    return nullptr;
}

}  // namespace mooncake

#endif  // CONTEXT_LOOKUP_H_
