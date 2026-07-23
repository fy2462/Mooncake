// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "transport/rdma_transport/context_lookup.h"

namespace mooncake {
namespace {

class FakeContext {
   public:
    explicit FakeContext(std::string device_name)
        : device_name_(std::move(device_name)) {}

    std::string deviceName() const { return device_name_; }

   private:
    std::string device_name_;
};

TEST(ContextLookupTest, FindsFilteredContextByDeviceName) {
    std::vector<std::shared_ptr<FakeContext>> contexts{
        std::make_shared<FakeContext>("rxe_b")};

    auto context = findContextByDeviceName(contexts, "rxe_b");

    ASSERT_NE(context, nullptr);
    EXPECT_EQ(context->deviceName(), "rxe_b");
}

TEST(ContextLookupTest, IgnoresNullContextsAndReturnsNullWhenMissing) {
    std::vector<std::shared_ptr<FakeContext>> contexts{
        nullptr, std::make_shared<FakeContext>("rxe_b")};

    EXPECT_EQ(findContextByDeviceName(contexts, "rxe_c"), nullptr);
}

}  // namespace
}  // namespace mooncake
