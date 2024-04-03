// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include "connector_sink/utils.h"

namespace starrocks::connector {
namespace {

TEST(TaskExecutor, simple) {
    TaskExecutor executor(1);
    auto [t1, f1] = executor.submit([]() { return 1; });

    EXPECT_EQ(t1, 0);
    EXPECT_EQ(f1.get(), 1);

    auto [t2, f2] = executor.submit([]() { return 1.1f + 1.2f; });

    EXPECT_EQ(t2, 1);
    EXPECT_FLOAT_EQ(f2.get(), 2.3f);
}

TEST(TaskExecutor, submit) {
    TaskExecutor executor(10);

    std::vector<std::pair<uint64_t, std::future<int>>> results;
    for (int i = 0; i < 100; i++) {
        results.push_back(executor.submit([i]() { return i; }));
    }

    for (int i = 0; i < 100; i++) {
        EXPECT_EQ(results[i].first, i);
        EXPECT_EQ(results[i].second.get(), i);
    }
}

TEST(TaskExecutor, task_sequential_dependency) {
    TaskExecutor executor(3);

    std::promise<void> promise;
    std::future<void> future = promise.get_future();
    auto [t1, f1] = executor.submit([&]() {
        future.get(); // block until get signal
        return "first_task";
    });

    auto [t2, f2] = executor.submit([]() { return "second_task"; }, {t1});

    auto [t3, f3] = executor.submit([]() { return "third_task"; }, {t2});

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(is_ready(f1));
    EXPECT_FALSE(is_ready(f2));
    EXPECT_FALSE(is_ready(f3));

    promise.set_value();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_TRUE(is_ready(f1));
    EXPECT_TRUE(is_ready(f2));
    EXPECT_TRUE(is_ready(f3));
    EXPECT_EQ(f1.get(), "first_task");
    EXPECT_EQ(f2.get(), "second_task");
    EXPECT_EQ(f3.get(), "third_task");
}

TEST(TaskExecutor, task_parallel_dependency) {
    TaskExecutor executor(3);
    std::atomic<int64_t> sum{-99};

    auto [t1, f1] = executor.submit([&]() {
        sum.store(0);
        return sum.load();
    });

    auto [t2, f2] = executor.submit(
            [&]() {
                auto val = sum.fetch_add(1);
                return val;
            },
            {t1});

    auto [t3, f3] = executor.submit(
            [&]() {
                auto val = sum.fetch_add(1);
                return val;
            },
            {t1});

    auto [t4, f4] = executor.submit(
            [&]() {
                auto val = sum.load();
                return val;
            },
            {t2, t3});

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_EQ(f1.get(), 0);
    EXPECT_EQ(f2.get() + f3.get(), 0 + 1);
    EXPECT_EQ(f4.get(), 2);
}

} // namespace
} // namespace starrocks::connector
