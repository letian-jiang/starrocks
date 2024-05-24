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

#include "connector/file_chunk_sink.h"

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "connector/connector_chunk_sink.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/file_writer.h"
#include "formats/orc/orc_file_writer.h"
#include "formats/csv/csv_file_writer.h"
#include "fs/fs_memory.h"
#include "formats/utils.h"
#include "testutil/assert.h"
#include "util/defer_op.h"
#include "jemalloc/jemalloc.h"
#include "service/mem_hook.cpp"

namespace starrocks::connector {
namespace {

using ::testing::Return;
using ::testing::ByMove;
using CommitResult = formats::FileWriter::CommitResult;
using ::testing::_;

TEST(AsyncFlushTest, vector) {
    {
        uint64_t epoch = 1;
        size_t value = 0;
        size_t sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
        sz = sizeof(size_t);
        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
        }
    }

        {
            EXPECT_TRUE(tls_is_thread_status_init);
            MemTracker mem_tracker(-1, "unit_test");
            tls_thread_status.set_mem_tracker(&mem_tracker);
            RuntimeState state;
            state.init_instance_mem_tracker();
            auto& instance_tracker = *state.instance_mem_tracker();
            tls_thread_status.mem_tracker_ctx_shift();
            LOG(INFO) << "init: " << mem_tracker.consumption() << " " << instance_tracker.consumption();

            {
                tls_thread_status.mem_tracker_ctx_shift();
                LOG(INFO) << "scope A: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
                std::vector<int> nums(100000);
                EXPECT_EQ(nums[0], 0);
                tls_thread_status.mem_tracker_ctx_shift();
                LOG(INFO) << "scope B: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
            }

            tls_thread_status.mem_tracker_ctx_shift();
            LOG(INFO) << "final: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
        }

    {
        uint64_t epoch = 1;
        size_t value = 0;
        size_t sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
        sz = sizeof(size_t);
        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
        }
    }
}

template <typename Factory>
void func() {
    EXPECT_TRUE(tls_is_thread_status_init);
    tls_thread_status.cnt = 0;
    MemTracker mem_tracker(-1, "unit_test");
    RuntimeState state;
    state.init_instance_mem_tracker();
    auto& instance_tracker = *state.instance_mem_tracker();
    tls_thread_status.set_mem_tracker(&instance_tracker);
    tls_thread_status.mem_tracker_ctx_shift();
    LOG(INFO) << "init: " << mem_tracker.consumption() << " " << instance_tracker.consumption();

    {
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << mem_tracker.consumption();

        auto fs = std::make_shared<MemoryFileSystem>();
        std::vector<TypeDescriptor> type_descs{
                TypeDescriptor::from_logical_type(TYPE_TINYINT),
                TypeDescriptor::from_logical_type(TYPE_SMALLINT),
                TypeDescriptor::from_logical_type(TYPE_INT),
                TypeDescriptor::from_logical_type(TYPE_BIGINT),
        };

        std::vector<std::string> column_names = {"a", "b", "c", "d"};
        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);


        PriorityThreadPool io_executor("test", 1, 100);

        auto factory = Factory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, std::move(column_evaluators), &io_executor, &state);
        EXPECT_OK(factory.init());
        auto maybe_writer_and_stream = factory.createAsync("/dummy_file.parquet");
        EXPECT_OK(maybe_writer_and_stream.status());
        auto writer = std::move(maybe_writer_and_stream.value().writer);
        auto stream = std::move(maybe_writer_and_stream.value().stream);
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "scope A: " << mem_tracker.consumption() << " " << instance_tracker.consumption();

        EXPECT_OK(writer->init());
        EXPECT_OK(writer->commit().io_status);
        EXPECT_OK(stream->io_status().get());
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "scope B: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
    }

    tls_thread_status.mem_tracker_ctx_shift();
    LOG(INFO) << "final: " << mem_tracker.consumption() << " " << instance_tracker.consumption();

    std::cout << "history";
    for (int i = 0; i < tls_thread_status.cnt; i++) {
        std::cout << tls_thread_status.history[i] << ' ';
    }
    std::cout << std::endl;
}

//TEST(AsyncFlushTest, parquet_test) {
//    EXPECT_TRUE(tls_is_thread_status_init);
//    MemTracker mem_tracker(-1, "unit_test");
//    RuntimeState state;
//    state.init_instance_mem_tracker();
//    auto& instance_tracker = *state.instance_mem_tracker();
//    tls_thread_status.set_mem_tracker(&instance_tracker);
//    tls_thread_status.mem_tracker_ctx_shift();
//    LOG(INFO) << "init: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
//
//    {
//        tls_thread_status.mem_tracker_ctx_shift();
//        LOG(INFO) << mem_tracker.consumption();
//
//        auto fs = std::make_shared<MemoryFileSystem>();
//        std::vector<TypeDescriptor> type_descs{
//                TypeDescriptor::from_logical_type(TYPE_TINYINT),
//                TypeDescriptor::from_logical_type(TYPE_SMALLINT),
//                TypeDescriptor::from_logical_type(TYPE_INT),
//                TypeDescriptor::from_logical_type(TYPE_BIGINT),
//        };
//
//        std::vector<std::string> column_names = {"a", "b", "c", "d"};
//        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);
//
//
//        PriorityThreadPool io_executor("test", 1, 100);
//
//        auto factory = formats::ParquetFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, std::move(column_evaluators), std::nullopt, &io_executor, &state);
//        EXPECT_OK(factory.init());
//        auto maybe_writer_and_stream = factory.createAsync("/dummy_file.parquet");
//        EXPECT_OK(maybe_writer_and_stream.status());
//        auto writer = std::move(maybe_writer_and_stream.value().writer);
//        auto stream = std::move(maybe_writer_and_stream.value().stream);
//        tls_thread_status.mem_tracker_ctx_shift();
//        LOG(INFO) << "scope A: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
//
//        EXPECT_OK(stream->close());
//        EXPECT_OK(stream->io_status().get());
//        tls_thread_status.mem_tracker_ctx_shift();
//        LOG(INFO) << "scope B: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
//    }
//
//    tls_thread_status.mem_tracker_ctx_shift();
//    LOG(INFO) << "final: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
//}

TEST(AsyncFlushTest, orc_test) {
    {
        uint64_t epoch = 1;
        size_t value = 0;
        size_t sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
        sz = sizeof(size_t);
        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
        }
    }

    func<formats::ORCFileWriterFactory>();

    {
        uint64_t epoch = 1;
        size_t value = 0;
        size_t sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
        sz = sizeof(size_t);
        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
        }
    }
}

TEST(AsyncFlushTest, csv_test) {
    {
        uint64_t epoch = 1;
        size_t value = 0;
        size_t sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
        sz = sizeof(size_t);
        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
        }
    }

    func<formats::CSVFileWriterFactory>();

    {
        uint64_t epoch = 1;
        size_t value = 0;
        size_t sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
        sz = sizeof(size_t);
        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
        }
    }
}

TEST(AsyncFlushTest, csv_test2) {
    EXPECT_TRUE(tls_is_thread_status_init);
    MemTracker mem_tracker(-1, "unit_test");
    // tls_thread_status.set_mem_tracker(&mem_tracker);
    RuntimeState state;
    state.init_instance_mem_tracker();
    auto& instance_tracker = *state.instance_mem_tracker();
    tls_thread_status.set_mem_tracker(&instance_tracker);
    tls_thread_status.mem_tracker_ctx_shift();
    LOG(INFO) << "init: " << mem_tracker.consumption() << " " << instance_tracker.consumption();

    {
        auto fs = std::make_shared<MemoryFileSystem>();
        std::vector<TypeDescriptor> type_descs{
                TypeDescriptor::from_logical_type(TYPE_TINYINT),
                TypeDescriptor::from_logical_type(TYPE_SMALLINT),
                TypeDescriptor::from_logical_type(TYPE_INT),
                TypeDescriptor::from_logical_type(TYPE_BIGINT),
        };

        std::vector<std::string> column_names = {"a", "b", "c", "d"};
        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);


        PriorityThreadPool io_executor("test", 1, 100);

        auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, std::move(column_evaluators), &io_executor, &state);
        EXPECT_OK(factory.init());
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "scope A: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
//        auto maybe_writer_and_stream = factory.createAsync("/dummy_file.parquet");
//        EXPECT_OK(maybe_writer_and_stream.status());
//        auto writer = std::move(maybe_writer_and_stream.value().writer);
//        auto stream = std::move(maybe_writer_and_stream.value().stream);
//        tls_thread_status.mem_tracker_ctx_shift();
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "scope B: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
//
//        EXPECT_OK(writer->init());
//        EXPECT_OK(writer->commit().io_status);
//        EXPECT_OK(stream->io_status().get());
//        tls_thread_status.mem_tracker_ctx_shift();
//        LOG(INFO) << "scope B: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
    }

    tls_thread_status.mem_tracker_ctx_shift();
    LOG(INFO) << "final: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
}

TEST(AsyncFlushTest, csv_test3) {
    EXPECT_TRUE(tls_is_thread_status_init);
    MemTracker mem_tracker(-1, "unit_test");
    // tls_thread_status.set_mem_tracker(&mem_tracker);
    RuntimeState state;
    state.init_instance_mem_tracker();
    auto& instance_tracker = *state.instance_mem_tracker();
    tls_thread_status.set_mem_tracker(&instance_tracker);
    tls_thread_status.mem_tracker_ctx_shift();
    LOG(INFO) << "init: " << mem_tracker.consumption() << " " << instance_tracker.consumption();

    {
        auto fs = std::make_shared<MemoryFileSystem>();
        std::vector<TypeDescriptor> type_descs{
                TypeDescriptor::from_logical_type(TYPE_TINYINT),
                TypeDescriptor::from_logical_type(TYPE_SMALLINT),
                TypeDescriptor::from_logical_type(TYPE_INT),
                TypeDescriptor::from_logical_type(TYPE_BIGINT),
        };

        std::vector<std::string> column_names = {"a", "b", "c", "d"};
        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);


        PriorityThreadPool io_executor("test", 1, 100);

        auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, std::move(column_evaluators), &io_executor, &state);
        EXPECT_OK(factory.init());
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "scope A: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
        auto maybe_writer_and_stream = factory.createAsync("/dummy_file.parquet");
        EXPECT_OK(maybe_writer_and_stream.status());
//        auto writer = std::move(maybe_writer_and_stream.value().writer);
//        auto stream = std::move(maybe_writer_and_stream.value().stream);
//        tls_thread_status.mem_tracker_ctx_shift();
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "scope B: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
//
//        EXPECT_OK(writer->init());
//        EXPECT_OK(writer->commit().io_status);
//        EXPECT_OK(stream->io_status().get());
//        tls_thread_status.mem_tracker_ctx_shift();
//        LOG(INFO) << "scope B: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
    }

    tls_thread_status.mem_tracker_ctx_shift();
    LOG(INFO) << "final: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
}

TEST(AsyncFlushTest, csv_test4) {
    EXPECT_TRUE(tls_is_thread_status_init);
    MemTracker mem_tracker(-1, "unit_test");
    // tls_thread_status.set_mem_tracker(&mem_tracker);
    RuntimeState state;
    state.init_instance_mem_tracker();
    auto& instance_tracker = *state.instance_mem_tracker();
    tls_thread_status.set_mem_tracker(&instance_tracker);
    tls_thread_status.mem_tracker_ctx_shift();
    LOG(INFO) << "init: " << mem_tracker.consumption() << " " << instance_tracker.consumption();

    {
        auto fs = std::make_shared<MemoryFileSystem>();
        std::vector<TypeDescriptor> type_descs{
                TypeDescriptor::from_logical_type(TYPE_TINYINT),
                TypeDescriptor::from_logical_type(TYPE_SMALLINT),
                TypeDescriptor::from_logical_type(TYPE_INT),
                TypeDescriptor::from_logical_type(TYPE_BIGINT),
        };

        std::vector<std::string> column_names = {"a", "b", "c", "d"};
        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);


        PriorityThreadPool io_executor("test", 1, 100);

        auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, std::move(column_evaluators), &io_executor, &state);
        EXPECT_OK(factory.init());
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "scope A: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
        auto maybe_writer_and_stream = factory.createAsync("/dummy_file.parquet");
        EXPECT_OK(maybe_writer_and_stream.status());

        auto writer = std::move(maybe_writer_and_stream.value().writer);
        auto stream = std::move(maybe_writer_and_stream.value().stream);
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "scope B: " << mem_tracker.consumption() << " " << instance_tracker.consumption();

        EXPECT_OK(writer->init());
        EXPECT_OK(writer->commit().io_status);
        EXPECT_OK(stream->io_status().get());
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "scope C: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
    }

    tls_thread_status.mem_tracker_ctx_shift();
    LOG(INFO) << "final: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
    LOG(INFO) << mem_tracker.debug_string();
    LOG(INFO) << instance_tracker.debug_string();
}


TEST(AsyncFlushTest, csv_test5) {
    {
        uint64_t epoch = 1;
        size_t value = 0;
        size_t sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
        sz = sizeof(size_t);
        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
        }
    }
    {
        RuntimeState state;
        EXPECT_TRUE(tls_is_thread_status_init);
        // tls_thread_status.set_mem_tracker(&mem_tracker);
        state.init_instance_mem_tracker();
        auto& instance_tracker = *state.instance_mem_tracker();
        tls_thread_status.cnt = 0;
        tls_thread_status.set_mem_tracker(&instance_tracker);
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "init: " << instance_tracker.consumption();

        {
            auto fs = std::make_shared<MemoryFileSystem>();
            std::vector<TypeDescriptor> type_descs{
                    TypeDescriptor::from_logical_type(TYPE_TINYINT),
                    TypeDescriptor::from_logical_type(TYPE_SMALLINT),
                    TypeDescriptor::from_logical_type(TYPE_INT),
                    TypeDescriptor::from_logical_type(TYPE_BIGINT),
            };

            std::vector<std::string> column_names = {"a", "b", "c", "d"};
            auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

            {
                PriorityThreadPool io_executor("test", 1, 100);
                auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, std::move(column_evaluators), &io_executor, &state);
                EXPECT_OK(factory.init());
                auto maybe_writer_and_stream = factory.createAsync("/dummy_file.parquet");
                EXPECT_OK(maybe_writer_and_stream.status());

                auto writer = std::move(maybe_writer_and_stream.value().writer);
                auto stream = std::move(maybe_writer_and_stream.value().stream);

                EXPECT_OK(writer->init());
                EXPECT_OK(writer->commit().io_status);
                EXPECT_OK(stream->io_status().get());
            }
        }

        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << "final: " << instance_tracker.consumption();
    }

    {
        uint64_t epoch = 1;
        size_t value = 0;
        size_t sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
        sz = sizeof(size_t);
        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
        }
    }
}

TEST(AsyncFlushTest, csv_test6) {
    {
        uint64_t epoch = 1;
        size_t value = 0;
        size_t sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
        sz = sizeof(size_t);
        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
        }
    }
    {
        RuntimeState state;
//        EXPECT_TRUE(tls_is_thread_status_init);
//        MemTracker mem_tracker(-1, "unit_test");
//        // tls_thread_status.set_mem_tracker(&mem_tracker);
//        state.init_instance_mem_tracker();
//        auto& instance_tracker = *state.instance_mem_tracker();
//        tls_thread_status.cnt = 0;
//        tls_thread_status.set_mem_tracker(&instance_tracker);
//        tls_thread_status.mem_tracker_ctx_shift();
//        LOG(INFO) << "init: " << mem_tracker.consumption() << " " << instance_tracker.consumption();

        {
            auto fs = std::make_shared<MemoryFileSystem>();
            std::vector<TypeDescriptor> type_descs{
                    TypeDescriptor::from_logical_type(TYPE_TINYINT),
                    TypeDescriptor::from_logical_type(TYPE_SMALLINT),
                    TypeDescriptor::from_logical_type(TYPE_INT),
                    TypeDescriptor::from_logical_type(TYPE_BIGINT),
            };

            std::vector<std::string> column_names = {"a", "b", "c", "d"};
            auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

            {
                auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, std::move(column_evaluators), nullptr, &state);
                EXPECT_OK(factory.init());
                auto maybe_writer = factory.create("/dummy_file.parquet");
                EXPECT_OK(maybe_writer.status());

                auto writer = maybe_writer.value();

                EXPECT_OK(writer->init());
                EXPECT_OK(writer->commit().io_status);
            }
        }

//        tls_thread_status.mem_tracker_ctx_shift();
//        LOG(INFO) << "final: " << mem_tracker.consumption() << " " << instance_tracker.consumption();
//        LOG(INFO) << mem_tracker.debug_string();
//        LOG(INFO) << instance_tracker.debug_string();
    }

    {
        uint64_t epoch = 1;
        size_t value = 0;
        size_t sz = sizeof(epoch);
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
        sz = sizeof(size_t);
        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
        }
    }
}

} // namespace
} // namespace starrocks::connector
