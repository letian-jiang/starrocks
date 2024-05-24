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

namespace starrocks::connector {
namespace {

using ::testing::Return;
using ::testing::ByMove;
using CommitResult = formats::FileWriter::CommitResult;
using ::testing::_;
using namespace std::chrono_literals;
using SliceChunk = io::AsyncFlushOutputStream::SliceChunk;

//TEST(AsyncFlushTest2, test_jemalloc) {
//    for (int i = 0; i < 10; i++) {
//        uint64_t epoch = 1;
//        size_t value = 0;
//        size_t sz = sizeof(epoch);
//        je_mallctl("epoch", &epoch, &sz, &epoch, sz);
//        sz = sizeof(size_t);
//        je_mallctl("thread.tcache.flush", NULL, NULL, NULL, 0);
//
//        if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
//            LOG(INFO) << "epoch = " << epoch << " allocated bytes " << value;
//        }
//        std::this_thread::sleep_for(1s);
//    }
//}


//TEST(AsyncFlushTest2, test_memtracker) {
//    RuntimeState state;
//    state.init_instance_mem_tracker();
//    auto tracker = state.instance_mem_tracker();
//    auto prev = tls_thread_status.set_mem_tracker(tracker);
//
//    auto print = [&]() {
//        tls_thread_status.mem_tracker_ctx_shift();
//        LOG(INFO) << tracker->debug_string();
//    };
//
//    print();
//    {
//        auto p1 = std::malloc(1000);
//        auto p2 = std::malloc(2048);
//        print();
//        std::free(p1);
//        print();
//        std::free(p2);
//    }
//    print();
//
//    tls_thread_status.set_mem_tracker(prev);
//}
//
//TEST(AsyncFlushTest2, test_io_executor) {
//    RuntimeState state;
//    state.init_instance_mem_tracker();
//    auto tracker = state.instance_mem_tracker();
//    auto prev = tls_thread_status.set_mem_tracker(tracker);
//
//    auto print = [&]() {
//        tls_thread_status.mem_tracker_ctx_shift();
//        LOG(INFO) << tracker->debug_string();
//    };
//
//    print();
//    {
//        PriorityThreadPool io_executor("test", 10, 100);
//        print();
//        io_executor.shutdown();
//        io_executor.join();
//    }
//    print();
//
//    tls_thread_status.set_mem_tracker(prev);
//}
//
//TEST(AsyncFlushTest2, test_csv) {
//    DCHECK(tls_is_thread_status_init);
//    PriorityThreadPool io_executor("test", 10, 100);
//
//    RuntimeState state;
//    state.init_instance_mem_tracker();
//    auto tracker = state.instance_mem_tracker();
//    auto prev = tls_thread_status.set_mem_tracker(tracker);
//
//    auto print = [&]() {
//        tls_thread_status.mem_tracker_ctx_shift();
//        LOG(INFO) << tracker->debug_string();
//    };
//
//    print();
//    // 100 -> 8928
//    // 200 -> 17920
//    // 300 -> 27072
//    // 1000 -> 91382
//    // 10'000 -> 909184
//    // 100'000 -> 909248
//    for (int i = 0; i < 10000; i++) {
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
//        auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, std::move(column_evaluators), &io_executor, &state);
//        EXPECT_OK(factory.init());
//
//        auto maybe_writer_and_stream = factory.createAsync("/dummy_file.parquet");
//        EXPECT_OK(maybe_writer_and_stream.status());
//        auto writer = std::move(maybe_writer_and_stream.value().writer);
//        auto stream = std::move(maybe_writer_and_stream.value().stream);
//        EXPECT_OK(writer->init());
//        EXPECT_OK(writer->commit().io_status);
//        EXPECT_OK(stream->io_status().get());
//    }
//    print();
//
//    tls_thread_status.set_mem_tracker(prev);
//}

TEST(AsyncFlushTest2, test_orc) {
    DCHECK(tls_is_thread_status_init);
    PriorityThreadPool io_executor("test", 10, 100);

    RuntimeState state;
    state.init_instance_mem_tracker();
    auto tracker = state.instance_mem_tracker();
    auto prev = tls_thread_status.set_mem_tracker(tracker);

    auto print = [&]() {
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << tracker->debug_string();
    };

    print();
    // 32 per round
    for (int i = 0; i < 1000'0; i++) {
        auto fs = std::make_shared<MemoryFileSystem>();
        std::vector<TypeDescriptor> type_descs{
                TypeDescriptor::from_logical_type(TYPE_TINYINT),
                TypeDescriptor::from_logical_type(TYPE_SMALLINT),
                TypeDescriptor::from_logical_type(TYPE_INT),
                TypeDescriptor::from_logical_type(TYPE_BIGINT),
        };

        std::vector<std::string> column_names = {"a", "b", "c", "d"};
        auto column_evaluators = ColumnSlotIdEvaluator::from_types(type_descs);

        auto factory = formats::CSVFileWriterFactory(fs, TCompressionType::NO_COMPRESSION, {}, column_names, std::move(column_evaluators), &io_executor, &state);
        EXPECT_OK(factory.init());

        auto maybe_writer_and_stream = factory.createAsync("/dummy_file.parquet");
        EXPECT_OK(maybe_writer_and_stream.status());
        auto writer = std::move(maybe_writer_and_stream.value().writer);
        auto stream = std::move(maybe_writer_and_stream.value().stream);
        EXPECT_OK(writer->init());

        auto chunk = std::make_shared<Chunk>();
        {
            auto col0 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_TINYINT), true);
            std::vector<int8_t> int8_nums{INT8_MIN, INT8_MAX, 0, 1};
            auto count = col0->append_numbers(int8_nums.data(), size(int8_nums) * sizeof(int8_t));
            ASSERT_EQ(4, count);
            chunk->append_column(col0, chunk->num_columns());

            auto col1 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_SMALLINT), true);
            std::vector<int16_t> int16_nums{INT16_MIN, INT16_MAX, 0, 1};
            count = col1->append_numbers(int16_nums.data(), size(int16_nums) * sizeof(int16_t));
            ASSERT_EQ(4, count);
            chunk->append_column(col1, chunk->num_columns());

            auto col2 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
            std::vector<int32_t> int32_nums{INT32_MIN, INT32_MAX, 0, 1};
            count = col2->append_numbers(int32_nums.data(), size(int32_nums) * sizeof(int32_t));
            ASSERT_EQ(4, count);
            chunk->append_column(col2, chunk->num_columns());

            auto col3 = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
            std::vector<int64_t> int64_nums{INT64_MIN, INT64_MAX, 0, 1};
            count = col3->append_numbers(int64_nums.data(), size(int64_nums) * sizeof(int64_t));
            ASSERT_EQ(4, count);
            chunk->append_column(col3, chunk->num_columns());
        }

        EXPECT_OK(writer->write(chunk));
        EXPECT_OK(writer->commit().io_status);
        EXPECT_OK(stream->io_status().get());
    }
    print();

    tls_thread_status.set_mem_tracker(prev);
}


TEST(AsyncFlushTest2, test_ptr) {
    DCHECK(tls_is_thread_status_init);
    PriorityThreadPool io_executor("test", 1, 1000);

    RuntimeState state;
    state.init_instance_mem_tracker();
    auto tracker = state.instance_mem_tracker();
    auto prev = tls_thread_status.set_mem_tracker(tracker);

    auto print = [&]() {
        tls_thread_status.mem_tracker_ctx_shift();
        LOG(INFO) << tracker->debug_string();
    };

    print();
    // 1000 -> 66624
    // 10'000 -> 667152
    // 100'000 -> 6412464
    for (int i = 0; i < 1000; i++) {
        auto ptr = std::make_shared<SliceChunk>(1024 * 1024);
        auto task = [&, p = std::move(ptr)] () mutable {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(tracker);
            auto pp = std::move(p);
            // LOG(INFO) << p.get() << ' ' << pp.get();
            EXPECT_TRUE(p.get() == nullptr);
            EXPECT_TRUE(pp != nullptr);
        };
        io_executor.offer(std::move(task));
    }
    std::this_thread::sleep_for(1s);
    print();


    tls_thread_status.set_mem_tracker(prev);
}

} // namespace
} // namespace starrocks::connector
