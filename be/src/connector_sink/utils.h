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

#pragma once

#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include <queue>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "connector_chunk_sink.h"
#include "exprs/expr_context.h"
#include "fmt/format.h"
#include "formats/column_evaluator.h"
#include "formats/parquet/parquet_file_writer.h"
#include "io/output_stream.h"
#include "runtime/types.h"

namespace starrocks::connector {

class LocationProvider;

class HiveUtils {
public:
    static StatusOr<std::string> make_partition_name(
            const std::vector<std::string> &column_names,
            const std::vector<std::unique_ptr<ColumnEvaluator>> &column_evaluators, Chunk *chunk);

    static StatusOr<std::string> make_partition_name_nullable(
            const std::vector<std::string> &column_names,
            const std::vector<std::unique_ptr<ColumnEvaluator>> &column_evaluators, Chunk *chunk);

    static StatusOr<ConnectorChunkSink::Futures> hive_style_partitioning_write_chunk(
            const ChunkPtr &chunk, bool partitioned, const std::string &partition, int64_t max_file_size,
            const formats::FileWriterFactory *file_writer_factory, LocationProvider *location_provider,
            std::map<std::string, std::shared_ptr<formats::FileWriter>> &partition_writers);

private:
    static StatusOr<std::string> column_value(const TypeDescriptor &type_desc, const ColumnPtr &column);
};

class IcebergUtils {
public:
    static std::vector<formats::FileColumnId> generate_parquet_field_ids(
            const std::vector<TIcebergSchemaField> &fields);

    inline const static std::string DATA_DIRECTORY = "/data";
};

class PathUtils {
public:
    // requires: path contains "/"
    static std::string get_parent_path(const std::string &path) {
        std::size_t i = path.find_last_of("/");
        CHECK_NE(i, std::string::npos);
        return path.substr(0, i);
    }

    // requires: path contains "/"
    static std::string get_filename(const std::string &path) {
        std::size_t i = path.find_last_of("/");
        CHECK_NE(i, std::string::npos);
        return path.substr(i + 1);
    }

    static std::string remove_trailing_slash(const std::string &path) {
        if (path.ends_with("/")) {
            return path.substr(0, path.size() - 1);
        }
        return path;
    }
};

// Location provider provides file location for every output file. The name format depends on if the write is partitioned or not.
class LocationProvider {
public:
    // file_name_prefix = {query_id}_{be_number}_{driver_id}
    LocationProvider(const std::string &base_path, const std::string &query_id, int be_number, int driver_id,
                     const std::string &file_suffix)
            : _base_path(PathUtils::remove_trailing_slash(base_path)),
              _file_name_prefix(fmt::format("{}_{}_{}", query_id, be_number, driver_id)),
              _file_name_suffix(file_suffix) {}

    // location = base_path/partition/{query_id}_{be_number}_{driver_id}_index.file_suffix
    std::string get(const std::string &partition) {
        return fmt::format("{}/{}/{}_{}.{}", _base_path, PathUtils::remove_trailing_slash(partition),
                           _file_name_prefix,
                           _partition2index[partition]++, _file_name_suffix);
    }

    // location = base_path/{query_id}_{be_number}_{driver_id}_index.file_suffix
    std::string get() {
        return fmt::format("{}/{}_{}.{}", _base_path, _file_name_prefix, _index++, _file_name_suffix);
    }

private:
    const std::string _base_path;
    const std::string _file_name_prefix;
    const std::string _file_name_suffix;
    int _index = 0;
    std::map<std::string, int> _partition2index;
};

} // namespace starrocks::connector

namespace starrocks {

class TaskExecutor {
public:
    using Token = uint64_t;

    class TaskQueue {
    public:
        using Task = std::function<void()>;

        std::optional<Task> pop() {
            Token token;
            Task task;
            {
                std::lock_guard lock(_mu);
                if (_ready_task_tokens.empty()) {
                    return std::nullopt;
                }
                token = _ready_task_tokens.front();
                _ready_task_tokens.pop();
                task = _token2task[token];
            }

            return [&, task, token]() {
                task();
                LOG(INFO) << "TaskQueue: task of token " << token << " is finished";
                // clean up once task is done
                {
                    std::lock_guard lock(_mu);
                    for (Token downstream_token: _graph[token]) {
                        _reversed_graph[downstream_token].erase(token);
                        if (_reversed_graph[downstream_token].empty()) {
                            LOG(INFO) << "TaskQueue: task of token " << downstream_token << " is ready";
                            _ready_task_tokens.push(downstream_token);
                        }
                    }
                    _graph.erase(token);
                    _token2task.erase(token);
                }
            };
        }

        void push(Task task, Token token, std::set<Token> upstream_tokens) {
            std::lock_guard lock(_mu);
            _token2task[token] = std::move(task);
            if (upstream_tokens.empty()) {
                LOG(INFO) << "TaskQueue: task of token " << token << " is ready";
                _ready_task_tokens.push(token);
                return;
            }

            // remove in-existent task tokens (which may have been done)
            std::erase_if(upstream_tokens, [&](auto t) { return !_token2task.contains(t); });
            for (Token upstream_token: upstream_tokens) {
                _graph[upstream_token].insert(token);
            }
            _reversed_graph[token] = std::move(upstream_tokens);
        }

    private:
        std::mutex _mu;
        std::map<Token, std::set<Token>> _graph;          // task -> downstream tasks
        std::map<Token, std::set<Token>> _reversed_graph; // task -> upstream tasks
        std::map<Token, Task> _token2task;
        std::queue<Token> _ready_task_tokens;
    };

    TaskExecutor(int num_threads) : _done(false) {
        for (int i = 0; i < num_threads; i++) {
            _threads.push_back(std::thread(&TaskExecutor::work_function, this));
        }
    }

    ~TaskExecutor() {
        _done.store(true);
        for (auto &t: _threads) {
            t.join();
        }
    }

    void work_function() {
        while (!_done) {
            // fetch task
            auto maybe_task = _task_queue.pop();
            if (!maybe_task.has_value()) {
                std::this_thread::yield();
                continue;
            }

            auto task = maybe_task.value();
            task();
        }
    }

    template<typename F, typename R = std::invoke_result_t<std::decay_t<F>>>
    [[nodiscard]] std::shared_future<R> submit(F &&task, Token *token = nullptr) {
        return submit(std::move(task), {}, token);
    }

    template<typename F, typename R = std::invoke_result_t<std::decay_t<F>>>
    [[nodiscard]] std::shared_future<R> submit(F &&task, std::set<Token> upstream_tasks, Token *token = nullptr) {
        const std::shared_ptr<std::promise<R>> task_promise = std::make_shared<std::promise<R>>();
        Token task_token = _next_token.fetch_add(1);
        if (token != nullptr) {
            *token = task_token;
        }
        _task_queue.push(
                [task = std::move(task), task_promise]() {
                    if constexpr (std::is_void_v<R>) {
                        task();
                        task_promise->set_value();
                    } else {
                        task_promise->set_value(task());
                    }
                },
                task_token, std::move(upstream_tasks));

        return task_promise->get_future();
    }

private:
    // thread-safe DAG-aware task queue
    TaskQueue _task_queue;
    std::atomic<bool> _done;
    std::atomic<uint64_t> _next_token;

    // thread pool
    std::vector<std::thread> _threads;
};

namespace io {
// live until all io tasks are finished
    class BufferedOutputStream {
    public:
        BufferedOutputStream(std::unique_ptr<io::DirectOutputStream> output_stream, TaskExecutor *task_executor)
                : _output_stream(std::move(output_stream)), _task_executor(task_executor) {};

        Status init() {
            _task_execution_context.init_future = _task_executor->submit(
                    [s = _output_stream.get()]() { return s->init(); },
                    &_task_execution_context.init_token);

            return Status::OK();
        }

        Status write(const uint8_t *data, size_t size) {
            if (_slice_chunk == nullptr) {
                _slice_chunk = std::make_shared<SliceChunk>(64 * 1024); // TODO: config
            }

            while (size != 0) {
                size_t appended_size = _slice_chunk->append(data, size);
                data += appended_size;
                size -= appended_size;
                if (_slice_chunk->is_full()) {
                    // submit;
                    Token task_token;
                    _task_execution_context.flush_part_futures.push_back(_task_executor->submit(
                            [s = _output_stream.get(), slice_chunk = _slice_chunk,
                                    init_future = _task_execution_context.init_future]() {
                                DCHECK(is_ready(init_future));
                                Status init_status = init_future.get();
                                if (!init_status.ok()) {
                                    return init_status;
                                }
                                return s->write(slice_chunk->data(), slice_chunk->size());
                            },
                            {_task_execution_context.flush_part_tokens.back()}, &task_token));
                    _task_execution_context.flush_part_tokens.push_back(task_token);
                    _slice_chunk = std::make_shared<SliceChunk>(64 * 1024);
                }
            }

            return Status::OK();
        }

        Status close() {
            if (_slice_chunk != nullptr && _slice_chunk->is_empty()) {
                // submit
                Token task_token;
                _task_execution_context.flush_part_futures.push_back(_task_executor->submit(
                        [s = _output_stream.get(), slice_chunk = _slice_chunk,
                                init_future = _task_execution_context.init_future]() {
                            DCHECK(is_ready(init_future));
                            Status init_status = init_future.get();
                            if (!init_status.ok()) {
                                return init_status;
                            }
                            return s->write(slice_chunk->data(), slice_chunk->size());
                        },
                        {_task_execution_context.flush_part_tokens.back()}, &task_token));
                _task_execution_context.flush_part_tokens.push_back(task_token);
                _slice_chunk = std::make_shared<SliceChunk>(64 * 1024);
            }

            // close
            _task_execution_context.close_future =
                    _task_executor->submit([s = _output_stream.get(), slice_chunk = _slice_chunk,
                                                   init_future = _task_execution_context.init_future]() { return s->close(); },
                                           {_task_execution_context.flush_part_tokens.back()});
            _slice_chunk = std::make_shared<SliceChunk>(64 * 1024);

            return Status::OK();
        }

        size_t tell() const { return 0; }

        std::string name() const { return "filename"; }

        std::shared_future<Status> io_status() { return _task_execution_context.close_future; }

    private:
        using Token = TaskExecutor::Token;

        struct TaskExecutionContext {
            Token init_token;
            std::shared_future<Status> init_future;
            std::vector<Token> flush_part_tokens;
            std::vector<std::shared_future<Status>> flush_part_futures;
            std::shared_future<Status> close_future;
        };

        class SliceChunk {
        public:
            SliceChunk(size_t n_bytes) : _data(n_bytes), _capacity(n_bytes) {}

            size_t append(const uint8_t *data, size_t size) {
                size_t available_bytes = _capacity - _size;
                size_t to_append_bytes = std::min(available_bytes, size);
                memcpy(_data.data() + _size, data, size);
                _size += to_append_bytes;
                return to_append_bytes;
            }

            bool is_full() { return _size == _capacity; }

            bool is_empty() { return _size == 0; }

            const uint8_t *data() { return _data.data(); }

            size_t size() { return _size; }

        private:
            std::vector<uint8_t> _data;
            size_t _size{0};
            size_t _capacity;
        };

        TaskExecutionContext _task_execution_context;

        std::unique_ptr<io::DirectOutputStream> _output_stream;
        std::shared_ptr<SliceChunk> _slice_chunk;
        TaskExecutor *_task_executor;
    };

} // namespace io

} // namespace starrocks
