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

#include <gen_cpp/DataSinks_types.h>
#include <parquet/arrow/writer.h>

#include <utility>

#include "common/logging.h"
#include "exec/parquet_writer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "fs/fs.h"

namespace starrocks {
namespace pipeline {

class TableFunctionTableSinkOperator final : public Operator {
public:
    TableFunctionTableSinkOperator(OperatorFactory* factory, const int32_t id, const int32_t plan_node_id, const int32_t driver_sequence, const string& path, const string& file_format,
                                   const TCompressionType::type& compression_codec, const std::vector<ExprContext*>& output_expr_ctxs, std::shared_ptr<::parquet::schema::GroupNode> parquet_file_schema,
                                   const FragmentContext* fragment_ctx)
            : Operator(factory, id, "table_function_table_sink", plan_node_id, driver_sequence),
              _path(path),
              _file_format(file_format),
              _compression_codec(compression_codec),
              _output_expr(output_expr_ctxs),
              _parquet_file_schema(std::move(parquet_file_schema)),
              _fragment_ctx(fragment_ctx) {}

    ~TableFunctionTableSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    bool pending_finish() const override;

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    std::string _path;
    std::string _file_format;
    TCompressionType::type _compression_codec;
    TCloudConfiguration _cloud_conf;

    std::vector<ExprContext*> _output_expr;
    std::shared_ptr<::parquet::schema::GroupNode> _parquet_file_schema;
    const FragmentContext* _fragment_ctx = nullptr;

    std::vector<ExprContext*> _partition_expr;
    std::unordered_map<std::string, std::unique_ptr<starrocks::RollingAsyncParquetWriter>> _partition_writers;
    std::atomic<bool> _is_finished = false;
};

class TableFunctionTableSinkOperatorFactory final : public OperatorFactory {
public:
    TableFunctionTableSinkOperatorFactory(const int32_t id, const string& path, const string& file_format,
                                          const TCompressionType::type& compression_codec, const vector<TExpr> t_output_expr,
                                          const FragmentContext* fragment_ctx);

    ~TableFunctionTableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TableFunctionTableSinkOperator>(this, _id, _plan_node_id, driver_sequence, _path, _file_format, _compression_codec,
                                                                _output_expr_ctxs, _parquet_file_schema, _fragment_ctx);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    const std::string _path;
    const std::string _file_format;
    const TCompressionType::type _compression_codec;
    TCloudConfiguration _cloud_conf;

    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::shared_ptr<::parquet::schema::GroupNode> _parquet_file_schema;
    std::vector<ExprContext*> _partition_expr_ctxs;

    const FragmentContext* _fragment_ctx;
};

} // namespace pipeline
} // namespace starrocks
