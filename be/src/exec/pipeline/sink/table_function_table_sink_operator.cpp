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

#include "table_function_table_sink_operator.h"

#include <utility>

#include "exec/parquet_builder.h"
#include "formats/parquet/file_writer.h"
#include "glog/logging.h"

namespace starrocks::pipeline {

Status TableFunctionTableSinkOperator::prepare(RuntimeState* state) {
    LOG(INFO) << "TableFunctionTableSinkOperator::prepare";
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void TableFunctionTableSinkOperator::close(RuntimeState* state) {
    LOG(INFO) << "TableFunctionTableSinkOperator::close";
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            writer.second->close(state);
        }
    }
    Operator::close(state);
}

bool TableFunctionTableSinkOperator::need_input() const {
    for (const auto& writer : _partition_writers) {
        if (!writer.second->writable()) {
            return false;
        }
    }

    return true;
}

bool TableFunctionTableSinkOperator::is_finished() const {
    if (_partition_writers.size() == 0) {
        return _is_finished.load();
    }
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            return false;
        }
    }

    return true;
}

Status TableFunctionTableSinkOperator::set_finishing(RuntimeState* state) {
    LOG(INFO) << "TableFunctionTableSinkOperator::set_finishing";
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            writer.second->close(state);
        }
    }

    if (_partition_writers.size() == 0) {
        _is_finished = true;
    }
    return Status::OK();
}

bool TableFunctionTableSinkOperator::pending_finish() const {
    return !is_finished();
}

Status TableFunctionTableSinkOperator::set_cancelled(RuntimeState* state) {
    return Status::OK();
}

StatusOr<ChunkPtr> TableFunctionTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from iceberg table sink operator");
}

Status TableFunctionTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    LOG(INFO) << "TableFunctionTableSinkOperator::push_chunk";
    TableInfo tableInfo;
    tableInfo.schema = _parquet_file_schema;
    tableInfo.compress_type = _compression_codec;
    tableInfo.cloud_conf = _cloud_conf;


    if (_partition_writers.empty()) {
        tableInfo.partition_location = _path;
        LOG(INFO) << "create parquet writer, path = " << _path << ", driver seq = " << _driver_sequence;
        auto writer = std::make_unique<RollingAsyncParquetWriter>(tableInfo, _output_expr, _common_metrics.get(),
                                                                  nullptr, state, _driver_sequence);
        _partition_writers.insert({"ICEBERG_UNPARTITIONED_TABLE_LOCATION", std::move(writer)});
    }

    _partition_writers["ICEBERG_UNPARTITIONED_TABLE_LOCATION"]->append_chunk(chunk.get(), state);

    LOG(INFO) << "TableFunctionTableSinkOperator::push_chunk done";
    return Status::OK();
}

TableFunctionTableSinkOperatorFactory::TableFunctionTableSinkOperatorFactory(const int32_t id, const string& path, const string& file_format,
                                                                             const TCompressionType::type& compression_codec, const vector<TExpr> t_output_expr,
                                                                             const FragmentContext* fragment_ctx)
        : OperatorFactory(id, "table_function_table_sink", Operator::s_pseudo_plan_node_id_for_table_function_table_sink),
          _path(path),
          _file_format(file_format),
          _compression_codec(compression_codec),
          _t_output_expr(t_output_expr),
          _fragment_ctx(fragment_ctx) {}

Status TableFunctionTableSinkOperatorFactory::prepare(RuntimeState* state) {
    LOG(INFO) << "TableFunctionTableSinkOperatorFactory::prepare";
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));

    if (_file_format == "parquet") {
        std::vector<std::string> field_names;
        std::vector<parquet::FileColumnId> ids;
        for (size_t i = 0; i < _output_expr_ctxs.size(); i++) {
            field_names.push_back("col" + std::to_string(i));
            ids.push_back({int32_t(i), {}});
        }
        auto result = parquet::ParquetBuildHelper::make_schema(field_names, _output_expr_ctxs, ids);
        _parquet_file_schema = result.ValueOrDie();
    } else {
        return Status::InternalError("unsupported format" + _file_format);
    }

    return Status::OK();
}

void TableFunctionTableSinkOperatorFactory::close(RuntimeState* state) {
    LOG(INFO) << "TableFunctionTableSinkOperatorFactory::close";
    Expr::close(_partition_expr_ctxs, state);
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
