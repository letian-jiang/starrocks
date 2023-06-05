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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.UnloadStmt;

import java.util.ArrayList;
import java.util.List;

public class UnloadStmtAnalyzer {
    public static void analyze(UnloadStmt unloadStmt, ConnectContext session) {
        analyzeTableProperties(unloadStmt);
        new QueryAnalyzer(session).analyze(unloadStmt.getQueryStatement());

        QueryRelation query = unloadStmt.getQueryStatement().getQueryRelation();
        List<Field> allFields = query.getRelationFields().getAllFields();
        List<Column> columns = new ArrayList<>();
        for (Field field : allFields) {
            Column column = new Column(field.getName(), field.getType(), field.isNullable());
            columns.add(column);
        }
        TableFunctionTable table = new TableFunctionTable(unloadStmt.getPath(), unloadStmt.getFormat(), columns);
        unloadStmt.setTargetTable(table);
    }

    public static void analyzeTableProperties(UnloadStmt stmt) {
        String path = stmt.getTblProperties().get("path");
        if (path == null) {
            throw new SemanticException("path is null");
        }
        stmt.setPath(path);

        String format = "parquet";
        stmt.setFormat(format);
    }
}
