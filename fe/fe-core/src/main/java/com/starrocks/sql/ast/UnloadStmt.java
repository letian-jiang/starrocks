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

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

public class UnloadStmt extends DmlStmt {

    private final Map<String, String> tblProperties; // used for unnamed table (aka. TABLE(...))

    private final QueryStatement queryStatement;

    private String path;
    private String format;

    private Table targetTable;

    public UnloadStmt(Map<String, String> tblProperties, QueryStatement queryStatement, NodePosition pos) {
        super(pos);
        this.tblProperties = tblProperties;
        this.queryStatement = queryStatement;
    }

    public UnloadStmt(Map<String, String> tblProperties, QueryStatement queryStatement) {
        this(tblProperties, queryStatement, NodePosition.ZERO);
    }

    @Override
    public TableName getTableName() {
        return null;
    }

    public QueryStatement getQueryStatement() {
        return this.queryStatement;
    }

    public Map<String, String> getTblProperties() {
        return this.tblProperties;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setTargetTable(Table targetTable) {
        this.targetTable = targetTable;
    }

    public List<String> getTargetColumnNames() {
        return null;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUnloadStatement(this, context);
    }

    public Table getTargetTable() {
        return targetTable;
    }
}
