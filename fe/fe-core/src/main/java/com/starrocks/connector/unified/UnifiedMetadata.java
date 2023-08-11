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

package com.starrocks.connector.unified;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TSinkCommitInfo;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.hive.HiveMetastoreApiConverter.validateHiveTableType;

public class UnifiedMetadata implements ConnectorMetadata {
    public static final String ICEBERG_TABLE_TYPE_NAME = "table_type";
    public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
    public static final String SPARK_TABLE_PROVIDER_KEY = "spark.sql.sources.provider";
    public static final String DELTA_LAKE_PROVIDER = "delta";
    private final Map<String, ConnectorMetadata> metadataMap;
    private final IHiveMetastore hms;

    public UnifiedMetadata(Map<String, ConnectorMetadata> metadataMap, IHiveMetastore hms) {
        this.metadataMap = metadataMap;
        this.hms = hms;
    }

    private String getTableType(String dbName, String tblName) {
        org.apache.hadoop.hive.metastore.api.Table hmsTable = hms.getHmsTable(dbName, tblName);
        hmsTable.getParameters();
        if (isIcebergTable(hmsTable)) {
            return "iceberg";
        } else if (isHudiTable(hmsTable)) {
            return "hudi";
        } else if (isDeltaLakeTable(hmsTable)) {
            return "deltalake";
        }
        validateHiveTableType(hmsTable.getTableType());
        return "hive";
    }

    private String getTableType(Table table) {
        if (table.isHiveTable()) {
            return "hive";
        } else if (table.isIcebergTable()) {
            return "iceberg";
        } else if (table.isHudiTable()) {
            return "hudi";
        } else if (table.isDeltalakeTable()) {
            return "deltalake";
        }
        return "unknown";
    }

    private ConnectorMetadata metadataOfTable(String dbName, String tblName) {
        String type = getTableType(dbName, tblName);
        return metadataMap.get(type);
    }

    private ConnectorMetadata metadataOfTable(Table table) {
        String type = getTableType(table);
        return metadataMap.get(type);
    }

    private boolean isIcebergTable(org.apache.hadoop.hive.metastore.api.Table table) {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(ICEBERG_TABLE_TYPE_NAME));
    }

    private boolean isHudiTable(org.apache.hadoop.hive.metastore.api.Table table) {
        return HiveMetastoreApiConverter.isHudiTable(table.getSd().getInputFormat());
    }

    private boolean isDeltaLakeTable(org.apache.hadoop.hive.metastore.api.Table table) {
        return DELTA_LAKE_PROVIDER.equalsIgnoreCase(table.getParameters().get(SPARK_TABLE_PROVIDER_KEY));
    }

    @Override
    public List<String> listDbNames() {
        return metadataMap.get("hive").listDbNames();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return metadataMap.get("hive").listTableNames(dbName);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        ConnectorMetadata metadata = metadataOfTable(databaseName, tableName);
        return metadata.listPartitionNames(databaseName, tableName);
    }

    @Override
    public List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                  List<Optional<String>> partitionValues) {
        ConnectorMetadata metadata = metadataOfTable(databaseName, tableName);
        return metadata.listPartitionNamesByValue(databaseName, tableName, partitionValues);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        ConnectorMetadata metadata = metadataOfTable(dbName, tblName);
        return metadata.getTable(dbName, tblName);
    }

    @Override
    public Pair<Table, MaterializedIndexMeta> getMaterializedViewIndex(String dbName, String tblName) {
        ConnectorMetadata metadata = metadataOfTable(dbName, tblName);
        return metadata.getMaterializedViewIndex(dbName, tblName);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys, long snapshotId,
                                                   ScalarOperator predicate, List<String> fieldNames) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getRemoteFileInfos(table, partitionKeys, snapshotId, predicate, fieldNames);
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getPartitions(table, partitionNames);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table, Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys, ScalarOperator predicate) {
        ConnectorMetadata metadata = metadataOfTable(table);
        return metadata.getTableStatistics(session, table, columns, partitionKeys, predicate);
    }

    @Override
    public void clear() {
        metadataMap.forEach((k, v) -> v.clear());
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        ConnectorMetadata metadata = metadataOfTable(table);
        metadata.refreshTable(srDbName, table, partitionNames, onlyCachedPartitions);
    }

    @Override
    public void createDb(String dbName) throws DdlException, AlreadyExistsException {
        // TODO: any difference between a hive database between a iceberg database
        metadataMap.get("hive").createDb(dbName);
    }

    @Override
    public boolean dbExists(String dbName) {
        return metadataMap.get("hive").dbExists(dbName);
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) throws DdlException, AlreadyExistsException {
        metadataMap.get("hive").createDb(dbName, properties);
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
        metadataMap.get("hive").dropDb(dbName, isForceDrop);
    }

    @Override
    public Database getDb(long dbId) {
        return metadataMap.get("hive").getDb(dbId);
    }

    @Override
    public Database getDb(String name) {
        return metadataMap.get("hive").getDb(name);
    }

    @Override
    public List<Long> getDbIds() {
        return metadataMap.get("hive").getDbIds();
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        // TODO: force a using engine clause? or default to hive table?
        return metadataMap.get("hive").createTable(stmt);
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        // no-op
    }

    @Override
    public void finishSink(String dbName, String table, List<TSinkCommitInfo> commitInfos) {
        // no-op
    }

    @Override
    public void alterTable(AlterTableStmt stmt) throws UserException {
        // no-op
    }

    @Override
    public void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
        // no-op
    }

    @Override
    public void alterTableComment(Database db, Table table, AlterTableCommentClause clause) {
        // no-op
    }

    @Override
    public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
        // no-op
    }

    @Override
    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        // no-op
    }

    @Override
    public void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {
        // no-op
    }

    @Override
    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        // no-op
    }

    @Override
    public void renamePartition(Database db, Table table, PartitionRenameClause renameClause) throws DdlException {
        // no-op
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStmt stmt) throws AnalysisException, DdlException {
        // no-op
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStatement statement) throws DdlException {
        // no-op
    }

    @Override
    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        // no-op
    }

    @Override
    public void alterMaterializedView(AlterMaterializedViewStmt stmt)
            throws DdlException, MetaNotFoundException, AnalysisException {
        // no-op
    }

    @Override
    public String refreshMaterializedView(RefreshMaterializedViewStatement refreshMaterializedViewStatement)
            throws DdlException, MetaNotFoundException {
        // no-op
        return null;
    }

    @Override
    public void cancelRefreshMaterializedView(String dbName, String mvName) throws DdlException, MetaNotFoundException {
        // no-op
    }

    @Override
    public void createView(CreateViewStmt stmt) throws DdlException {
        // no-op
    }

    @Override
    public void alterView(AlterViewStmt stmt) throws DdlException, UserException {
        // no-op
    }
}
