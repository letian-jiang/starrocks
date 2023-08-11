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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.connector.delta.DeltaLakeConnector;
import com.starrocks.connector.hive.HiveConnector;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.connector.hudi.HudiConnector;
import com.starrocks.connector.iceberg.IcebergConnector;
import com.starrocks.credential.CloudConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnifiedConnector implements Connector {
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String HIVE_METASTORE_TYPE = "hive.metastore.type";
    public static final List<String> SUPPORTED_METASTORE_TYPE = Lists.newArrayList("hive", "glue", "dlf");
    private final Map<String, String> properties;
    private final String catalogName;
    private final Map<String, Connector> connectorMap;
    private final IHiveMetastore hiveMetastore;

    public UnifiedConnector(ConnectorContext context) {
        this.properties = context.getProperties();
        this.catalogName = context.getCatalogName();
        this.connectorMap = ImmutableMap.of(
                "hive", new HiveConnector(context),
                "iceberg", new IcebergConnector(context),
                "hudi", new HudiConnector(context),
                "deltalake", new DeltaLakeConnector(context)
        );

        HiveMetaClient metaClient = HiveMetaClient.createHiveMetaClient(properties);
        this.hiveMetastore = new HiveMetastore(metaClient, catalogName); // wrap it with a table dispatcher
        validate();
        onCreate();
    }

    public void validate() {
        // no-op
    }

    public void onCreate() {
        // no-op
    }

    @Override
    public ConnectorMetadata getMetadata() {
        Map<String, ConnectorMetadata> metadataMap = new HashMap<>();
        connectorMap.forEach((k, v) -> metadataMap.put(k, v.getMetadata()));

        return new UnifiedMetadata(metadataMap, hiveMetastore);
    }

    @Override
    public void shutdown() {
        connectorMap.forEach((k, v) -> v.shutdown());
    }

    @Override
    public void bindConfig(ConnectorConfig config) {
        connectorMap.forEach((k, v) -> v.bindConfig(config));
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return connectorMap.get("hive").getCloudConfiguration();
    }
}
