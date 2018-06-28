/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.geode.util;

import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.Map;

import com.facebook.presto.geode.GeodePlugin;
import com.facebook.presto.geode.GeodeTableDescription;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;

public final class RedisTestUtils
{
    private RedisTestUtils() {}

    public static void installGeodePlugin(QueryRunner queryRunner, Map<SchemaTableName, GeodeTableDescription> tableDescriptions)
    {
        GeodePlugin geodePlugin = new GeodePlugin();
        geodePlugin.setTableDescriptionSupplier(() -> tableDescriptions);
        queryRunner.installPlugin(geodePlugin);

        Map<String, String> redisConfig = ImmutableMap.of(
                "geode.nodes", "localhost"+ ":" + 40404,
                "geode.table-names", Joiner.on(",").join(tableDescriptions.keySet())
//                "geode.default-schema", "default",
//                "geode.hide-internal-columns", "true",
//                "geode.key-prefix-schema-table", "true"
 );
        queryRunner.createCatalog("geode", "geode", redisConfig);
    }

    public static void loadTpchTable(GeodeServer geodeServer, TestingPrestoClient prestoClient, String tableName, QualifiedObjectName tpchTableName, String dataFormat)
    {
        GeodeLoader
            tpchLoader = new GeodeLoader(prestoClient.getServer(), prestoClient.getDefaultSession(), geodeServer, tableName, dataFormat);
        tpchLoader.execute(format("SELECT * from %s", tpchTableName));
    }

    public static Map.Entry<SchemaTableName, GeodeTableDescription> loadTpchTableDescription(
            JsonCodec<GeodeTableDescription> tableDescriptionJsonCodec,
            SchemaTableName schemaTableName,
            String dataFormat)
            throws IOException
    {
        GeodeTableDescription tpchTemplate;
        try (InputStream data = RedisTestUtils.class.getResourceAsStream(format("/tpch/%s/%s.json", dataFormat, schemaTableName.getTableName()))) {
            tpchTemplate = tableDescriptionJsonCodec.fromJson(ByteStreams.toByteArray(data));
        }

        GeodeTableDescription tableDescription = new GeodeTableDescription(
                schemaTableName.getTableName(),
                schemaTableName.getSchemaName(),
                tpchTemplate.getKey(),
                tpchTemplate.getValue());

        return new AbstractMap.SimpleImmutableEntry<>(schemaTableName, tableDescription);
    }

    public static Map.Entry<SchemaTableName, GeodeTableDescription> createEmptyTableDescription(SchemaTableName schemaTableName)
    {
        GeodeTableDescription tableDescription = new GeodeTableDescription(
                schemaTableName.getTableName(),
                schemaTableName.getSchemaName(),
                null,
                null);

        return new AbstractMap.SimpleImmutableEntry<>(schemaTableName, tableDescription);
    }
}