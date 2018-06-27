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
package com.facebook.presto.geode;

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class GeodeTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, GeodeTableDescription>>
{
    private static final Logger log = Logger.get(GeodeTableDescriptionSupplier.class);

    private final GeodeConnectorConfig geodeConnectorConfig;
    private final JsonCodec<GeodeTableDescription> tableDescriptionCodec;

    @Inject
    GeodeTableDescriptionSupplier(GeodeConnectorConfig geodeConnectorConfig, JsonCodec<GeodeTableDescription> tableDescriptionCodec)
    {
        this.geodeConnectorConfig = requireNonNull(geodeConnectorConfig, "geodeConnectorConfig is null");
        this.tableDescriptionCodec = requireNonNull(tableDescriptionCodec, "tableDescriptionCodec is null");
    }

    @Override
    public Map<SchemaTableName, GeodeTableDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, GeodeTableDescription> builder = ImmutableMap.builder();

        try {
            for (File file : listFiles(geodeConnectorConfig.getTableDescriptionDir())) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    GeodeTableDescription table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = firstNonNull(table.getSchemaName(), geodeConnectorConfig.getDefaultSchema());
                    log.debug("Redis table %s.%s: %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, GeodeTableDescription> tableDefinitions = builder.build();

            log.debug("Loaded table definitions: %s", tableDefinitions.keySet());

            builder = ImmutableMap.builder();
            for (String definedTable : geodeConnectorConfig.getTableNames()) {
                SchemaTableName tableName;
                try {
                    tableName = parseTableName(definedTable);
                }
                catch (IllegalArgumentException iae) {
                    tableName = new SchemaTableName(geodeConnectorConfig.getDefaultSchema(), definedTable);
                }

                if (tableDefinitions.containsKey(tableName)) {
                    GeodeTableDescription redisTable = tableDefinitions.get(tableName);
                    log.debug("Found Table definition for %s: %s", tableName, redisTable);
                    builder.put(tableName, redisTable);
                }
                else {
                    // A dummy table definition only supports the internal columns.
                    log.debug("Created dummy Table definition for %s", tableName);
                    builder.put(tableName, new GeodeTableDescription(tableName.getTableName(),
                            tableName.getSchemaName(),
                            new GeodeTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.of()),
                            new GeodeTableFieldGroup(DummyRowDecoder.NAME, null, ImmutableList.of())));
                }
            }

            return builder.build();
        }
        catch (IOException e) {
            log.warn(e, "Error: ");
            throw new UncheckedIOException(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static SchemaTableName parseTableName(String schemaTableName)
    {
        checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
        List<String> parts = Splitter.on('.').splitToList(schemaTableName);
        checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
        return new SchemaTableName(parts.get(0), parts.get(1));
    }
}
