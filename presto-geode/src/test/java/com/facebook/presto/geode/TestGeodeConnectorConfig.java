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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

public class TestGeodeConnectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(GeodeConnectorConfig.class)
                .setNodes("")
                .setDefaultSchema("")
                .setTableNames("")
                .setTableDescriptionDir(new File("etc/geode/")));
//                .setKeyPrefixSchemaTable(false)
//                .setRedisKeyDelimiter(":")
//                .setGeodeConnectTimeout("2000ms")
//                .setRedisDataBaseIndex(0)
//                .setGeodePassword(null)
//                .setRedisScanCount(100)
//                .setHideInternalColumns(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("geode.table-description-dir", "/var/lib/geode")
                .put("geode.table-names", "table1, table2, table3")
                .put("geode.default-schema", "geode")
                .put("geode.nodes", "localhost:12345,localhost:23456")
//                .put("geode.key-delimiter", ",")
//                .put("geode.key-prefix-schema-table", "true")
//                .put("geode.scan-count", "20")
//                .put("geode.hide-internal-columns", "false")
//                .put("geode.connect-timeout", "10s")
//                .put("geode.database-index", "5")
//                .put("geode.password", "secret")
                .build();

        GeodeConnectorConfig expected = new GeodeConnectorConfig()
                .setTableDescriptionDir(new File("/var/lib/geode"))
                .setTableNames("table1, table2, table3")
                .setDefaultSchema("geode")
                .setNodes("localhost:12345, localhost:23456");
//                .setHideInternalColumns(false)
//                .setRedisScanCount(20)
//                .setGeodeConnectTimeout("10s")
//                .setRedisDataBaseIndex(5)
//                .setGeodePassword("secret")
//                .setRedisKeyDelimiter(",")
//                .setKeyPrefixSchemaTable(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
