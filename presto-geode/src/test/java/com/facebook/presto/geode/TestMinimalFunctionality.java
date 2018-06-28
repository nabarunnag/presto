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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.geode.util.GeodeServer;
import com.facebook.presto.geode.util.JsonEncoder;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.geode.util.RedisTestUtils.createEmptyTableDescription;
import static com.facebook.presto.geode.util.RedisTestUtils.installGeodePlugin;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertTrue;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.internal.cache.GemFireCacheImpl;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("geode")
            .setSchema("default")
            .build();

    private String tableName;
    private StandaloneQueryRunner queryRunner;
    private ClientCache clientCache;

    @BeforeClass
    public void startGeode()
            throws Exception
    {
//        geodeServer = GeodeServer.createGeodeServerLauncher();
//        geodeServer.start();
        ClientCacheFactory
            clientCacheFactory = new ClientCacheFactory().set("cache-xml-file","/Users/nnag/Development/prestodb/presto/presto-geode/src/test/java/com/facebook/presto/geode/util/client-cache.xml");
//        clientCache = clientCacheFactory.create();
    }

    @AfterClass(alwaysRun = true)
    public void stopGeodeServer()
    {
//        geodeServer.close();
//        geodeServer = null;
    }

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
//        this.tableName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");
        this.tableName = "region";

        this.queryRunner = new StandaloneQueryRunner(SESSION);

        installGeodePlugin(queryRunner,
                ImmutableMap.<SchemaTableName, GeodeTableDescription>builder()
                        .put(createEmptyTableDescription(new SchemaTableName("default", tableName)))
                        .build());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    private void populateData(int count)
    {
        System.out.println("Doing puts");
        JsonEncoder jsonEncoder = new JsonEncoder();
        Region region = GemFireCacheImpl.getInstance().getRegion("default.region");
        for (long i = 0; i < count; i++) {
            Object value = ImmutableMap.of("id", Long.toString(i), "value", UUID.randomUUID().toString());

             {
                region.put(tableName + ":" + i, jsonEncoder.toString(value));
            }
        }
        System.out.println("Done with puts");
    }

    @Test
    public void testTableExists()
    {
        QualifiedObjectName name = new QualifiedObjectName("geode", "default", tableName);
        System.out.println("NABA name of the table " + name.toString());
        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(session, name);
                    assertTrue(handle.isPresent());
                });
    }

    @Test
    public void testTableHasData()
    {
//        MaterializedResult result = queryRunner.execute("SELECT count(1) from " + tableName);

//        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
//                .row(0L)
//                .build();
//
//        assertEquals(result, expected);

        int count = 1000;
//        populateData(count);

        MaterializedResult result = queryRunner.execute("SELECT count(1) from " + tableName);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row((long) count)
                .build();

        assertEquals(result, expected);
    }
}