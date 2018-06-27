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

import com.facebook.presto.geode.util.GeodeServer;
import com.facebook.presto.tests.AbstractTestQueries;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.geode.util.GeodeServer.createGeodeServerLauncher;

@Test
public class TestRedisDistributed
        extends AbstractTestQueries
{
    private final GeodeServer geodeServer;

    public TestRedisDistributed()
            throws Exception
    {
        this(createGeodeServerLauncher());
    }

    public TestRedisDistributed(GeodeServer geodeServer)
    {
        super(() -> RedisQueryRunner.createRedisQueryRunner(geodeServer, "string", TpchTable.getTables()));
        this.geodeServer = geodeServer;
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        geodeServer.close();
    }
}
