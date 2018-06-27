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

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import io.airlift.log.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;

/**
 * Manages connections to the Redis nodes
 */
public class GeodeClientConnections
{
    private static final Logger log = Logger.get(GeodeClientConnections.class);

//    private final LoadingCache<HostAddress, ClientCache> jedisPoolCache;
    private final ClientCache clientCache;

    private final GeodeConnectorConfig geodeConnectorConfig;

    @Inject
    GeodeClientConnections(
            GeodeConnectorConfig geodeConnectorConfig,
            NodeManager nodeManager)
    {
        this.geodeConnectorConfig = requireNonNull(geodeConnectorConfig, "redisConfig is null");
        this.clientCache = createClientCache();
    }

    private ClientCache createClientCache() {
        ClientCacheFactory clientCacheFactory = new ClientCacheFactory().addPoolLocator(geodeConnectorConfig.getHost()
            ,geodeConnectorConfig.getPort());
        return clientCacheFactory.create();
    }

    public ClientCache getClientCache()
    {
      return clientCache;
    }

    @PreDestroy
    public void tearDown()
    {
        this.clientCache.close();
    }

//    public GeodeConnectorConfig getGeodeConnectorConfig()
//    {
//        return geodeConnectorConfig;
//    }


}
