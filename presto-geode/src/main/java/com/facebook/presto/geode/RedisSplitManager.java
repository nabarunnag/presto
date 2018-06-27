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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import redis.clients.jedis.Jedis;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.geode.RedisHandleResolver.convertLayout;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Redis specific implementation of {@link ConnectorSplitManager}.
 */
public class RedisSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final GeodeConnectorConfig geodeConnectorConfig;
    private final GeodeClientConnections jedisManager;

    private static final long REDIS_MAX_SPLITS = 100;
    private static final long REDIS_STRIDE_SPLITS = 100;

    @Inject
    public RedisSplitManager(
            GeodeConnectorId connectorId,
            GeodeConnectorConfig geodeConnectorConfig,
            GeodeClientConnections jedisManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.geodeConnectorConfig = requireNonNull(geodeConnectorConfig, "redisConfig is null");
        this.jedisManager = requireNonNull(jedisManager, "jedisManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        GeodeTableHandle geodeTableHandle = convertLayout(layout).getTable();

        List<HostAddress> nodes = new ArrayList<>(geodeConnectorConfig.getNodes());
        Collections.shuffle(nodes);

        checkState(!nodes.isEmpty(), "No Redis nodes available");
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        long numberOfKeys = 1;
        // when Redis keys are provides in a zset, create multiple
        // splits by splitting zset in chunks
        if (geodeTableHandle.getKeyDataFormat().equals("zset")) {
            try (Jedis jedis = jedisManager.getClientCache(nodes.get(0)).getResource()) {
                numberOfKeys = jedis.zcount(geodeTableHandle.getKeyName(), "-inf", "+inf");
            }
        }

        long stride = REDIS_STRIDE_SPLITS;

        if (numberOfKeys / stride > REDIS_MAX_SPLITS) {
            stride = numberOfKeys / REDIS_MAX_SPLITS;
        }

        for (long startIndex = 0; startIndex < numberOfKeys; startIndex += stride) {
            long endIndex = startIndex + stride - 1;
            if (endIndex >= numberOfKeys) {
                endIndex = -1;
            }

            GeodeSplit split = new GeodeSplit(connectorId,
                    geodeTableHandle.getSchemaName(),
                    geodeTableHandle.getTableName(),
                    geodeTableHandle.getKeyDataFormat(),
                    geodeTableHandle.getValueDataFormat(),
                    geodeTableHandle.getKeyName(),
                    startIndex,
                    endIndex,
                    nodes);

            builder.add(split);
        }
        return new FixedSplitSource(builder.build());
    }
}
