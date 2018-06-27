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

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderRegistry;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.geode.GeodeHandleResolver.convertColumnHandle;
import static com.facebook.presto.geode.GeodeHandleResolver.convertSplit;
import static java.util.Objects.requireNonNull;

/**
 * Factory for Redis specific {@link RecordSet} instances.
 */
public class GeodeRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final GeodeClientConnections geodeClientConnections;
    private final DecoderRegistry registry;

    @Inject
    public GeodeRecordSetProvider(DecoderRegistry registry, GeodeClientConnections geodeClientConnections)
    {
        this.registry = requireNonNull(registry, "registry is null");
        this.geodeClientConnections = requireNonNull(geodeClientConnections, "geodeClientConnections is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        GeodeSplit geodeSplit = convertSplit(split);

        ImmutableList.Builder<DecoderColumnHandle> handleBuilder = ImmutableList.builder();
        ImmutableMap.Builder<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoderBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<DecoderColumnHandle, FieldDecoder<?>> valueFieldDecoderBuilder = ImmutableMap.builder();

        RowDecoder keyDecoder = registry.getRowDecoder(geodeSplit.getKeyDataFormat());
        RowDecoder valueDecoder = registry.getRowDecoder(geodeSplit.getValueDataFormat());

        for (ColumnHandle handle : columns) {
            GeodeColumnHandle columnHandle = convertColumnHandle(handle);
            handleBuilder.add(columnHandle);

            if (!columnHandle.isInternal()) {
                if (columnHandle.isKeyDecoder()) {
                    FieldDecoder<?> fieldDecoder = registry.getFieldDecoder(
                            geodeSplit.getKeyDataFormat(),
                            columnHandle.getType().getJavaType(),
                            columnHandle.getDataFormat());

                    keyFieldDecoderBuilder.put(columnHandle, fieldDecoder);
                }
                else {
                    FieldDecoder<?> fieldDecoder = registry.getFieldDecoder(
                            geodeSplit.getValueDataFormat(),
                            columnHandle.getType().getJavaType(),
                            columnHandle.getDataFormat());

                    valueFieldDecoderBuilder.put(columnHandle, fieldDecoder);
                }
            }
        }

        ImmutableList<DecoderColumnHandle> handles = handleBuilder.build();
        ImmutableMap<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders = keyFieldDecoderBuilder.build();
        ImmutableMap<DecoderColumnHandle, FieldDecoder<?>> valueFieldDecoders = valueFieldDecoderBuilder.build();

        return new GeodeRecordSet(geodeSplit, geodeClientConnections, handles, keyDecoder, valueDecoder, keyFieldDecoders, valueFieldDecoders);
    }
}
