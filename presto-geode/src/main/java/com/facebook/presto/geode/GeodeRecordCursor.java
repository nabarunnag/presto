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
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.geode.RedisInternalFieldDescription.KEY_CORRUPT_FIELD;
import static com.facebook.presto.geode.RedisInternalFieldDescription.KEY_FIELD;
import static com.facebook.presto.geode.RedisInternalFieldDescription.KEY_LENGTH_FIELD;
import static com.facebook.presto.geode.RedisInternalFieldDescription.VALUE_CORRUPT_FIELD;
import static com.facebook.presto.geode.RedisInternalFieldDescription.VALUE_FIELD;
import static com.facebook.presto.geode.RedisInternalFieldDescription.VALUE_LENGTH_FIELD;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import org.apache.geode.cache.client.ClientCache;

public class GeodeRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(GeodeRecordCursor.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final RowDecoder keyDecoder;
    private final RowDecoder valueDecoder;
    private final Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders;
    private final Map<DecoderColumnHandle, FieldDecoder<?>> valueFieldDecoders;

    private final GeodeSplit split;
    private final List<DecoderColumnHandle> columnHandles;
    private final ClientCache clientCache;

    private Iterator<String> keysIterator;

    private FieldValueProvider[] fieldValueProviders;

    private String valueString;
    private Map<String, String> valueMap;

    private long totalBytes;
    private long totalValues;

    GeodeRecordCursor(
            RowDecoder keyDecoder,
            RowDecoder valueDecoder,
            Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders,
            Map<DecoderColumnHandle, FieldDecoder<?>> valueFieldDecoders,
            GeodeSplit split,
            List<DecoderColumnHandle> columnHandles,
            GeodeClientConnections geodeClientConnections)
    {
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.keyFieldDecoders = keyFieldDecoders;
        this.valueFieldDecoders = valueFieldDecoders;
        this.split = split;
        this.columnHandles = columnHandles;
        this.clientCache = geodeClientConnections.getClientCache();

        fetchKeys();
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        while (!keysIterator.hasNext()) {
            fetchKeys();
        }

        return nextRow(keysIterator.next());
    }

    private boolean nextRow(String keyString)
    {
        fetchData(keyString);

        byte[] keyData = keyString.getBytes(StandardCharsets.UTF_8);

        byte[] valueData = EMPTY_BYTE_ARRAY;
        if (valueString != null) {
            valueData = valueString.getBytes(StandardCharsets.UTF_8);
        }

        totalBytes += valueData.length;
        totalValues++;

        Set<FieldValueProvider> fieldValueProviders = new HashSet<>();

        fieldValueProviders.add(KEY_FIELD.forByteValue(keyData));
        fieldValueProviders.add(VALUE_FIELD.forByteValue(valueData));
        fieldValueProviders.add(KEY_LENGTH_FIELD.forLongValue(keyData.length));
        fieldValueProviders.add(VALUE_LENGTH_FIELD.forLongValue(valueData.length));
        fieldValueProviders.add(KEY_CORRUPT_FIELD.forBooleanValue(keyDecoder.decodeRow(
                keyData,
                null,
                fieldValueProviders,
                columnHandles,
                keyFieldDecoders)));
        fieldValueProviders.add(VALUE_CORRUPT_FIELD.forBooleanValue(valueDecoder.decodeRow(
                valueData,
                valueMap,
                fieldValueProviders,
                columnHandles,
                valueFieldDecoders)));

        this.fieldValueProviders = new FieldValueProvider[columnHandles.size()];

        // If a value provider for a requested internal column is present, assign the
        // value to the internal cache. It is possible that an internal column is present
        // where no value provider exists (e.g. the '_corrupt' column with the DummyRowDecoder).
        // In that case, the cache is null (and the column is reported as null).
        for (int i = 0; i < columnHandles.size(); i++) {
            for (FieldValueProvider fieldValueProvider : fieldValueProviders) {
                if (fieldValueProvider.accept(columnHandles.get(i))) {
                    this.fieldValueProviders[i] = fieldValueProvider;
                    break;
                }
            }
        }

        // Advanced successfully.
        return true;
    }

    @SuppressWarnings("SimplifiableConditionalExpression")
    @Override
    public boolean getBoolean(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");

        checkFieldType(field, boolean.class);
        return isNull(field) ? false : fieldValueProviders[field].getBoolean();
    }

    @Override
    public long getLong(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");

        checkFieldType(field, long.class);
        return isNull(field) ? 0L : fieldValueProviders[field].getLong();
    }

    @Override
    public double getDouble(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");

        checkFieldType(field, double.class);
        return isNull(field) ? 0.0d : fieldValueProviders[field].getDouble();
    }

    @Override
    public Slice getSlice(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");

        checkFieldType(field, Slice.class);
        return isNull(field) ? Slices.EMPTY_SLICE : fieldValueProviders[field].getSlice();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");

        return fieldValueProviders[field] == null || fieldValueProviders[field].isNull();
    }

    @Override
    public Object getObject(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");

        throw new IllegalArgumentException(format("Type %s is not supported", getType(field)));
    }

    private void checkFieldType(int field, Class<?> expected)
    {
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }


    // Redis keys can be contained in the user-provided ZSET
    // Otherwise they need to be found by scanning Redis
    private void fetchKeys()
    {
        keysIterator = clientCache.<String, Object>getRegion(split.getRegionName()).keySetOnServer().iterator();
    }

    private void fetchData(String keyString)
    {
        valueString = null;

        valueString = clientCache.getRegion(split.getRegionName()).get(keyString).toString();
    }
}
