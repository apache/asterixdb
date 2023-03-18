/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.column.test.filter;

import static org.apache.hyracks.util.string.UTF8StringUtil.charAt;
import static org.apache.hyracks.util.string.UTF8StringUtil.charSize;
import static org.apache.hyracks.util.string.UTF8StringUtil.getNumBytesToStoreLength;
import static org.apache.hyracks.util.string.UTF8StringUtil.getUTFLength;

import java.util.UUID;

import org.apache.asterix.column.values.writer.filters.StringColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.UUIDColumnFilterWriter;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUUIDSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AMutableUUID;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.junit.Assert;
import org.junit.Test;

public class StringUUIDFilterTest {
    private final AMutableString stringValue;
    private final AMutableUUID uuidValue;
    private final AStringSerializerDeserializer stringSerDer;
    private final AUUIDSerializerDeserializer uuidSerDer;

    private final ArrayBackedValueStorage storage;

    public StringUUIDFilterTest() {
        stringValue = new AMutableString("");
        uuidValue = new AMutableUUID();
        stringSerDer = new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());
        uuidSerDer = AUUIDSerializerDeserializer.INSTANCE;
        storage = new ArrayBackedValueStorage();
    }

    @Test
    public void testAsciiString() throws HyracksDataException {
        String[] asciiStrings = { "t", "test", "hello world", "filter" };

        StringColumnFilterWriter filter = new StringColumnFilterWriter();
        filter.reset();
        for (String value : asciiStrings) {
            filter.addValue(getSerializedString(value));
        }

        long min = filter.getMinNormalizedValue();
        long max = filter.getMaxNormalizedValue();

        Assert.assertTrue(min < max);

        long aNorm = getNormalizedValue("a");
        Assert.assertTrue(aNorm < min);

        long filtNorm = getNormalizedValue("filt");
        Assert.assertEquals(filtNorm, min);

        long tNorm = getNormalizedValue("t");
        Assert.assertTrue(tNorm >= min && tNorm <= max);

        long sentenceNorm = getNormalizedValue("filter test");
        Assert.assertTrue(sentenceNorm >= min && sentenceNorm < max);

        long testNorm = getNormalizedValue("test");
        Assert.assertEquals(testNorm, max);

        long greaterThanMaxNorm = getNormalizedValue("zookeeper");
        Assert.assertTrue(greaterThanMaxNorm > max);
    }

    @Test
    public void testUTF8Strings() throws HyracksDataException {
        StringColumnFilterWriter filter = new StringColumnFilterWriter();
        filter.reset();

        //A number 5,
        //An emoji,
        //and 你好世界 = hello world,
        String[] utf8Strings = { "5", "\uD83E\uDD71", "你好世界" };
        for (String value : utf8Strings) {
            filter.addValue(getSerializedString(value));
        }

        long min = filter.getMinNormalizedValue();
        long max = filter.getMaxNormalizedValue();

        Assert.assertTrue(min < max);

        long aNorm = getNormalizedValue("0");
        Assert.assertTrue(aNorm < min);

        long fiveNorm = getNormalizedValue("5");
        Assert.assertEquals(fiveNorm, min);

        long helloNorm = getNormalizedValue("你好");
        Assert.assertTrue(helloNorm > min && helloNorm < max);
    }

    @Test
    public void testUUID() throws HyracksDataException {
        UUIDColumnFilterWriter filter = new UUIDColumnFilterWriter();
        filter.reset();
        long[] msb = { 1L, 2L, 5L, 1L };
        for (long m : msb) {
            for (int i = 0; i < 10; i++) {
                filter.addValue(getSerializedUUID(m, i));
            }
        }

        long min = filter.getMinNormalizedValue();
        long max = filter.getMaxNormalizedValue();

        Assert.assertTrue(min < max);

        Assert.assertEquals(0, min);
        Assert.assertEquals(9, max);
    }

    private IValueReference getSerializedString(String value) throws HyracksDataException {
        storage.reset();
        stringValue.setValue(value);
        stringSerDer.serialize(stringValue, storage.getDataOutput());
        return storage;
    }

    private IValueReference getSerializedUUID(long msb, long lsb) throws HyracksDataException {
        storage.reset();
        char[] uuid = new UUID(msb, lsb).toString().toCharArray();
        uuidValue.parseUUIDString(uuid, 0, uuid.length);
        uuidSerDer.serialize(uuidValue, storage.getDataOutput());
        return storage;
    }

    private long getNormalizedValue(String value) throws HyracksDataException {
        return normalize(getSerializedString(value));
    }

    /**
     * Similar to the string normalizer in {@link StringColumnFilterWriter}
     */
    private static long normalize(IValueReference value) {
        byte[] bytes = value.getByteArray();
        int start = value.getStartOffset();

        int len = getUTFLength(bytes, start);
        long nk = 0;
        int offset = start + getNumBytesToStoreLength(len);
        for (int i = 0; i < 4; ++i) {
            nk <<= 16;
            if (i < len) {
                nk += (charAt(bytes, offset)) & 0xffff;
                offset += charSize(bytes, offset);
            }
        }
        //Make it always positive
        return nk >>> 1;
    }
}
