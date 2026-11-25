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
package org.apache.asterix.builders;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.junit.Assert;
import org.junit.Test;

public class BuilderCapacityTest {

    private static final int COUNT = 10_000;
    // the serialized header (type tag, lengths, counts) is written but not held in the builder's arrays
    private static final int HEADER_ALLOWANCE = 64;

    @Test
    public void recordBuilderCountsFieldOffsets() throws Exception {
        @SuppressWarnings("unchecked")
        ISerializerDeserializer<AString> stringSerDe =
                SerializerDeserializerProvider.INSTANCE.getAStringSerializerDeserializer();
        AMutableString aString = new AMutableString("");
        ArrayBackedValueStorage name = new ArrayBackedValueStorage();
        ArrayBackedValueStorage value = int8Value();
        RecordBuilder builder = new RecordBuilder();
        builder.reset(RecordUtil.FULLY_OPEN_RECORD_TYPE);
        builder.init();
        for (int i = 0; i < COUNT; i++) {
            name.reset();
            aString.setValue("f" + i);
            stringSerDe.serialize(aString, name.getDataOutput());
            builder.addField(name, value);
        }
        int written = write(builder);
        // openPartOffsets (8 B) and openFieldNameLengths (4 B) per field are kept but not serialized
        assertCapacityAtLeast(builder.getCapacity(),
                written - HEADER_ALLOWANCE + (long) COUNT * (Long.BYTES + Integer.BYTES));
    }

    @Test
    public void listBuilderCountsItemOffsets() throws Exception {
        ArrayBackedValueStorage item = int8Value();
        OrderedListBuilder builder = new OrderedListBuilder();
        builder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        for (int i = 0; i < COUNT; i++) {
            builder.addItem(item);
        }
        int written = write(builder);
        // offsets keeps 4 B per item besides the serialized offset array
        assertCapacityAtLeast(builder.getCapacity(), written - HEADER_ALLOWANCE + (long) COUNT * Integer.BYTES);
    }

    private static void assertCapacityAtLeast(long capacity, long expected) {
        Assert.assertTrue("capacity " + capacity + " < " + expected, capacity >= expected);
    }

    private static ArrayBackedValueStorage int8Value() throws Exception {
        ArrayBackedValueStorage value = new ArrayBackedValueStorage();
        value.getDataOutput().writeByte(ATypeTag.SERIALIZED_INT8_TYPE_TAG);
        value.getDataOutput().writeByte(1);
        return value;
    }

    private static int write(RecordBuilder builder) throws Exception {
        ArrayBackedValueStorage out = new ArrayBackedValueStorage();
        builder.write(out.getDataOutput(), true);
        return out.getLength();
    }

    private static int write(IAsterixListBuilder builder) throws Exception {
        ArrayBackedValueStorage out = new ArrayBackedValueStorage();
        builder.write(out.getDataOutput(), true);
        return out.getLength();
    }
}
