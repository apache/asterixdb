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

package org.apache.hyracks.dataflow.std.sort.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.std.sort.Utility;
import org.apache.hyracks.util.IntSerDeUtils;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.junit.Before;
import org.junit.Test;

/**
 * @see org.apache.hyracks.dataflow.std.sort.util.DeletableFrameTupleAppender
 */
public class DeletableFrameTupleAppenderTest {
    private static final int META_DATA_SIZE = 4 + 4 + 4 + 4;
    private static final int SLOT_SIZE = 4 + 4;
    private static final char TEST_CH = 'x';
    private static final int TEST_TUPLE_COUNT = 8;
    private static final int TEST_FRAME_SIZE = 256;

    DeletableFrameTupleAppender appender;
    ISerializerDeserializer[] fields = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer(), };
    RecordDescriptor recordDescriptor = new RecordDescriptor(fields);
    ArrayTupleBuilder builder = new ArrayTupleBuilder(recordDescriptor.getFieldCount());

    @Before
    public void initial() throws HyracksDataException {
        appender = new DeletableFrameTupleAppender(recordDescriptor);
    }

    @Test
    public void testClear() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(TEST_FRAME_SIZE);
        appender.clear(buffer);
        assertTrue(appender.getBuffer() == buffer);
        assertTrue(appender.getTupleCount() == 0);
        assertTrue(appender.getTotalFreeSpace() == TEST_FRAME_SIZE - META_DATA_SIZE);
        assertTrue(appender.getContiguousFreeSpace() == TEST_FRAME_SIZE - META_DATA_SIZE);
    }

    ByteBuffer makeAFrame(int capacity, int count, int deletedBytes) throws HyracksDataException {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        int metaOffset = capacity - 4;
        buffer.putInt(metaOffset, count);
        metaOffset -= 4;
        buffer.putInt(metaOffset, deletedBytes);
        // next index
        metaOffset -= 4;
        buffer.putInt(metaOffset, count);
        // append slot
        metaOffset -= 4;
        int appendOffset = metaOffset;
        buffer.putInt(metaOffset, 0);
        metaOffset -= 4;

        int start = 0;
        for (int i = 0; i < count; i++, metaOffset -= 4) {
            makeARecord(builder, i);
            for (int x = 0; x < builder.getFieldEndOffsets().length; x++) {
                buffer.putInt(builder.getFieldEndOffsets()[x]);
            }
            buffer.put(builder.getByteArray(), 0, builder.getSize());

            // Add slot information
            buffer.putInt(metaOffset, start);
            metaOffset -= 4;
            buffer.putInt(metaOffset, buffer.position());

            start = buffer.position();
            assert (metaOffset > buffer.position());
        }
        buffer.putInt(appendOffset, start);
        return buffer;
    }

    void makeARecord(ArrayTupleBuilder builder, int i) throws HyracksDataException {
        builder.reset();
        builder.addField(fields[0], i + 1);
        builder.addField(fields[1], Utility.repeatString(TEST_CH, i + 1));
    }

    int assertTupleIsExpected(int i, int dataOffset, int testString) {
        int lenStrMeta = UTF8StringUtil.getNumBytesToStoreLength(testString);
        int tupleLength = 2 * 4 + 4 + lenStrMeta + testString + 1;
        assertEquals(dataOffset, appender.getTupleStartOffset(i));
        assertEquals(tupleLength, appender.getTupleLength(i));

        assertEquals(dataOffset + 2 * 4, appender.getAbsoluteFieldStartOffset(i, 0));
        assertEquals(4, appender.getFieldLength(i, 0));
        assertEquals(testString + 1,
                IntSerDeUtils.getInt(appender.getBuffer().array(), appender.getAbsoluteFieldStartOffset(i, 0)));
        assertEquals(dataOffset + 2 * 4 + 4, appender.getAbsoluteFieldStartOffset(i, 1));
        assertEquals(lenStrMeta + testString + 1, appender.getFieldLength(i, 1));
        return tupleLength;
    }

    @Test
    public void testReset() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(TEST_FRAME_SIZE);
        appender.reset(buffer);
        assertTrue(appender.getBuffer() == buffer);
        assertTrue(appender.getTupleCount() == 0);
        assertTrue(appender.getContiguousFreeSpace() == TEST_FRAME_SIZE - META_DATA_SIZE);

        int count = TEST_TUPLE_COUNT;
        int deleted = 7;
        buffer = makeAFrame(TEST_FRAME_SIZE, count, deleted);
        int pos = buffer.position();
        appender.reset(buffer);
        assertTrue(appender.getBuffer() == buffer);
        assertTrue(appender.getTupleCount() == count);
        assertTrue(appender.getContiguousFreeSpace() == TEST_FRAME_SIZE - META_DATA_SIZE - count * SLOT_SIZE - pos);
        assertTrue(appender.getTotalFreeSpace() == appender.getContiguousFreeSpace() + deleted);

        int dataOffset = 0;
        for (int i = 0; i < count; i++) {
            dataOffset += assertTupleIsExpected(i, dataOffset, i);
        }
    }

    @Test
    public void testAppend() throws Exception {
        int count = TEST_TUPLE_COUNT;
        ByteBuffer bufferRead = makeAFrame(TEST_FRAME_SIZE, count, 0);
        DeletableFrameTupleAppender accessor = new DeletableFrameTupleAppender(recordDescriptor);
        accessor.reset(bufferRead);
        ByteBuffer bufferWrite = ByteBuffer.allocate(TEST_FRAME_SIZE);
        appender.clear(bufferWrite);
        for (int i = 0; i < accessor.getTupleCount(); i++) {
            appender.append(accessor, i);
        }
        for (int i = 0; i < bufferRead.capacity(); i++) {
            assertEquals(bufferRead.get(i), bufferWrite.get(i));
        }
    }

    @Test
    public void testDelete() throws Exception {
        int count = TEST_TUPLE_COUNT;
        int deleteSpace = 0;
        ByteBuffer buffer = makeAFrame(TEST_FRAME_SIZE, count, deleteSpace);
        appender.reset(buffer);

        int freeSpace = appender.getContiguousFreeSpace();
        for (int i = 0; i < appender.getTupleCount(); i++) {
            deleteSpace += assertDeleteSucceed(i, freeSpace, deleteSpace);
            int innerOffset = deleteSpace;
            for (int j = i + 1; j < appender.getTupleCount(); j++) {
                innerOffset += assertTupleIsExpected(j, innerOffset, j);
            }
        }
    }

    @Test
    public void testResetAfterDelete() throws Exception {
        testDelete();
        appender.reset(appender.getBuffer());
        assertEquals(TEST_FRAME_SIZE - appender.getTupleCount() * SLOT_SIZE - META_DATA_SIZE,
                appender.getTotalFreeSpace());

    }

    int assertDeleteSucceed(int i, int freeSpaceBeforeDelete, int deleteSpace) {
        int startOffset = appender.getTupleStartOffset(i);
        int endOffset = appender.getTupleEndOffset(i);
        int tupleLength = appender.getTupleLength(i);

        appender.delete(i);

        assertEquals(startOffset, appender.getTupleStartOffset(i));
        assertEquals(-endOffset, appender.getTupleEndOffset(i));
        assertEquals(-tupleLength, appender.getTupleLength(i));
        assertEquals(freeSpaceBeforeDelete, appender.getContiguousFreeSpace());
        assertEquals(deleteSpace + tupleLength + freeSpaceBeforeDelete, appender.getTotalFreeSpace());
        return tupleLength;
    }

    @Test
    public void testAppendAndDelete() throws Exception {
        int cap = 1024;
        int count = TEST_TUPLE_COUNT;
        int deleteSpace = 0;
        ByteBuffer buffer = makeAFrame(cap, count, deleteSpace);
        int dataOffset = buffer.position();
        appender.reset(buffer);

        int freeSpace = appender.getContiguousFreeSpace();
        int[] deleteSet = new int[] { 1, 3, 5, 7 };
        for (int i = 0; i < deleteSet.length; i++) {
            deleteSpace += assertDeleteSucceed(deleteSet[i], freeSpace, deleteSpace);
        }

        ByteBuffer bufferRead = makeAFrame(cap, count * 2, 0);
        DeletableFrameTupleAppender accessor = new DeletableFrameTupleAppender(recordDescriptor);
        accessor.reset(bufferRead);

        int[] appendSet = new int[] { 1, 3, 5, 7, 8, 9, 10, 11 };
        for (int i = count; i < accessor.getTupleCount(); i++) {
            int id = appender.append(accessor, i);
            dataOffset += assertTupleIsExpected(id, dataOffset, i);
            assertEquals(appendSet[i - count], id);
        }

        appender.reOrganizeBuffer();
        dataOffset = 0;
        int[] appendOrder = new int[] { 0, 2, 4, 6, 1, 3, 5, 7, 8, 9, 10, 11 };
        int[] stringSize = new int[] { 0, 2, 4, 6, 8, 9, 10, 11, 12, 13, 14, 15 };
        for (int i = 0; i < appender.getTupleCount(); i++) {
            dataOffset += assertTupleIsExpected(appendOrder[i], dataOffset, stringSize[i]);
        }
    }

    @Test
    public void testReOrganizeBuffer() throws Exception {
        int count = TEST_TUPLE_COUNT;
        testDelete();
        appender.reOrganizeBuffer();
        ByteBuffer bufferRead = makeAFrame(TEST_FRAME_SIZE, count, 0);
        DeletableFrameTupleAppender accessor = new DeletableFrameTupleAppender(recordDescriptor);
        accessor.reset(bufferRead);
        for (int i = 0; i < accessor.getTupleCount(); i++) {
            appender.append(accessor, i);
        }
        for (int i = 0; i < bufferRead.capacity(); i++) {
            assertEquals(bufferRead.get(i), appender.getBuffer().get(i));
        }
    }

}
