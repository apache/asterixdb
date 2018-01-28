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

package org.apache.hyracks.dataflow.common.comm.io.largeobject;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.junit.Before;
import org.junit.Test;

public class FrameFixedFieldTupleAppenderTest {

    static final int INPUT_BUFFER_SIZE = 4096;
    static final int TEST_FRAME_SIZE = 256;

    FrameFixedFieldAppender appender;
    static ISerializerDeserializer[] fields = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer(), };
    static RecordDescriptor recordDescriptor = new RecordDescriptor(fields);
    static ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(recordDescriptor.getFieldCount());

    class SequentialDataVerifier implements IFrameWriter {

        private final IFrameTupleAccessor accessor;
        private IFrameTupleAccessor innerAccessor;
        private int tid;

        public SequentialDataVerifier(IFrameTupleAccessor accessor) {
            this.accessor = accessor;
            this.innerAccessor = new FrameTupleAccessor(recordDescriptor);
        }

        @Override
        public void open() throws HyracksDataException {
            this.tid = 0;
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            innerAccessor.reset(buffer);
            for (int i = 0; i < innerAccessor.getTupleCount(); ++i) {
                validate(innerAccessor, i);
            }
        }

        private void validate(IFrameTupleAccessor innerAccessor, int i) {
            assertTrue(tid < accessor.getTupleCount());
            assertEquals(accessor.getTupleLength(tid), innerAccessor.getTupleLength(i));
            assertArrayEquals(
                    Arrays.copyOfRange(accessor.getBuffer().array(), accessor.getTupleStartOffset(tid),
                            accessor.getTupleEndOffset(tid)),
                    Arrays.copyOfRange(innerAccessor.getBuffer().array(), innerAccessor.getTupleStartOffset(i),
                            innerAccessor.getTupleEndOffset(i)));
            tid++;
        }

        @Override
        public void fail() throws HyracksDataException {
            assert false;
        }

        @Override
        public void close() throws HyracksDataException {
            assertEquals(accessor.getTupleCount(), tid);
        }

        @Override
        public void flush() throws HyracksDataException {
        }
    }

    @Before
    public void createAppender() throws HyracksDataException {
        appender = new FrameFixedFieldAppender(fields.length);
        FrameManager manager = new FrameManager(TEST_FRAME_SIZE);
        IFrame frame = new VSizeFrame(manager);
        appender.reset(frame, true);
    }

    private void testProcess(IFrameTupleAccessor accessor) throws HyracksDataException {
        IFrameWriter writer = prepareValidator(accessor);
        writer.open();
        for (int tid = 0; tid < accessor.getTupleCount(); tid++) {
            for (int fid = 0; fid < fields.length; fid++) {
                if (!appender.appendField(accessor, tid, fid)) {
                    appender.write(writer, true);
                    if (!appender.appendField(accessor, tid, fid)) {
                    }
                }
            }
        }
        appender.write(writer, true);
        writer.close();
    }

    @Test
    public void testAppendFieldShouldSucceed() throws HyracksDataException {
        IFrameTupleAccessor accessor = prepareData(DATA_TYPE.NORMAL_RECORD);
        testProcess(accessor);
    }

    @Test
    public void testResetShouldWork() throws HyracksDataException {
        testAppendFieldShouldSucceed();
        appender.reset(new VSizeFrame(new FrameManager(TEST_FRAME_SIZE)), true);
        testAppendFieldShouldSucceed();
    }

    private IFrameWriter prepareValidator(IFrameTupleAccessor accessor) throws HyracksDataException {
        return new SequentialDataVerifier(accessor);
    }

    enum DATA_TYPE {
        NORMAL_RECORD,
        ONE_FIELD_LONG,
        ONE_RECORD_LONG,
    }

    private IFrameTupleAccessor prepareData(DATA_TYPE type) throws HyracksDataException {
        IFrameTupleAccessor accessor = new FrameTupleAccessor(recordDescriptor);
        IFrameTupleAppender appender =
                new FrameTupleAppender(new VSizeFrame(new FrameManager(INPUT_BUFFER_SIZE)), true);
        int i = 0;
        do {
            switch (type) {
                case NORMAL_RECORD:
                    makeATuple(tupleBuilder, i++);
                    break;
                case ONE_FIELD_LONG:
                    makeASizeUpTuple(tupleBuilder, i++);
                    break;
                case ONE_RECORD_LONG:
                    makeABigObjectTuple(tupleBuilder, i++);
                    break;
            }
        } while (appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                tupleBuilder.getSize()));
        accessor.reset(appender.getBuffer());
        return accessor;
    }

    private void makeATuple(ArrayTupleBuilder tupleBuilder, int i) throws HyracksDataException {
        tupleBuilder.reset();
        tupleBuilder.addField(fields[0], i);
        tupleBuilder.addField(fields[1], String.valueOf(i));
        tupleBuilder.addField(fields[2], -i);
        tupleBuilder.addField(fields[3], String.valueOf(-i));
    }

    private String makeALongString(int length, char ch) {
        char[] array = new char[length];
        Arrays.fill(array, ch);
        return new String(array);
    }

    private void makeASizeUpTuple(ArrayTupleBuilder tupleBuilder, int i) throws HyracksDataException {
        tupleBuilder.reset();
        tupleBuilder.addField(fields[0], i);
        tupleBuilder.addField(fields[1], makeALongString(Math.min(Math.abs(1 << i), INPUT_BUFFER_SIZE), (char) i));
        tupleBuilder.addField(fields[2], -i);
        tupleBuilder.addField(fields[3], String.valueOf(-i));
    }

    private void makeABigObjectTuple(ArrayTupleBuilder tupleBuilder, int i) throws HyracksDataException {
        tupleBuilder.reset();
        tupleBuilder.addField(fields[0], i);
        tupleBuilder.addField(fields[1], makeALongString(Math.min(i * 20, TEST_FRAME_SIZE), (char) i));
        tupleBuilder.addField(fields[2], -i);
        tupleBuilder.addField(fields[3], makeALongString(Math.min(i * 20, TEST_FRAME_SIZE), (char) i));
    }

    @Test
    public void testAppendLargeFieldShouldSucceed() throws HyracksDataException {
        IFrameTupleAccessor accessor = prepareData(DATA_TYPE.ONE_FIELD_LONG);
        testProcess(accessor);
    }

    @Test
    public void testAppendSmallFieldButLargeObjectWithShouldSucceed() throws HyracksDataException {
        IFrameTupleAccessor accessor = prepareData(DATA_TYPE.ONE_RECORD_LONG);
        testProcess(accessor);
    }
}
