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

package org.apache.hyracks.dataflow.std.buffermanager;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.std.sort.Utility;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.util.IntSerDeUtils;

public abstract class AbstractTupleMemoryManagerTest {
    ISerializerDeserializer[] fieldsSerDer = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer() };
    RecordDescriptor recordDescriptor = new RecordDescriptor(fieldsSerDer);
    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(recordDescriptor.getFieldCount());
    FrameTupleAccessor inFTA = new FrameTupleAccessor(recordDescriptor);
    Random random = new Random(System.currentTimeMillis());

    abstract ITuplePointerAccessor getTuplePointerAccessor();

    protected void assertEachTupleInFTAIsInBuffer(Map<Integer, Integer> map, Map<TuplePointer, Integer> mapInserted) {
        ITuplePointerAccessor accessor = getTuplePointerAccessor();
        mapInserted.forEach((key, value) -> {
            accessor.reset(key);
            int dataLength = map.get(value);
            assertEquals((int) value,
                    IntSerDeUtils.getInt(accessor.getBuffer().array(), accessor.getAbsFieldStartOffset(0)));
            assertEquals(dataLength, accessor.getTupleLength());
        });
        assertEquals(map.size(), mapInserted.size());
    }

    protected Map<Integer, Integer> prepareFixedSizeTuples(int tuplePerFrame, int extraMetaBytePerFrame,
            int extraMetaBytePerRecord) throws HyracksDataException {
        Map<Integer, Integer> dataSet = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(Common.BUDGET);
        FixedSizeFrame frame = new FixedSizeFrame(buffer);
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(frame, true);

        int sizePerTuple = (Common.MIN_FRAME_SIZE - 1 - tuplePerFrame * 4 - 4 - extraMetaBytePerFrame) / tuplePerFrame;
        int sizeChar = sizePerTuple - extraMetaBytePerRecord - fieldsSerDer.length * 4 - 4 - 2; //2byte to write str length
        assert (sizeChar > 0);
        for (int i = 0; i < Common.NUM_MIN_FRAME * tuplePerFrame; i++) {
            tupleBuilder.reset();
            tupleBuilder.addField(fieldsSerDer[0], i);
            tupleBuilder.addField(fieldsSerDer[1], Utility.repeatString('a', sizeChar));
            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                assert false;
            }
            dataSet.put(i, tupleBuilder.getSize() + tupleBuilder.getFieldEndOffsets().length * 4);
        }
        inFTA.reset(buffer);
        return dataSet;
    }

    protected Map<Integer, Integer> prepareVariableSizeTuples() throws HyracksDataException {
        Map<Integer, Integer> dataSet = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(Common.BUDGET);
        FixedSizeFrame frame = new FixedSizeFrame(buffer);
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(frame, true);

        for (int i = 0; true; i++) {
            tupleBuilder.reset();
            tupleBuilder.addField(fieldsSerDer[0], i);
            tupleBuilder.addField(fieldsSerDer[1], Utility.repeatString('a', i));
            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                break;
            }
            dataSet.put(i, tupleBuilder.getSize() + tupleBuilder.getFieldEndOffsets().length * 4);
        }
        inFTA.reset(buffer);
        return dataSet;
    }

}
