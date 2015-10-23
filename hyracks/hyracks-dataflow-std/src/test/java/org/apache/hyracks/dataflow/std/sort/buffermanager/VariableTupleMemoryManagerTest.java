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

package org.apache.hyracks.dataflow.std.sort.buffermanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.util.IntSerDeUtils;
import org.apache.hyracks.dataflow.std.sort.Utility;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class VariableTupleMemoryManagerTest {
    ISerializerDeserializer[] fieldsSerDer = new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };
    RecordDescriptor recordDescriptor = new RecordDescriptor(fieldsSerDer);
    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(recordDescriptor.getFieldCount());
    VariableTupleMemoryManager tupleMemoryManager;
    FrameTupleAccessor inFTA = new FrameTupleAccessor(recordDescriptor);
    Random random = new Random(System.currentTimeMillis());

    @Before
    public void setup() {
        VariableFramePool framePool = new VariableFramePool(Common.commonFrameManager, Common.BUDGET);
        tupleMemoryManager = new VariableTupleMemoryManager(framePool, recordDescriptor);
    }

    @Test
    public void testInsertTupleToMemoryManager() throws HyracksDataException {
        int iTuplePerFrame = 3;
        Map<Integer, Integer> mapPrepare = prepareFixedSizeTuples(iTuplePerFrame);
        Map<TuplePointer, Integer> mapInserted = insertInFTAToBufferShouldAllSuccess();
        assertEachTupleInFTAIsInBuffer(mapPrepare, mapInserted);
    }

    @Test
    public void testReset() throws HyracksDataException {
        testInsertVariableSizeTupleToMemoryManager();
        tupleMemoryManager.reset();
        testInsertTupleToMemoryManager();
        tupleMemoryManager.reset();
        testInsertVariableSizeTupleToMemoryManager();
    }

    @Test
    public void testDeleteTupleInMemoryManager() throws HyracksDataException {
        int iTuplePerFrame = 3;
        Map<Integer, Integer> map = prepareFixedSizeTuples(iTuplePerFrame);
        Map<TuplePointer, Integer> mapInserted = insertInFTAToBufferShouldAllSuccess();
        deleteRandomSelectedTuples(map, mapInserted, 1);
        assertEachTupleInFTAIsInBuffer(map, mapInserted);
    }

    @Test
    public void testReOrganizeSpace() throws HyracksDataException {
        int iTuplePerFrame = 3;
        Map<Integer, Integer> map = prepareFixedSizeTuples(iTuplePerFrame);
        Map<Integer, Integer> copyMap = new HashMap<>(map);
        Map<TuplePointer, Integer> mapInserted = insertInFTAToBufferShouldAllSuccess();
        ByteBuffer buffer = deleteRandomSelectedTuples(map, mapInserted, map.size() / 2);
        inFTA.reset(buffer);
        Map<TuplePointer, Integer> mapInserted2 = insertInFTAToBufferShouldAllSuccess();
        Map<TuplePointer, Integer> mergedMap = new HashMap<>(mapInserted);
        mergedMap.putAll(mapInserted2);
        assertEachTupleInFTAIsInBuffer(copyMap, mergedMap);
    }

    @Test
    public void testReOrganizeVariableSizeTuple() throws HyracksDataException {
        Map<Integer, Integer> map = prepareVariableSizeTuples();
        Map<TuplePointer, Integer> mapInserted = insertInFTAToBufferCouldFailForLargerTuples(map);
        Map<Integer, Integer> copyMap = new HashMap<>(map);

        ByteBuffer buffer = deleteRandomSelectedTuples(map, mapInserted, map.size() / 2);
        inFTA.reset(buffer);

        Map<TuplePointer, Integer> mapInserted2 = insertInFTAToBufferCouldFailForLargerTuples(copyMap);
        Map<TuplePointer, Integer> mergedMap = new HashMap<>(mapInserted);
        mergedMap.putAll(mapInserted2);

        assertEachTupleInFTAIsInBuffer(copyMap, mergedMap);
    }

    @Test
    public void testInsertVariableSizeTupleToMemoryManager() throws HyracksDataException {
        Map<Integer, Integer> map = prepareVariableSizeTuples();
        Map<TuplePointer, Integer> mapInserted = insertInFTAToBufferCouldFailForLargerTuples(map);
        assertEachTupleInFTAIsInBuffer(map, mapInserted);
    }

    private void assertEachTupleInFTAIsInBuffer(Map<Integer, Integer> map, Map<TuplePointer, Integer> mapInserted) {
        ITupleBufferAccessor accessor = tupleMemoryManager.getTupleAccessor();
        for (Map.Entry<TuplePointer, Integer> entry : mapInserted.entrySet()) {
            accessor.reset(entry.getKey());
            int dataLength = map.get(entry.getValue());
            assertEquals((int) entry.getValue(),
                    IntSerDeUtils.getInt(accessor.getTupleBuffer().array(), accessor.getAbsFieldStartOffset(0)));
            assertEquals(dataLength, accessor.getTupleLength());
        }
        assertEquals(map.size(), mapInserted.size());
    }

    private Map<Integer, Integer> prepareFixedSizeTuples(int tuplePerFrame) throws HyracksDataException {
        Map<Integer, Integer> dataSet = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(Common.BUDGET);
        FixedSizeFrame frame = new FixedSizeFrame(buffer);
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(frame, true);

        int sizePerTuple = (Common.MIN_FRAME_SIZE - 1 - 4 - tuplePerFrame * 4 - 4) / tuplePerFrame;
        int sizeChar = sizePerTuple - fieldsSerDer.length * 4 - 4 - 4;
        assert (sizeChar > 0);
        for (int i = 0; i < Common.NUM_MIN_FRAME * tuplePerFrame; i++) {
            tupleBuilder.reset();
            tupleBuilder.addField(fieldsSerDer[0], i);
            tupleBuilder.addField(fieldsSerDer[1], Utility.repeatString('a', sizeChar));
            assertTrue(appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize()));
            dataSet.put(i, tupleBuilder.getSize() + tupleBuilder.getFieldEndOffsets().length * 4);
        }
        inFTA.reset(buffer);
        return dataSet;
    }

    private Map<Integer, Integer> prepareVariableSizeTuples() throws HyracksDataException {
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

    private Map<TuplePointer, Integer> insertInFTAToBufferShouldAllSuccess() throws HyracksDataException {
        Map<TuplePointer, Integer> tuplePointerIntegerMap = new HashMap<>();
        for (int i = 0; i < inFTA.getTupleCount(); i++) {
            TuplePointer tuplePointer = new TuplePointer();
            assertTrue(tupleMemoryManager.insertTuple(inFTA, i, tuplePointer));
            tuplePointerIntegerMap.put(tuplePointer,
                    IntSerDeUtils.getInt(inFTA.getBuffer().array(), inFTA.getAbsoluteFieldStartOffset(i, 0)));
        }
        return tuplePointerIntegerMap;
    }

    private Map<TuplePointer, Integer> insertInFTAToBufferCouldFailForLargerTuples(Map<Integer, Integer> map)
            throws HyracksDataException {
        Map<TuplePointer, Integer> tuplePointerIdMap = new HashMap<>();
        int i = 0;
        for (; i < inFTA.getTupleCount(); i++) {
            TuplePointer tuplePointer = new TuplePointer();
            if (!tupleMemoryManager.insertTuple(inFTA, i, tuplePointer)) {
                break;
            }
            tuplePointerIdMap.put(tuplePointer,
                    IntSerDeUtils.getInt(inFTA.getBuffer().array(), inFTA.getAbsoluteFieldStartOffset(i, 0)));
        }
        for (; i < inFTA.getTupleCount(); i++) {
            map.remove(IntSerDeUtils.getInt(inFTA.getBuffer().array(), inFTA.getAbsoluteFieldStartOffset(i, 0)));
        }
        return tuplePointerIdMap;
    }

    private ByteBuffer deleteRandomSelectedTuples(Map<Integer, Integer> map, Map<TuplePointer, Integer> mapInserted,
            int minNumOfRecordTobeDeleted)
            throws HyracksDataException {
        ByteBuffer buffer = ByteBuffer.allocate(Common.BUDGET);
        FixedSizeFrame frame = new FixedSizeFrame(buffer);
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(frame, true);

        assert (minNumOfRecordTobeDeleted < mapInserted.size());
        int countDeleted = minNumOfRecordTobeDeleted + random.nextInt(mapInserted.size() - minNumOfRecordTobeDeleted);

        ITupleBufferAccessor accessor = tupleMemoryManager.getTupleAccessor();
        for (int i = 0; i < countDeleted; i++) {
            Iterator<Map.Entry<TuplePointer, Integer>> iter = mapInserted.entrySet().iterator();
            assert (iter.hasNext());
            Map.Entry<TuplePointer, Integer> pair = iter.next();
            accessor.reset(pair.getKey());
            appender.append(accessor.getTupleBuffer().array(), accessor.getTupleStartOffset(),
                    accessor.getTupleLength());
            map.remove(pair.getValue());
            tupleMemoryManager.deleteTuple(pair.getKey());
            iter.remove();
        }
        return buffer;
    }
}