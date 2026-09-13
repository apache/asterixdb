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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.sort.Utility;
import org.apache.hyracks.dataflow.std.sort.util.DeletableFrameTupleAppender;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.util.IntSerDeUtils;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Before;
import org.junit.Test;

public class VariableTupleMemoryManagerTest extends AbstractTupleMemoryManagerTest {
    VariableDeletableTupleMemoryManager tupleMemoryManager;
    final int EXTRA_BYTES_FOR_DELETABLE_FRAME = 4;

    @Before
    public void setup() {
        VariableFramePool framePool = new VariableFramePool(Common.commonFrameManager, Common.BUDGET);
        tupleMemoryManager = new VariableDeletableTupleMemoryManager(framePool, recordDescriptor);
    }

    @Test
    public void testInsertTupleToMemoryManager() throws HyracksDataException {
        int iTuplePerFrame = 3;
        Map<Integer, Integer> mapPrepare = prepareFixedSizeTuples(iTuplePerFrame, EXTRA_BYTES_FOR_DELETABLE_FRAME, 0);
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
        Map<Integer, Integer> mapPrepare = prepareFixedSizeTuples(iTuplePerFrame, EXTRA_BYTES_FOR_DELETABLE_FRAME, 0);
        Map<TuplePointer, Integer> mapInserted = insertInFTAToBufferShouldAllSuccess();
        deleteRandomSelectedTuples(mapPrepare, mapInserted, 1);
        assertEachTupleInFTAIsInBuffer(mapPrepare, mapInserted);
    }

    @Test
    public void testReOrganizeSpace() throws HyracksDataException {
        int iTuplePerFrame = 3;
        Map<Integer, Integer> mapPrepare = prepareFixedSizeTuples(iTuplePerFrame, EXTRA_BYTES_FOR_DELETABLE_FRAME, 0);
        Map<Integer, Integer> copyMap = new HashMap<>(mapPrepare);
        Map<TuplePointer, Integer> mapInserted = insertInFTAToBufferShouldAllSuccess();
        ByteBuffer buffer = deleteRandomSelectedTuples(mapPrepare, mapInserted, mapPrepare.size() / 2);
        inFTA.reset(buffer);
        //The deletable frame buffer will keep the deleted slot untouched, which will take more space.
        // the reason is to not reuse the same TuplePointer outside.
        Map<TuplePointer, Integer> mapInserted2 = insertInFTAToBufferMayNotAllSuccess();
        assertTrue(mapInserted2.size() > 0);
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

    /**
     * A tuple whose size leaves only the frame's own trailing bookkeeping unused must still be
     * stored intact. Sizing a fresh frame from the tuple alone leaves it up to
     * {@link DeletableFrameTupleAppender#FRAME_META_SIZE} bytes short, and the appender then writes
     * the tuple's end offset over the tail of the tuple it has just copied in -- silently changing
     * the last bytes of the last field. Sweeping the tuple size over a whole frame's worth of
     * lengths covers every such boundary regardless of the frame size.
     */
    @Test
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    public void testTupleEndingAtFrameBoundaryIsStoredIntact() throws HyracksDataException {
        for (int charCount = 0; charCount < Common.MIN_FRAME_SIZE; charCount++) {
            tupleMemoryManager.reset();
            ByteBuffer buffer = ByteBuffer.allocate(Common.BUDGET);
            FrameTupleAppender appender = new FrameTupleAppender();
            appender.reset(new FixedSizeFrame(buffer), true);
            tupleBuilder.reset();
            tupleBuilder.addField(fieldsSerDer[0], charCount);
            tupleBuilder.addField(fieldsSerDer[1], Utility.repeatString('a', charCount));
            assertTrue(appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize()));
            inFTA.reset(buffer);

            TuplePointer tuplePointer = new TuplePointer();
            assertTrue("insert failed for a tuple of " + charCount + " chars",
                    tupleMemoryManager.insertTuple(inFTA, 0, tuplePointer));

            ITuplePointerAccessor accessor = tupleMemoryManager.createTuplePointerAccessor();
            accessor.reset(tuplePointer);
            byte[] expected = Arrays.copyOfRange(inFTA.getBuffer().array(), inFTA.getTupleStartOffset(0),
                    inFTA.getTupleEndOffset(0));
            byte[] actual = Arrays.copyOfRange(accessor.getBuffer().array(), accessor.getTupleStartOffset(),
                    accessor.getTupleStartOffset() + accessor.getTupleLength());
            assertArrayEquals("tuple of " + charCount + " chars was altered in the buffer", expected, actual);
        }
    }

    @Override
    ITuplePointerAccessor getTuplePointerAccessor() {
        return tupleMemoryManager.createTuplePointerAccessor();
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

    private Map<TuplePointer, Integer> insertInFTAToBufferMayNotAllSuccess() throws HyracksDataException {
        Map<TuplePointer, Integer> tuplePointerIntegerMap = new HashMap<>();
        for (int i = 0; i < inFTA.getTupleCount(); i++) {
            TuplePointer tuplePointer = new TuplePointer();
            if (!tupleMemoryManager.insertTuple(inFTA, i, tuplePointer)) {
                break;
            }
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
            int minNumOfRecordTobeDeleted) throws HyracksDataException {
        ByteBuffer buffer = ByteBuffer.allocate(Common.BUDGET);
        FixedSizeFrame frame = new FixedSizeFrame(buffer);
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(frame, true);

        assert (minNumOfRecordTobeDeleted < mapInserted.size());
        int countDeleted = minNumOfRecordTobeDeleted + random.nextInt(mapInserted.size() - minNumOfRecordTobeDeleted);

        ITuplePointerAccessor accessor = tupleMemoryManager.createTuplePointerAccessor();
        for (int i = 0; i < countDeleted; i++) {
            Iterator<Map.Entry<TuplePointer, Integer>> iter = mapInserted.entrySet().iterator();
            assert (iter.hasNext());
            Map.Entry<TuplePointer, Integer> pair = iter.next();
            accessor.reset(pair.getKey());
            appender.append(accessor.getBuffer().array(), accessor.getTupleStartOffset(), accessor.getTupleLength());
            map.remove(pair.getValue());
            tupleMemoryManager.deleteTuple(pair.getKey());
            iter.remove();
        }
        return buffer;
    }
}
