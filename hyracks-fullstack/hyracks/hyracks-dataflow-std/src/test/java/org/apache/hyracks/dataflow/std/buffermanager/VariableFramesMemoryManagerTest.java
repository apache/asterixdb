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

import static org.apache.hyracks.dataflow.std.buffermanager.Common.BUDGET;
import static org.apache.hyracks.dataflow.std.buffermanager.Common.MIN_FRAME_SIZE;
import static org.apache.hyracks.dataflow.std.buffermanager.Common.NUM_MIN_FRAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.junit.Before;
import org.junit.Test;

public class VariableFramesMemoryManagerTest {
    VariableFrameMemoryManager framesMemoryManager;
    FrameTupleAccessor fta;
    Random random;
    List<IFrame> frameList;

    @Before
    public void setUp() throws Exception {
        VariableFramePool framePool = new VariableFramePool(Common.commonFrameManager, BUDGET);
        FrameFreeSlotLastFit policy = new FrameFreeSlotLastFit(NUM_MIN_FRAME);
        framesMemoryManager = new VariableFrameMemoryManager(framePool, policy);
        RecordDescriptor recordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] { null });
        fta = new FrameTupleAccessor(recordDescriptor);
        random = new Random(System.currentTimeMillis());
        frameList = new ArrayList<>();
    }

    @Test
    public void testNormalIncomingFrames() throws HyracksDataException {
        HashMap<Integer, Integer> tupleSet = prepareTuples();
        for (IFrame frame : frameList) {
            assertTrue(framesMemoryManager.insertFrame(frame.getBuffer()) >= 0);
        }
        assertEquals(NUM_MIN_FRAME, framesMemoryManager.getNumFrames());
        assertEveryTupleInFTAIsInFrameMemoryManager(tupleSet, framesMemoryManager);
    }

    @Test
    public void testRandomTuplesAreAllStoredInBuffer() throws HyracksDataException {
        Map<Integer, Integer> tupleSet = prepareRandomTuples();
        for (IFrame frame : frameList) {
            if (framesMemoryManager.insertFrame(frame.getBuffer()) < 0) {
                fta.reset(frame.getBuffer());
                for (int i = 0; i < fta.getTupleCount(); ++i) {
                    int id = parseTuple(fta.getBuffer(),
                            fta.getTupleStartOffset(i) + fta.getFieldStartOffset(i, 0) + fta.getFieldSlotsLength());
                    tupleSet.remove(id);
                    //                    System.out.println(
                    //                            "can't appended id:" + id + ",frameSize:" + frame.getInitialFrameSize());
                }
            }
        }
        assertEveryTupleInFTAIsInFrameMemoryManager(tupleSet, framesMemoryManager);
        framesMemoryManager.reset();
    }

    @Test
    public void testResetShouldWork() throws HyracksDataException {
        testNormalIncomingFrames();
        framesMemoryManager.reset();
        testRandomTuplesAreAllStoredInBuffer();
        framesMemoryManager.reset();
        testRandomTuplesAreAllStoredInBuffer();
    }

    @Test
    public void testCloseShouldAlsoWork() throws HyracksDataException {
        testRandomTuplesAreAllStoredInBuffer();
        framesMemoryManager.close();
        testRandomTuplesAreAllStoredInBuffer();
        framesMemoryManager.close();
        testRandomTuplesAreAllStoredInBuffer();
    }

    private HashMap<Integer, Integer> prepareRandomTuples() throws HyracksDataException {
        frameList.clear();
        HashMap<Integer, Integer> set = new HashMap<>(NUM_MIN_FRAME);
        int[] fieldSlot = { 0 };
        int id = 0;
        int size = 0;
        while (size < BUDGET) {
            int tupleLength = random.nextInt(BUDGET / 3) + 4;
            IFrame frame = new FixedSizeFrame(Common.commonFrameManager
                    .allocateFrame(FrameHelper.calcAlignedFrameSizeToStore(1, tupleLength, MIN_FRAME_SIZE)));
            IFrameTupleAppender appender = new FrameTupleAppender();
            appender.reset(frame, true);
            //            System.out.println("id:" + id + ",frameSize:" + frame.getInitialFrameSize() / MIN_FRAME_SIZE);
            ByteBuffer buffer = ByteBuffer.allocate(tupleLength);
            buffer.putInt(0, id);
            assertTrue(appender.append(fieldSlot, buffer.array(), 0, buffer.capacity()));
            set.put(id++, tupleLength);
            size += frame.getFrameSize();
            frameList.add(frame);
        }
        return set;
    }

    private HashMap<Integer, Integer> prepareTuples() throws HyracksDataException {
        frameList.clear();
        HashMap<Integer, Integer> set = new HashMap<>(NUM_MIN_FRAME);
        for (int i = 0; i < NUM_MIN_FRAME; ++i) {
            IFrame frame = new FixedSizeFrame(Common.commonFrameManager.allocateFrame(MIN_FRAME_SIZE));
            IFrameTupleAppender appender = new FrameTupleAppender();
            appender.reset(frame, true);

            int[] fieldSlot = { 0 };
            ByteBuffer buffer = ByteBuffer.allocate(MIN_FRAME_SIZE / 2);
            buffer.putInt(0, i);
            appender.append(fieldSlot, buffer.array(), 0, buffer.capacity());
            set.put(i, buffer.capacity());
            frameList.add(frame);
        }
        return set;
    }

    private void assertEveryTupleInFTAIsInFrameMemoryManager(Map<Integer, Integer> tupleSet,
            VariableFrameMemoryManager framesMemoryManager) {
        BufferInfo info = new BufferInfo(null, -1, -1);
        for (int i = 0; i < framesMemoryManager.getNumFrames(); ++i) {
            framesMemoryManager.getFrame(i, info);
            fta.reset(info.getBuffer(), info.getStartOffset(), info.getLength());
            for (int t = 0; t < fta.getTupleCount(); t++) {
                int id = parseTuple(fta.getBuffer(),
                        fta.getTupleStartOffset(t) + fta.getFieldSlotsLength() + fta.getFieldStartOffset(t, 0));
                //                System.out.println("frameid:" + i + ",tuple:" + t + ",has id:" + id + ",length:" +
                //                        (fta.getTupleEndOffset(t) - fta.getTupleStartOffset(t) - fta.getFieldSlotsLength()));
                assertTrue(tupleSet.remove(id) == fta.getTupleEndOffset(t) - fta.getTupleStartOffset(t)
                        - fta.getFieldSlotsLength());
            }
        }
        assertTrue(tupleSet.isEmpty());
    }

    private int parseTuple(ByteBuffer buffer, int fieldStartOffset) {
        return buffer.getInt(fieldStartOffset);
    }

}
