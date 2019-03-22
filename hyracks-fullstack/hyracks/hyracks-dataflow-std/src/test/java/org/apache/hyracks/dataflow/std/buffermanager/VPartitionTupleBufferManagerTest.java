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
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.util.IntSerDeUtils;
import org.junit.Before;
import org.junit.Test;

public class VPartitionTupleBufferManagerTest extends AbstractTupleMemoryManagerTest {

    VPartitionTupleBufferManager bufferManager;
    final int PARTITION = 4;

    @Before
    public void setup() throws HyracksDataException {
        bufferManager = new VPartitionTupleBufferManager(Common.commonFrameManager,
                VPartitionTupleBufferManager.NO_CONSTRAIN, PARTITION, Common.BUDGET);
    }

    @Test
    public void testGetNumPartitions() throws Exception {
        assertEquals(PARTITION, bufferManager.getNumPartitions());
    }

    @Test
    public void testGetNumTuples() throws Exception {
        testNumTuplesAndSizeIsZero();
    }

    @Test
    public void testInsertToFull() throws HyracksDataException {
        Map<Integer, Integer> inMap = prepareFixedSizeTuples(10, 0, 0);
        for (int pid = 0; pid < PARTITION; pid++) {
            assertInsertOnePartitionToFull(pid, inMap);
            bufferManager.reset();
        }
    }

    @Test
    public void testInsertClearSequence() throws HyracksDataException {
        Map<Integer, Integer> inMap = prepareFixedSizeTuples(10, 0, 0);
        for (int pid = 0; pid < PARTITION; pid++) {
            assertInsertOnePartitionToFull(pid, inMap);
            bufferManager.reset();
        }
    }

    private void assertInsertOnePartitionToFull(int pid, Map<Integer, Integer> inMap) throws HyracksDataException {
        testNumTuplesAndSizeIsZero();

        Map<TuplePointer, Integer> outMap = testInsertOnePartitionToFull(pid);
        assertEquals(outMap.size(), bufferManager.getNumTuples(pid));
        assertEquals(Common.BUDGET, bufferManager.getPhysicalSize(pid));
        testCanNotInsertToAnyPartitions();
        assertEachTupleInFTAIsInBuffer(inMap, outMap);

    }

    private void testCanNotInsertToAnyPartitions() throws HyracksDataException {
        for (int i = 0; i < PARTITION; i++) {
            assertFalse(bufferManager.insertTuple(i, tupleBuilder.getByteArray(), tupleBuilder.getFieldEndOffsets(), 0,
                    tupleBuilder.getSize(), null));
        }
    }

    private Map<TuplePointer, Integer> testInsertOnePartitionToFull(int idpart) throws HyracksDataException {
        Map<TuplePointer, Integer> tuplePointerIntegerMap = new HashMap<>();

        for (int i = 0; i < inFTA.getTupleCount(); i++) {
            TuplePointer tuplePointer = new TuplePointer();
            copyDataToTupleBuilder(inFTA, i, tupleBuilder);
            if (!bufferManager.insertTuple(idpart, tupleBuilder.getByteArray(), tupleBuilder.getFieldEndOffsets(), 0,
                    tupleBuilder.getSize(), tuplePointer)) {
                assert false;
            }
            tuplePointerIntegerMap.put(tuplePointer,
                    IntSerDeUtils.getInt(inFTA.getBuffer().array(), inFTA.getAbsoluteFieldStartOffset(i, 0)));
        }
        return tuplePointerIntegerMap;

    }

    private static void copyDataToTupleBuilder(FrameTupleAccessor inFTA, int tid, ArrayTupleBuilder tupleBuilder)
            throws HyracksDataException {
        tupleBuilder.reset();
        for (int fid = 0; fid < inFTA.getFieldCount(); fid++) {
            tupleBuilder.addField(inFTA.getBuffer().array(), inFTA.getAbsoluteFieldStartOffset(tid, fid),
                    inFTA.getFieldLength(tid, fid));
        }
    }

    private void testNumTuplesAndSizeIsZero() {
        for (int i = 0; i < bufferManager.getNumPartitions(); ++i) {
            assertEquals(0, bufferManager.getNumTuples(i));
            assertEquals(0, bufferManager.getPhysicalSize(0));
        }
    }

    @Test
    public void testClearPartition() throws Exception {

        Map<Integer, Integer> inMap = prepareFixedSizeTuples(10, 0, 0);
        for (int pid = 0; pid < PARTITION; pid++) {
            assertInsertOnePartitionToFull(pid, inMap);
            assertClearFullPartitionIsTheSameAsReset();
        }
    }

    private void assertClearFullPartitionIsTheSameAsReset() throws HyracksDataException {
        for (int i = 0; i < PARTITION; i++) {
            bufferManager.clearPartition(i);
        }
        testNumTuplesAndSizeIsZero();
    }

    @Test
    public void testClose() throws Exception {
        testInsertToFull();
        bufferManager.close();
    }

    @Override
    ITuplePointerAccessor getTuplePointerAccessor() {
        return bufferManager.getTuplePointerAccessor(recordDescriptor);
    }
}
