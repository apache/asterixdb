/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.btree;

import org.junit.Test;

import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.AbstractModificationOperationCallbackTest;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpOperationTrackerProvider;

public class LSMBTreeModificationOperationCallbackTest extends AbstractModificationOperationCallbackTest {
    private static final int NUM_TUPLES = 11;

    private final LSMBTreeTestHarness harness;
    private final BlockingIOOperationCallbackWrapper ioOpCallback;

    public LSMBTreeModificationOperationCallbackTest() {
        super();
        this.ioOpCallback = new BlockingIOOperationCallbackWrapper(NoOpIOOperationCallback.INSTANCE);
        harness = new LSMBTreeTestHarness();
    }

    @Override
    protected void createIndexInstance() throws Exception {
        index = LSMBTreeUtils.createLSMTree(harness.getVirtualBufferCaches(), harness.getFileReference(),
                harness.getDiskBufferCache(), harness.getDiskFileMapProvider(),
                SerdeUtils.serdesToTypeTraits(keySerdes),
                SerdeUtils.serdesToComparatorFactories(keySerdes, keySerdes.length), bloomFilterKeyFields,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(),
                NoOpOperationTrackerProvider.INSTANCE.getOperationTracker(null), harness.getIOScheduler(),
                harness.getIOOperationCallback());
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        super.setup();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        harness.tearDown();
    }

    @Test
    public void modificationCallbackTest() throws Exception {
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(cb, NoOpOperationCallback.INSTANCE);

        for (int j = 0; j < 2; j++) {
            isFoundNull = true;
            for (int i = 0; i < NUM_TUPLES; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                accessor.insert(tuple);
            }

            if (j == 1) {
                accessor.scheduleFlush(ioOpCallback);
                ioOpCallback.waitForIO();
                isFoundNull = true;
            } else {
                isFoundNull = false;
            }

            for (int i = 0; i < NUM_TUPLES; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                accessor.upsert(tuple);
            }

            if (j == 1) {
                accessor.scheduleFlush(ioOpCallback);
                ioOpCallback.waitForIO();
                isFoundNull = true;
            } else {
                isFoundNull = false;
            }

            for (int i = 0; i < NUM_TUPLES; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                accessor.delete(tuple);
            }

            accessor.scheduleFlush(ioOpCallback);
            ioOpCallback.waitForIO();
        }
    }
}
