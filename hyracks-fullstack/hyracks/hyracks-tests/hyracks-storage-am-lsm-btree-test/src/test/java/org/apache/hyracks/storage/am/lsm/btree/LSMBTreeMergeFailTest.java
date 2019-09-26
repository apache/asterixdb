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

package org.apache.hyracks.storage.am.lsm.btree;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestUtils;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.MergeOperation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class LSMBTreeMergeFailTest {

    private final OrderedIndexTestUtils orderedIndexTestUtils = new OrderedIndexTestUtils();

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    private final TestIoScheduler scheduler = new TestIoScheduler();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Test
    public void testMergeFail() throws Exception {
        ISerializerDeserializer[] fieldSerdes =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        LSMBTreeTestContext ctx = createTestContext(fieldSerdes, 1, BTreeLeafFrameType.REGULAR_NSM, true);
        LSMBTree btree = (LSMBTree) ctx.getIndex();
        btree.create();
        btree.activate();
        ILSMIndexAccessor accessor = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);

        ITupleReference tuple1 = TupleUtils.createIntegerTuple(1, 1, 1);
        accessor.insert(tuple1);
        // flush component
        accessor.scheduleFlush();

        ITupleReference tuple2 = TupleUtils.createIntegerTuple(2, 2, 2);
        accessor.insert(tuple2);
        // flush component
        accessor.scheduleFlush();

        ITupleReference tuple3 = TupleUtils.createIntegerTuple(3, 3, 3);
        accessor.insert(tuple3);
        // flush component
        accessor.scheduleFlush();
        scheduler.modify = true;
        ILSMIOOperation merge = accessor.scheduleMerge(btree.getDiskComponents());
        merge.sync();
        Assert.assertEquals(LSMIOOperationStatus.FAILURE, merge.getStatus());
        scheduler.modify = false;
        accessor.scheduleMerge(btree.getDiskComponents());
        Assert.assertEquals(1, btree.getDiskComponents().size());
        btree.deactivate();
        btree.destroy();
    }

    protected LSMBTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            BTreeLeafFrameType leafType, boolean filtered) throws Exception {
        return LSMBTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, numKeys,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(), harness.getOperationTracker(),
                scheduler, harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                harness.getMetadataPageManagerFactory(), filtered, true, false);
    }

    private class TestIoScheduler implements ILSMIOOperationScheduler {
        boolean modify = false;

        @Override
        public void scheduleOperation(ILSMIOOperation operation) throws HyracksDataException {
            try {
                if (modify) {
                    try {
                        modifyOperation(operation);
                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }
                operation.call();
            } finally {
                operation.complete();
            }
        }

        @Override
        public void completeOperation(ILSMIOOperation operation) throws HyracksDataException {
            // No op
        }

        private void modifyOperation(ILSMIOOperation operation) throws Exception {
            if (!(operation instanceof MergeOperation)) {
                return;
            }
            Field field = MergeOperation.class.getDeclaredField("cursor");
            field.setAccessible(true);
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            LSMBTreeRangeSearchCursor originalCursor = (LSMBTreeRangeSearchCursor) field.get(operation);
            field.set(operation, new TestCursor(originalCursor.getOpCtx()));
        }
    }

    private class TestCursor extends LSMBTreeRangeSearchCursor {
        public TestCursor(ILSMIndexOperationContext opCtx) {
            super(opCtx);
        }

        @Override
        public boolean doHasNext() throws HyracksDataException {
            throw new UnsupportedOperationException();
        }
    }
}
