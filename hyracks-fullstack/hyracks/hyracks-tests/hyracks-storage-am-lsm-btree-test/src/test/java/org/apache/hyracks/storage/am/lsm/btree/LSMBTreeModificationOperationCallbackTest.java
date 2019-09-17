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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.AbstractModificationOperationCallbackTest;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.btree.utils.LSMBTreeUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpOperationTrackerFactory;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.util.trace.ITracer;
import org.junit.Test;

public class LSMBTreeModificationOperationCallbackTest extends AbstractModificationOperationCallbackTest {
    private static final int NUM_TUPLES = 11;

    private final LSMBTreeTestHarness harness;

    public LSMBTreeModificationOperationCallbackTest() {
        super();
        harness = new LSMBTreeTestHarness();
    }

    @Override
    protected void createIndexInstance() throws Exception {
        index = LSMBTreeUtil.createLSMTree(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), SerdeUtils.serdesToTypeTraits(keySerdes),
                SerdeUtils.serdesToComparatorFactories(keySerdes, keySerdes.length), bloomFilterKeyFields,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(),
                NoOpOperationTrackerFactory.INSTANCE.getOperationTracker(null, null), harness.getIOScheduler(),
                harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(), true, null, null, null,
                null, true, harness.getMetadataPageManagerFactory(), false, ITracer.NONE,
                NoOpCompressorDecompressorFactory.INSTANCE);
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

    @Override
    @Test
    public void modificationCallbackTest() throws Exception {
        IndexAccessParameters actx = new IndexAccessParameters(cb, NoOpOperationCallback.INSTANCE);
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(actx);

        for (int j = 0; j < 2; j++) {
            isFoundNull = true;
            for (int i = 0; i < NUM_TUPLES; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                accessor.insert(tuple);
            }

            if (j == 1) {
                ILSMIOOperation flush = accessor.scheduleFlush();
                flush.sync();
                if (flush.getStatus() == LSMIOOperationStatus.FAILURE) {
                    throw HyracksDataException.create(flush.getFailure());
                }
                isFoundNull = true;
            } else {
                isFoundNull = false;
            }

            for (int i = 0; i < NUM_TUPLES; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                accessor.upsert(tuple);
            }

            if (j == 1) {
                accessor.scheduleFlush().sync();
                isFoundNull = true;
            } else {
                isFoundNull = false;
            }

            for (int i = 0; i < NUM_TUPLES; i++) {
                TupleUtils.createIntegerTuple(builder, tuple, i);
                accessor.delete(tuple);
            }
            accessor.scheduleFlush().sync();
        }
    }
}
