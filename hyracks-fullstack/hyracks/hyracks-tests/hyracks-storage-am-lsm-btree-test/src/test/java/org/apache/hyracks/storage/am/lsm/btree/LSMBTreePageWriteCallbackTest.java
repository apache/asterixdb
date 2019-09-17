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

import java.util.Random;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestDriver;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestUtils;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexPageWriteCallback;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

@SuppressWarnings("rawtypes")
public class LSMBTreePageWriteCallbackTest extends OrderedIndexTestDriver {

    private final OrderedIndexTestUtils orderedIndexTestUtils;

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    private final int PAGES_PER_FORCE = 16;

    private LSMIndexPageWriteCallback lastCallback = null;

    private final ILSMPageWriteCallbackFactory pageWriteCallbackFactory = new ILSMPageWriteCallbackFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public void initialize(INCServiceContext ncCtx, IResource resource) {
            // no op
        }

        @Override
        public IPageWriteCallback createPageWriteCallback() throws HyracksDataException {
            lastCallback = new LSMIndexPageWriteCallback(PAGES_PER_FORCE);
            return lastCallback;
        }
    };

    public LSMBTreePageWriteCallbackTest() {
        super(LSMBTreeTestHarness.LEAF_FRAMES_TO_TEST);
        this.orderedIndexTestUtils = new OrderedIndexTestUtils();

    }

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected OrderedIndexTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            BTreeLeafFrameType leafType, boolean filtered) throws Exception {
        return LSMBTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, numKeys,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(), pageWriteCallbackFactory,
                harness.getMetadataPageManagerFactory(), false, true, false);
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }

    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, BTreeLeafFrameType leafType,
            ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey,
            ITupleReference prefixHighKey) throws Exception {
        OrderedIndexTestContext ctx = createTestContext(fieldSerdes, numKeys, leafType, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();
        // Start off with one tree bulk loaded.
        // We assume all fieldSerdes are of the same type. Check the first one
        // to determine which field types to generate.
        if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
            orderedIndexTestUtils.bulkLoadIntTuples(ctx, numTuplesToInsert, getRandom());
        } else if (fieldSerdes[0] instanceof UTF8StringSerializerDeserializer) {
            orderedIndexTestUtils.bulkLoadStringTuples(ctx, numTuplesToInsert, getRandom());
        }

        int maxTreesToMerge = AccessMethodTestsConfig.LSM_BTREE_MAX_TREES_TO_MERGE;
        for (int i = 0; i < maxTreesToMerge; i++) {
            for (int j = 0; j < i; j++) {
                if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
                    orderedIndexTestUtils.insertIntTuples(ctx, numTuplesToInsert, getRandom());
                    // Deactivate and the re-activate the index to force it flush its in memory component
                    ctx.getIndex().deactivate();
                    ctx.getIndex().activate();
                } else if (fieldSerdes[0] instanceof UTF8StringSerializerDeserializer) {
                    orderedIndexTestUtils.insertStringTuples(ctx, numTuplesToInsert, getRandom());
                    // Deactivate and the re-activate the index to force it flush its in memory component
                    ctx.getIndex().deactivate();
                    ctx.getIndex().activate();
                }
            }
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) ctx.getIndexAccessor();
            ILSMIOOperation mergeOp = accessor.scheduleMerge(((LSMBTree) ctx.getIndex()).getDiskComponents());
            mergeOp.addCompleteListener(op -> {
                if (op.getIOOpertionType() == LSMIOOperationType.MERGE) {
                    long numPages = op.getNewComponent().getComponentSize()
                            / harness.getDiskBufferCache().getPageSizeWithHeader() - 1;
                    // we skipped the metadata page for simplicity
                    Assert.assertEquals(numPages / PAGES_PER_FORCE, lastCallback.getTotalForces());
                }
            });

        }
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    @Override
    protected String getTestOpName() {
        return "Write Page Callback";
    }

}
