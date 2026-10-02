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

package org.apache.hyracks.storage.am.lsm.vector.quantized;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.QuantizedSearchTestDriver;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * Asserts that a top-K search releases the LSM components {@code LSMHarness.search()} entered for it.
 * A component left with a live reader is never reset, so the index keeps its scheduled-flush count and
 * later writers to it spin in {@code getAndEnterComponents}. {@code reset()} also clears the modified
 * flag, which makes the recycling observable through {@link ILSMMemoryComponent#isModified()}.
 */
public class LSMVTreeTopKSearchComponentReleaseTest extends QuantizedSearchTestDriver {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int vectorDimension, List<List<ITupleReference>> leafRecords, List<QueryCase> queryCases) throws Exception {

        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, vectorDimension, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(),
                harness.getDataTupleBuilderFactory());

        // LSMVTreeTopKSearchCursor's SpillableTopKBuffer needs a real IHyracksTaskContext.
        ctx.setHyracksTaskContext(harness.getHyracksTastContext());

        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(numClustersPerLevel);
        ctx.setNumCentroidsPerLevel(centroidsPerCluster);
        ctx.setDataRecords(leafRecords);

        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();

            testUtils.buildStaticStructure(ctx);
            testUtils.bulkLoadRecords(ctx);

            // Populate the memory component; this is the component the search will enter and must release.
            VectorTestStructure struct = vectorDimension == 2 ? STRUCT_2D : STRUCT_3D;
            List<List<ITupleReference>> insertRecords = struct.generateInsertRecords(INSERT_RECORDS_PER_CLUSTER);
            int insertedCount = testUtils.insertRecordsIntoMemoryComponent(ctx, insertRecords);
            LOGGER.info("Inserted {} records into the memory component", insertedCount);

            // Capture the component now: a flush switches the index to the next one.
            ILSMMemoryComponent memComponent = ((LSMVTree) ctx.getIndex()).getCurrentMemoryComponent();
            assertTrue("precondition: the memory component should be modified after inserts",
                    memComponent.isModified());

            // No expected PKs: the inserts above change which records win, and this test covers only
            // component release.
            QueryCase qc = queryCases.get(0);
            ctx.setQueryVector(qc.queryVector);
            ctx.setQueryK(qc.queryK);
            ctx.setExpectedPrimaryKeys(Collections.emptyList());
            testUtils.naiveBlockedSearch(ctx);

            flush(ctx);

            // Still modified means reset() never ran, so the search left a reader on the component.
            assertFalse("memory component was not recycled after flush; the top-K search left a reader on it",
                    memComponent.isModified());
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    private void flush(AbstractVectorTreeTestContext ctx) throws HyracksDataException, InterruptedException {
        ILSMIndexAccessor accessor =
                (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        ILSMIOOperation flushOp = accessor.scheduleFlush();
        flushOp.sync();
        if (flushOp.getStatus() == LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(flushOp.getFailure());
        }
    }
}
