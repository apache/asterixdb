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

package org.apache.hyracks.storage.am.lsm.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorIndexTestDriver;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.TestDoubleArrayVectorAccessor;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * Regression test for "flush persists a LEAF page as the component root": searches a flushed
 * (NOT merged) disk component and asserts inserted records from leaf clusters other than
 * cluster 0 are returned. With the wrong root, only the first static leaf page (clusters
 * c10-c12 in the 3-level structure) is reachable; all other clusters silently resolve to the
 * -1 directory-page sentinel and return nothing.
 *
 * Flow:
 * 1. Build static structure and bulk load data records → disk component 1
 * 2. Insert additional records into ALL leaf clusters (memory component)
 * 3. Flush memory component → additional disk component(s); do NOT merge
 * 4. Run top-k searches near leaf centroids on DIFFERENT static leaf pages and assert the
 *    inserted PKs (which only live in the flushed component) are found
 *
 * Data tuple format (standard): <distance, centroid_id, primary_key>
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class LSMVTreeFlushSearchTest extends VectorIndexTestDriver {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    private static final int INSERT_RECORDS_PER_CLUSTER = 30;
    private static final int QUERY_K = 100;

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    /**
     * Implementation of runTest from VectorIndexTestDriver.
     * Performs: build static structure → bulk load → insert → flush (no merge) → verify.
     */
    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int vectorDimension, List<List<ITupleReference>> leafRecords) throws Exception {

        LOGGER.info("LSMVTree Flush Search Test: {} levels, {} centroids, {} leaf clusters, {}D vectors",
                numClustersPerLevel.size(), centroids.size(), leafRecords.size(), vectorDimension);

        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, vectorDimension, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory());

        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(numClustersPerLevel);
        ctx.setNumCentroidsPerLevel(centroidsPerCluster);
        ctx.setDataRecords(leafRecords);

        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();

        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();

            testUtils.buildStaticStructure(ctx);
            testUtils.bulkLoadRecords(ctx);
            assertEquals("Should have 1 disk component after bulk load", 1, lsmvTree.getDiskComponents().size());

            VectorTestStructure struct = VectorTestStructure.threeDim3Level();
            List<List<ITupleReference>> insertRecords = struct.generateInsertRecords(INSERT_RECORDS_PER_CLUSTER);
            int insertedCount = testUtils.insertRecordsIntoMemoryComponent(ctx, insertRecords);
            LOGGER.info("Inserted {} records into memory component", insertedCount);

            flush(ctx);
            LOGGER.info("Flushed memory component to disk (no merge); {} disk components",
                    lsmvTree.getDiskComponents().size());
            assertTrue("Should have more than 1 disk component after flush", lsmvTree.getDiskComponents().size() > 1);

            // Query near leaf centroids that live on DIFFERENT static leaf pages. Inserted
            // records only exist in the flushed component, so if its persisted root is wrong,
            // only cluster page 0 (c10-c12) yields inserted PKs and the rest come back empty.
            int firstLeafCid = struct.getFirstLeafCentroidId();
            double[][] leafCentroids = struct.getLeafCentroids();
            int[] targetCentroidIndexes = { 0 /* c10, cluster 2.1 */, 3 /* c13, cluster 2.2 */,
                    12 /* c22, cluster 2.5 */, 23 /* c33, cluster 2.8 */ };

            for (int centroidIndex : targetCentroidIndexes) {
                int cid = firstLeafCid + centroidIndex;
                verifyInsertedRecordsFound(ctx, leafCentroids[centroidIndex], cid);
            }
            LOGGER.info("Verification: inserted records found in flushed-unmerged component for all target clusters");
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    /**
     * Flush the current memory component to disk.
     */
    private void flush(AbstractVectorTreeTestContext ctx) throws HyracksDataException, InterruptedException {
        ILSMIndexAccessor accessor =
                (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        ILSMIOOperation flushOp = accessor.scheduleFlush();
        flushOp.sync();
        if (flushOp.getStatus() == LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(flushOp.getFailure());
        }
    }

    /**
     * Run a top-k search at the given query vector and assert that inserted PKs of the given
     * centroid's cluster ("pk_ins_c&lt;cid&gt;_*") are among the results.
     */
    private void verifyInsertedRecordsFound(AbstractVectorTreeTestContext ctx, double[] queryVector, int centroidId)
            throws Exception {
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        VTreeSearchPredicate predicate = new VTreeSearchPredicate();
        predicate.setMinProbeFraction(0.4);
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setK(QUERY_K);

        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        String insertedPkPrefix = "pk_ins_c" + centroidId + "_";
        try {
            accessor.search(cursor, predicate);

            List<String> foundPKs = new ArrayList<>();
            int bulkLoadCount = 0;
            int insertedFromTargetCluster = 0;

            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                String pk = extractPrimaryKeyFromTuple(tuple);
                foundPKs.add(pk);

                if (pk.startsWith(insertedPkPrefix)) {
                    insertedFromTargetCluster++;
                } else if (!pk.startsWith("pk_ins_")) {
                    bulkLoadCount++;
                }
            }

            LOGGER.info("Query at c{}: {} total records, {} bulk-loaded, {} inserted from cluster c{}", centroidId,
                    foundPKs.size(), bulkLoadCount, insertedFromTargetCluster, centroidId);

            assertTrue("Should find bulk-loaded records near c" + centroidId, bulkLoadCount > 0);
            assertTrue(
                    "Should find inserted records of cluster c" + centroidId
                            + " in the flushed-unmerged component (missing => flushed component root is wrong)",
                    insertedFromTargetCluster > 0);
        } finally {
            cursor.close();
            cursor.destroy();
        }
    }

    /**
     * Extract primary key from a result tuple.
     * Standard result tuple format: <distance, centroid_id, primary_key>
     * PK is at field index 2.
     */
    private String extractPrimaryKeyFromTuple(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (String) values[2];
    }
}
