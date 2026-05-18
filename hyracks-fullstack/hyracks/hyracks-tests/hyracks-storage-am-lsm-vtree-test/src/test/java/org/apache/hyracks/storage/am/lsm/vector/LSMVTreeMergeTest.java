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

import static org.junit.Assert.*;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * LSMVTree merge test.
 * Tests explicit merge of multiple disk components into a single disk component.
 *
 * Inherits test case from VectorIndexTestDriver:
 * - threeDimensionThreeLevels(): 3D three-layer structure (3 levels, 24 leaf centroids, 2400 bulk-loaded records)
 *
 * Flow:
 * 1. Build static structure and bulk load data records → disk component 1
 * 2. Insert additional records into memory component
 * 3. Flush memory component → disk component 2
 * 4. Explicitly merge disk components 1 and 2 into a single disk component
 * 5. Verify search returns records from both original components after merge
 *
 * Data tuple format (standard): <distance, centroid_id, primary_key>
 */
public class LSMVTreeMergeTest extends VectorIndexTestDriver {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    private static final int INSERT_RECORDS_PER_CLUSTER = 30;

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
     * Performs: build static structure → bulk load → insert → flush → merge → verify.
     */
    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int vectorDimension, List<List<ITupleReference>> leafRecords) throws Exception {

        LOGGER.info("LSMVTree Merge Test: {} levels, {} centroids, {} leaf clusters, {}D vectors",
                numClustersPerLevel.size(), centroids.size(), leafRecords.size(), vectorDimension);

        // Create test context with default (standard) data tuple creator factory
        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, vectorDimension, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory());

        // Set test data in context
        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(numClustersPerLevel);
        ctx.setNumCentroidsPerLevel(centroidsPerCluster);
        ctx.setDataRecords(leafRecords);

        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();

        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();

            testUtils.buildStaticStructure(ctx);
            LOGGER.info("Static structure built with {} centroids", centroids.size());

            testUtils.bulkLoadRecords(ctx);
            int bulkLoadedCount = leafRecords.stream().mapToInt(List::size).sum();
            LOGGER.info("Bulk loaded {} records", bulkLoadedCount);
            assertEquals("Should have 1 disk component after bulk load", 1, lsmvTree.getDiskComponents().size());

            VectorTestStructure struct = VectorTestStructure.threeDim3Level();
            List<List<ITupleReference>> insertRecords = struct.generateInsertRecords(INSERT_RECORDS_PER_CLUSTER);
            int insertedCount = testUtils.insertRecordsIntoMemoryComponent(ctx, insertRecords);
            LOGGER.info("Inserted {} records into memory component", insertedCount);

            flush(ctx);
            LOGGER.info("Flushed memory component to disk");
            assertEquals("Should have 3 disk components after flush", 3, lsmvTree.getDiskComponents().size());

            ILSMIndexAccessor lsmAccessor =
                    (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
            ILSMIOOperation mergeOp = lsmAccessor.scheduleMerge(lsmvTree.getDiskComponents());
            mergeOp.sync();
            if (mergeOp.getStatus() == LSMIOOperationStatus.FAILURE) {
                throw HyracksDataException.create(mergeOp.getFailure());
            }
            LOGGER.info("Merged disk components into single component");
            assertEquals("Should have 1 disk component after merge", 1, lsmvTree.getDiskComponents().size());

            double[] queryVector = { 20.0, 30.0, 20.0 };
            int queryK = 500;
            verifyMergedRecords(ctx, queryVector, queryK);
            LOGGER.info("Verification: Records from both components found after merge");
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
     * Verify that records from both bulk load and insert are findable after merge.
     */
    private void verifyMergedRecords(AbstractVectorTreeTestContext ctx, double[] queryVector, int k) throws Exception {
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        VTreeSearchPredicate predicate = new VTreeSearchPredicate();
        predicate.setMinProbeFraction(0.4);
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setK(k);

        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        try {
            accessor.search(cursor, predicate);

            List<String> foundPKs = new ArrayList<>();
            int bulkLoadCount = 0;
            int insertCount = 0;

            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                String pk = extractPrimaryKeyFromTuple(tuple);
                foundPKs.add(pk);

                if (pk.startsWith("pk_ins_")) {
                    insertCount++;
                } else {
                    bulkLoadCount++;
                }
            }

            LOGGER.info("Post-merge search returned {} total records: {} bulk-loaded, {} inserted", foundPKs.size(),
                    bulkLoadCount, insertCount);

            // Verify we got records from both original components
            assertTrue("Should find bulk-loaded records after merge", bulkLoadCount > 0);
            assertTrue("Should find inserted records after merge", insertCount > 0);

            int sampleSize = Math.min(10, foundPKs.size());
            LOGGER.info("Sample of found PKs: {}", foundPKs.subList(0, sampleSize));

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
