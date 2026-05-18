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
import org.junit.Test;

/**
 * Regression for multi-version antimatter reconciliation in {@code LSMVTreeSearchCursor} across
 * &gt;=3 components that share one {@code <distance, PK>} key (ASTERIXDB-3754).
 *
 * <p>The streaming cursor previously reconciled matter/antimatter <em>pairwise</em> — it cancelled a
 * single pair and stopped, so a 3rd same-key version leaked. Concretely, re-inserting the same vector
 * after a delete produced three components for one key (matter / antimatter / matter): the query
 * returned the PK twice and resurfaced the deleted version.
 *
 * <p>Correct behaviour: the newest version wins and every older same-key version is drained — a live
 * key appears exactly once, a net-deleted key not at all.
 */
public class LSMVTreeMultiVersionReconciliationTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    private static final int BULK_RECORDS_PER_CLUSTER = 50;

    // Victim: pk_c_10_0 is the first bulk-loaded record of leaf centroid c10 {20,30,20}
    // (ring distance 0.2, +x direction). Re-inserting the SAME vector keeps the same <distance, PK>.
    private static final String VICTIM_PK = "pk_c_10_0";
    private static final double[] VICTIM_VECTOR = { 20.2, 30.0, 20.0 };
    private static final double[] QUERY_VECTOR = { 20.0, 30.0, 20.0 };
    private static final int QUERY_K = 500;

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    /** insert -> delete -> re-insert (same vector), each in its own disk component: key is LIVE, must appear once. */
    @Test
    public void reinsertAfterDeleteAppearsExactlyOnce() throws Exception {
        AbstractVectorTreeTestContext ctx = newContext();
        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();
        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();
            testUtils.buildStaticStructure(ctx);

            // C(oldest): bulk load holds the victim's original matter.
            testUtils.bulkLoadRecords(ctx);
            // C(middle): delete the victim -> antimatter, flush.
            testUtils.deleteRecordsFromIndex(ctx, List.of(createVectorPkTuple(VICTIM_VECTOR, VICTIM_PK)));
            flush(ctx);
            // C(newest): re-insert the same vector -> matter, flush.
            testUtils.insertRecordsIntoMemoryComponent(ctx,
                    List.of(List.of(createVectorPkTuple(VICTIM_VECTOR, VICTIM_PK))));
            flush(ctx);
            assertTrue(">= 3 disk components expected for the multi-version case, got "
                    + lsmvTree.getDiskComponents().size(), lsmvTree.getDiskComponents().size() >= 3);

            long occurrences = searchPKs(ctx).stream().filter(VICTIM_PK::equals).count();
            // Pre-fix: 2 (the re-insert plus the resurrected original). Correct: exactly 1.
            assertEquals("re-inserted key must appear exactly once (no duplicate / resurrected version)", 1L,
                    occurrences);
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    /** insert -> delete -> re-insert -> delete: net-deleted, must be absent. */
    @Test
    public void netDeletedAcrossFourVersionsIsAbsent() throws Exception {
        AbstractVectorTreeTestContext ctx = newContext();
        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();
            testUtils.buildStaticStructure(ctx);

            testUtils.bulkLoadRecords(ctx); // matter (oldest)
            testUtils.deleteRecordsFromIndex(ctx, List.of(createVectorPkTuple(VICTIM_VECTOR, VICTIM_PK)));
            flush(ctx); // antimatter
            testUtils.insertRecordsIntoMemoryComponent(ctx,
                    List.of(List.of(createVectorPkTuple(VICTIM_VECTOR, VICTIM_PK))));
            flush(ctx); // matter (re-insert)
            testUtils.deleteRecordsFromIndex(ctx, List.of(createVectorPkTuple(VICTIM_VECTOR, VICTIM_PK)));
            flush(ctx); // antimatter (newest)

            long occurrences = searchPKs(ctx).stream().filter(VICTIM_PK::equals).count();
            assertEquals("net-deleted key must be absent", 0L, occurrences);
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    private AbstractVectorTreeTestContext newContext() throws Exception {
        VectorTestStructure struct = VectorTestStructure.threeDim3Level();
        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                struct.getDataRecordSerdes(VectorTestStructure.BulkLoadRecordFormat.NAIVE), struct.getVectorDimension(),
                harness.getMergePolicy(), harness.getOperationTracker(), harness.getIOScheduler(),
                harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                harness.getMetadataPageManagerFactory());
        ctx.setStaticStructureCentroids(struct.buildCentroidTuples());
        ctx.setNumClustersPerLevel(struct.getNumClustersPerLevel());
        ctx.setNumCentroidsPerLevel(struct.getCentroidsPerCluster());
        ctx.setDataRecords(struct.generateBulkLoadRecords(VectorTestStructure.BulkLoadRecordFormat.NAIVE,
                BULK_RECORDS_PER_CLUSTER));
        return ctx;
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

    private List<String> searchPKs(AbstractVectorTreeTestContext ctx) throws Exception {
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, QUERY_VECTOR);
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
        List<String> foundPKs = new ArrayList<>();
        try {
            accessor.search(cursor, predicate);
            try {
                while (cursor.hasNext()) {
                    cursor.next();
                    foundPKs.add(extractPrimaryKey(cursor.getTuple()));
                }
            } finally {
                cursor.close();
            }
        } finally {
            cursor.destroy();
        }
        LOGGER.info("Search returned {} PKs", foundPKs.size());
        return foundPKs;
    }

    /** Insert/delete tuple format is identical: {@code <vector, pk>}. */
    private ITupleReference createVectorPkTuple(double[] vector, String primaryKey) throws HyracksDataException {
        try {
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
            ArrayTupleReference tupleRef = new ArrayTupleReference();
            DoubleArraySerializerDeserializer.INSTANCE.serialize(vector, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
            new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
            tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            return tupleRef;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /** Naive result tuple format: {@code <distance, centroid_id, primary_key>}; PK at field 2. */
    private String extractPrimaryKey(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (String) values[2];
    }
}
