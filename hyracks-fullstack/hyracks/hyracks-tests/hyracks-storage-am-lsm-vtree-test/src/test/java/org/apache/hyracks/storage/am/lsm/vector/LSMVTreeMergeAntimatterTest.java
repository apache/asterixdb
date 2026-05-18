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
import static org.junit.Assert.assertFalse;
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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
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
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for antimatter preservation through a PARTIAL merge (a merge that excludes the
 * oldest disk component, as scheduled by production merge policies such as
 * size-bounded-concurrent).
 *
 * Scenario:
 * <ol>
 *   <li>Bulk load records → oldest disk component holds the matter twin of the victim PK.</li>
 *   <li>Insert filler records, flush → middle disk component(s).</li>
 *   <li>DML-delete the victim PK (antimatter into memory), flush → newest disk component.</li>
 *   <li>scheduleMerge over all disk components EXCEPT the oldest → returnDeletedTuples=true, so
 *       the merge must PRESERVE the antimatter tuple in the merged component.</li>
 *   <li>Search: the victim PK must stay invisible (merged antimatter cancels the oldest
 *       component's matter at query time).</li>
 * </ol>
 *
 * Pre-fix failure mode: the merge drains preserved antimatter into VTreeBulkLoader whose data
 * frames use the insert (matter) tuple writer, which re-encodes the antimatter tuple as matter —
 * the delete resurrects.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class LSMVTreeMergeAntimatterTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    private static final int BULK_RECORDS_PER_CLUSTER = 50;
    private static final int INSERT_RECORDS_PER_CLUSTER = 5;

    // Victim: pk_c_10_0 is the first bulk-loaded record of leaf centroid c10 {20,30,20}:
    // ring distance 0.2, +x direction → vector {20.2, 30.0, 20.0}.
    private static final String VICTIM_PK = "pk_c_10_0";
    private static final double[] VICTIM_VECTOR = { 20.2, 30.0, 20.0 };

    // Query at c10's centroid so the victim's cluster is the closest one.
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

    @Test
    public void partialMergePreservesAntimatter() throws Exception {
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

        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();

        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();

            testUtils.buildStaticStructure(ctx);
            // 1. Bulk load → the OLDEST disk component; it holds the matter twin of the victim.
            testUtils.bulkLoadRecords(ctx);
            assertEquals("one disk component after bulk load", 1, lsmvTree.getDiskComponents().size());

            // 2. Filler inserts + flush → middle component(s) so the partial merge has >= 2 inputs.
            testUtils.insertRecordsIntoMemoryComponent(ctx, struct.generateInsertRecords(INSERT_RECORDS_PER_CLUSTER));
            flush(ctx);

            // 3. Delete the victim (matter lives on disk → antimatter into memory), then flush.
            testUtils.deleteRecordsFromIndex(ctx, List.of(createDeleteTuple(VICTIM_VECTOR, VICTIM_PK)));
            flush(ctx);

            List<String> preMergePKs = searchPKs(ctx);
            assertFalse("sanity: victim must be invisible before the merge (antimatter on disk)",
                    preMergePKs.contains(VICTIM_PK));

            // 4. Partial merge: all disk components EXCEPT the oldest → returnDeletedTuples=true.
            List<ILSMDiskComponent> diskComponents = lsmvTree.getDiskComponents();
            int numComponents = diskComponents.size();
            assertTrue("need >= 3 disk components to merge a strict subset, got " + numComponents, numComponents >= 3);
            List<ILSMDiskComponent> mergingComponents = new ArrayList<>(diskComponents.subList(0, numComponents - 1));
            LOGGER.info("Merging {} of {} disk components (oldest excluded)", mergingComponents.size(), numComponents);

            ILSMIndexAccessor lsmAccessor =
                    (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
            ILSMIOOperation mergeOp = lsmAccessor.scheduleMerge(mergingComponents);
            mergeOp.sync();
            if (mergeOp.getStatus() == LSMIOOperationStatus.FAILURE) {
                throw HyracksDataException.create(mergeOp.getFailure());
            }
            assertEquals("merged component + excluded oldest", 2, lsmvTree.getDiskComponents().size());

            // 5. The victim must STILL be invisible: the merged component must carry the antimatter
            // tuple so it cancels the oldest component's matter at query time.
            List<String> postMergePKs = searchPKs(ctx);
            assertFalse("deleted PK '" + VICTIM_PK + "' resurrected by partial merge (antimatter re-encoded"
                    + " as matter in the merged component)", postMergePKs.contains(VICTIM_PK));

            // Sanity: the merge did not lose live records.
            assertTrue("bulk-loaded sibling record should survive", postMergePKs.contains("pk_c_10_1"));
            assertTrue("inserted record should survive",
                    postMergePKs.stream().anyMatch(pk -> pk.startsWith("pk_ins_")));
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

    /**
     * Query-mode streaming search around the victim's cluster; returns all PKs found.
     */
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

    /** Delete tuple format is the same as insert: {@code <vector, pk>}. */
    private ITupleReference createDeleteTuple(double[] vector, String primaryKey) throws HyracksDataException {
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
