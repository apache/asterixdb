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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for matter/antimatter cancellation during a FULL merge (COMPACT) of a
 * QUANTIZED index whose {@code quantized_distance} (field 2) carries different write semantics
 * on the two sides:
 * <ul>
 *   <li>bulk load writes a quantized-space distance (this test mimics that with
 *       {@code distance * QUANTIZED_SPACE_SCALE});</li>
 *   <li>DML delete ({@code VTreeDataTupleBuilder#writeQuantizedFields}) duplicates the
 *       full-precision field-0 distance.</li>
 * </ul>
 *
 * Pre-fix failure mode: {@code LSMVTree#doMerge} builds its merge predicate with the default
 * {@code pkStartField = 2}, so the merge cursor's cancellation key covers fields 2/3
 * (quantized_distance, quantized_embedding) before the PKs. The bulk-loaded matter tuple and its
 * DML antimatter twin compare unequal on field 2, cancellation is missed, and the deleted PK
 * survives COMPACT (the orphan antimatter is silently dropped because a full merge runs with
 * returnDeletedTuples=false).
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class LSMVTreeCompactQuantizedTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    /**
     * Scale applied to the full-precision distance to obtain the stored quantized-space distance,
     * mimicking production bulk load (Job 3) writing field 2 in quantized space. Any value != 1
     * makes field 2 diverge from the DML path's full-precision duplicate.
     */
    private static final double QUANTIZED_SPACE_SCALE = 12.75;

    private static final VectorTestStructure STRUCT_3D = VectorTestStructure.threeDim1Centroid();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Test
    public void compactCancelsDeletesDespiteQuantizedDistanceMismatch() throws Exception {
        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                STRUCT_3D.getDataRecordSerdes(VectorTestStructure.BulkLoadRecordFormat.QUANTIZED),
                STRUCT_3D.getVectorDimension(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(),
                harness.getDataTupleBuilderFactory());

        ctx.setHyracksTaskContext(harness.getHyracksTastContext());
        ctx.setStaticStructureCentroids(STRUCT_3D.buildCentroidTuples());
        ctx.setNumClustersPerLevel(STRUCT_3D.getNumClustersPerLevel());
        ctx.setNumCentroidsPerLevel(STRUCT_3D.getCentroidsPerCluster());
        ctx.setDataRecords(generateQuantizedSpaceRecords());

        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();

        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();

            testUtils.buildStaticStructure(ctx);
            // 1. Bulk load: field 2 is a quantized-space distance (differs from field 0).
            testUtils.bulkLoadRecords(ctx);
            assertEquals("one disk component after bulk load", 1, lsmvTree.getDiskComponents().size());

            // 2. DML-delete the two records closest to the query. Their antimatter twins carry a
            // full-precision field 2 (VTreeDataTupleBuilder duplicates field 0).
            List<ITupleReference> deleteTuples = new ArrayList<>();
            deleteTuples.add(createDeleteTuple(new double[] { 5.0, 0.0, 0.0 }, "pk_opt_5"));
            deleteTuples.add(createDeleteTuple(new double[] { 6.0, 0.0, 0.0 }, "pk_opt_6"));
            testUtils.deleteRecordsFromIndex(ctx, deleteTuples);

            // 3. Flush so the antimatter is on disk and the full merge has two inputs.
            flush(ctx);
            assertEquals("two disk components after flush", 2, lsmvTree.getDiskComponents().size());

            // Sanity: query-time reconciliation (correctly keyed on <distance, PK>) hides the
            // deleted records before the compact.
            runQueryCase(ctx);

            // 4. COMPACT: full merge (includes the oldest component) → returnDeletedTuples=false,
            // so matter/antimatter twins must cancel INSIDE the merge.
            ILSMIndexAccessor lsmAccessor =
                    (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
            ILSMIOOperation mergeOp = lsmAccessor.scheduleFullMerge();
            mergeOp.sync();
            if (mergeOp.getStatus() == LSMIOOperationStatus.FAILURE) {
                throw HyracksDataException.create(mergeOp.getFailure());
            }
            assertEquals("one disk component after full merge", 1, lsmvTree.getDiskComponents().size());
            LOGGER.info("Full merge (COMPACT) completed");

            // 5. Deleted PKs must not survive the compact.
            runQueryCase(ctx);
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    /**
     * Query at [5,0,0] with K=5. After deleting pk_opt_5 (D(q,x)=0) and pk_opt_6 (D(q,x)=1),
     * the top-5 is pk_opt_4, pk_opt_3, pk_opt_7, pk_opt_2, pk_opt_8 (same case as
     * {@link LSMVTreeDeleteQuantizedTest}).
     */
    private void runQueryCase(AbstractVectorTreeTestContext ctx) throws Exception {
        ctx.setQueryVector(new double[] { 5.0, 0.0, 0.0 });
        ctx.setQueryK(5);
        ctx.setExpectedPrimaryKeys(Arrays.asList("pk_opt_4", "pk_opt_3", "pk_opt_7", "pk_opt_2", "pk_opt_8"));
        ctx.setExcludedPrimaryKeys(Arrays.asList("pk_opt_5", "pk_opt_6"));
        testUtils.naiveBlockedSearch(ctx);
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
     * 20 records at integer distances 1-20 along the x-axis from the centroid at the origin
     * (same layout as {@code QuantizedSearchTestDriver#generateOptimizedSearchRecords}), except
     * that field 2 carries a QUANTIZED-SPACE distance instead of a copy of field 0.
     */
    private List<List<ITupleReference>> generateQuantizedSpaceRecords() throws Exception {
        List<ITupleReference> clusterRecords = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            double distance = i;
            double[] vector = { distance, 0.0, 0.0 };
            clusterRecords.add(createQuantizedSpaceRecordTuple(distance, 0, vector, "pk_opt_" + i));
        }
        List<List<ITupleReference>> allRecords = new ArrayList<>();
        allRecords.add(clusterRecords);
        return allRecords;
    }

    /**
     * Record tuple {@code <distance, centroid_id, quantized_distance, quantized_embedding, pk>}
     * with {@code quantized_distance = distance * QUANTIZED_SPACE_SCALE} (quantized space).
     */
    private ITupleReference createQuantizedSpaceRecordTuple(double distance, int centroidId, double[] vector,
            String primaryKey) throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(5);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        // Field 0: distance_to_centroid (raw double)
        tupleBuilder.getDataOutput().writeDouble(distance);
        tupleBuilder.addFieldEndOffset();
        // Field 1: centroid_id (raw int)
        tupleBuilder.getDataOutput().writeInt(centroidId);
        tupleBuilder.addFieldEndOffset();
        // Field 2: quantized_distance in QUANTIZED SPACE — intentionally != field 0
        tupleBuilder.getDataOutput().writeDouble(distance * QUANTIZED_SPACE_SCALE);
        tupleBuilder.addFieldEndOffset();
        // Field 3: quantized_embedding (VarLen prefix + raw big-endian doubles, test mode)
        ByteBuffer buf = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double d : vector) {
            buf.putDouble(d);
        }
        byte[] rawDoubles = buf.array();
        int metaLen = ByteArrayPointable.getNumberBytesToStoreMeta(rawDoubles.length);
        byte[] meta = new byte[metaLen];
        VarLenIntEncoderDecoder.encode(rawDoubles.length, meta, 0);
        tupleBuilder.getDataOutput().write(meta);
        tupleBuilder.getDataOutput().write(rawDoubles);
        tupleBuilder.addFieldEndOffset();
        // Field 4: primary_key
        new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleRef;
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
}
