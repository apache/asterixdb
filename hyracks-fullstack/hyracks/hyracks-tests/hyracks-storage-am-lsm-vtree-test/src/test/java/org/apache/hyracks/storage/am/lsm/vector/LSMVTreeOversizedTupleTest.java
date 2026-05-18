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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for the oversized-tuple guard in {@code VTreeStaticStructureBuilder.add()} and
 * {@code VTreeBulkLoader.add()} (reviewer feedback, ASTERIXDB-3754). Before the guard, a tuple too
 * large to fit in an otherwise-empty page caused an infinite empty-page loop / silent page overrun:
 * the overflow branch allocated a fresh page and then {@code insertSorted()} was called without
 * re-checking that the tuple fits. The guard makes both paths throw {@link ErrorCode#RECORD_IS_TOO_LARGE}
 * instead.
 * <p>
 * The harness uses 512-byte pages, so a full-precision centroid {@code double[]} of dimension 100
 * (800 bytes) — or a data record with a comparably large embedding — cannot fit in a fresh page.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class LSMVTreeOversizedTupleTest {

    // Dimension chosen so that a full-precision centroid (8 bytes/dim = 800 bytes) far exceeds the
    // 512-byte page's usable space, guaranteeing the tuple cannot fit in a fresh empty page.
    private static final int OVERSIZED_DIMENSION = 100;

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    private AbstractVectorTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int vectorDimensions)
            throws Exception {
        return LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes,
                vectorDimensions, harness.getMergePolicy(), harness.getOperationTracker(), harness.getIOScheduler(),
                harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                harness.getMetadataPageManagerFactory());
    }

    /**
     * Drive a single oversized centroid through the static-structure build path
     * ({@code VTreeStaticStructureBuilder.add()}) and assert it throws RECORD_IS_TOO_LARGE.
     */
    @Test
    public void oversizedCentroidInStaticStructureBuildThrows() throws Exception {
        // Data-record serdes (NAIVE: <distance, centroid_id, pk>) only fix the data-frame layout;
        // the static-structure path uses the fixed leaf/interior frame layout, independent of these.
        ISerializerDeserializer[] dataRecordSerdes =
                new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(dataRecordSerdes, OVERSIZED_DIMENSION);
        // Simplest possible structure: 1 level, 1 cluster, 1 centroid.
        ctx.setNumClustersPerLevel(Arrays.asList(1));
        ctx.setNumCentroidsPerLevel(Arrays.asList(Arrays.asList(1)));

        double[] oversizedVector = new double[OVERSIZED_DIMENSION];
        Arrays.fill(oversizedVector, 1.0);
        List<ITupleReference> centroids = new ArrayList<>();
        centroids.add(createCentroidTuple(0, oversizedVector));
        ctx.setStaticStructureCentroids(centroids);

        ctx.getIndex().create();
        ctx.getIndex().activate();
        try {
            HyracksDataException thrown =
                    Assert.assertThrows(HyracksDataException.class, () -> buildStaticStructure(ctx));
            Assert.assertEquals("expected RECORD_IS_TOO_LARGE from oversized centroid", ErrorCode.RECORD_IS_TOO_LARGE,
                    errorCodeOf(thrown));
        } finally {
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
        }
    }

    /**
     * Drive an oversized data record through the bulk-load path ({@code VTreeBulkLoader.add()}) and
     * assert it throws RECORD_IS_TOO_LARGE. A tiny valid static structure is built first so the bulk
     * loader can initialize from it.
     */
    @Test
    public void oversizedDataRecordInBulkLoadThrows() throws Exception {
        // Data record layout: <distance: double, centroid_id: int, oversized_embedding: double[], pk: UTF8>.
        // The oversized embedding (dim 100 = 800 bytes) makes the record exceed a fresh 512-byte page.
        ISerializerDeserializer[] dataRecordSerdes = new ISerializerDeserializer[] {
                DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                DoubleArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(dataRecordSerdes, OVERSIZED_DIMENSION);
        // Small valid static structure: 1 level, 1 cluster, 1 leaf centroid (id 0, small vector).
        ctx.setNumClustersPerLevel(Arrays.asList(1));
        ctx.setNumCentroidsPerLevel(Arrays.asList(Arrays.asList(1)));
        List<ITupleReference> centroids = new ArrayList<>();
        centroids.add(createCentroidTuple(0, new double[] { 0.0, 0.0, 0.0 }));
        ctx.setStaticStructureCentroids(centroids);

        ctx.getIndex().create();
        ctx.getIndex().activate();
        try {
            buildStaticStructure(ctx);

            double[] oversizedEmbedding = new double[OVERSIZED_DIMENSION];
            Arrays.fill(oversizedEmbedding, 1.0);
            ITupleReference oversizedRecord = createNaiveWithVectorRecord(0.1, 0, oversizedEmbedding, "pk_big_0");

            HyracksDataException thrown =
                    Assert.assertThrows(HyracksDataException.class, () -> bulkLoadRecord(ctx, oversizedRecord));
            Assert.assertEquals("expected RECORD_IS_TOO_LARGE from oversized data record",
                    ErrorCode.RECORD_IS_TOO_LARGE, errorCodeOf(thrown));
        } finally {
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
        }
    }

    // ===== Helpers =====

    private void buildStaticStructure(AbstractVectorTreeTestContext ctx) throws Exception {
        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();
        Map<String, Object> parameters = new java.util.HashMap<>();
        parameters.put("numLevels", ctx.getNumCentroidsPerLevel().size());
        parameters.put("clustersPerLevel", ctx.getNumClustersPerLevel());
        parameters.put("centroidsPerCluster", ctx.getNumCentroidsPerLevel());
        parameters.put("maxEntriesPerPage", 100);
        List<ITupleReference> centroids = ctx.getStaticStructureCentroids();
        IIndexBulkLoader ssBuilder = lsmvTree.createBulkLoader(1.0f, false, centroids.size(), parameters);
        for (ITupleReference tuple : centroids) {
            ssBuilder.add(tuple);
        }
        ssBuilder.end();
    }

    private void bulkLoadRecord(AbstractVectorTreeTestContext ctx, ITupleReference record) throws Exception {
        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();
        Map<String, Object> parameters = new java.util.HashMap<>();
        IIndexBulkLoader bulkLoader = lsmvTree.createBulkLoader(1.0f, false, 1, parameters);
        bulkLoader.add(record);
        bulkLoader.end();
    }

    /** Unwrap the underlying {@link ErrorCode} regardless of whether the throw was wrapped in end(). */
    private static ErrorCode errorCodeOf(HyracksDataException e) {
        Throwable t = e;
        while (t != null) {
            if (t instanceof HyracksDataException) {
                HyracksDataException hde = (HyracksDataException) t;
                if (ErrorCode.HYRACKS.equals(hde.getComponent())
                        && hde.getErrorCode() == ErrorCode.RECORD_IS_TOO_LARGE.intValue()) {
                    return ErrorCode.RECORD_IS_TOO_LARGE;
                }
            }
            t = t.getCause();
        }
        return null;
    }

    private static ITupleReference createCentroidTuple(int centroidId, double[] vector) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tupleRef = new ArrayTupleReference();
        ISerializerDeserializer[] fieldSerdes =
                { IntegerSerializerDeserializer.INSTANCE, DoubleArraySerializerDeserializer.INSTANCE };
        Object[] fieldValues = { centroidId, vector };
        TupleUtils.createTuple(tupleBuilder, tupleRef, fieldSerdes, fieldValues);
        return tupleRef;
    }

    /** Data record: {@code <distance: double, centroid_id: int, embedding: double[], pk: UTF8String>}. */
    private static ITupleReference createNaiveWithVectorRecord(double distance, int centroidId, double[] embedding,
            String primaryKey) throws HyracksDataException {
        try {
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);
            ArrayTupleReference tupleRef = new ArrayTupleReference();
            tupleBuilder.getDataOutput().writeDouble(distance);
            tupleBuilder.addFieldEndOffset();
            tupleBuilder.getDataOutput().writeInt(centroidId);
            tupleBuilder.addFieldEndOffset();
            DoubleArraySerializerDeserializer.INSTANCE.serialize(embedding, tupleBuilder.getDataOutput());
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
