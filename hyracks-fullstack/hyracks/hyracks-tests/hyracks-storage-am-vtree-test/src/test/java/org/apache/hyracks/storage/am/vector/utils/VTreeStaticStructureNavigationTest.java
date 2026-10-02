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

package org.apache.hyracks.storage.am.vector.utils;

import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.LongBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.vector.tuples.LSMVTreeDataTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.TestDoubleArrayVectorAccessor;
import org.apache.hyracks.storage.am.vector.TestVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.frames.VTreeDataFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeMetadataFrameFactory;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * {@code VTreeStaticStructureBuilder} writes the static clustering structure and {@link VTreeNavigationUtils}
 * reads it back, so the two are tested together on pages the real builder lays out. Navigation must yield the
 * nearest leaf centroid for a query vector, which every ANN query starts from.
 */
public class VTreeStaticStructureNavigationTest {

    private static final int PAGE_SIZE = 512;
    private static final int NUM_PAGES = 200;
    private static final int MAX_OPEN_FILES = 10;
    private static final int FRAME_SIZE = 32768;
    private static final int DIMENSIONS = 2;
    /** One placement cluster per record and the canonical RNG factor, as the LSM fixtures place records. */
    private static final CrossPollinationConfig SINGLE_CLOSEST = new CrossPollinationConfig(1, 1.0);
    private static final double EPSILON = 0.25;
    private static final int MAX_ENTRIES_PER_PAGE = 8;

    /** Four leaf centroids, one per quadrant, far enough apart that the nearest is unambiguous. */
    private static final double[][] LEAF_CENTROIDS = { { 10, 10 }, { -10, 10 }, { -10, -10 }, { 10, -10 } };
    private static final int FIRST_LEAF_CENTROID_ID = 0;

    private IHyracksTaskContext ctx;
    private IBufferCache bufferCache;
    private FileReference file;
    private VTree tree;
    private ITreeIndexFrameFactory interiorFrameFactory;
    private ITreeIndexFrameFactory leafFrameFactory;
    private final IVTreeDistanceFunction euclidean =
            new TestVTreeDistanceFunctionFactory("euclidean").createDistanceFunction();

    @Before
    public void setUp() throws HyracksDataException {
        ctx = TestUtils.create(FRAME_SIZE);
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        file = ctx.getIoManager().getFileReference(0, "vtree-static-structure-navigation");

        interiorFrameFactory = new VTreeInteriorFrameFactory(DIMENSIONS, null, null);
        leafFrameFactory = new VTreeLeafFrameFactory(DIMENSIONS, false, null, null);
        // Non-quantized data tuple [distance, centroidId, pk: long], keyed on <distance, pk> as LSMVTreeUtils keys it.
        ITypeTraits[] dataTraits =
                { DoublePointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS, LongPointable.TYPE_TRAITS };
        int[] comparatorFields = { VTreeDataTupleAccessor.DISTANCE_FIELD, VTreeDataTupleAccessor.NQ_KEY_FIELDS_START };
        IBinaryComparatorFactory[] keyCmpFactories =
                { DoubleBinaryComparatorFactory.INSTANCE, LongBinaryComparatorFactory.INSTANCE };
        ITypeTraits[] keyTypeTraits = { DoublePointable.TYPE_TRAITS, LongPointable.TYPE_TRAITS };
        ITreeIndexFrameFactory dataFrameFactory =
                new VTreeDataFrameFactory(new LSMVTreeDataTupleWriterFactory(dataTraits, false, null, null), DIMENSIONS,
                        comparatorFields, keyCmpFactories, null);
        IPageManager pageManager = new LinkedMetadataPageManagerFactory().createPageManager(bufferCache);

        tree = new VTree(bufferCache, pageManager, interiorFrameFactory, leafFrameFactory,
                new VTreeMetadataFrameFactory(DIMENSIONS, keyTypeTraits, keyCmpFactories, null, null), dataFrameFactory,
                new IBinaryComparatorFactory[] { null }, DIMENSIONS, DIMENSIONS, file,
                TestDoubleArrayVectorAccessor.Factory.INSTANCE, new VTreeDataTupleBuilderFactory(0, 1, false), null,
                new TestVTreeDistanceFunctionFactory("euclidean"), null, SINGLE_CLOSEST, EPSILON);
        tree.create();
        tree.activate();
    }

    @After
    public void tearDown() throws HyracksDataException {
        tree.deactivate();
        tree.destroy();
        bufferCache.close();
    }

    /** Every leaf centroid must be the nearest to a query sitting right on top of it. */
    @Test
    public void navigationFindsTheNearestCentroidForEachOfThem() throws HyracksDataException {
        buildSingleLevelStructure();

        for (int i = 0; i < LEAF_CENTROIDS.length; i++) {
            ClusterSearchResult result = navigate(LEAF_CENTROIDS[i]);
            Assert.assertNotNull("navigation returned nothing for centroid " + i, result);
            Assert.assertEquals("query on centroid " + i + " should find it", FIRST_LEAF_CENTROID_ID + i,
                    result.centroidId);
            Assert.assertEquals("distance to a coincident centroid is 0", 0.0, result.distance, 1e-9);
        }
    }

    /** A query nearer one centroid than the others resolves to that centroid. */
    @Test
    public void navigationPicksTheNearerOfTwoCandidates() throws HyracksDataException {
        buildSingleLevelStructure();

        // Just inside the first quadrant: nearest is {10,10} (id 0), next nearest {-10,10} (id 1).
        ClusterSearchResult result = navigate(new double[] { 4, 6 });

        Assert.assertNotNull(result);
        Assert.assertEquals(FIRST_LEAF_CENTROID_ID, result.centroidId);
    }

    /** The centroid it returns is the one it measured: the reported distance matches a direct computation. */
    @Test
    public void theReportedDistanceMatchesTheCentroidItReturned() throws HyracksDataException {
        buildSingleLevelStructure();
        double[] query = { 3, -7 };

        ClusterSearchResult result = navigate(query);

        Assert.assertNotNull(result);
        int index = result.centroidId - FIRST_LEAF_CENTROID_ID;
        Assert.assertTrue("centroid id out of range: " + result.centroidId,
                index >= 0 && index < LEAF_CENTROIDS.length);
        Assert.assertEquals("reported distance must be to the centroid it returned",
                euclidean.apply(query, LEAF_CENTROIDS[index]), result.distance, 1e-9);
        for (double[] centroid : LEAF_CENTROIDS) {
            Assert.assertTrue("a nearer centroid exists than the one returned",
                    result.distance <= euclidean.apply(query, centroid) + 1e-9);
        }
    }

    /** The structure the builder wrote carries the centroid vectors back, not just their ids. */
    @Test
    public void navigationReturnsTheCentroidVector() throws HyracksDataException {
        buildSingleLevelStructure();

        ClusterSearchResult result = navigate(LEAF_CENTROIDS[2]);

        Assert.assertNotNull(result);
        Assert.assertNotNull("the centroid vector must come back for the RNG/replica logic above", result.centroid);
        Assert.assertArrayEquals(LEAF_CENTROIDS[2], result.centroid, 1e-9);
    }

    /** One level and one cluster of {@link #LEAF_CENTROIDS}, written by the production builder. */
    private void buildSingleLevelStructure() throws HyracksDataException {
        List<Integer> clustersPerLevel = Arrays.asList(1);
        List<List<Integer>> centroidsPerCluster = Arrays.asList(Arrays.asList(LEAF_CENTROIDS.length));

        IIndexBulkLoader builder = tree.createStaticStructureBulkLoader(1, clustersPerLevel, centroidsPerCluster,
                MAX_ENTRIES_PER_PAGE, NoOpPageWriteCallback.INSTANCE);
        for (int i = 0; i < LEAF_CENTROIDS.length; i++) {
            builder.add(centroidTuple(FIRST_LEAF_CENTROID_ID + i, LEAF_CENTROIDS[i]));
        }
        builder.end();
        // The builder publishes the root through the page manager; copying it onto the tree is the
        // caller's step, the way LSMVTreeDiskComponent.setInitialized() does it. Without this the tree
        // still points at its create-time root and navigation lands on an empty page.
        tree.setRootPageId(tree.getPageManager().getRootPageId());
    }

    private ClusterSearchResult navigate(double[] queryVector) throws HyracksDataException {
        return VTreeNavigationUtils.findClosestCentroid(bufferCache, tree.getFileId(), tree.getRootPageId(),
                interiorFrameFactory, leafFrameFactory, queryVector, euclidean, null, null);
    }

    /** {@code <cid: int, embedding: double[]>}: the builder assigns the trailing pointer itself. */
    private static ITupleReference centroidTuple(int centroidId, double[] vector) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tupleRef = new ArrayTupleReference();
        ISerializerDeserializer[] serdes =
                { IntegerSerializerDeserializer.INSTANCE, DoubleArraySerializerDeserializer.INSTANCE };
        TupleUtils.createTuple(tupleBuilder, tupleRef, serdes, new Object[] { centroidId, vector });
        return tupleRef;
    }
}
