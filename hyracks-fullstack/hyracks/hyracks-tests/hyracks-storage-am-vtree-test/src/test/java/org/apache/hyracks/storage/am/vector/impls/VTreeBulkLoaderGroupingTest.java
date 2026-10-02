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
package org.apache.hyracks.storage.am.vector.impls;

import java.io.IOException;
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
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.vector.tuples.LSMVTreeDataTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.TestDoubleArrayVectorAccessor;
import org.apache.hyracks.storage.am.vector.TestVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeDataFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeMetadataFrameFactory;
import org.apache.hyracks.storage.am.vector.utils.CrossPollinationConfig;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.am.vector.utils.VTreeMetadataKeys;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.ISketchSampler;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The input contract of {@link VTreeBulkLoader}: tuples arrive grouped by centroid id in non-decreasing id order,
 * which the producer's sort supplies. Returning to a loaded cluster would record a second directory head over the
 * first and leave the first chain's records unreachable, so it must fail. An id outside the static structure's leaf
 * range must fail at the tuple that carries it.
 */
public class VTreeBulkLoaderGroupingTest {

    private static final int PAGE_SIZE = 512;
    private static final int NUM_PAGES = 200;
    private static final int MAX_OPEN_FILES = 10;
    private static final int FRAME_SIZE = 32768;
    private static final int DIMENSIONS = 2;
    /** One placement cluster per record and the canonical RNG factor, as the LSM fixtures place records. */
    private static final CrossPollinationConfig SINGLE_CLOSEST = new CrossPollinationConfig(1, 1.0);
    private static final double EPSILON = 0.25;
    private static final int MAX_ENTRIES_PER_PAGE = 8;

    /** Four leaf centroids, one per quadrant, so a load can leave a cluster and return to it. */
    private static final double[][] LEAF_CENTROIDS = { { 10, 10 }, { -10, 10 }, { -10, -10 }, { 10, -10 } };

    private IHyracksTaskContext ctx;
    private IBufferCache bufferCache;
    private VTree staticTree;
    private VTree dataTree;
    private VTree.VTreeAccessor staticAccessor;

    /** Read from the static structure's metadata, so the fixture cannot drift from the builder. */
    private int firstLeafCentroidId;
    private int numLeafCentroid;

    @Before
    public void setUp() throws HyracksDataException {
        ctx = TestUtils.create(FRAME_SIZE);
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());

        staticTree = newTree(ctx.getIoManager().getFileReference(0, "vtree-grouping-static"));
        staticTree.create();
        staticTree.activate();
        buildStaticStructure();
        readCentroidRange();

        dataTree = newTree(ctx.getIoManager().getFileReference(0, "vtree-grouping-data"));
        dataTree.create();
        dataTree.activate();
    }

    @After
    public void tearDown() throws HyracksDataException {
        if (staticAccessor != null) {
            staticAccessor.destroy();
        }
        dataTree.deactivate();
        dataTree.destroy();
        staticTree.deactivate();
        staticTree.destroy();
        bufferCache.close();
    }

    /** The shape the producer's sort yields: groups in ascending id order with repeats inside a group. */
    @Test
    public void groupedInputWithRepeatsWithinAGroupIsAccepted() throws HyracksDataException {
        CountingSampler sampler = new CountingSampler();
        IIndexBulkLoader loader = newLoader(sampler);

        loader.add(dataTuple(0.5, centroid(0), 1L));
        loader.add(dataTuple(1.5, centroid(0), 2L));
        loader.add(dataTuple(0.2, centroid(1), 3L));
        loader.add(dataTuple(0.9, centroid(1), 4L));
        loader.add(dataTuple(0.1, centroid(2), 5L));
        loader.end();

        // end() publishes the centroid range onto the loaded component, so reaching it means every cluster
        // was finalized.
        Assert.assertEquals(numLeafCentroid, readLong(dataTree, VTreeMetadataKeys.NUM_LEAF_CENTROIDS));
        Assert.assertEquals(firstLeafCentroidId, readLong(dataTree, VTreeMetadataKeys.FIRST_LEAF_CENTROID_ID));
        // The sketch call precedes the grouping check, so every added tuple reaches the sampler.
        Assert.assertEquals("every added tuple must reach the sketch sampler", 5, sampler.tuples());
    }

    /** Coming back to cluster 0 after cluster 1 fails at the tuple that breaks the order, naming both ids. */
    @Test
    public void returningToAnEarlierClusterIsRejected() throws HyracksDataException {
        IIndexBulkLoader loader = newLoader();
        loader.add(dataTuple(0.5, centroid(0), 1L));
        loader.add(dataTuple(0.5, centroid(1), 2L));

        HyracksDataException failure =
                Assert.assertThrows(HyracksDataException.class, () -> loader.add(dataTuple(0.7, centroid(0), 3L)));

        Assert.assertTrue("the message should name both ids, got: " + failure.getMessage(),
                failure.getMessage().contains(String.valueOf(centroid(0)))
                        && failure.getMessage().contains(String.valueOf(centroid(1))));
    }

    /** Unsorted input fails on its first backward step. */
    @Test
    public void descendingCentroidIdsAreRejected() throws HyracksDataException {
        IIndexBulkLoader loader = newLoader();
        loader.add(dataTuple(0.5, centroid(2), 1L));

        Assert.assertThrows(HyracksDataException.class, () -> loader.add(dataTuple(0.5, centroid(1), 2L)));
    }

    /** The first tuple bypasses {@code loadToNextLeafCluster}, so {@code add} bounds-checks its centroid id. */
    @Test
    public void anOutOfRangeCentroidIdOnTheFirstTupleFailsAtAdd() throws HyracksDataException {
        IIndexBulkLoader above = newLoader();
        HyracksDataException failure = Assert.assertThrows(HyracksDataException.class,
                () -> above.add(dataTuple(0.5, firstLeafCentroidId + numLeafCentroid, 1L)));
        Assert.assertTrue("the message should name the offending id, got: " + failure.getMessage(),
                failure.getMessage().contains(String.valueOf(firstLeafCentroidId + numLeafCentroid)));

        IIndexBulkLoader below = newLoader();
        Assert.assertThrows(HyracksDataException.class, () -> below.add(dataTuple(0.5, firstLeafCentroidId - 1, 2L)));
    }

    /** An out-of-range id also fails on the cluster-switch path. */
    @Test
    public void anOutOfRangeCentroidIdOnASwitchFailsAtAdd() throws HyracksDataException {
        IIndexBulkLoader loader = newLoader();
        loader.add(dataTuple(0.5, centroid(0), 1L));

        Assert.assertThrows(HyracksDataException.class,
                () -> loader.add(dataTuple(0.5, firstLeafCentroidId + numLeafCentroid, 2L)));
    }

    /**
     * {@code loadToNextLeafCluster} is public, so a caller can move backwards without {@code add}'s ordering
     * check. The guard where a directory head is recorded catches that path.
     */
    @Test
    public void aBackwardJumpThroughThePublicSwitchIsRejected() throws HyracksDataException {
        VTreeBulkLoader loader = newLoader();
        loader.add(dataTuple(0.5, centroid(0), 1L));
        loader.add(dataTuple(0.5, centroid(1), 2L));

        // Cluster 0's chain is already recorded; re-opening it must not overwrite that head.
        loader.loadToNextLeafCluster(0);

        HyracksDataException failure = Assert.assertThrows(HyracksDataException.class, loader::end);
        Assert.assertTrue("the message should say the chain would be orphaned, got: " + failure.getMessage(),
                failure.getMessage().contains("orphan"));
    }

    private int centroid(int clusterIndex) {
        return firstLeafCentroidId + clusterIndex;
    }

    private VTreeBulkLoader newLoader() throws HyracksDataException {
        return newLoader(new CountingSampler());
    }

    private VTreeBulkLoader newLoader(ISketchSampler sampler) throws HyracksDataException {
        if (staticAccessor != null) {
            staticAccessor.destroy();
        }
        staticAccessor = (VTree.VTreeAccessor) staticTree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        // No data-frame override: the initial-load path, which writes with the tree's own data frames.
        return (VTreeBulkLoader) dataTree.createComponentBulkLoader(NoOpPageWriteCallback.INSTANCE, staticAccessor,
                sampler, null);
    }

    /** {@code [distance: double, centroidId: int, pk: long]}: the non-quantized data-tuple layout. */
    private static ITupleReference dataTuple(double distance, int centroidId, long primaryKey)
            throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);
        ArrayTupleReference tupleRef = new ArrayTupleReference();
        ISerializerDeserializer[] serdes = { DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, Integer64SerializerDeserializer.INSTANCE };
        TupleUtils.createTuple(tupleBuilder, tupleRef, serdes, new Object[] { distance, centroidId, primaryKey });
        return tupleRef;
    }

    private void buildStaticStructure() throws HyracksDataException {
        List<Integer> clustersPerLevel = Arrays.asList(1);
        List<List<Integer>> centroidsPerCluster = Arrays.asList(Arrays.asList(LEAF_CENTROIDS.length));

        IIndexBulkLoader builder = staticTree.createStaticStructureBulkLoader(1, clustersPerLevel, centroidsPerCluster,
                MAX_ENTRIES_PER_PAGE, NoOpPageWriteCallback.INSTANCE);
        for (int i = 0; i < LEAF_CENTROIDS.length; i++) {
            builder.add(centroidTuple(i, LEAF_CENTROIDS[i]));
        }
        builder.end();
        // The builder publishes the root through the page manager; copying it onto the tree is the caller's
        // step, the way LSMVTreeDiskComponent.setInitialized() does it.
        staticTree.setRootPageId(staticTree.getPageManager().getRootPageId());
    }

    private void readCentroidRange() throws HyracksDataException {
        numLeafCentroid = (int) readLong(staticTree, VTreeMetadataKeys.NUM_LEAF_CENTROIDS);
        firstLeafCentroidId = (int) readLong(staticTree, VTreeMetadataKeys.FIRST_LEAF_CENTROID_ID);
        Assert.assertEquals("the fixture needs every leaf centroid addressable", LEAF_CENTROIDS.length,
                numLeafCentroid);
    }

    private static long readLong(VTree tree, IValueReference key) throws HyracksDataException {
        IPageManager pageManager = tree.getPageManager();
        ITreeIndexMetadataFrame metaFrame = pageManager.createMetadataFrame();
        pageManager.getMaxPageId(metaFrame);
        LongPointable value = LongPointable.FACTORY.createPointable();
        metaFrame.get(key, value);
        return value.longValue();
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

    private VTree newTree(FileReference file) throws HyracksDataException {
        ITreeIndexFrameFactory interiorFrameFactory = new VTreeInteriorFrameFactory(DIMENSIONS, null, null);
        ITreeIndexFrameFactory leafFrameFactory = new VTreeLeafFrameFactory(DIMENSIONS, false, null, null);
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
        return new VTree(bufferCache, pageManager, interiorFrameFactory, leafFrameFactory,
                new VTreeMetadataFrameFactory(DIMENSIONS, keyTypeTraits, keyCmpFactories, null, null), dataFrameFactory,
                new IBinaryComparatorFactory[] { null }, DIMENSIONS, DIMENSIONS, file,
                TestDoubleArrayVectorAccessor.Factory.INSTANCE, new VTreeDataTupleBuilderFactory(0, 1, false), null,
                new TestVTreeDistanceFunctionFactory("euclidean"), null, SINGLE_CLOSEST, EPSILON);
    }

    /** Counts {@code addTuple} calls. {@code serialize()} belongs to the LSM layer, so a loader calling it fails. */
    private static final class CountingSampler implements ISketchSampler {
        private int tuples;

        @Override
        public IValueReference serialize() throws IOException {
            throw new UnsupportedOperationException("the bulk loader is not expected to serialize the sketch");
        }

        @Override
        public void addTuple(ITupleReference tuple) {
            tuples++;
        }

        int tuples() {
            return tuples;
        }
    }
}
