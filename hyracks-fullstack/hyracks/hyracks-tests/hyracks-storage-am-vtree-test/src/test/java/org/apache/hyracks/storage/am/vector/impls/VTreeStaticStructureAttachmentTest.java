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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.LongBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
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
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

/**
 * The attach, recycle and re-attach lifecycle by which a memory component borrows another component's static
 * clustering structure. A recycle must release the whole attachment, so every navigation accessor a search consults
 * returns to the component's own tree. The two trees differ in file id and root page, and the borrowed cache is a
 * distinct delegating object, so each accessor can tell borrowing from the component's own tree.
 */
public class VTreeStaticStructureAttachmentTest {

    private static final int PAGE_SIZE = 512;
    private static final int NUM_PAGES = 200;
    private static final int MAX_OPEN_FILES = 10;
    private static final int FRAME_SIZE = 32768;
    private static final int DIMENSIONS = 4;
    /** Non-quantized data tuple [distance, centroidId, pk: long], keyed on <distance, pk> as LSMVTreeUtils keys it. */
    private static final ITypeTraits[] DATA_TYPE_TRAITS =
            { DoublePointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS, LongPointable.TYPE_TRAITS };
    private static final int[] COMPARATOR_FIELDS =
            { VTreeDataTupleAccessor.DISTANCE_FIELD, VTreeDataTupleAccessor.NQ_KEY_FIELDS_START };
    private static final ITypeTraits[] KEY_TYPE_TRAITS = { DoublePointable.TYPE_TRAITS, LongPointable.TYPE_TRAITS };
    private static final IBinaryComparatorFactory[] KEY_CMP_FACTORIES =
            { DoubleBinaryComparatorFactory.INSTANCE, LongBinaryComparatorFactory.INSTANCE };
    /** One placement cluster per record and the canonical RNG factor, as the LSM fixtures place records. */
    private static final CrossPollinationConfig SINGLE_CLOSEST = new CrossPollinationConfig(1, 1.0);
    private static final double EPSILON = 0.25;

    /** Distinct from whatever the borrower's own root page is, so a stale borrow is visible. */
    private static final int STATIC_ROOT_PAGE = 5;
    private static final int NUM_LEAF_CENTROIDS = 3;
    private static final int FIRST_LEAF_CENTROID_ID = 11;

    private IHyracksTaskContext ctx;
    private IBufferCache bufferCache;
    /**
     * The static component's cache: a distinct object that delegates to {@link #bufferCache}, so an assertion can
     * tell a released attachment from a live one.
     */
    private IBufferCache staticCache;
    private FileReference staticFile;
    private FileReference memFile;
    private VTree staticTree;
    private VTree memTree;

    @Before
    public void setUp() throws HyracksDataException {
        ctx = TestUtils.create(FRAME_SIZE);
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        staticFile = ctx.getIoManager().getFileReference(0, "vtree-attachment-static");
        memFile = ctx.getIoManager().getFileReference(0, "vtree-attachment-mem");

        staticCache = Mockito.mock(IBufferCache.class, AdditionalAnswers.delegatesTo(bufferCache));
        staticTree = newTree(staticFile, staticCache);
        staticTree.create();
        staticTree.activate();
        staticTree.setRootPageId(STATIC_ROOT_PAGE);
        publishCentroidCounts(staticTree);

        memTree = newTree(memFile, bufferCache);
        memTree.create();
        memTree.activate();

        Assert.assertNotEquals("the two trees must differ for the fallback to be observable", staticTree.getFileId(),
                memTree.getFileId());
    }

    @After
    public void tearDown() throws HyracksDataException {
        memTree.deactivate();
        memTree.destroy();
        staticTree.deactivate();
        staticTree.destroy();
        bufferCache.close();
    }

    /** Before attaching, a component navigates its own tree. */
    @Test
    public void anUnattachedComponentNavigatesItsOwnTree() throws HyracksDataException {
        Assert.assertFalse(memTree.isInitialized());
        Assert.assertEquals(memTree.getFileId(), memTree.getNavigationFileId());
        Assert.assertEquals(memTree.getRootPageId(), memTree.getNavigationRootPageId());
        Assert.assertSame(bufferCache, memTree.getNavigationBufferCache());
    }

    /** After attaching, every navigation accessor reports the borrowed structure. */
    @Test
    public void anAttachedComponentNavigatesTheBorrowedStructure() throws HyracksDataException {
        attach();

        Assert.assertTrue(memTree.isInitialized());
        Assert.assertEquals(staticTree.getFileId(), memTree.getNavigationFileId());
        Assert.assertEquals(STATIC_ROOT_PAGE, memTree.getNavigationRootPageId());
        Assert.assertSame(staticCache, memTree.getNavigationBufferCache());
        Assert.assertEquals(NUM_LEAF_CENTROIDS, memTree.getNumLeafCentroidMem());
        Assert.assertEquals(FIRST_LEAF_CENTROID_ID, memTree.getFirstLeafCentroidIdMem());
    }

    /** A recycle releases the whole attachment, so no navigation accessor points at the released structure. */
    @Test
    public void recycleReleasesEveryPartOfTheAttachment() throws HyracksDataException {
        attach();
        Assert.assertEquals("precondition: the component is borrowing", staticTree.getFileId(),
                memTree.getNavigationFileId());

        memTree.resetInitialization();

        Assert.assertFalse(memTree.isInitialized());
        Assert.assertEquals("navigation still uses the released file", memTree.getFileId(),
                memTree.getNavigationFileId());
        Assert.assertEquals("navigation still uses the released root page", memTree.getRootPageId(),
                memTree.getNavigationRootPageId());
        // The delegating stand-in forwards toString(), so assertNotSame keeps a failure message legible.
        Assert.assertNotSame("navigation still uses the released buffer cache", staticCache,
                memTree.getNavigationBufferCache());
        Assert.assertSame(bufferCache, memTree.getNavigationBufferCache());
        // The centroid counts belong to the released structure, so reading them fails.
        Assert.assertThrows(HyracksDataException.class, memTree::getNumLeafCentroidMem);
        Assert.assertThrows(HyracksDataException.class, memTree::getFirstLeafCentroidIdMem);
    }

    /** A recycled component can be attached again, which is the post-flush path the LSM layer drives. */
    @Test
    public void aRecycledComponentCanAttachAgain() throws HyracksDataException {
        attach();
        memTree.resetInitialization();
        attach();

        Assert.assertTrue(memTree.isInitialized());
        Assert.assertEquals(staticTree.getFileId(), memTree.getNavigationFileId());
        Assert.assertEquals(NUM_LEAF_CENTROIDS, memTree.getNumLeafCentroidMem());
    }

    /** Attaching twice without a recycle is a no-op: the LSM layer relies on that idempotence. */
    @Test
    public void attachingTwiceIsIdempotent() throws HyracksDataException {
        attach();
        int rootAfterFirst = memTree.getNavigationRootPageId();
        int countAfterFirst = memTree.getNumLeafCentroidMem();

        attach();

        Assert.assertEquals(rootAfterFirst, memTree.getNavigationRootPageId());
        Assert.assertEquals(countAfterFirst, memTree.getNumLeafCentroidMem());
    }

    private void attach() throws HyracksDataException {
        VTree.VTreeAccessor accessor =
                (VTree.VTreeAccessor) staticTree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        try {
            memTree.setStaticStructure(accessor);
        } finally {
            accessor.destroy();
        }
    }

    /** The two counts {@code setStaticStructure} reads out of the static tree's metadata page. */
    private static void publishCentroidCounts(VTree tree) throws HyracksDataException {
        IPageManager pageManager = tree.getPageManager();
        ITreeIndexMetadataFrame metaFrame = pageManager.createMetadataFrame();
        pageManager.getMaxPageId(metaFrame);
        metaFrame.put(VTreeMetadataKeys.NUM_LEAF_CENTROIDS, LongPointable.FACTORY.createPointable(NUM_LEAF_CENTROIDS));
        metaFrame.put(VTreeMetadataKeys.FIRST_LEAF_CENTROID_ID,
                LongPointable.FACTORY.createPointable(FIRST_LEAF_CENTROID_ID));
    }

    private VTree newTree(FileReference file, IBufferCache cache) throws HyracksDataException {
        ITreeIndexFrameFactory interiorFrameFactory = new VTreeInteriorFrameFactory(DIMENSIONS, null, null);
        ITreeIndexFrameFactory leafFrameFactory = new VTreeLeafFrameFactory(DIMENSIONS, false, null, null);
        ITreeIndexFrameFactory metadataFrameFactory =
                new VTreeMetadataFrameFactory(DIMENSIONS, KEY_TYPE_TRAITS, KEY_CMP_FACTORIES, null, null);
        ITreeIndexFrameFactory dataFrameFactory =
                new VTreeDataFrameFactory(new LSMVTreeDataTupleWriterFactory(DATA_TYPE_TRAITS, false, null, null),
                        DIMENSIONS, COMPARATOR_FIELDS, KEY_CMP_FACTORIES, null);
        IPageManager pageManager = new LinkedMetadataPageManagerFactory().createPageManager(cache);
        return new VTree(cache, pageManager, interiorFrameFactory, leafFrameFactory, metadataFrameFactory,
                dataFrameFactory, new IBinaryComparatorFactory[] { null }, DIMENSIONS, DIMENSIONS, file,
                TestDoubleArrayVectorAccessor.Factory.INSTANCE, new VTreeDataTupleBuilderFactory(0, 1, false), null,
                new TestVTreeDistanceFunctionFactory("euclidean"), null, SINGLE_CLOSEST, EPSILON);
    }
}
