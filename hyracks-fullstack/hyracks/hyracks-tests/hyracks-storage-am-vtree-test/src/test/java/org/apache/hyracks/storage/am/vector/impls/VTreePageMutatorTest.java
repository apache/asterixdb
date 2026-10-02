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

import static org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider.NEW;

import java.util.HashSet;
import java.util.Set;

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
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.vector.tuples.LSMVTreeDataTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.TestDoubleArrayVectorAccessor;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.frames.VTreeDataFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.frames.VTreeMetadataFrameFactory;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link VTreePageMutator} against a real buffer cache, with no LSM component. Data pages split, and a directory
 * page splits while it is not last in its chain. Assertions are on reachability: everything inserted must be found
 * by walking the directory chain the way the mutator walks it, so an orphaned chain shows up as missing tuples.
 */
public class VTreePageMutatorTest {

    /** Small pages so that a few hundred tuples are enough to split data and directory pages. */
    private static final int PAGE_SIZE = 512;
    private static final int NUM_PAGES = 4000;
    private static final int MAX_OPEN_FILES = 10;
    private static final int FRAME_SIZE = 32768;
    private static final int DIMENSIONS = 4;
    private static final int CENTROID_ID = 7;

    private IHyracksTaskContext ctx;
    private IBufferCache bufferCache;
    private FileReference file;
    private int fileId;
    private IPageManager freePageManager;
    private ITreeIndexFrameFactory metadataFrameFactory;
    private ITreeIndexFrameFactory dataFrameFactory;
    private VTreeOpContext opCtx;
    private VTreePageMutator mutator;
    private long headDirectoryPage;
    private int inserted;

    @Before
    public void setUp() throws HyracksDataException {
        ctx = TestUtils.create(FRAME_SIZE);
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        file = ctx.getIoManager().getFileReference(0, "vtree-page-mutator-test");

        ITreeIndexFrameFactory interiorFrameFactory = new VTreeInteriorFrameFactory(DIMENSIONS, null, null);
        ITreeIndexFrameFactory leafFrameFactory = new VTreeLeafFrameFactory(DIMENSIONS, false, null, null);
        // Non-quantized data tuple: <distance, centroidId, pk>, keyed on <distance, pk> as LSMVTreeUtils keys
        // it. The production tuple writer is used so the layout under test is the real one.
        ITypeTraits[] dataTraits =
                { DoublePointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS, LongPointable.TYPE_TRAITS };
        int[] comparatorFields = { VTreeDataTupleAccessor.DISTANCE_FIELD, VTreeDataTupleAccessor.NQ_KEY_FIELDS_START };
        IBinaryComparatorFactory[] keyCmpFactories =
                { DoubleBinaryComparatorFactory.INSTANCE, LongBinaryComparatorFactory.INSTANCE };
        ITypeTraits[] keyTypeTraits = { DoublePointable.TYPE_TRAITS, LongPointable.TYPE_TRAITS };
        metadataFrameFactory = new VTreeMetadataFrameFactory(DIMENSIONS, keyTypeTraits, keyCmpFactories, null, null);
        dataFrameFactory = new VTreeDataFrameFactory(new LSMVTreeDataTupleWriterFactory(dataTraits, false, null, null),
                DIMENSIONS, comparatorFields, keyCmpFactories, null);

        freePageManager = new LinkedMetadataPageManagerFactory().createPageManager(bufferCache);
        fileId = bufferCache.createFile(file);
        bufferCache.openFile(fileId);
        freePageManager.open(fileId);
        freePageManager.init(interiorFrameFactory, leafFrameFactory);

        opCtx = new VTreeOpContext(null, interiorFrameFactory, leafFrameFactory, metadataFrameFactory, dataFrameFactory,
                freePageManager, null, DIMENSIONS, NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE,
                new VTreeDataTupleBuilderFactory(0, 1, false), null, TestDoubleArrayVectorAccessor.Factory.INSTANCE);
        opCtx.setOperation(IndexOperation.INSERT);
        mutator = new VTreePageMutator(bufferCache, freePageManager, metadataFrameFactory);
        headDirectoryPage = newDirectoryPage();
        inserted = 0;
    }

    @After
    public void tearDown() throws HyracksDataException {
        bufferCache.closeFile(fileId);
        bufferCache.close();
        file.delete();
    }

    /**
     * The first insert into an empty directory takes the create-the-first-data-page path, which is the
     * one that must leave the data-page chain alone because there is no predecessor to link from.
     */
    @Test
    public void firstInsertCreatesAndRegistersOneDataPage() throws HyracksDataException {
        insert(1.0, 1L);

        Assert.assertEquals(1, directoryEntryCount());
        Assert.assertEquals(1, reachableTupleCount());
        Assert.assertEquals(1, chainLength());
    }

    /**
     * Ascending distances fill and split data pages. Every tuple stays reachable, and the separators stay
     * ordered across each split, since key routing depends on it.
     */
    @Test
    public void tuplesSurviveDataPageSplits() throws HyracksDataException {
        for (int i = 0; i < 200; i++) {
            insert(100.0 + i, i);
        }

        Assert.assertTrue("expected data pages to have split", directoryEntryCount() > 1);
        Assert.assertEquals(inserted, reachableTupleCount());
        assertDirectoryOrdering();
    }

    /**
     * A directory page that splits while it is not last in its chain keeps its successor. Ascending inserts grow
     * the chain past one page, then low-distance inserts land on the first directory page and overflow it again.
     */
    @Test
    public void aNonLastDirectoryPageKeepsItsChainWhenItSplits() throws HyracksDataException {
        // Phase 1: ascending distances until the directory itself has split at least once.
        for (int i = 0; chainLength() < 2; i++) {
            Assert.assertTrue("directory never split; raise the insert bound or lower PAGE_SIZE", i < 4000);
            insert(1000.0 + i, i);
        }
        int chainAfterFirstSplit = chainLength();

        // Phase 2: distances below everything inserted so far, so routing lands on the first directory
        // page and its data pages split, adding entries to a page that is not last.
        for (int i = 0; i < 600; i++) {
            insert(i / 600.0, 100000L + i);
        }

        Assert.assertTrue("the first directory page never split again, so the case was not exercised",
                chainLength() > chainAfterFirstSplit);
        Assert.assertEquals("directory chain lost pages after a non-last split", inserted, reachableTupleCount());
        assertDirectoryOrdering();
    }

    /** A physically deleted tuple is gone; a primary key that was never inserted is reported as absent. */
    @Test
    public void physicalDeleteRemovesOnlyTheMatchingTuple() throws HyracksDataException {
        for (int i = 0; i < 50; i++) {
            insert(10.0 + i, i);
        }
        int before = reachableTupleCount();

        Assert.assertTrue(mutator.tryPhysicalDelete(headDirectoryPage, 20.0, sourceTuple(10L), opCtx, fileId));
        Assert.assertEquals(before - 1, reachableTupleCount());

        Assert.assertFalse(mutator.tryPhysicalDelete(headDirectoryPage, 20.0, sourceTuple(999L), opCtx, fileId));
        Assert.assertEquals(before - 1, reachableTupleCount());
    }

    private void insert(double distance, long pk) throws HyracksDataException {
        mutator.insertIntoDataPages(headDirectoryPage, new double[DIMENSIONS], distance, CENTROID_ID, sourceTuple(pk),
                opCtx, fileId);
        inserted++;
    }

    /** Source tuple shape the data-tuple builder expects with no include fields: [vector, pk]. */
    private static ITupleReference sourceTuple(long pk) throws HyracksDataException {
        ArrayTupleBuilder builder = new ArrayTupleBuilder(2);
        try {
            builder.getDataOutput().writeDouble(0.0); // stands in for the vector; the builder never reads it
            builder.addFieldEndOffset();
            builder.getDataOutput().writeLong(pk);
            builder.addFieldEndOffset();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(builder.getFieldEndOffsets(), builder.getByteArray());
        return tuple;
    }

    private long newDirectoryPage() throws HyracksDataException {
        ITreeIndexMetadataFrame metaFrame = freePageManager.createMetadataFrame();
        int pageId = freePageManager.takePage(metaFrame);
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), NEW);
        try {
            page.acquireWriteLatch();
            IVTreeMetadataFrame frame = (IVTreeMetadataFrame) metadataFrameFactory.createFrame();
            frame.setPage(page);
            frame.initBuffer((byte) 0);
            frame.setNextPage(VTreeDataTupleAccessor.NO_NEXT_PAGE);
            page.releaseWriteLatch(true);
        } finally {
            bufferCache.unpin(page);
        }
        return pageId;
    }

    private int chainLength() throws HyracksDataException {
        int[] count = { 0 };
        walkDirectory((frame, dirPageId) -> count[0]++);
        return count[0];
    }

    private int directoryEntryCount() throws HyracksDataException {
        int[] count = { 0 };
        walkDirectory((frame, dirPageId) -> count[0] += frame.getTupleCount());
        return count[0];
    }

    private int reachableTupleCount() throws HyracksDataException {
        int[] total = { 0 };
        Set<Long> dataPages = new HashSet<>();
        walkDirectory((frame, dirPageId) -> {
            for (int i = 0; i < frame.getTupleCount(); i++) {
                long dataPageId = frame.getDataPagePointer(i);
                Assert.assertTrue("data page " + dataPageId + " registered twice", dataPages.add(dataPageId));
                total[0] += tupleCountIn(dataPageId);
            }
        });
        return total[0];
    }

    /**
     * Separators must ascend by {@code <distance, pk>} within a page and across the chain: routing relies on
     * it. Each separator is the key of the last record on its data page, and keys are unique, so no two match.
     */
    private void assertDirectoryOrdering() throws HyracksDataException {
        double[] previousDistance = { Double.NEGATIVE_INFINITY };
        long[] previousPk = { Long.MIN_VALUE };
        walkDirectory((frame, dirPageId) -> {
            ITreeIndexTupleReference separator = ((VTreeMetadataFrame) frame).createTupleReference();
            for (int i = 0; i < frame.getTupleCount(); i++) {
                separator.resetByTupleIndex(frame, i);
                double distance = DoublePointable.getDouble(separator.getFieldData(0), separator.getFieldStart(0));
                long pk = LongPointable.getLong(separator.getFieldData(1), separator.getFieldStart(1));
                int order = Double.compare(distance, previousDistance[0]);
                Assert.assertTrue(
                        "directory out of order on page " + dirPageId + " entry " + i + ": <" + distance + ", " + pk
                                + "> after <" + previousDistance[0] + ", " + previousPk[0] + ">",
                        order > 0 || (order == 0 && pk > previousPk[0]));
                previousDistance[0] = distance;
                previousPk[0] = pk;
            }
        });
    }

    private int tupleCountIn(long dataPageId) throws HyracksDataException {
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) dataPageId));
        try {
            page.acquireReadLatch();
            try {
                IVTreeDataFrame frame = (IVTreeDataFrame) dataFrameFactory.createFrame();
                frame.setPage(page);
                return frame.getTupleCount();
            } finally {
                page.releaseReadLatch();
            }
        } finally {
            bufferCache.unpin(page);
        }
    }

    private void walkDirectory(DirectoryVisitor visitor) throws HyracksDataException {
        Set<Long> visited = new HashSet<>();
        long dirPageId = headDirectoryPage;
        while (dirPageId != VTreeDataTupleAccessor.NO_NEXT_PAGE) {
            Assert.assertTrue("cycle in the directory chain at page " + dirPageId, visited.add(dirPageId));
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) dirPageId));
            try {
                page.acquireReadLatch();
                try {
                    IVTreeMetadataFrame frame = (IVTreeMetadataFrame) metadataFrameFactory.createFrame();
                    frame.setPage(page);
                    visitor.visit(frame, dirPageId);
                    dirPageId = frame.getNextPage();
                } finally {
                    page.releaseReadLatch();
                }
            } finally {
                bufferCache.unpin(page);
            }
        }
    }

    private interface DirectoryVisitor {
        void visit(IVTreeMetadataFrame frame, long dirPageId) throws HyracksDataException;
    }
}
