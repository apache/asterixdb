/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.io.File;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree.RTreeBulkLoader;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTreeWithAntiMatterTuples extends AbstractLSMRTree {

    private TreeTupleSorter bTreeTupleSorter = null;

    // On-disk components.
    // For creating RTree's used in bulk load. Different from diskRTreeFactory
    // because it should have a different tuple writer in it's leaf frames.
    private final TreeFactory<RTree> bulkLoadRTreeFactory;

    public LSMRTreeWithAntiMatterTuples(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMFileManager fileManager, TreeFactory<RTree> diskRTreeFactory, TreeFactory<RTree> bulkLoadRTreeFactory,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, ILSMFlushController flushController,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOScheduler ioScheduler) {
        super(memBufferCache, memFreePageManager, rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, fileManager, diskRTreeFactory, diskFileMapProvider,
                new TreeIndexComponentFinalizer(diskFileMapProvider), fieldCount, rtreeCmpFactories, btreeCmpFactories,
                linearizer, comparatorFields, linearizerArray, flushController, mergePolicy, opTracker, ioScheduler);
        this.bulkLoadRTreeFactory = bulkLoadRTreeFactory;

    }

    /**
     * Opens LSMRTree, cleaning up invalid files from base dir, and registering
     * all valid files as on-disk RTrees and BTrees.
     * 
     * @param fileReference
     *            Dummy file id.
     * @throws HyracksDataException
     */
    @Override
    public synchronized void open() throws HyracksDataException {
        super.open();
        List<Object> validFileNames = fileManager.cleanupAndGetValidFiles(componentFinalizer);
        for (Object o : validFileNames) {
            String fileName = (String) o;
            FileReference fileRef = new FileReference(new File(fileName));
            RTree rtree = (RTree) createDiskTree(diskRTreeFactory, fileRef, false);
            diskComponents.add(rtree);
        }
    }

    @Override
    public synchronized void close() throws HyracksDataException {
        for (Object o : diskComponents) {
            RTree rtree = (RTree) o;
            rtree.close();
        }
        diskComponents.clear();
        super.close();
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        for (Object o : diskComponents) {
            RTree rtree = (RTree) o;
            rtree.destroy();
        }
        super.destroy();
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        for (Object o : diskComponents) {
            RTree rtree = (RTree) o;
            rtree.close();
            rtree.destroy();
        }
        diskComponents.clear();
        super.clear();
    }

    private RTree createFlushTarget() throws HyracksDataException {
        String relFlushFileName = (String) fileManager.getRelFlushFileName();
        FileReference fileRef = fileManager.createFlushFile(relFlushFileName);
        return (RTree) createDiskTree(diskRTreeFactory, fileRef, true);
    }

    private RTree createMergeTarget(List<Object> mergingDiskRTrees) throws HyracksDataException {
        RTree firstRTree = (RTree) mergingDiskRTrees.get(0);
        RTree lastRTree = (RTree) mergingDiskRTrees.get(mergingDiskRTrees.size() - 1);
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstRTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastRTree.getFileId());
        String relMergeFileName = (String) fileManager.getRelMergeFileName(firstFile.getFile().getName(), lastFile
                .getFile().getName());
        FileReference fileRef = fileManager.createMergeFile(relMergeFileName);
        return (RTree) createDiskTree(diskRTreeFactory, fileRef, true);
    }

    public void search(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred, IIndexOpContext ictx,
            boolean includeMemComponent, AtomicInteger searcherRefCount) throws HyracksDataException, IndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        LSMRTreeWithAntiMatterTuplesSearchCursor lsmTreeCursor = (LSMRTreeWithAntiMatterTuplesSearchCursor) cursor;
        int numDiskRTrees = diskComponents.size();

        LSMRTreeCursorInitialState initialState;
        ITreeIndexAccessor[] bTreeAccessors = null;
        if (includeMemComponent) {
            // Only in-memory BTree
            bTreeAccessors = new ITreeIndexAccessor[1];
            bTreeAccessors[0] = ctx.memBTreeAccessor;
        }

        initialState = new LSMRTreeCursorInitialState(numDiskRTrees, rtreeLeafFrameFactory, rtreeInteriorFrameFactory,
                btreeLeafFrameFactory, ctx.getBTreeMultiComparator(), null, bTreeAccessors, searcherRefCount,
                includeMemComponent, lsmHarness, comparatorFields, linearizerArray, ctx.searchCallback);

        lsmTreeCursor.open(initialState, pred);

        if (includeMemComponent) {
            // Open cursor of in-memory RTree
            ctx.memRTreeAccessor.search(lsmTreeCursor.getMemRTreeCursor(), pred);
        }

        // Open cursors of on-disk RTrees.
        ITreeIndexAccessor[] diskRTreeAccessors = new ITreeIndexAccessor[numDiskRTrees];
        int diskRTreeIx = 0;
        ListIterator<Object> diskRTreesIter = diskComponents.listIterator();
        while (diskRTreesIter.hasNext()) {
            RTree diskRTree = (RTree) diskRTreesIter.next();
            diskRTreeAccessors[diskRTreeIx] = diskRTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            diskRTreeAccessors[diskRTreeIx].search(lsmTreeCursor.getCursor(diskRTreeIx), pred);
            diskRTreeIx++;
        }
        lsmTreeCursor.initPriorityQueue();
    }

    @Override
    public ITreeIndex flush() throws HyracksDataException, IndexException {
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.

        // scan the memory RTree
        ITreeIndexAccessor memRTreeAccessor = memComponent.getRTree().createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        RTreeSearchCursor rtreeScanCursor = (RTreeSearchCursor) memRTreeAccessor.createSearchCursor();
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);
        RTree diskRTree = createFlushTarget();

        // scan the memory BTree
        ITreeIndexAccessor memBTreeAccessor = memComponent.getBTree().createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        BTreeRangeSearchCursor btreeScanCursor = (BTreeRangeSearchCursor) memBTreeAccessor.createSearchCursor();
        RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
        memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);

        // Since the LSM-RTree is used as a secondary assumption, the
        // primary key will be the last comparator in the BTree comparators
        if (rTreeTupleSorter == null) {
            rTreeTupleSorter = new TreeTupleSorter(memRTreeTuples, memComponent.getRTree().getFileId(),
                    linearizerArray, rtreeLeafFrameFactory.createFrame(), rtreeLeafFrameFactory.createFrame(),
                    memComponent.getRTree().getBufferCache(), comparatorFields);

            bTreeTupleSorter = new TreeTupleSorter(memBTreeTuples, memComponent.getBTree().getFileId(),
                    linearizerArray, btreeLeafFrameFactory.createFrame(), btreeLeafFrameFactory.createFrame(),
                    memComponent.getBTree().getBufferCache(), comparatorFields);
        } else {
            rTreeTupleSorter.reset();
            bTreeTupleSorter.reset();
        }
        // BulkLoad the tuples from the in-memory tree into the new disk
        // RTree.

        boolean isEmpty = true;
        if (rtreeScanCursor.hasNext()) {
            isEmpty = false;
        }
        try {
            while (rtreeScanCursor.hasNext()) {
                rtreeScanCursor.next();
                rTreeTupleSorter.insertTupleEntry(rtreeScanCursor.getPageId(), rtreeScanCursor.getTupleOffset());
            }
        } finally {
            rtreeScanCursor.close();
        }
        if (!isEmpty) {
            rTreeTupleSorter.sort();
        }

        isEmpty = true;
        if (btreeScanCursor.hasNext()) {
            isEmpty = false;
        }
        try {
            while (btreeScanCursor.hasNext()) {
                btreeScanCursor.next();
                bTreeTupleSorter.insertTupleEntry(btreeScanCursor.getPageId(), btreeScanCursor.getTupleOffset());
            }
        } finally {
            btreeScanCursor.close();
        }
        if (!isEmpty) {
            bTreeTupleSorter.sort();
        }

        IIndexBulkLoader rTreeBulkloader = diskRTree.createBulkLoader(1.0f);
        LSMRTreeFlushCursor cursor = new LSMRTreeFlushCursor(rTreeTupleSorter, bTreeTupleSorter, comparatorFields,
                linearizerArray);
        cursor.open(null, null);

        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();

                rTreeBulkloader.add(frameTuple);
            }
        } finally {
            cursor.close();
        }

        rTreeBulkloader.end();

        return diskRTree;
    }

    @Override
    public ITreeIndex merge(List<Object> mergedComponents) throws HyracksDataException, IndexException {
        LSMRTreeOpContext ctx = createOpContext();
        ITreeIndexCursor cursor = new LSMRTreeWithAntiMatterTuplesSearchCursor();
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        // Ordered scan, ignoring the in-memory RTree.
        // We get back a snapshot of the on-disk RTrees that are going to be
        // merged now, so we can clean them up after the merge has completed.
        List<Object> mergingDiskRTrees = lsmHarness.search(cursor, (SearchPredicate) rtreeSearchPred, ctx, false);
        mergedComponents.addAll(mergingDiskRTrees);

        // Nothing to merge.
        if (mergedComponents.size() <= 1) {
            cursor.close();
            return null;
        }

        // Bulk load the tuples from all on-disk RTrees into the new RTree.
        RTree mergedRTree = createMergeTarget(mergingDiskRTrees);
        IIndexBulkLoader bulkloader = mergedRTree.createBulkLoader(1.0f);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                bulkloader.add(frameTuple);
            }
        } finally {
            cursor.close();
        }
        bulkloader.end();
        return mergedRTree;
    }

    @Override
    public void cleanUpAfterMerge(List<Object> mergedComponents) throws HyracksDataException {
        for (Object o : mergedComponents) {
            RTree oldRTree = (RTree) o;
            FileReference fileRef = diskFileMapProvider.lookupFileName(oldRTree.getFileId());
            oldRTree.close();
            fileRef.getFile().delete();
        }
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMRTreeWithAntiMatterTuplesAccessor(lsmHarness, createOpContext());
    }

    public class LSMRTreeWithAntiMatterTuplesAccessor extends LSMTreeIndexAccessor {
        public LSMRTreeWithAntiMatterTuplesAccessor(LSMHarness lsmHarness, IIndexOpContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            return new LSMRTreeWithAntiMatterTuplesSearchCursor();
        }

        public MultiComparator getMultiComparator() {
            LSMRTreeOpContext concreteCtx = (LSMRTreeOpContext) ctx;
            return concreteCtx.rtreeOpContext.cmp;
        }
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel) throws TreeIndexException {
        return new LSMRTreeWithAntiMatterTuplesBulkLoader(fillLevel);
    }

    private RTree createBulkLoadTarget() throws HyracksDataException {
        String relFlushFileName = (String) fileManager.getRelFlushFileName();
        FileReference fileRef = fileManager.createFlushFile(relFlushFileName);
        return (RTree) createDiskTree(bulkLoadRTreeFactory, fileRef, true);
    }

    public class LSMRTreeWithAntiMatterTuplesBulkLoader implements IIndexBulkLoader {
        private final RTree diskRTree;
        private final RTreeBulkLoader bulkLoader;

        public LSMRTreeWithAntiMatterTuplesBulkLoader(float fillFactor) throws TreeIndexException {
            // Note that by using a flush target file name, we state that the
            // new bulk loaded tree is "newer" than any other merged tree.
            try {
                diskRTree = createBulkLoadTarget();
            } catch (HyracksDataException e) {
                throw new TreeIndexException(e);
            }
            bulkLoader = (RTreeBulkLoader) diskRTree.createBulkLoader(fillFactor);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            bulkLoader.add(tuple);
        }

        @Override
        public void end() throws HyracksDataException {
            bulkLoader.end();
            lsmHarness.addBulkLoadedComponent(diskRTree);
        }

    }
}
