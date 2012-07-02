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
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
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
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeFileManager.LSMRTreeFileNameComponent;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree.RTreeBulkLoader;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTree extends AbstractLSMRTree {

    // On-disk components.
    // For creating BTree's used in flush and merge.
    private final BTreeFactory diskBTreeFactory;

    public LSMRTree(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMFileManager fileManager, RTreeFactory diskRTreeFactory, BTreeFactory diskBTreeFactory,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, ILSMFlushController flushController,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOScheduler ioScheduler) {
        super(memBufferCache, memFreePageManager, rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, fileManager, diskRTreeFactory, diskFileMapProvider,
                new LSMRTreeComponentFinalizer(diskFileMapProvider), fieldCount, rtreeCmpFactories, btreeCmpFactories,
                linearizer, comparatorFields, linearizerArray, flushController, mergePolicy, opTracker, ioScheduler);
        this.diskBTreeFactory = diskBTreeFactory;
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
    public void open(FileReference fileReference) throws HyracksDataException {
        super.open(fileReference);
        RTree dummyRTree = diskRTreeFactory.createIndexInstance();
        dummyRTree.create(new FileReference(new File("dummyrtree")));
        BTree dummyBTree = diskBTreeFactory.createIndexInstance();
        dummyBTree.create(new FileReference(new File("dummybtree")));
        LSMRTreeComponent dummyComponent = new LSMRTreeComponent(dummyRTree, dummyBTree);
        List<Object> validFileNames = fileManager.cleanupAndGetValidFiles(dummyComponent, componentFinalizer);
        for (Object o : validFileNames) {
            LSMRTreeFileNameComponent component = (LSMRTreeFileNameComponent) o;
            FileReference rtreeFile = new FileReference(new File(component.getRTreeFileName()));
            FileReference btreeFile = new FileReference(new File(component.getBTreeFileName()));
            RTree rtree = (RTree) createDiskTree(diskRTreeFactory, rtreeFile, false);
            BTree btree = (BTree) createDiskTree(diskBTreeFactory, btreeFile, false);
            LSMRTreeComponent diskComponent = new LSMRTreeComponent(rtree, btree);
            diskComponents.add(diskComponent);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        for (Object o : diskComponents) {
            LSMRTreeComponent diskComponent = (LSMRTreeComponent) o;
            RTree rtree = diskComponent.getRTree();
            BTree btree = diskComponent.getBTree();
            rtree.close();
            btree.close();
        }
        diskComponents.clear();
        super.close();
    }

    private LSMRTreeFileNameComponent getMergeTargetFileName(List<Object> mergingDiskTrees) throws HyracksDataException {
        RTree firstTree = ((LSMRTreeComponent) mergingDiskTrees.get(0)).getRTree();
        RTree lastTree = ((LSMRTreeComponent) mergingDiskTrees.get(mergingDiskTrees.size() - 1)).getRTree();
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastTree.getFileId());
        LSMRTreeFileNameComponent component = (LSMRTreeFileNameComponent) ((LSMRTreeFileManager) fileManager)
                .getRelMergeFileName(firstFile.getFile().getName(), lastFile.getFile().getName());
        return component;
    }

    public void search(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred, IIndexOpContext ictx,
            boolean includeMemComponent, AtomicInteger searcherRefCount) throws HyracksDataException, IndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        int numDiskTrees = diskComponents.size();
        int numTrees = (includeMemComponent) ? numDiskTrees + 1 : numDiskTrees;

        ITreeIndexAccessor[] rTreeAccessors = new ITreeIndexAccessor[numTrees];
        ITreeIndexAccessor[] bTreeAccessors = new ITreeIndexAccessor[numTrees];
        int diskTreeIx = 0;
        if (includeMemComponent) {
            rTreeAccessors[0] = ctx.memRTreeAccessor;
            bTreeAccessors[0] = ctx.memBTreeAccessor;
            diskTreeIx++;
        }

        ListIterator<Object> diskTreesIter = diskComponents.listIterator();
        while (diskTreesIter.hasNext()) {
            LSMRTreeComponent component = (LSMRTreeComponent) diskTreesIter.next();
            RTree diskRTree = component.getRTree();
            BTree diskBTree = component.getBTree();
            rTreeAccessors[diskTreeIx] = diskRTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            bTreeAccessors[diskTreeIx] = diskBTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            diskTreeIx++;
        }

        LSMRTreeCursorInitialState initialState = new LSMRTreeCursorInitialState(numTrees, rtreeLeafFrameFactory,
                rtreeInteriorFrameFactory, btreeLeafFrameFactory, ctx.getBTreeMultiComparator(), rTreeAccessors,
                bTreeAccessors, searcherRefCount, includeMemComponent, lsmHarness, comparatorFields, linearizerArray,
                ctx.searchCallback);
        cursor.open(initialState, pred);
    }

    @Override
    public Object flush() throws HyracksDataException, IndexException {
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.

        // scan the memory RTree
        ITreeIndexAccessor memRTreeAccessor = memComponent.getRTree().createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        RTreeSearchCursor rtreeScanCursor = (RTreeSearchCursor) memRTreeAccessor.createSearchCursor();
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);
        LSMRTreeFileNameComponent fileNames = (LSMRTreeFileNameComponent) fileManager.getRelFlushFileName();
        FileReference rtreeFile = fileManager.createFlushFile(fileNames.getRTreeFileName());
        RTree diskRTree = (RTree) createDiskTree(diskRTreeFactory, rtreeFile, true);
        IIndexBulkLoader rTreeBulkloader;
        ITreeIndexCursor cursor;

        IBinaryComparatorFactory[] linearizerArray = { linearizer };

        if (rTreeTupleSorter == null) {
            rTreeTupleSorter = new TreeTupleSorter(memRTreeTuples, memComponent.getRTree().getFileId(),
                    linearizerArray, rtreeLeafFrameFactory.createFrame(), rtreeLeafFrameFactory.createFrame(),
                    memComponent.getRTree().getBufferCache(), comparatorFields);
        } else {
            rTreeTupleSorter.reset();
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
        rTreeBulkloader = diskRTree.createBulkLoader(1.0f);
        cursor = rTreeTupleSorter;

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

        // scan the memory BTree
        ITreeIndexAccessor memBTreeAccessor = memComponent.getBTree().createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        IIndexCursor btreeScanCursor = memBTreeAccessor.createSearchCursor();
        RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
        memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);
        FileReference btreeFile = fileManager.createFlushFile(fileNames.getBTreeFileName());
        BTree diskBTree = (BTree) createDiskTree(diskBTreeFactory, btreeFile, true);

        // BulkLoad the tuples from the in-memory tree into the new disk BTree.
        IIndexBulkLoader bTreeBulkloader = diskBTree.createBulkLoader(1.0f);
        try {
            while (btreeScanCursor.hasNext()) {
                btreeScanCursor.next();
                ITupleReference frameTuple = btreeScanCursor.getTuple();
                bTreeBulkloader.add(frameTuple);
            }
        } finally {
            btreeScanCursor.close();
        }
        bTreeBulkloader.end();
        return new LSMRTreeComponent(diskRTree, diskBTree);
    }

    @Override
    public Object merge(List<Object> mergedComponents) throws HyracksDataException, IndexException {
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.

        IIndexOpContext ctx = createOpContext();
        ITreeIndexCursor cursor;
        cursor = new LSMRTreeSortedCursor(linearizer);
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        // Scan the RTrees, ignoring the in-memory RTree.
        List<Object> mergingComponents = lsmHarness.search(cursor, rtreeSearchPred, ctx, false);
        mergedComponents.addAll(mergingComponents);

        // Nothing to merge.
        if (mergedComponents.size() <= 1) {
            cursor.close();
            return null;
        }

        // Bulk load the tuples from all on-disk RTrees into the new RTree.
        LSMRTreeFileNameComponent fileNames = getMergeTargetFileName(mergingComponents);
        FileReference rtreeFile = fileManager.createMergeFile(fileNames.getRTreeFileName());
        FileReference btreeFile = fileManager.createMergeFile(fileNames.getBTreeFileName());
        RTree mergedRTree = (RTree) createDiskTree(diskRTreeFactory, rtreeFile, true);
        BTree mergedBTree = (BTree) createDiskTree(diskBTreeFactory, btreeFile, true);

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

        // Load an empty BTree tree.
        mergedBTree.createBulkLoader(1.0f).end();

        return new LSMRTreeComponent(mergedRTree, mergedBTree);
    }

    @Override
    public void cleanUpAfterMerge(List<Object> mergedComponents) throws HyracksDataException {
        for (Object o : mergedComponents) {
            LSMRTreeComponent component = (LSMRTreeComponent) o;
            BTree oldBTree = component.getBTree();
            FileReference btreeFileRef = diskFileMapProvider.lookupFileName(oldBTree.getFileId());
            oldBTree.close();
            btreeFileRef.getFile().delete();
            RTree oldRTree = component.getRTree();
            FileReference rtreeFileRef = diskFileMapProvider.lookupFileName(oldRTree.getFileId());
            oldRTree.close();
            rtreeFileRef.getFile().delete();
        }
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMRTreeAccessor(lsmHarness, createOpContext());
    }

    public class LSMRTreeAccessor extends LSMTreeIndexAccessor {
        public LSMRTreeAccessor(LSMHarness lsmHarness, IIndexOpContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            return new LSMRTreeSearchCursor();
        }

        public MultiComparator getMultiComparator() {
            LSMRTreeOpContext concreteCtx = (LSMRTreeOpContext) ctx;
            return concreteCtx.rtreeOpContext.cmp;
        }
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel) throws TreeIndexException {
        return new LSMRTreeBulkLoader(fillLevel);
    }

    public class LSMRTreeBulkLoader implements IIndexBulkLoader {
        private final RTree diskRTree;
        private final BTree diskBTree;
        private final RTreeBulkLoader bulkLoader;

        public LSMRTreeBulkLoader(float fillFactor) throws TreeIndexException {
            // Note that by using a flush target file name, we state that the
            // new bulk loaded tree is "newer" than any other merged tree.
            try {
                LSMRTreeFileNameComponent fileNames = (LSMRTreeFileNameComponent) fileManager.getRelFlushFileName();
                FileReference rtreeFile = fileManager.createFlushFile(fileNames.getRTreeFileName());
                diskRTree = (RTree) createDiskTree(diskRTreeFactory, rtreeFile, true);
                // For each RTree, we require to have a buddy BTree. thus, we
                // create an
                // empty BTree.
                FileReference btreeFile = fileManager.createFlushFile(fileNames.getBTreeFileName());
                diskBTree = (BTree) createDiskTree(diskBTreeFactory, btreeFile, true);
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
            LSMRTreeComponent diskComponent = new LSMRTreeComponent(diskRTree, diskBTree);
            lsmHarness.addBulkLoadedComponent(diskComponent);
        }

    }
}
