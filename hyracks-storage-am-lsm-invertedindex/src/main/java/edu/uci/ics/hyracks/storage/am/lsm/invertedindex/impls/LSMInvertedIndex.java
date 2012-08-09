/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.TokenIterator;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexFileManager.LSMInvertedFileNameComponent;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMInvertedIndex implements ILSMIndex, IInvertedIndex {

    private final LSMHarness lsmHarness;
    private final IInvertedIndex memoryInvertedIndex;
    private final BTreeFactory diskBTreeFactory;
    private final InvertedIndexFactory diskInvertedIndexFactory;
    private final ILSMFileManager fileManager;
    private final IFileMapProvider diskFileMapProvider;
    private final ILSMComponentFinalizer componentFinalizer;
    private LinkedList<Object> diskInvertedIndexList = new LinkedList<Object>();
    private final IBufferCache diskBufferCache;

    private final IIndexAccessor memAccessor;

    private boolean isOpen;

    public LSMInvertedIndex(IInvertedIndex memoryInvertedIndex, BTreeFactory diskBTreeFactory,
            InvertedIndexFactory diskInvertedIndexFactory, ILSMFileManager fileManager,
            IFileMapProvider diskFileMapProvider) {
        this.memoryInvertedIndex = memoryInvertedIndex;
        this.diskBTreeFactory = diskBTreeFactory;
        this.diskInvertedIndexFactory = diskInvertedIndexFactory;
        this.fileManager = fileManager;
        this.diskFileMapProvider = diskFileMapProvider;
        this.lsmHarness = new LSMHarness(this);
        this.componentFinalizer = new InvertedIndexComponentFinalizer(diskFileMapProvider);
        this.diskBufferCache = diskBTreeFactory.getBufferCache();
        this.memAccessor = memoryInvertedIndex.createAccessor();
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
        // TODO: What else is needed here?
        memoryInvertedIndex.create(indexFileId);
        fileManager.createDirs();
    }

    @Override
    public void open(int indexFileId) throws HyracksDataException {
        synchronized (this) {
            if (isOpen)
                return;

            isOpen = true;
            memoryInvertedIndex.open(indexFileId);
            // TODO: What else is needed here?
            // ...
        }

    }

    @Override
    public void close() throws HyracksDataException {
        synchronized (this) {
            if (!isOpen) {
                return;
            }
            // TODO: What else is needed here?
            // ...
            memoryInvertedIndex.close();
            isOpen = false;
        }
    }

    public IIndexAccessor createAccessor() {
        return new LSMInvertedIndexAccessor(lsmHarness, createOpContext());
    }

    private LSMInvertedIndexOpContext createOpContext() {
        return new LSMInvertedIndexOpContext(memoryInvertedIndex);
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws IndexException, HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public IBufferCache getBufferCache() {
        return diskBufferCache;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.INVERTED;
    }

    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException,
            IndexException {
        // TODO: Only insert is supported for now. Will need the context for later when update and delete 
        // are also supported.
        LSMInvertedIndexOpContext ctx = (LSMInvertedIndexOpContext) ictx;
        memAccessor.insert(tuple);

        return true;
    }

    @Override
    public void search(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred, IIndexOpContext ictx,
            boolean includeMemComponent, AtomicInteger searcherRefCount) throws HyracksDataException, IndexException {
        IIndexAccessor componentAccessor;

        // Over-provision by 1 if includeMemComponent == false, but that's okay!
        ArrayList<IIndexAccessor> indexAccessors = new ArrayList<IIndexAccessor>(diskComponents.size() + 1);

        if (includeMemComponent) {
            componentAccessor = memoryInvertedIndex.createAccessor();
            indexAccessors.add(componentAccessor);
        }

        for (int i = 0; i < diskComponents.size(); i++) {
            componentAccessor = ((IInvertedIndex) diskComponents.get(i)).createAccessor();
            indexAccessors.add(componentAccessor);
        }

        LSMInvertedIndexCursorInitialState initState = new LSMInvertedIndexCursorInitialState(indexAccessors, ictx,
                includeMemComponent, searcherRefCount, lsmHarness);
        LSMInvertedIndexSearchCursor lsmCursor = (LSMInvertedIndexSearchCursor) cursor;
        lsmCursor.open(initState, pred);
    }

    public void mergeSearch(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred,
            IIndexOpContext ictx, boolean includeMemComponent, AtomicInteger searcherRefCount)
            throws HyracksDataException, IndexException {
        IIndexAccessor componentAccessor;

        // Over-provision by 1 if includeMemComponent == false, but that's okay!
        ArrayList<IIndexAccessor> indexAccessors = new ArrayList<IIndexAccessor>(diskComponents.size() + 1);

        if (includeMemComponent) {
            componentAccessor = memoryInvertedIndex.createAccessor();
            indexAccessors.add(componentAccessor);
        }

        for (int i = 0; i < diskComponents.size(); i++) {
            componentAccessor = ((IInvertedIndex) diskComponents.get(i)).createAccessor();
            indexAccessors.add(componentAccessor);
        }

        LSMInvertedIndexCursorInitialState initState = new LSMInvertedIndexCursorInitialState(indexAccessors, ictx,
                includeMemComponent, searcherRefCount, lsmHarness);
        LSMInvertedIndexRangeSearchCursor rangeSearchCursor = (LSMInvertedIndexRangeSearchCursor) cursor;
        rangeSearchCursor.open(initState, pred);
    }

    @Override
    public Object merge(List<Object> mergedComponents) throws HyracksDataException, IndexException {
        LSMInvertedIndexOpContext ctx = createOpContext();

        IIndexCursor cursor = new LSMInvertedIndexRangeSearchCursor();
        RangePredicate mergePred = new RangePredicate(null, null, true, true, null, null);

        //Scan diskInvertedIndexes ignoring the memoryInvertedIndex.
        List<Object> mergingComponents = lsmHarness.mergeSearch(cursor, mergePred, ctx, false);
        mergedComponents.addAll(mergingComponents);

        // Nothing to merge.
        if (mergedComponents.size() <= 1) {
            cursor.close();
            return null;
        }

        // Bulk load the tuples from all diskInvertedIndexes into the new diskInvertedIndex.
        LSMInvertedFileNameComponent fNameComponent = getMergeTargetFileName(mergedComponents);
        BTree diskBTree = createDiskBTree(fileManager.createMergeFile(fNameComponent.getBTreeFileName()), true);
        //    - Create an InvertedIndex instance
        InvertedIndex mergedDiskInvertedIndex = createDiskInvertedIndex(
                fileManager.createMergeFile(fNameComponent.getInvertedFileName()), true, diskBTree);

        IIndexBulkLoadContext bulkLoadCtx = mergedDiskInvertedIndex.beginBulkLoad(1.0f);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                mergedDiskInvertedIndex.bulkLoadAddTuple(tuple, bulkLoadCtx);
            }
        } finally {
            cursor.close();
        }
        mergedDiskInvertedIndex.endBulkLoad(bulkLoadCtx);

        return mergedDiskInvertedIndex;
    }

    private LSMInvertedFileNameComponent getMergeTargetFileName(List<Object> mergingDiskTrees)
            throws HyracksDataException {
        BTree firstTree = ((InvertedIndex) mergingDiskTrees.get(0)).getBTree();
        BTree lastTree = ((InvertedIndex) mergingDiskTrees.get(mergingDiskTrees.size() - 1)).getBTree();
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastTree.getFileId());
        LSMInvertedFileNameComponent component = (LSMInvertedFileNameComponent) ((LSMInvertedIndexFileManager) fileManager)
                .getRelMergeFileName(firstFile.getFile().getName(), lastFile.getFile().getName());
        return component;
    }

    @Override
    public void addMergedComponent(Object newComponent, List<Object> mergedComponents) {
        diskInvertedIndexList.removeAll(mergedComponents);
        diskInvertedIndexList.addLast(newComponent);
    }

    @Override
    public void cleanUpAfterMerge(List<Object> mergedComponents) throws HyracksDataException {
        for (Object o : mergedComponents) {
            InvertedIndex oldInvertedIndex = (InvertedIndex) o;
            BTree oldBTree = oldInvertedIndex.getBTree();

            //delete a diskBTree file.
            FileReference fileRef = diskFileMapProvider.lookupFileName(oldBTree.getFileId());
            diskBufferCache.closeFile(oldBTree.getFileId());
            diskBufferCache.deleteFile(oldBTree.getFileId(), false);
            oldBTree.close();
            fileRef.getFile().delete();

            //delete a diskInvertedIndex file.
            fileRef = diskFileMapProvider.lookupFileName(oldInvertedIndex.getFileId());
            diskBufferCache.closeFile(oldInvertedIndex.getFileId());
            diskBufferCache.deleteFile(oldInvertedIndex.getFileId(), false);
            oldInvertedIndex.close();
            fileRef.getFile().delete();
        }
    }

    @Override
    public Object flush() throws HyracksDataException, IndexException {

        // ---------------------------------------------------
        // [Flow]
        // #. Create a scanCursor for the BTree of the memoryInvertedIndex to iterate all keys in it.
        // #. Create an diskInvertedIndex where all keys of memoryInvertedIndex will be bulkloaded.
        //    - Create a BTree instance for diskBTree 
        //    - Create an InvertedIndex instance
        // #. Begin the bulkload of the diskInvertedIndex.
        // #. While iterating the scanCursor, add each key into the diskInvertedIndex in the bulkload mode.
        // #. End the bulkload.
        // #. Return the newly created diskInvertedIndex.
        // ---------------------------------------------------

        // #. Create a scanCursor of memoryInvertedIndex to iterate all keys in it.
        BTree inMemBtree = ((InMemoryBtreeInvertedIndex) memoryInvertedIndex).getBTree();
        IIndexAccessor btreeAccessor = inMemBtree.createAccessor();
        MultiComparator btreeMultiComparator = MultiComparator.create(inMemBtree.getComparatorFactories());
        RangePredicate scanPred = new RangePredicate(null, null, true, true, btreeMultiComparator, btreeMultiComparator);

        IIndexCursor scanCursor = btreeAccessor.createSearchCursor();
        btreeAccessor.search(scanCursor, scanPred);

        // #. Create a diskInvertedIndex where all keys of memoryInvertedIndex will be bulkloaded.
        //    - Create a BTree instance for diskBTree
        LSMInvertedFileNameComponent fNameComponent = (LSMInvertedFileNameComponent) fileManager.getRelFlushFileName();
        BTree diskBTree = createDiskBTree(fileManager.createFlushFile(fNameComponent.getBTreeFileName()), true);
        //    - Create an InvertedIndex instance
        InvertedIndex diskInvertedIndex = createDiskInvertedIndex(
                fileManager.createFlushFile(fNameComponent.getInvertedFileName()), true, diskBTree);

        // #. Begin the bulkload of the diskInvertedIndex.
        IIndexBulkLoadContext bulkLoadCtx = diskInvertedIndex.beginBulkLoad(1.0f);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                diskInvertedIndex.bulkLoadAddTuple(scanCursor.getTuple(), bulkLoadCtx);
            }
        } finally {
            scanCursor.close();
        }
        diskInvertedIndex.endBulkLoad(bulkLoadCtx);

        return diskInvertedIndex;
    }

    private BTree createBTreeFlushTarget() throws HyracksDataException {
        LSMInvertedFileNameComponent fNameComponent = (LSMInvertedFileNameComponent) fileManager.getRelFlushFileName();
        FileReference fileRef = fileManager.createFlushFile(fNameComponent.getBTreeFileName());
        return createDiskBTree(fileRef, true);
    }

    private BTree createDiskBTree(FileReference fileRef, boolean createBTree) throws HyracksDataException {
        // File will be deleted during cleanup of merge().
        diskBufferCache.createFile(fileRef);
        int diskBTreeFileId = diskFileMapProvider.lookupFileId(fileRef);
        // File will be closed during cleanup of merge().
        diskBufferCache.openFile(diskBTreeFileId);
        // Create new BTree instance.
        BTree diskBTree = diskBTreeFactory.createIndexInstance();
        if (createBTree) {
            diskBTree.create(diskBTreeFileId);
        }
        // BTree will be closed during cleanup of merge().
        diskBTree.open(diskBTreeFileId);
        return diskBTree;
    }

    private InvertedIndex createInvertedIndexFlushTarget(BTree diskBTree) throws HyracksDataException {
        FileReference fileRef = fileManager.createFlushFile((String) fileManager.getRelFlushFileName());
        return createDiskInvertedIndex(fileRef, true, diskBTree);
    }

    private InvertedIndex createDiskInvertedIndex(FileReference fileRef, boolean createInvertedIndex, BTree diskBTree)
            throws HyracksDataException {
        // File will be deleted during cleanup of merge().
        diskBufferCache.createFile(fileRef);
        int diskInvertedIndexFileId = diskFileMapProvider.lookupFileId(fileRef);
        // File will be closed during cleanup of merge().
        diskBufferCache.openFile(diskInvertedIndexFileId);
        // Create new InvertedIndex instance.
        InvertedIndex diskInvertedIndex = (InvertedIndex) diskInvertedIndexFactory.createIndexInstance(diskBTree);
        if (createInvertedIndex) {
            diskInvertedIndex.create(diskInvertedIndexFileId);
        }
        // InvertedIndex will be closed during cleanup of merge().
        diskInvertedIndex.open(diskInvertedIndexFileId);
        return diskInvertedIndex;
    }

    public void addFlushedComponent(Object index) {
        diskInvertedIndexList.addFirst(index);
    }

    @Override
    public InMemoryFreePageManager getInMemoryFreePageManager() {
        // TODO This code should be changed more generally if IInMemoryInvertedIndex interface is defined and
        //      InMemoryBtreeInvertedIndex implements IInMemoryInvertedIndex
        InMemoryBtreeInvertedIndex memoryBTreeInvertedIndex = (InMemoryBtreeInvertedIndex) memoryInvertedIndex;
        return (InMemoryFreePageManager) memoryBTreeInvertedIndex.getBTree().getFreePageManager();
    }

    @Override
    public void resetInMemoryComponent() throws HyracksDataException {
        // TODO This code should be changed more generally if IInMemoryInvertedIndex interface is defined and
        //      InMemoryBtreeInvertedIndex implements IInMemoryInvertedIndex
        InMemoryBtreeInvertedIndex memoryBTreeInvertedIndex = (InMemoryBtreeInvertedIndex) memoryInvertedIndex;
        BTree memBTree = memoryBTreeInvertedIndex.getBTree();
        InMemoryFreePageManager memFreePageManager = (InMemoryFreePageManager) memBTree.getFreePageManager();
        memFreePageManager.reset();
        memBTree.create(memBTree.getFileId());
    }

    @Override
    public List<Object> getDiskComponents() {
        return diskInvertedIndexList;
    }

    @Override
    public ILSMComponentFinalizer getComponentFinalizer() {
        return componentFinalizer;
    }

    @Override
    public IInvertedListCursor createInvertedListCursor() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference tupleReference)
            throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public IBinaryComparatorFactory[] getInvListElementCmpFactories() {
        return memoryInvertedIndex.getInvListElementCmpFactories();
    }

    @Override
    public ITypeTraits[] getTypeTraits() {
        return memoryInvertedIndex.getTypeTraits();
    }

}
