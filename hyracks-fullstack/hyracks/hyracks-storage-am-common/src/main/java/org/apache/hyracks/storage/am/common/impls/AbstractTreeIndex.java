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

package org.apache.hyracks.storage.am.common.impls;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageQueue;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractTreeIndex implements ITreeIndex {

    public static final int MINIMAL_TREE_PAGE_COUNT = 2;
    public static final int MINIMAL_TREE_PAGE_COUNT_WITH_FILTER = 3;
    protected int rootPage = 1;

    protected final IBufferCache bufferCache;
    protected final IFileMapProvider fileMapProvider;
    protected final IMetaDataPageManager freePageManager;

    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;

    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int fieldCount;

    protected FileReference file;
    protected int fileId = -1;

    protected boolean isActive = false;
    //hasEverBeenActivated is to stop the throwing of an exception of deactivating an index that
    //was never activated or failed to activate in try/finally blocks, as there's no way to know if
    //an index is activated or not from the outside.
    protected boolean hasEverBeenActivated = false;
    protected boolean appendOnly = false;

    protected int bulkloadLeafStart = 0;

    public AbstractTreeIndex(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IMetaDataPageManager freePageManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount,
            FileReference file) {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.freePageManager = freePageManager;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.fieldCount = fieldCount;
        this.file = file;
    }

    public synchronized void create() throws HyracksDataException {
        create(false);
    }

    private synchronized void create(boolean appendOnly) throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }

        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(file);
            if (!fileIsMapped) {
                bufferCache.createFile(file);
            }
            fileId = fileMapProvider.lookupFileId(file);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }

        freePageManager.open(fileId);
        if (!appendOnly) {
            initEmptyTree();
            freePageManager.close();
        } else {
            this.appendOnly = true;
            initCachedMetadataPage();
        }
        setRootPage(appendOnly);
        bufferCache.closeFile(fileId);
    }

    private void initEmptyTree() throws HyracksDataException {
        ITreeIndexFrame frame = leafFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();
        freePageManager.init(metaFrame, rootPage);
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
        rootNode.acquireWriteLatch();
        try {
            frame.setPage(rootNode);
            frame.initBuffer((byte) 0);
        } finally {
            rootNode.releaseWriteLatch(true);
            bufferCache.unpin(rootNode);
        }
    }

    private void setRootPage(boolean appendOnly) throws HyracksDataException {
        if (!appendOnly) {
            // regular or empty tree
            rootPage = 1;
            bulkloadLeafStart = 2;
        } else {
            //root page is stored in MD page
            rootPage = freePageManager.getRootPage();
            //leaves start from the very beginning of the file.
            bulkloadLeafStart = 0;
        }
    }

    private void initCachedMetadataPage() throws HyracksDataException {
        ITreeIndexMetaDataFrame metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();
        freePageManager.init(metaFrame);
    }

    public synchronized void activate() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }

        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(file);
            if (!fileIsMapped) {
                bufferCache.createFile(file);
            }
            fileId = fileMapProvider.lookupFileId(file);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }
        freePageManager.open(fileId);
        int mdPageLoc = freePageManager.getFirstMetadataPage();
        ITreeIndexMetaDataFrame metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();
        int numPages = freePageManager.getMaxPage(metaFrame);
        if (mdPageLoc > 1 || (mdPageLoc == 1 && numPages <= MINIMAL_TREE_PAGE_COUNT - 1)) { //md page doesn't count
            appendOnly = true;
        } else {
            appendOnly = false;
        }
        setRootPage(appendOnly);

        // TODO: Should probably have some way to check that the tree is physically consistent
        // or that the file we just opened actually is a tree

        isActive = true;
        hasEverBeenActivated = true;
    }

    public synchronized void deactivate() throws HyracksDataException {
        if (!isActive && hasEverBeenActivated) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }
        if (isActive) {
            freePageManager.close();
            bufferCache.closeFile(fileId);
        }

        isActive = false;
    }

    public synchronized void deactivateCloseHandle() throws HyracksDataException {
        deactivate();
        bufferCache.purgeHandle(fileId);

    }

    public synchronized void destroy() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }

        if (fileId == -1) {
            return;
        }
        bufferCache.deleteFile(fileId, false);
        file.delete();
        fileId = -1;
    }

    public synchronized void clear() throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        initEmptyTree();
    }

    public boolean isEmptyTree(ITreeIndexFrame frame) throws HyracksDataException {
        if (rootPage == -1) {
            return true;
        }
        if (freePageManager.appendOnlyMode() && bufferCache.getNumPagesOfFile(fileId) <= MINIMAL_TREE_PAGE_COUNT) {
            return true;
        }
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            frame.setPage(rootNode);
            if (frame.getLevel() == 0 && frame.getTupleCount() == 0) {
                return true;
            } else {
                return false;
            }
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }

    public byte getTreeHeight(ITreeIndexFrame frame) throws HyracksDataException {
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            frame.setPage(rootNode);
            return frame.getLevel();
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }

    public int getFileId() {
        return fileId;
    }

    public FileReference getFileReference() {
        return file;
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    public IMetaDataPageManager getMetaManager() {
        return freePageManager;
    }

    public int getRootPageId() {
        return rootPage;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public abstract class AbstractTreeIndexBulkLoader implements IIndexBulkLoader {
        protected final MultiComparator cmp;
        protected final int slotSize;
        protected final int leafMaxBytes;
        protected final int interiorMaxBytes;
        protected final ArrayList<NodeFrontier> nodeFrontiers = new ArrayList<NodeFrontier>();
        protected final ITreeIndexMetaDataFrame metaFrame;
        protected final ITreeIndexTupleWriter tupleWriter;
        protected ITreeIndexFrame leafFrame;
        protected ITreeIndexFrame interiorFrame;
        // Immutable bulk loaders write their root page at page -2, as needed e.g. by append-only file systems such as
        // HDFS.  Since loading this tree relies on the root page actually being at that point, no further inserts into
        // that tree are allowed.  Currently, this is not enforced.
        protected boolean releasedLatches;
        public boolean appendOnly = false;
        protected final IFIFOPageQueue queue;
        protected List<ICachedPage> pagesToWrite;

        public AbstractTreeIndexBulkLoader(float fillFactor, boolean appendOnly)
                throws TreeIndexException, HyracksDataException {
            //Initialize the tree
            if (appendOnly) {
                create(appendOnly);
                this.appendOnly = appendOnly;
                activate();
            }

            leafFrame = leafFrameFactory.createFrame();
            interiorFrame = interiorFrameFactory.createFrame();
            metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();

            queue = bufferCache.createFIFOQueue();

            if (!isEmptyTree(leafFrame)) {
                throw new TreeIndexException("Cannot bulk-load a non-empty tree.");
            }

            this.cmp = MultiComparator.create(cmpFactories);

            leafFrame.setMultiComparator(cmp);
            interiorFrame.setMultiComparator(cmp);

            tupleWriter = leafFrame.getTupleWriter();

            NodeFrontier leafFrontier = new NodeFrontier(leafFrame.createTupleReference());
            leafFrontier.pageId = freePageManager.getFreePage(metaFrame);
            leafFrontier.page = bufferCache
                    .confiscatePage(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId));

            interiorFrame.setPage(leafFrontier.page);
            interiorFrame.initBuffer((byte) 0);
            interiorMaxBytes = (int) ((float) interiorFrame.getBuffer().capacity() * fillFactor);

            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
            leafMaxBytes = (int) ((float) leafFrame.getBuffer().capacity() * fillFactor);
            slotSize = leafFrame.getSlotSize();

            nodeFrontiers.add(leafFrontier);
            pagesToWrite = new ArrayList<>();
        }

        public abstract void add(ITupleReference tuple) throws IndexException, HyracksDataException;

        protected void handleException() throws HyracksDataException {
            // Unlatch and unpin pages that weren't in the queue to avoid leaking memory.
            for (NodeFrontier nodeFrontier : nodeFrontiers) {
                ICachedPage frontierPage = nodeFrontier.page;
                if (frontierPage.confiscated()) {
                    bufferCache.returnPage(frontierPage, false);
                }
            }
            for (ICachedPage pageToDiscard : pagesToWrite) {
                bufferCache.returnPage(pageToDiscard, false);
            }
            releasedLatches = true;
        }

        @Override
        public void end() throws HyracksDataException {
            //move the root page to the first data page if necessary
            bufferCache.finishQueue();
            if (!appendOnly) {
                ICachedPage newRoot = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
                newRoot.acquireWriteLatch();
                //root will be the highest frontier
                NodeFrontier lastNodeFrontier = nodeFrontiers.get(nodeFrontiers.size() - 1);
                ICachedPage oldRoot = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, lastNodeFrontier.pageId),
                        false);
                oldRoot.acquireReadLatch();
                lastNodeFrontier.page = oldRoot;
                try {
                    System.arraycopy(lastNodeFrontier.page.getBuffer().array(), 0, newRoot.getBuffer().array(), 0,
                            lastNodeFrontier.page.getBuffer().capacity());
                } finally {
                    newRoot.releaseWriteLatch(true);
                    bufferCache.flushDirtyPage(newRoot);
                    bufferCache.unpin(newRoot);
                    oldRoot.releaseReadLatch();
                    bufferCache.unpin(oldRoot);

                    // register old root as a free page
                    freePageManager.addFreePage(metaFrame, lastNodeFrontier.pageId);

                }
            }
            freePageManager.setRootPage(rootPage);
        }

        protected void addLevel() throws HyracksDataException {
            NodeFrontier frontier = new NodeFrontier(tupleWriter.createTupleReference());
            frontier.page = bufferCache.confiscatePage(IBufferCache.INVALID_DPID);
            frontier.pageId = -1;
            frontier.lastTuple.setFieldCount(cmp.getKeyFieldCount());
            interiorFrame.setPage(frontier.page);
            interiorFrame.initBuffer((byte) nodeFrontiers.size());
            nodeFrontiers.add(frontier);
        }

        public ITreeIndexFrame getLeafFrame() {
            return leafFrame;
        }

        public void setLeafFrame(ITreeIndexFrame leafFrame) {
            this.leafFrame = leafFrame;
        }

    }

    public class TreeIndexInsertBulkLoader implements IIndexBulkLoader {
        ITreeIndexAccessor accessor;

        public TreeIndexInsertBulkLoader() throws HyracksDataException {
            accessor = (ITreeIndexAccessor) createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            try {
                accessor.insert(tuple);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void end() throws HyracksDataException {
            // do nothing
        }

        @Override
        public void abort() {

        }

    }

    @Override
    public long getMemoryAllocationSize() {
        return 0;
    }

    public IBinaryComparatorFactory[] getCmpFactories() {
        return cmpFactories;
    }

    @Override
    public boolean hasMemoryComponents() {
        return true;
    }
}
