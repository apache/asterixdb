package edu.uci.ics.hyracks.storage.am.common.impls;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public abstract class AbstractTreeIndex implements ITreeIndex {

    protected final static int rootPage = 1;

    protected final IBufferCache bufferCache;
    protected final IFreePageManager freePageManager;
    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;
    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int fieldCount;

    protected int fileId;

    public AbstractTreeIndex(IBufferCache bufferCache, IFreePageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            IBinaryComparatorFactory[] cmpFactories, int fieldCount) {
        this.bufferCache = bufferCache;
        this.freePageManager = freePageManager;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.fieldCount = fieldCount;
    }

    public boolean isEmptyTree(ITreeIndexFrame frame) throws HyracksDataException {
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

    public synchronized void create(int fileId) throws HyracksDataException {
        ITreeIndexFrame frame = leafFrameFactory.createFrame();
        ITreeIndexMetaDataFrame metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();
        freePageManager.open(fileId);
        freePageManager.init(metaFrame, rootPage);

        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
        rootNode.acquireWriteLatch();
        try {
            frame.setPage(rootNode);
            frame.initBuffer((byte) 0);
        } finally {
            rootNode.releaseWriteLatch();
            bufferCache.unpin(rootNode);
        }
    }

    public void open(int fileId) {
        this.fileId = fileId;
        freePageManager.open(fileId);
    }

    public void close() {
        fileId = -1;
        freePageManager.close();
    }

    public int getFileId() {
        return fileId;
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
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

    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    public IFreePageManager getFreePageManager() {
        return freePageManager;
    }

    public int getRootPageId() {
        return rootPage;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public abstract IIndexBulkLoader createBulkLoader(float fillFactor) throws TreeIndexException;

    public TreeIndexInsertBulkLoader createInsertBulkLoader() throws TreeIndexException {
        final Logger LOGGER = Logger.getLogger(TreeIndexInsertBulkLoader.class.getName());

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Using insert bulkload. This might negatively impact your performance.");
        }

        return new TreeIndexInsertBulkLoader();
    }

    public abstract class AbstractTreeIndexBulkLoader implements IIndexBulkLoader {
        protected final MultiComparator cmp;
        protected final int slotSize;
        protected final int leafMaxBytes;
        protected final int interiorMaxBytes;
        // we maintain a frontier of nodes for each level
        protected final ArrayList<NodeFrontier> nodeFrontiers = new ArrayList<NodeFrontier>();
        protected final ITreeIndexMetaDataFrame metaFrame;
        protected final ITreeIndexTupleWriter tupleWriter;

        protected ITreeIndexFrame leafFrame, interiorFrame;

        public AbstractTreeIndexBulkLoader(float fillFactor) throws TreeIndexException, HyracksDataException {
            leafFrame = leafFrameFactory.createFrame();
            interiorFrame = interiorFrameFactory.createFrame();
            metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();

            if (!isEmptyTree(leafFrame)) {
                throw new TreeIndexException("Cannot bulk-load a non-empty tree.");
            }

            this.cmp = MultiComparator.create(cmpFactories);

            leafFrame.setMultiComparator(cmp);
            interiorFrame.setMultiComparator(cmp);

            tupleWriter = leafFrame.getTupleWriter();

            NodeFrontier leafFrontier = new NodeFrontier(leafFrame.createTupleReference());
            leafFrontier.pageId = freePageManager.getFreePage(metaFrame);
            leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId), true);
            leafFrontier.page.acquireWriteLatch();

            interiorFrame.setPage(leafFrontier.page);
            interiorFrame.initBuffer((byte) 0);
            interiorMaxBytes = (int) ((float) interiorFrame.getBuffer().capacity() * fillFactor);

            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
            leafMaxBytes = (int) ((float) leafFrame.getBuffer().capacity() * fillFactor);
            slotSize = leafFrame.getSlotSize();

            nodeFrontiers.add(leafFrontier);
        }

        public abstract void add(ITupleReference tuple) throws HyracksDataException;

        @Override
        public void end() throws HyracksDataException {
            // copy the root generated from the bulk-load to *the* root page location
            ICachedPage newRoot = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
            newRoot.acquireWriteLatch();
            NodeFrontier lastNodeFrontier = nodeFrontiers.get(nodeFrontiers.size() - 1);
            try {
                System.arraycopy(lastNodeFrontier.page.getBuffer().array(), 0, newRoot.getBuffer().array(), 0,
                        lastNodeFrontier.page.getBuffer().capacity());
            } finally {
                newRoot.releaseWriteLatch();
                bufferCache.unpin(newRoot);

                // register old root as a free page
                freePageManager.addFreePage(metaFrame, lastNodeFrontier.pageId);

                for (int i = 0; i < nodeFrontiers.size(); i++) {
                    nodeFrontiers.get(i).page.releaseWriteLatch();
                    bufferCache.unpin(nodeFrontiers.get(i).page);
                }
            }
        }

        protected void addLevel() throws HyracksDataException {
            NodeFrontier frontier = new NodeFrontier(tupleWriter.createTupleReference());
            frontier.pageId = freePageManager.getFreePage(metaFrame);
            frontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, frontier.pageId), true);
            frontier.page.acquireWriteLatch();
            frontier.lastTuple.setFieldCount(cmp.getKeyFieldCount());
            interiorFrame.setPage(frontier.page);
            interiorFrame.initBuffer((byte) nodeFrontiers.size());
            nodeFrontiers.add(frontier);
        }
    }

    public class TreeIndexInsertBulkLoader implements IIndexBulkLoader {
        ITreeIndexAccessor accessor = (ITreeIndexAccessor) createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);

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

    }

}
