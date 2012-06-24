package edu.uci.ics.hyracks.storage.am.common.impls;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexBulkLoader;
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
	
    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;
    protected final IFreePageManager freePageManager;
    protected final IBufferCache bufferCache;
    protected final int fieldCount;
    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final ReadWriteLock treeLatch;
    protected int fileId;
	
    public AbstractTreeIndex(IBufferCache bufferCache, int fieldCount, IBinaryComparatorFactory[] cmpFactories, IFreePageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory) {
        this.bufferCache = bufferCache;
        this.fieldCount = fieldCount;
        this.cmpFactories = cmpFactories;       
        this.freePageManager = freePageManager;
        this.treeLatch = new ReentrantReadWriteLock(true);
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
    }
    
    public boolean isEmptyTree(ITreeIndexFrame leafFrame) throws HyracksDataException {
    	ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            leafFrame.setPage(rootNode);
            if (leafFrame.getLevel() == 0 && leafFrame.getTupleCount() == 0) {
            	return true;
            } else {
            	return false;
            }
        } finally {
            rootNode.releaseReadLatch();
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
    
    public byte getTreeHeight(ITreeIndexFrame leafFrame) throws HyracksDataException {
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            leafFrame.setPage(rootNode);
            return leafFrame.getLevel();
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
    
    public abstract AbstractTreeIndexBulkLoader createBulkLoader(float fillFactor) throws TreeIndexException;
    
    public TreeIndexInsertBulkLoader createInsertBulkLoader() throws TreeIndexException {
        final Logger LOGGER = Logger.getLogger(TreeIndexInsertBulkLoader.class.getName());
        
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Using insert bulkload. This might negatively impact your performance.");
        }

    	return new TreeIndexInsertBulkLoader();
    }


	public abstract class AbstractTreeIndexBulkLoader implements ITreeIndexBulkLoader {
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
	    		throw new TreeIndexException("Trying to Bulk-load a non-empty tree.");
	    	}
            
            this.cmp = MultiComparator.create(cmpFactories);
            
	    	leafFrame.setMultiComparator(cmp);
        	interiorFrame.setMultiComparator(cmp);
        	
            tupleWriter = leafFrame.getTupleWriter();

            NodeFrontier leafFrontier = new NodeFrontier(leafFrame.createTupleReference());
            leafFrontier.pageId = freePageManager.getFreePage(metaFrame);
            leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId),
                    true);
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
	    	// copy root
	        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
	        rootNode.acquireWriteLatch();
	        NodeFrontier lastNodeFrontier = nodeFrontiers.get(nodeFrontiers.size() - 1);
	        try {
	            ICachedPage toBeRoot = lastNodeFrontier.page;
	            System.arraycopy(toBeRoot.getBuffer().array(), 0, rootNode.getBuffer().array(), 0, toBeRoot.getBuffer()
	                    .capacity());
	        } finally {
	            rootNode.releaseWriteLatch();
	            bufferCache.unpin(rootNode);

	            // register old root as free page
	            freePageManager.addFreePage(metaFrame, lastNodeFrontier.pageId);

	            // make old root a free page
	            interiorFrame.setPage(lastNodeFrontier.page);
	            interiorFrame.initBuffer(freePageManager.getFreePageLevelIndicator());

	            // cleanup
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
	
	public class TreeIndexInsertBulkLoader implements ITreeIndexBulkLoader {
		// TODO: What callback to pass here?
		ITreeIndexAccessor accessor = (ITreeIndexAccessor) createAccessor(
				NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);

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
	
	@Deprecated
	protected ITreeIndexBulkLoader bulkloader;
	
	@Deprecated
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws HyracksDataException, TreeIndexException {
		if(this.bulkloader == null) this.bulkloader = this.createBulkLoader(fillFactor);
		return null;
	}

	@Deprecated
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
		this.bulkloader.add(tuple);
	}

	@Deprecated
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
		this.bulkloader.end();
	}
}
