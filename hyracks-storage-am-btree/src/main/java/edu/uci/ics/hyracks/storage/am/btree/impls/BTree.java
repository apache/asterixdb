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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleWriter;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.FileHandle;

public class BTree {
	
    private final static int RESTART_OP = Integer.MIN_VALUE;
    private final static int MAX_RESTARTS = 10;
    
    private final int metaDataPage = 0; // page containing meta data, e.g., maxPage
    private final int rootPage = 1; // the root page never changes
    
    private boolean created = false;
    private boolean loaded = false;
    
    private final IBufferCache bufferCache;
    private int fileId;
    private final IBTreeInteriorFrameFactory interiorFrameFactory;
    private final IBTreeLeafFrameFactory leafFrameFactory;    
    private final MultiComparator cmp;
    private final ReadWriteLock treeLatch;    
    private final RangePredicate diskOrderScanPredicate;
    
    public int rootSplits = 0;
    public int[] splitsByLevel = new int[500];
    public long readLatchesAcquired = 0;
    public long readLatchesReleased = 0;
    public long writeLatchesAcquired = 0;
    public long writeLatchesReleased = 0;
    public long pins = 0;
    public long unpins = 0;

    public long treeLatchesAcquired = 0;
    public long treeLatchesReleased = 0;

    public byte currentLevel = 0;

    public int usefulCompression = 0;
    public int uselessCompression = 0;
        
    
    public void treeLatchStatus() {
    	System.out.println(treeLatch.writeLock().toString());
    }
    
    public String printStats() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("\n");
        strBuilder.append("ROOTSPLITS: " + rootSplits + "\n");
        strBuilder.append("SPLITS BY LEVEL\n");
        for (int i = 0; i < currentLevel; i++) {
            strBuilder.append(String.format("%3d ", i) + String.format("%8d ", splitsByLevel[i]) + "\n");
        }
        strBuilder.append(String.format("READ LATCHES:  %10d %10d\n", readLatchesAcquired, readLatchesReleased));
        strBuilder.append(String.format("WRITE LATCHES: %10d %10d\n", writeLatchesAcquired, writeLatchesReleased));
        strBuilder.append(String.format("PINS:          %10d %10d\n", pins, unpins));
        return strBuilder.toString();
    }

    public BTree(IBufferCache bufferCache, IBTreeInteriorFrameFactory interiorFrameFactory,
            IBTreeLeafFrameFactory leafFrameFactory, MultiComparator cmp) {
        this.bufferCache = bufferCache;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;       
        this.cmp = cmp;
        this.treeLatch = new ReentrantReadWriteLock(true);
        this.diskOrderScanPredicate = new RangePredicate(true, null, null, true, true, cmp, cmp);             
    }
    
    public void create(int fileId, IBTreeLeafFrame leafFrame, IBTreeMetaDataFrame metaFrame) throws Exception {

    	if(created) return;
    	
    	treeLatch.writeLock().lock();	
    	try {
    	
    		// check if another thread beat us to it
    		if(created) return;    		    		
    		
    		// initialize meta data page
    		ICachedPage metaNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, metaDataPage), false);
    		pins++;
    		
    		metaNode.acquireWriteLatch();
    		writeLatchesAcquired++;
    		try {
    			metaFrame.setPage(metaNode);
    			metaFrame.initBuffer((byte) -1);
    			metaFrame.setMaxPage(rootPage);
    		} finally {
    			metaNode.releaseWriteLatch();
    			writeLatchesReleased++;
    			bufferCache.unpin(metaNode);
    			unpins++;
    		}

    		// initialize root page
    		ICachedPage rootNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, rootPage), true);
    		pins++;

    		rootNode.acquireWriteLatch();
    		writeLatchesAcquired++;
    		try {
    			leafFrame.setPage(rootNode);
    			leafFrame.initBuffer((byte) 0);
    		} finally {
    			rootNode.releaseWriteLatch();
    			writeLatchesReleased++;            
    			bufferCache.unpin(rootNode);
    			unpins++;
    		}
    		currentLevel = 0;
    		
    		created = true;
    	}
    	finally {
    		treeLatch.writeLock().unlock();
    	}
    }

    public void open(int fileId) {
        this.fileId = fileId;
    }

    public void close() {
        fileId = -1;
    }

    private int getFreePage(IBTreeMetaDataFrame metaFrame) throws Exception {
        ICachedPage metaNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, metaDataPage), false);
        pins++;
        
        metaNode.acquireWriteLatch();
        writeLatchesAcquired++;
        
        int freePage = -1;
        try {
            metaFrame.setPage(metaNode);
            freePage = metaFrame.getFreePage();
            if (freePage < 0) { // no free page entry on this page
                int nextPage = metaFrame.getNextPage();
                if (nextPage > 0) { // sibling may have free pages
                    ICachedPage nextNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, nextPage), false);
                    pins++;
                    
                    nextNode.acquireWriteLatch();
                    writeLatchesAcquired++;
                    // we copy over the free space entries of nextpage into the
                    // first meta page (metaDataPage)
                    // we need to link the first page properly to the next page
                    // of nextpage
                    try {
                        // remember entries that remain unchanged
                        int maxPage = metaFrame.getMaxPage();

                        // copy entire page (including sibling pointer, free
                        // page entries, and all other info)
                        // after this copy nextPage is considered a free page
                        System.arraycopy(nextNode.getBuffer().array(), 0, metaNode.getBuffer().array(), 0, nextNode.getBuffer().capacity());
                        
                        // reset unchanged entry
                        metaFrame.setMaxPage(maxPage);

                        freePage = metaFrame.getFreePage();
                        // sibling also has no free pages, this "should" not
                        // happen, but we deal with it anyway just to be safe
                        if (freePage < 0) {
                            freePage = nextPage;
                        } else {
                            metaFrame.addFreePage(nextPage);
                        }
                    } finally {
                        nextNode.releaseWriteLatch();
                        writeLatchesReleased++;
                        bufferCache.unpin(nextNode);
                        unpins++;
                    }
                } else {
                    freePage = metaFrame.getMaxPage();
                    freePage++;
                    metaFrame.setMaxPage(freePage);
                }
            }
        } finally {
            metaNode.releaseWriteLatch();
            writeLatchesReleased++;
            bufferCache.unpin(metaNode);
            unpins++;
        }

        return freePage;
    }
    
    private void addFreePages(BTreeOpContext ctx) throws Exception {
        for(int i = 0; i < ctx.freePages.size(); i++) {
            addFreePage(ctx.metaFrame, ctx.freePages.get(i));
        }
        ctx.freePages.clear();
    }

    private void addFreePage(IBTreeMetaDataFrame metaFrame, int freePage) throws Exception {
        // root page is special, don't add it to free pages
        if (freePage == rootPage) return;
        
        ICachedPage metaNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, metaDataPage), false);
        pins++;
        
        metaNode.acquireWriteLatch();
        writeLatchesAcquired++;
        
        metaFrame.setPage(metaNode);

        try {
            if(metaFrame.hasSpace()) {
                metaFrame.addFreePage(freePage);                
            }
            else {                
                // allocate a new page in the chain of meta pages
                int newPage = metaFrame.getFreePage();
                if(newPage < 0) {
                    throw new Exception("Inconsistent Meta Page State. It has no space, but it also has no entries.");
                }
                
                ICachedPage newNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, newPage), false);
                pins++;
                
                newNode.acquireWriteLatch();
                writeLatchesAcquired++;
                
                
                try {                
                    int metaMaxPage = metaFrame.getMaxPage();

                    // copy metaDataPage to newNode
                    System.arraycopy(metaNode.getBuffer().array(), 0, newNode.getBuffer().array(), 0, metaNode.getBuffer().capacity());
                    
                    metaFrame.initBuffer(-1);
                    metaFrame.setNextPage(newPage);
                    metaFrame.setMaxPage(metaMaxPage);
                    metaFrame.addFreePage(freePage);              
                } finally {
                    newNode.releaseWriteLatch();
                    writeLatchesReleased++;
                    
                    bufferCache.unpin(newNode);
                    unpins++;
                }
            }                                   
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metaNode.releaseWriteLatch();
            writeLatchesReleased++;
            
            bufferCache.unpin(metaNode);
            unpins++;
        }
    }
    
    public int getMaxPage(IBTreeMetaDataFrame metaFrame) throws HyracksDataException {
    	ICachedPage metaNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, metaDataPage), false);
		pins++;
		
		metaNode.acquireWriteLatch();
		writeLatchesAcquired++;
		int maxPage = -1;
		try {
			metaFrame.setPage(metaNode);			
			maxPage = metaFrame.getMaxPage();
		} finally {
			metaNode.releaseWriteLatch();
			writeLatchesReleased++;
			bufferCache.unpin(metaNode);
			unpins++;
		}
		
		return maxPage;
    }
    
    public void printTree(IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame, ISerializerDeserializer[] fields) throws Exception {
        printTree(rootPage, null, false, leafFrame, interiorFrame, fields);
    }

    public void printTree(int pageId, ICachedPage parent, boolean unpin, IBTreeLeafFrame leafFrame,
            IBTreeInteriorFrame interiorFrame, ISerializerDeserializer[] fields) throws Exception {
        
        ICachedPage node = bufferCache.pin(FileHandle.getDiskPageId(fileId, pageId), false);
        pins++;
        node.acquireReadLatch();
        readLatchesAcquired++;
        
        try {
            if (parent != null && unpin == true) {
                parent.releaseReadLatch();
                readLatchesReleased++;
                
                bufferCache.unpin(parent);
                unpins++;
            }

            interiorFrame.setPage(node);
            int level = interiorFrame.getLevel();
            
            System.out.format("%1d ", level);
            System.out.format("%3d ", pageId);
            for (int i = 0; i < currentLevel - level; i++)
            	System.out.format("    ");

            String keyString;
            if(interiorFrame.isLeaf()) {
            	leafFrame.setPage(node);
            	keyString = leafFrame.printKeys(cmp, fields);
            }
            else {
            	keyString = interiorFrame.printKeys(cmp, fields);
            }
            
            System.out.format(keyString);
            if (!interiorFrame.isLeaf()) {
            	ArrayList<Integer> children = ((NSMInteriorFrame) (interiorFrame)).getChildren(cmp);
            	            	
                for (int i = 0; i < children.size(); i++) {
                    printTree(children.get(i), node, i == children.size() - 1, leafFrame, interiorFrame, fields);
                }
            } else {
                node.releaseReadLatch();
                readLatchesReleased++;
                
                bufferCache.unpin(node);
                unpins++;
            }
        } catch (Exception e) {
            node.releaseReadLatch();
            readLatchesReleased++;
            
            bufferCache.unpin(node);
            unpins++;
            e.printStackTrace();
        }
    }

    public void diskOrderScan(DiskOrderScanCursor cursor, IBTreeLeafFrame leafFrame, IBTreeMetaDataFrame metaFrame) throws Exception {
        int currentPageId = rootPage + 1;
        int maxPageId = -1;
        
        ICachedPage metaNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, metaDataPage), false);
        pins++;
        
        metaNode.acquireReadLatch();
        readLatchesAcquired++;
        
        try {
            metaFrame.setPage(metaNode);
            maxPageId = metaFrame.getMaxPage();
        } finally {
            metaNode.releaseReadLatch();
            readLatchesAcquired++;
            
            bufferCache.unpin(metaNode);
            unpins++;
        }
        
        ICachedPage page = bufferCache.pin(FileHandle.getDiskPageId(fileId, currentPageId), false);
        page.acquireReadLatch();
        cursor.setBufferCache(bufferCache);
        cursor.setFileId(fileId);
        cursor.setCurrentPageId(currentPageId);
        cursor.setMaxPageId(maxPageId);
        cursor.open(page, diskOrderScanPredicate);
    }
    
    public void search(IBTreeCursor cursor, RangePredicate pred, BTreeOpContext ctx) throws Exception {        
    	ctx.reset();
    	ctx.pred = pred;
        ctx.cursor = cursor;       
        // simple index scan
        if (ctx.pred.getLowKeyComparator() == null)
            ctx.pred.setLowKeyComparator(cmp);
        if (ctx.pred.getHighKeyComparator() == null)
            ctx.pred.setHighKeyComparator(cmp);
        
        boolean repeatOp = true;
        // we use this loop to deal with possibly multiple operation restarts
        // due to ongoing structure modifications during the descent
        while (repeatOp && ctx.opRestarts < MAX_RESTARTS) {
            performOp(rootPage, null, ctx);

            // if we reach this stage then we need to restart from the (possibly
            // new) root
            if (!ctx.pageLsns.isEmpty() && ctx.pageLsns.getLast() == RESTART_OP) {
                ctx.pageLsns.removeLast(); // pop the restart op indicator
                continue;
            }

            repeatOp = false;
        }

        cursor.setBufferCache(bufferCache);
        cursor.setFileId(fileId);
    }

    private void unsetSmPages(BTreeOpContext ctx) throws Exception {
        ICachedPage originalPage = ctx.interiorFrame.getPage();
        for(int i = 0; i < ctx.smPages.size(); i++) {
            int pageId = ctx.smPages.get(i);
        	ICachedPage smPage = bufferCache.pin(FileHandle.getDiskPageId(fileId, pageId), false);
            pins++;
            smPage.acquireWriteLatch(); // TODO: would like to set page dirty without latching
            writeLatchesAcquired++;
            try {
                ctx.interiorFrame.setPage(smPage);
                ctx.interiorFrame.setSmFlag(false);
            } finally {
                smPage.releaseWriteLatch();
                writeLatchesReleased++;
                bufferCache.unpin(smPage);
                unpins++;
            }
        }
        if (ctx.smPages.size() > 0) {
            treeLatch.writeLock().unlock();
            treeLatchesReleased++;
            ctx.smPages.clear();
        }
        ctx.interiorFrame.setPage(originalPage);
    }

    private void createNewRoot(BTreeOpContext ctx) throws Exception {
        rootSplits++; // debug
        splitsByLevel[currentLevel]++;
        currentLevel++;

        // make sure the root is always at the same level
        ICachedPage leftNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, ctx.splitKey.getLeftPage()), false);
        pins++;
        leftNode.acquireWriteLatch(); // TODO: think about whether latching is really required        
        writeLatchesAcquired++;
        try {
            ICachedPage rightNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, ctx.splitKey.getRightPage()), false);
            pins++;
            rightNode.acquireWriteLatch(); // TODO: think about whether latching is really required
            writeLatchesAcquired++;
            try {
                int newLeftId = getFreePage(ctx.metaFrame);
                ICachedPage newLeftNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, newLeftId), true);
                pins++;
                newLeftNode.acquireWriteLatch(); // TODO: think about whether latching is really required
                writeLatchesAcquired++;
                try {
                    // copy left child to new left child
                    System.arraycopy(leftNode.getBuffer().array(), 0, newLeftNode.getBuffer().array(), 0, newLeftNode
                            .getBuffer().capacity());
                    ctx.interiorFrame.setPage(newLeftNode);
                    ctx.interiorFrame.setSmFlag(false);

                    // change sibling pointer if children are leaves
                    ctx.leafFrame.setPage(rightNode);
                    if (ctx.leafFrame.isLeaf()) {
                        ctx.leafFrame.setPrevLeaf(newLeftId);
                    }

                    // initialize new root (leftNode becomes new root)
                    ctx.interiorFrame.setPage(leftNode);
                    ctx.interiorFrame.initBuffer((byte) (ctx.leafFrame.getLevel() + 1));
                    ctx.interiorFrame.setSmFlag(true); // will be cleared later
                    // in unsetSmPages
                    ctx.splitKey.setLeftPage(newLeftId);
                    ctx.interiorFrame.insert(ctx.splitKey.getTuple(), cmp);
                } finally {
                    newLeftNode.releaseWriteLatch();
                    writeLatchesReleased++;
                    bufferCache.unpin(newLeftNode);
                    unpins++;
                }
            } finally {
                rightNode.releaseWriteLatch();
                writeLatchesReleased++;
                bufferCache.unpin(rightNode);
                unpins++;
            }
        } finally {
            leftNode.releaseWriteLatch();
            writeLatchesReleased++;
            bufferCache.unpin(leftNode);
            unpins++;
        }
    }

    public void insert(ITupleReference tuple, BTreeOpContext ctx) throws Exception {       
    	ctx.reset();
    	ctx.pred.setLowKeyComparator(cmp);
    	ctx.pred.setHighKeyComparator(cmp);
        ctx.pred.setLowKey(tuple, true);
        ctx.pred.setHighKey(tuple, true);
        ctx.splitKey.reset();
        ctx.splitKey.getTuple().setFieldCount(cmp.getKeyFieldCount());
        
        boolean repeatOp = true;
        // we use this loop to deal with possibly multiple operation restarts
        // due to ongoing structure modifications during the descent
        while (repeatOp && ctx.opRestarts < MAX_RESTARTS) {
            performOp(rootPage, null, ctx);

            // if we reach this stage then we need to restart from the (possibly
            // new) root
            if (!ctx.pageLsns.isEmpty() && ctx.pageLsns.getLast() == RESTART_OP) {
                ctx.pageLsns.removeLast(); // pop the restart op indicator
                continue;
            }

            // we split the root, here is the key for a new root
            if (ctx.splitKey.getBuffer() != null) {
                createNewRoot(ctx);
            }

            unsetSmPages(ctx);

            repeatOp = false;
        }
    }
    
    public long uselessCompressionTime = 0;
    
    private void insertLeaf(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx) throws Exception {
    	ctx.leafFrame.setPage(node);
    	ctx.leafFrame.setPageTupleFieldCount(cmp.getFieldCount());
    	SpaceStatus spaceStatus = ctx.leafFrame.hasSpaceInsert(tuple, cmp);
    	switch (spaceStatus) {
    	
    	case SUFFICIENT_CONTIGUOUS_SPACE: {
    		//System.out.println("SUFFICIENT_CONTIGUOUS_SPACE");
    		ctx.leafFrame.insert(tuple, cmp);
    		ctx.splitKey.reset();
    	} break;

    	case SUFFICIENT_SPACE: {
    		//System.out.println("SUFFICIENT_SPACE");
    		ctx.leafFrame.compact(cmp);
    		ctx.leafFrame.insert(tuple, cmp);
    		ctx.splitKey.reset();
    	} break;

    	case INSUFFICIENT_SPACE: {
    		//System.out.println("INSUFFICIENT_SPACE");
    		
    		// try compressing the page first and see if there is space available    		
    		long start = System.currentTimeMillis();
    		boolean reCompressed = ctx.leafFrame.compress(cmp);
    		long end = System.currentTimeMillis();
    		if(reCompressed) spaceStatus = ctx.leafFrame.hasSpaceInsert(tuple, cmp);
    		
    		if(spaceStatus == SpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE) {    		
    			ctx.leafFrame.insert(tuple, cmp);
    			ctx.splitKey.reset();    			    			
    			
    			usefulCompression++;    			
    		}
    		else {
    			
    			uselessCompressionTime += (end - start);    			    			    			
    			uselessCompression++;
    			
    			// perform split
    			splitsByLevel[0]++; // debug
    			int rightSiblingPageId = ctx.leafFrame.getNextLeaf();
    			ICachedPage rightSibling = null;
    			if (rightSiblingPageId > 0) {
    				rightSibling = bufferCache.pin(FileHandle.getDiskPageId(fileId, rightSiblingPageId), false);
    				pins++;
    			}

    			treeLatch.writeLock().lock(); // lock is released in
    			// unsetSmPages(), after sm has
    			// fully completed
    			treeLatchesAcquired++;
    			try {

    				int rightPageId = getFreePage(ctx.metaFrame);
    				ICachedPage rightNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, rightPageId), true);
    				pins++;
    				rightNode.acquireWriteLatch();
    				writeLatchesAcquired++;
    				try {
    					IBTreeLeafFrame rightFrame = leafFrameFactory.getFrame();
    					rightFrame.setPage(rightNode);
    					rightFrame.initBuffer((byte) 0);
    					rightFrame.setPageTupleFieldCount(cmp.getFieldCount());
    					
    					int ret = ctx.leafFrame.split(rightFrame, tuple, cmp, ctx.splitKey);

    					ctx.smPages.add(pageId);
    					ctx.smPages.add(rightPageId);
    					ctx.leafFrame.setSmFlag(true);
    					rightFrame.setSmFlag(true);

    					rightFrame.setNextLeaf(ctx.leafFrame.getNextLeaf());
    					rightFrame.setPrevLeaf(pageId);
    					ctx.leafFrame.setNextLeaf(rightPageId);

    					// TODO: we just use increasing numbers as pageLsn, we
    					// should tie this together with the LogManager and
    					// TransactionManager
    					rightFrame.setPageLsn(rightFrame.getPageLsn() + 1);
    					ctx.leafFrame.setPageLsn(ctx.leafFrame.getPageLsn() + 1);

    					if (ret != 0) {
    						ctx.splitKey.reset();
    					} else {
    						// System.out.print("LEAF SPLITKEY: ");
    						// cmp.printKey(splitKey.getData(), 0);
    						// System.out.println("");

    						ctx.splitKey.setPages(pageId, rightPageId);
    					}
    					if (rightSibling != null) {
    						rightSibling.acquireWriteLatch();
    						writeLatchesAcquired++;
    						try {
    							rightFrame.setPage(rightSibling); // reuse
    							// rightFrame
    							// for
    							// modification
    							rightFrame.setPrevLeaf(rightPageId);
    						} finally {
    							rightSibling.releaseWriteLatch();
    							writeLatchesReleased++;
    						}
    					}
    				} finally {
    					rightNode.releaseWriteLatch();
    					writeLatchesReleased++;
    					bufferCache.unpin(rightNode);
    					unpins++;
    				}
    			} catch (Exception e) {
    				treeLatch.writeLock().unlock();
    				treeLatchesReleased++;
    				throw e;
    			} finally {
    				if (rightSibling != null) {
    					bufferCache.unpin(rightSibling);
    					unpins++;
    				}
    			}    	
    		}
    	} break;   
    	
    	}

    	node.releaseWriteLatch();
    	writeLatchesReleased++;
    	bufferCache.unpin(node);
    	unpins++;
    }

    private void insertInterior(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx) throws Exception {
        ctx.interiorFrame.setPage(node);
        ctx.interiorFrame.setPageTupleFieldCount(cmp.getKeyFieldCount());
        SpaceStatus spaceStatus = ctx.interiorFrame.hasSpaceInsert(tuple, cmp);
        switch (spaceStatus) {
            case INSUFFICIENT_SPACE: {
                splitsByLevel[ctx.interiorFrame.getLevel()]++; // debug
                int rightPageId = getFreePage(ctx.metaFrame);
                ICachedPage rightNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, rightPageId), true);
                pins++;
                rightNode.acquireWriteLatch();
                writeLatchesAcquired++;
                try {
                    IBTreeFrame rightFrame = interiorFrameFactory.getFrame();
                    rightFrame.setPage(rightNode);
                    rightFrame.initBuffer((byte) ctx.interiorFrame.getLevel());
                    rightFrame.setPageTupleFieldCount(cmp.getKeyFieldCount());
                    // instead of creating a new split key, use the existing
                    // splitKey
                    int ret = ctx.interiorFrame.split(rightFrame, ctx.splitKey.getTuple(), cmp, ctx.splitKey);

                    ctx.smPages.add(pageId);
                    ctx.smPages.add(rightPageId);
                    ctx.interiorFrame.setSmFlag(true);
                    rightFrame.setSmFlag(true);

                    // TODO: we just use increasing numbers as pageLsn, we
                    // should tie this together with the LogManager and
                    // TransactionManager
                    rightFrame.setPageLsn(rightFrame.getPageLsn() + 1);
                    ctx.interiorFrame.setPageLsn(ctx.interiorFrame.getPageLsn() + 1);

                    if (ret != 0) {
                        ctx.splitKey.reset();
                    } else {
                        // System.out.print("INTERIOR SPLITKEY: ");
                        // cmp.printKey(splitKey.getData(), 0);
                        // System.out.println("");

                        ctx.splitKey.setPages(pageId, rightPageId);
                    }
                } finally {
                    rightNode.releaseWriteLatch();
                    writeLatchesReleased++;
                    bufferCache.unpin(rightNode);
                    unpins++;
                }
            }
                break;

            case SUFFICIENT_CONTIGUOUS_SPACE: {
                // System.out.println("INSERT INTERIOR: " + pageId);
                ctx.interiorFrame.insert(tuple, cmp);
                ctx.splitKey.reset();
            }
                break;

            case SUFFICIENT_SPACE: {
                ctx.interiorFrame.compact(cmp);
                ctx.interiorFrame.insert(tuple, cmp);
                ctx.splitKey.reset();
            }
                break;

        }
    }
    
    public void delete(ITupleReference tuple, BTreeOpContext ctx) throws Exception {       
    	ctx.reset();
    	ctx.pred.setLowKeyComparator(cmp);
    	ctx.pred.setHighKeyComparator(cmp);
        ctx.pred.setLowKey(tuple, true);
        ctx.pred.setHighKey(tuple, true);        
        ctx.splitKey.reset();
        ctx.splitKey.getTuple().setFieldCount(cmp.getKeyFieldCount());        
        
        boolean repeatOp = true;
        // we use this loop to deal with possibly multiple operation restarts
        // due to ongoing structure modifications during the descent
        while (repeatOp && ctx.opRestarts < MAX_RESTARTS) {
            performOp(rootPage, null, ctx);

            // if we reach this stage then we need to restart from the (possibly
            // new) root
            if (!ctx.pageLsns.isEmpty() && ctx.pageLsns.getLast() == RESTART_OP) {
                ctx.pageLsns.removeLast(); // pop the restart op indicator
                continue;
            }

            // tree is empty, reset level to zero
            if (ctx.splitKey.getBuffer() != null) {
                ICachedPage rootNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, rootPage), false);
                pins++;
                rootNode.acquireWriteLatch();
                writeLatchesAcquired++;
                try {
                    ctx.leafFrame.setPage(rootNode);
                    ctx.leafFrame.initBuffer((byte) 0);
                    currentLevel = 0; // debug
                } finally {
                    rootNode.releaseWriteLatch();
                    writeLatchesReleased++;
                    bufferCache.unpin(rootNode);
                    unpins++;
                }
            }

            unsetSmPages(ctx);

            addFreePages(ctx);

            repeatOp = false;
        }
    }

    // TODO: to avoid latch deadlock, must modify cursor to detect empty leaves
    private void deleteLeaf(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx) throws Exception {
        ctx.leafFrame.setPage(node);
        
        // will this leaf become empty?
        if (ctx.leafFrame.getTupleCount() == 1) {
            IBTreeLeafFrame siblingFrame = leafFrameFactory.getFrame();
            
            ICachedPage leftNode = null;
            ICachedPage rightNode = null;
            int nextLeaf = ctx.leafFrame.getNextLeaf();
            int prevLeaf = ctx.leafFrame.getPrevLeaf();

            if (prevLeaf > 0)
                leftNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, prevLeaf), false);

            try {

                if (nextLeaf > 0)
                    rightNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, nextLeaf), false);

                try {
                    treeLatch.writeLock().lock();
                    treeLatchesAcquired++;

                    try {
                        ctx.leafFrame.delete(tuple, cmp, true);                        
                        // to propagate the deletion we only need to make the
                        // splitKey != null
                        // we can reuse data to identify which key to delete in
                        // the parent
                        ctx.splitKey.initData(1);
                    } catch (Exception e) {
                        // don't propagate deletion upwards if deletion at this
                        // level fails
                        ctx.splitKey.reset();
                        throw e;
                    }

                    ctx.leafFrame.setPageLsn(ctx.leafFrame.getPageLsn() + 1); // TODO:
                    // tie
                    // together
                    // with
                    // logging

                    ctx.smPages.add(pageId);
                    ctx.leafFrame.setSmFlag(true);

                    node.releaseWriteLatch();
                    writeLatchesReleased++;
                    bufferCache.unpin(node);
                    unpins++;

                    if (leftNode != null) {
                        leftNode.acquireWriteLatch();
                        try {
                            siblingFrame.setPage(leftNode);
                            siblingFrame.setNextLeaf(nextLeaf);
                            siblingFrame.setPageLsn(siblingFrame.getPageLsn() + 1); // TODO:
                            // tie
                            // together
                            // with
                            // logging
                        } finally {
                            leftNode.releaseWriteLatch();
                        }
                    }

                    if (rightNode != null) {
                        rightNode.acquireWriteLatch();
                        try {
                            siblingFrame.setPage(rightNode);
                            siblingFrame.setPrevLeaf(prevLeaf);
                            siblingFrame.setPageLsn(siblingFrame.getPageLsn() + 1); // TODO:
                            // tie
                            // together
                            // with
                            // logging
                        } finally {
                            rightNode.releaseWriteLatch();
                        }
                    }

                    // register pageId as a free
                    ctx.freePages.add(pageId);

                } catch (Exception e) {
                    treeLatch.writeLock().unlock();
                    treeLatchesReleased++;
                    throw e;
                } finally {
                    if (rightNode != null) {
                        bufferCache.unpin(rightNode);
                    }
                }
            } finally {
                if (leftNode != null) {
                    bufferCache.unpin(leftNode);
                }
            }
        } else { // leaf will not become empty
            ctx.leafFrame.delete(tuple, cmp, true);
            node.releaseWriteLatch();
            writeLatchesReleased++;
            bufferCache.unpin(node);
            unpins++;
        }
    }

    private void deleteInterior(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx) throws Exception {
        ctx.interiorFrame.setPage(node);

        // this means there is only a child pointer but no key, this case
        // propagates the split
        if (ctx.interiorFrame.getTupleCount() == 0) {
            ctx.interiorFrame.setPageLsn(ctx.interiorFrame.getPageLsn() + 1); // TODO:
            // tie
            // together
            // with
            // logging
            ctx.smPages.add(pageId);
            ctx.interiorFrame.setSmFlag(true);
            ctx.interiorFrame.setRightmostChildPageId(-1); // this node is
            // completely empty
            // register this pageId as a free page
            ctx.freePages.add(pageId);

        } else {
            ctx.interiorFrame.delete(tuple, cmp, false);
            ctx.interiorFrame.setPageLsn(ctx.interiorFrame.getPageLsn() + 1); // TODO:
            // tie
            // together
            // with
            // logging
            ctx.splitKey.reset(); // don't propagate deletion
        }
    }

    private final void acquireLatch(ICachedPage node, BTreeOp op, boolean isLeaf) {
        if (isLeaf && (op.equals(BTreeOp.BTO_INSERT) || op.equals(BTreeOp.BTO_DELETE))) {
            node.acquireWriteLatch();
            writeLatchesAcquired++;
        } else {
            node.acquireReadLatch();
            readLatchesAcquired++;
        }
    }

    private final void releaseLatch(ICachedPage node, BTreeOp op, boolean isLeaf) {
        if (isLeaf && (op.equals(BTreeOp.BTO_INSERT) || op.equals(BTreeOp.BTO_DELETE))) {
            node.releaseWriteLatch();
            writeLatchesReleased++;
        } else {
            node.releaseReadLatch();
            readLatchesReleased++;
        }
    }

    private boolean isConsistent(int pageId, BTreeOpContext ctx) throws Exception {
        ICachedPage node = bufferCache.pin(FileHandle.getDiskPageId(fileId, pageId), false);
        pins++;
        node.acquireReadLatch();
        readLatchesAcquired++;
        ctx.interiorFrame.setPage(node);
        boolean isConsistent = false;
        try {
            isConsistent = ctx.pageLsns.getLast() == ctx.interiorFrame.getPageLsn();
        } finally {
            node.releaseReadLatch();
            readLatchesReleased++;
            bufferCache.unpin(node);
            unpins++;
        }
        return isConsistent;
    }

    private void performOp(int pageId, ICachedPage parent, BTreeOpContext ctx) throws Exception {
        ICachedPage node = bufferCache.pin(FileHandle.getDiskPageId(fileId, pageId), false);
        pins++;

        ctx.interiorFrame.setPage(node);
        boolean isLeaf = ctx.interiorFrame.isLeaf();
        acquireLatch(node, ctx.op, isLeaf);
        boolean smFlag = ctx.interiorFrame.getSmFlag();

        // remember trail of pageLsns, to unwind recursion in case of an ongoing
        // structure modification
        ctx.pageLsns.add(ctx.interiorFrame.getPageLsn());
        
        try {

            // latch coupling, note: parent should never be write latched,
            // otherwise something is wrong.
            if (parent != null) {
                parent.releaseReadLatch();
                readLatchesReleased++;
                bufferCache.unpin(parent);
                unpins++;
            }

            if (!isLeaf || smFlag) {
                if (!smFlag) {
                    // we use this loop to deal with possibly multiple operation
                    // restarts due to ongoing structure modifications during
                    // the descent
                    boolean repeatOp = true;
                    while (repeatOp && ctx.opRestarts < MAX_RESTARTS) {
                        int childPageId = ctx.interiorFrame.getChildPageId(ctx.pred, cmp);
                        performOp(childPageId, node, ctx);

                        if (!ctx.pageLsns.isEmpty() && ctx.pageLsns.getLast() == RESTART_OP) {
                            ctx.pageLsns.removeLast(); // pop the restart op indicator
                            if (isConsistent(pageId, ctx)) {
                                node = null; // to avoid unpinning and
                                // unlatching node again in
                                // recursive call
                                continue; // descend the tree again
                            } else {
                                ctx.pageLsns.removeLast(); // pop pageLsn of this page
                                // (version seen by this op
                                // during descent)
                                ctx.pageLsns.add(RESTART_OP); // this node is
                                // not
                                // consistent,
                                // set the
                                // restart
                                // indicator for
                                // upper level
                                break;
                            }
                        }

                        switch (ctx.op) {

                            case BTO_INSERT: {
                                if (ctx.splitKey.getBuffer() != null) {
                                    node = bufferCache.pin(FileHandle.getDiskPageId(fileId, pageId), false);
                                    pins++;
                                    node.acquireWriteLatch();
                                    writeLatchesAcquired++;
                                    try {
                                        insertInterior(node, pageId, ctx.splitKey.getTuple(), ctx);
                                    } finally {
                                        node.releaseWriteLatch();
                                        writeLatchesReleased++;
                                        bufferCache.unpin(node);
                                        unpins++;
                                    }
                                } else {
                                    unsetSmPages(ctx);
                                }
                            }
                                break;

                            case BTO_DELETE: {
                            	if (ctx.splitKey.getBuffer() != null) {
                                    node = bufferCache.pin(FileHandle.getDiskPageId(fileId, pageId), false);
                                    pins++;
                                    node.acquireWriteLatch();
                                    writeLatchesAcquired++;
                                    try {
                                        deleteInterior(node, pageId, ctx.pred.getLowKey(), ctx);
                                    } finally {
                                        node.releaseWriteLatch();
                                        writeLatchesReleased++;
                                        bufferCache.unpin(node);
                                        unpins++;
                                    }
                                } else {
                                    unsetSmPages(ctx);
                                }
                            }
                                break;

                            case BTO_SEARCH: {
                                // do nothing
                            }
                                break;

                        }

                        repeatOp = false; // operation completed

                    } // end while
                } else { // smFlag
                    ctx.opRestarts++;
                    System.out.println("ONGOING SM ON PAGE " + pageId + " AT LEVEL " + ctx.interiorFrame.getLevel() + ", RESTARTS: " + ctx.opRestarts);                    
                    releaseLatch(node, ctx.op, isLeaf);
                    bufferCache.unpin(node);
                    unpins++;
                    
                    // TODO: this should be an instant duration lock, how to do
                    // this in java?
                    // instead we just immediately release the lock. this is
                    // inefficient but still correct and will not cause
                    // latch-deadlock
                    treeLatch.readLock().lock();
                    treeLatch.readLock().unlock();

                    // unwind recursion and restart operation, find lowest page
                    // with a pageLsn as seen by this operation during descent
                    ctx.pageLsns.removeLast(); // pop current page lsn
                    // put special value on the stack to inform caller of
                    // restart
                    ctx.pageLsns.add(RESTART_OP);
                }
            } else { // isLeaf and !smFlag
                switch (ctx.op) {
                    case BTO_INSERT: {                    	
                    	insertLeaf(node, pageId, ctx.pred.getLowKey(), ctx);
                    } break;

                    case BTO_DELETE: {
                        deleteLeaf(node, pageId, ctx.pred.getLowKey(), ctx);
                    } break;

                    case BTO_SEARCH: {
                        ctx.cursor.open(node, ctx.pred);
                    } break;
                }
            }
        } catch (BTreeException e) {
            //System.out.println("BTREE EXCEPTION");
            //System.out.println(e.getMessage());
            //e.printStackTrace();
            if (!e.getHandled()) {
                releaseLatch(node, ctx.op, isLeaf);
                bufferCache.unpin(node);
                unpins++;
                e.setHandled(true);
            }
            throw e;
        } catch (Exception e) { // this could be caused, e.g. by a
            // failure to pin a new node during a split
            System.out.println("ASTERIX EXCEPTION");
            e.printStackTrace();
            releaseLatch(node, ctx.op, isLeaf);
            bufferCache.unpin(node);
            unpins++;
            BTreeException propException = new BTreeException(e);
            propException.setHandled(true); // propagate a BTreeException,
            // indicating that the parent node
            // must not be unlatched and
            // unpinned
            throw propException;
        }
    }
    
    private boolean bulkNewPage = false;

    public final class BulkLoadContext {               
        public final int slotSize;
        public final int leafMaxBytes;
        public final int interiorMaxBytes;
        public final SplitKey splitKey;
        // we maintain a frontier of nodes for each level
        private final ArrayList<NodeFrontier> nodeFrontiers = new ArrayList<NodeFrontier>();         
        private final IBTreeLeafFrame leafFrame;
        private final IBTreeInteriorFrame interiorFrame;
        private final IBTreeMetaDataFrame metaFrame;
        
        private final IBTreeTupleWriter tupleWriter;
        
        public BulkLoadContext(float fillFactor, IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame,
                IBTreeMetaDataFrame metaFrame) throws Exception {

        	splitKey = new SplitKey(leafFrame.getTupleWriter().createTupleReference());
        	tupleWriter = leafFrame.getTupleWriter();
        	        	
        	NodeFrontier leafFrontier = new NodeFrontier(leafFrame.createTupleReference());
            leafFrontier.pageId = getFreePage(metaFrame);
            leafFrontier.page = bufferCache.pin(FileHandle.getDiskPageId(fileId, leafFrontier.pageId), bulkNewPage);
            leafFrontier.page.acquireWriteLatch();

            interiorFrame.setPage(leafFrontier.page);
            interiorFrame.initBuffer((byte) 0);
            interiorMaxBytes = (int) ((float) interiorFrame.getBuffer().capacity() * fillFactor);
            
            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
            leafMaxBytes = (int) ((float) leafFrame.getBuffer().capacity() * fillFactor);
            
            slotSize = leafFrame.getSlotSize();

            this.leafFrame = leafFrame;
            this.interiorFrame = interiorFrame;
            this.metaFrame = metaFrame;

            nodeFrontiers.add(leafFrontier);
        }

        private void addLevel() throws Exception {
            NodeFrontier frontier = new NodeFrontier(tupleWriter.createTupleReference());
            frontier.pageId = getFreePage(metaFrame);
            frontier.page = bufferCache.pin(FileHandle.getDiskPageId(fileId, frontier.pageId), bulkNewPage);
            frontier.page.acquireWriteLatch();
            frontier.lastTuple.setFieldCount(cmp.getKeyFieldCount());
            interiorFrame.setPage(frontier.page);
            interiorFrame.initBuffer((byte) nodeFrontiers.size());
            nodeFrontiers.add(frontier);
        }
    }

    private void propagateBulk(BulkLoadContext ctx, int level) throws Exception {

        if (ctx.splitKey.getBuffer() == null)
            return;
        
        if (level >= ctx.nodeFrontiers.size())
            ctx.addLevel();
        
        NodeFrontier frontier = ctx.nodeFrontiers.get(level);
        ctx.interiorFrame.setPage(frontier.page);
        
        ITupleReference tuple = ctx.splitKey.getTuple();   
        int spaceNeeded = ctx.tupleWriter.bytesRequired(tuple, 0, cmp.getKeyFieldCount()) + ctx.slotSize + 4;                
        int spaceUsed = ctx.interiorFrame.getBuffer().capacity() - ctx.interiorFrame.getTotalFreeSpace();
        if (spaceUsed + spaceNeeded > ctx.interiorMaxBytes) {
        	
        	SplitKey copyKey = ctx.splitKey.duplicate(ctx.leafFrame.getTupleWriter().createTupleReference());
        	tuple = copyKey.getTuple();
        	
            frontier.lastTuple.resetByOffset(frontier.page.getBuffer(), ctx.interiorFrame.getTupleOffset(ctx.interiorFrame.getTupleCount()-1));            
            int splitKeySize = ctx.tupleWriter.bytesRequired(frontier.lastTuple, 0, cmp.getKeyFieldCount());
            ctx.splitKey.initData(splitKeySize);
            ctx.tupleWriter.writeTupleFields(frontier.lastTuple, 0, cmp.getKeyFieldCount(), ctx.splitKey.getBuffer(), 0);
            ctx.splitKey.getTuple().resetByOffset(ctx.splitKey.getBuffer(), 0);
            ctx.splitKey.setLeftPage(frontier.pageId);
            
            ctx.interiorFrame.deleteGreatest(cmp);
            
            frontier.page.releaseWriteLatch();
            bufferCache.unpin(frontier.page);
            frontier.pageId = getFreePage(ctx.metaFrame);
            
            ctx.splitKey.setRightPage(frontier.pageId);
            propagateBulk(ctx, level + 1);

            frontier.page = bufferCache.pin(FileHandle.getDiskPageId(fileId, frontier.pageId), bulkNewPage);
            frontier.page.acquireWriteLatch();
            ctx.interiorFrame.setPage(frontier.page);
            ctx.interiorFrame.initBuffer((byte) level);
        }
        ctx.interiorFrame.insertSorted(tuple, cmp);
        
        // debug print
        //ISerializerDeserializer[] btreeSerde = { UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        //String s = ctx.interiorFrame.printKeys(cmp, btreeSerde);
        //System.out.println(s);
    }
    
    // assumes btree has been created and opened
    public BulkLoadContext beginBulkLoad(float fillFactor, IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame, IBTreeMetaDataFrame metaFrame) throws Exception {
        
    	if(loaded) throw new BTreeException("Trying to bulk-load BTree but has BTree already been loaded.");
    	
    	BulkLoadContext ctx = new BulkLoadContext(fillFactor, leafFrame, interiorFrame, metaFrame);
        ctx.nodeFrontiers.get(0).lastTuple.setFieldCount(cmp.getFieldCount());
        ctx.splitKey.getTuple().setFieldCount(cmp.getKeyFieldCount());
        return ctx;
    }
    
    public void bulkLoadAddTuple(BulkLoadContext ctx, ITupleReference tuple) throws Exception {                        
        NodeFrontier leafFrontier = ctx.nodeFrontiers.get(0);
        IBTreeLeafFrame leafFrame = ctx.leafFrame;
        
        int spaceNeeded =  ctx.tupleWriter.bytesRequired(tuple) + ctx.slotSize;
        int spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();
        
        // try to free space by compression
        if (spaceUsed + spaceNeeded > ctx.leafMaxBytes) {        	
        	leafFrame.compress(cmp);
        	spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();
        }
        
        if (spaceUsed + spaceNeeded > ctx.leafMaxBytes) {        	        	
        	leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount()-1);        	
        	int splitKeySize = ctx.tupleWriter.bytesRequired(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount());        	
            ctx.splitKey.initData(splitKeySize);
            ctx.tupleWriter.writeTupleFields(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount(), ctx.splitKey.getBuffer(), 0);
            ctx.splitKey.getTuple().resetByOffset(ctx.splitKey.getBuffer(), 0);
            ctx.splitKey.setLeftPage(leafFrontier.pageId);
            int prevPageId = leafFrontier.pageId;
            leafFrontier.pageId = getFreePage(ctx.metaFrame);
                        
            leafFrame.setNextLeaf(leafFrontier.pageId);
            leafFrontier.page.releaseWriteLatch();
            bufferCache.unpin(leafFrontier.page);
                       
            ctx.splitKey.setRightPage(leafFrontier.pageId);
            propagateBulk(ctx, 1);
                       
            leafFrontier.page = bufferCache.pin(FileHandle.getDiskPageId(fileId, leafFrontier.pageId), bulkNewPage);
            leafFrontier.page.acquireWriteLatch();
            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
            leafFrame.setPrevLeaf(prevPageId);
        }
        
        leafFrame.setPage(leafFrontier.page);
        leafFrame.insertSorted(tuple, cmp);
        
        // debug print
        //ISerializerDeserializer[] btreeSerde = { UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
        //String s = leafFrame.printKeys(cmp, btreeSerde);
        //System.out.println(s);        
    }
    
    public void endBulkLoad(BulkLoadContext ctx) throws Exception {                
        // copy root
        ICachedPage rootNode = bufferCache.pin(FileHandle.getDiskPageId(fileId, rootPage), bulkNewPage);
        rootNode.acquireWriteLatch();
        try {        
            ICachedPage toBeRoot = ctx.nodeFrontiers.get(ctx.nodeFrontiers.size() - 1).page;
            System.arraycopy(toBeRoot.getBuffer().array(), 0, rootNode.getBuffer().array(), 0, toBeRoot.getBuffer().capacity());        
        } finally {        
            rootNode.releaseWriteLatch();
            bufferCache.unpin(rootNode);

            // cleanup
            for (int i = 0; i < ctx.nodeFrontiers.size(); i++) {
                ctx.nodeFrontiers.get(i).page.releaseWriteLatch();
                bufferCache.unpin(ctx.nodeFrontiers.get(i).page);
            }
        }
        // debug
        currentLevel = (byte) ctx.nodeFrontiers.size();
        
        loaded = true;
    }
        
    public BTreeOpContext createOpContext(BTreeOp op, IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame,
            IBTreeMetaDataFrame metaFrame) {    	
    	// TODO: figure out better tree-height hint
    	return new BTreeOpContext(op, leafFrame, interiorFrame, metaFrame, 6);    	
    }
        
    public IBTreeInteriorFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    public IBTreeLeafFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }
    
    public MultiComparator getMultiComparator() {
        return cmp;
    }
}
