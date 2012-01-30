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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileNameManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class LSMTree implements ILSMTree {
    protected static final long AFTER_MERGE_CLEANUP_SLEEP = 100;

    // In-memory components.
    protected final BTree memBTree;
    protected final InMemoryFreePageManager memFreePageManager;

    // On-disk components.
    protected final ILSMFileNameManager fileNameManager;
    // For creating BTree's used in flush and merge.
    protected final BTreeFactory diskBTreeFactory;

    protected final IBufferCache diskBufferCache;
    protected final IFileMapProvider diskFileMapProvider;
    protected LinkedList<BTree> diskBTrees = new LinkedList<BTree>();

    protected final MultiComparator cmp;

    // For synchronizing all operations with flushes.
    // Currently, all operations block during a flush.
    private int threadRefCount;
    protected boolean flushFlag;

    // For synchronizing searchers with a concurrent merge.
    protected AtomicBoolean isMerging = new AtomicBoolean(false);
    protected AtomicInteger searcherRefCountA = new AtomicInteger(0);
    protected AtomicInteger searcherRefCountB = new AtomicInteger(0);
    // Represents the current number of searcher threads that are operating on
    // the unmerged on-disk BTrees.
    // We alternate between searcherRefCountA and searcherRefCountB.
    protected AtomicInteger searcherRefCount = searcherRefCountA;

    public LSMTree(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMFileNameManager fileNameManager, BTreeFactory diskBTreeFactory, IFileMapProvider diskFileMapProvider,
            int fieldCount, MultiComparator cmp) {
        memBTree = new BTree(memBufferCache, fieldCount, cmp, memFreePageManager, btreeInteriorFrameFactory,
                btreeLeafFrameFactory);
        this.memFreePageManager = memFreePageManager;
        this.diskBufferCache = diskBTreeFactory.getBufferCache();
        this.diskFileMapProvider = diskFileMapProvider;
        this.diskBTreeFactory = diskBTreeFactory;
        this.cmp = cmp;
        this.diskBTrees = new LinkedList<BTree>();
        this.threadRefCount = 0;
        this.flushFlag = false;
        this.fileNameManager = fileNameManager;
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
        memBTree.create(indexFileId);
    }

    @Override
    public void close() throws HyracksDataException {
        for (BTree btree : diskBTrees) {
            diskBufferCache.closeFile(btree.getFileId());
            btree.close();
        }
        diskBTrees.clear();
        memBTree.close();
    }

    public void threadEnter() {
        threadRefCount++;
    }

    public void threadExit() throws HyracksDataException, TreeIndexException {
        synchronized (this) {
            threadRefCount--;
            // Check if we've reached or exceeded the maximum number of pages.
            if (!flushFlag && memFreePageManager.isFull()) {
                flushFlag = true;
            }
            // Flush will only be handled by last exiting thread.
            if (flushFlag && threadRefCount == 0) {
                flush();
                flushFlag = false;
            }
        }
    }

    protected void cleanupTrees(List<ITreeIndex> mergingDiskTrees) throws HyracksDataException {
        for (ITreeIndex oldTree : mergingDiskTrees) {
            oldTree.close();
            FileReference fileRef = diskFileMapProvider.lookupFileName(oldTree.getFileId());
            diskBufferCache.closeFile(oldTree.getFileId());
            diskBufferCache.deleteFile(oldTree.getFileId());
            fileRef.getFile().delete();
        }
    }
    
    protected void resetMemBTree() throws HyracksDataException {
        memFreePageManager.reset();
        memBTree.create(memBTree.getFileId());
    }

    protected ITreeIndex createFlushTargetTree(String fileName) throws HyracksDataException {
        return createDiskTree(diskBTreeFactory, fileName, true);
    }

    protected ITreeIndex createMergeTargetTree(String fileName) throws HyracksDataException {
        return createDiskTree(diskBTreeFactory, fileName, true);
    }

    protected ITreeIndex createDiskTree(TreeFactory diskTreeFactory, String fileName, boolean createTree)
            throws HyracksDataException {
        // Register the new tree file.
        FileReference file = new FileReference(new File(fileName));
        // File will be deleted during cleanup of merge().
        diskBufferCache.createFile(file);
        int diskTreeFileId = diskFileMapProvider.lookupFileId(file);
        // File will be closed during cleanup of merge().
        diskBufferCache.openFile(diskTreeFileId);
        // Create new tree instance.
        ITreeIndex diskTree = diskTreeFactory.createIndexInstance(diskTreeFileId);
        if (createTree) {
            diskTree.create(diskTreeFileId);
        }
        // Tree will be closed during cleanup of merge().
        diskTree.open(diskTreeFileId);
        return diskTree;
    }

    @Override
    public abstract void flush() throws HyracksDataException, TreeIndexException;

}
