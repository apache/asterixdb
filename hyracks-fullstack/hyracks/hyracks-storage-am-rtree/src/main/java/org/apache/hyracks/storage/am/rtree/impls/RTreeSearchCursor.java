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

package org.apache.hyracks.storage.am.rtree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class RTreeSearchCursor extends EnforcedIndexCursor implements ITreeIndexCursor {

    private int fileId = -1;
    private ICachedPage page = null;
    private IRTreeInteriorFrame interiorFrame = null;
    protected IRTreeLeafFrame leafFrame = null;
    private IBufferCache bufferCache = null;

    private SearchPredicate pred;
    private PathList pathList;
    private int rootPage;
    protected ITupleReference searchKey;

    private int tupleIndex = 0;
    private int tupleIndexInc = 0;
    private int currentTupleIndex = 0;
    private int pageId = -1;

    protected MultiComparator cmp;

    private ITreeIndexTupleReference frameTuple;
    private boolean readLatched = false;

    private final IIndexCursorStats stats;

    public RTreeSearchCursor(IRTreeInteriorFrame interiorFrame, IRTreeLeafFrame leafFrame) {
        this(interiorFrame, leafFrame, NoOpIndexCursorStats.INSTANCE);
    }

    public RTreeSearchCursor(IRTreeInteriorFrame interiorFrame, IRTreeLeafFrame leafFrame, IIndexCursorStats stats) {
        this.interiorFrame = interiorFrame;
        this.leafFrame = leafFrame;
        this.frameTuple = leafFrame.createTupleReference();
        this.stats = stats;
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        if (readLatched) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            readLatched = false;
        }
        tupleIndex = 0;
        tupleIndexInc = 0;
        page = null;
        pathList = null;
    }

    @Override
    public ITupleReference doGetTuple() {
        return frameTuple;
    }

    public int getTupleOffset() {
        return leafFrame.getTupleOffset(currentTupleIndex);
    }

    public int getPageId() {
        return pageId;
    }

    protected boolean fetchNextLeafPage() throws HyracksDataException {
        boolean succeeded = false;
        if (readLatched) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            readLatched = false;
        }

        while (!pathList.isEmpty()) {
            int pageId = pathList.getLastPageId();
            long parentLsn = pathList.getLastPageLsn();
            pathList.moveLast();
            if (pageId < 0) {
                throw new IllegalStateException();
            }
            if (fileId < 0) {
                throw new IllegalStateException();
            }
            ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            node.acquireReadLatch();
            stats.getPageCounter().update(1);
            readLatched = true;
            try {
                interiorFrame.setPage(node);
                boolean isLeaf = interiorFrame.isLeaf();
                long pageLsn = interiorFrame.getPageLsn();

                if (pageId != rootPage && parentLsn < interiorFrame.getPageNsn()) {
                    // Concurrent split detected, we need to visit the right
                    // page
                    int rightPage = interiorFrame.getRightPage();
                    if (rightPage != -1) {
                        pathList.add(rightPage, parentLsn, -1);
                    }
                }

                if (!isLeaf) {
                    // We do DFS so that we get the tuples ordered (for disk
                    // RTrees only) in the case we we are using total order
                    // (such as Hilbert order)
                    if (searchKey != null) {
                        for (int i = interiorFrame.getTupleCount() - 1; i >= 0; i--) {
                            int childPageId = interiorFrame.getChildPageIdIfIntersect(searchKey, i, cmp);
                            if (childPageId != -1) {
                                pathList.add(childPageId, pageLsn, -1);
                            }
                        }
                    } else {
                        for (int i = interiorFrame.getTupleCount() - 1; i >= 0; i--) {
                            int childPageId = interiorFrame.getChildPageId(i);
                            pathList.add(childPageId, pageLsn, -1);
                        }
                    }

                } else {
                    page = node;
                    this.pageId = pageId; // This is only needed for the
                                          // LSMRTree flush operation
                    leafFrame.setPage(page);
                    tupleIndex = 0;
                    succeeded = true;
                    return true;
                }
            } finally {
                if (!succeeded) {
                    if (readLatched) {
                        node.releaseReadLatch();
                        readLatched = false;
                        bufferCache.unpin(node);
                    }
                }
            }
        }
        return false;
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        if (page == null) {
            return false;
        }

        if (tupleIndex == leafFrame.getTupleCount()) {
            if (!fetchNextLeafPage()) {
                return false;
            }
        }

        do {
            for (int i = tupleIndex; i < leafFrame.getTupleCount(); i++) {
                if (searchKey != null) {
                    if (leafFrame.intersect(searchKey, i, cmp)) {
                        frameTuple.resetByTupleIndex(leafFrame, i);
                        currentTupleIndex = i; // This is only needed for the
                                               // LSMRTree flush operation
                        tupleIndexInc = i + 1;
                        return true;
                    }
                } else {
                    frameTuple.resetByTupleIndex(leafFrame, i);
                    currentTupleIndex = i; // This is only needed for the
                                           // LSMRTree
                                           // flush operation
                    tupleIndexInc = i + 1;
                    return true;
                }
            }
        } while (fetchNextLeafPage());
        return false;
    }

    @Override
    public void doNext() throws HyracksDataException {
        tupleIndex = tupleIndexInc;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // in case open is called multiple times without closing
        if (this.page != null) {
            this.page.releaseReadLatch();
            readLatched = false;
            bufferCache.unpin(this.page);
            pathList.clear();
        }

        pathList = ((RTreeCursorInitialState) initialState).getPathList();
        rootPage = ((RTreeCursorInitialState) initialState).getRootPage();

        pred = (SearchPredicate) searchPred;
        cmp = pred.getLowKeyComparator();
        searchKey = pred.getLowKey();

        if (searchKey != null) {
            int maxFieldPos = cmp.getKeyFieldCount() / 2;
            for (int i = 0; i < maxFieldPos; i++) {
                int j = maxFieldPos + i;
                int c = cmp.getComparators()[i].compare(searchKey.getFieldData(i), searchKey.getFieldStart(i),
                        searchKey.getFieldLength(i), searchKey.getFieldData(j), searchKey.getFieldStart(j),
                        searchKey.getFieldLength(j));
                if (c > 0) {
                    throw new IllegalArgumentException(
                            "The low key point has larger coordinates than the high key point.");
                }
            }
        }

        pathList.add(this.rootPage, -1, -1);
        tupleIndex = 0;
        fetchNextLeafPage();
    }

    @Override
    public void doClose() throws HyracksDataException {
        doDestroy();
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public boolean isExclusiveLatchNodes() {
        return false;
    }
}
