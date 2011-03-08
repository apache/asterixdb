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

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class DiskOrderScanCursor implements IBTreeCursor {

    // TODO: might want to return tuples in physical order, not logical order to
    // speed up access

    private int tupleIndex = 0;
    private int fileId = -1;
    int currentPageId = -1;
    int maxPageId = -1; // TODO: figure out how to scan to the end of file, this
    // is dirty and may not with concurrent updates
    private ICachedPage page = null;
    private IBTreeLeafFrame frame = null;
    private IBufferCache bufferCache = null;

    private ITreeIndexTupleReference frameTuple;

    public DiskOrderScanCursor(IBTreeLeafFrame frame) {
        this.frame = frame;
        this.frameTuple = frame.getTupleWriter().createTupleReference();
    }

    @Override
    public void close() throws Exception {
        page.releaseReadLatch();
        bufferCache.unpin(page);
        page = null;
    }

    @Override
    public ITreeIndexTupleReference getTuple() {
        return frameTuple;
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    private boolean positionToNextLeaf(boolean skipCurrent) throws HyracksDataException {
        while ((frame.getLevel() != 0 || skipCurrent) && (currentPageId <= maxPageId) || (frame.getTupleCount() == 0)) {
            currentPageId++;

            ICachedPage nextPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
            nextPage.acquireReadLatch();

            page.releaseReadLatch();
            bufferCache.unpin(page);

            page = nextPage;
            frame.setPage(page);
            tupleIndex = 0;
            skipCurrent = false;
        }
        if (currentPageId <= maxPageId)
            return true;
        else
            return false;
    }

    @Override
    public boolean hasNext() throws Exception {
        if (tupleIndex >= frame.getTupleCount()) {
            boolean nextLeafExists = positionToNextLeaf(true);
            if (nextLeafExists) {
                frameTuple.resetByTupleIndex(frame, tupleIndex);
                return true;
            } else {
                return false;
            }
        }

        frameTuple.resetByTupleIndex(frame, tupleIndex);
        return true;
    }

    @Override
    public void next() throws Exception {
        tupleIndex++;
    }

    @Override
    public void open(ICachedPage page, ISearchPredicate searchPred) throws HyracksDataException {
        // in case open is called multiple times without closing
        if (this.page != null) {
            this.page.releaseReadLatch();
            bufferCache.unpin(this.page);
        }

        this.page = page;
        tupleIndex = 0;
        frame.setPage(page);
        RangePredicate pred = (RangePredicate) searchPred;
        MultiComparator lowKeyCmp = pred.getLowKeyComparator();
        frameTuple.setFieldCount(lowKeyCmp.getFieldCount());
        boolean leafExists = positionToNextLeaf(false);
        if (!leafExists) {
            throw new HyracksDataException(
                    "Failed to open disk-order scan cursor for B-tree. Traget B-tree has no leaves.");
        }
    }

    @Override
    public void reset() {
        tupleIndex = 0;
        currentPageId = -1;
        maxPageId = -1;
        page = null;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    public void setCurrentPageId(int currentPageId) {
        this.currentPageId = currentPageId;
    }

    public void setMaxPageId(int maxPageId) {
        this.maxPageId = maxPageId;
    }
}
