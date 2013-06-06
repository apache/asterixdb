/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class TreeIndexDiskOrderScanCursor implements ITreeIndexCursor {

    private int tupleIndex = 0;
    private int fileId = -1;
    private int currentPageId = -1;
    private int maxPageId = -1;
    private ICachedPage page = null;
    private IBufferCache bufferCache = null;

    private final ITreeIndexFrame frame;
    private final ITreeIndexTupleReference frameTuple;

    public TreeIndexDiskOrderScanCursor(ITreeIndexFrame frame) {
        this.frame = frame;
        this.frameTuple = frame.createTupleReference();
    }

    @Override
    public void close() throws HyracksDataException {
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
        while ((frame.getLevel() != 0 || skipCurrent || frame.getTupleCount() == 0) && (currentPageId <= maxPageId)) {
            currentPageId++;

            page.releaseReadLatch();
            bufferCache.unpin(page);

            ICachedPage nextPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
            nextPage.acquireReadLatch();

            page = nextPage;
            frame.setPage(page);
            tupleIndex = 0;
            skipCurrent = false;
        }
        if (currentPageId <= maxPageId) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (currentPageId > maxPageId) {
            return false;
        }
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
    public void next() throws HyracksDataException {
        tupleIndex++;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // in case open is called multiple times without closing
        if (page != null) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
        }
        page = initialState.getPage();
        tupleIndex = 0;
        frame.setPage(page);
        positionToNextLeaf(false);
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

    @Override
    public boolean exclusiveLatchNodes() {
        return false;
    }
}
