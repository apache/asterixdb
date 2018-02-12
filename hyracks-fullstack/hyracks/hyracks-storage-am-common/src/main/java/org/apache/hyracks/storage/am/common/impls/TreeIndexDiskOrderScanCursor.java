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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class TreeIndexDiskOrderScanCursor extends EnforcedIndexCursor implements ITreeIndexCursor {

    protected int tupleIndex = 0;
    protected int fileId = -1;
    protected int currentPageId = -1;
    protected int maxPageId = -1;
    protected ICachedPage page = null;
    protected IBufferCache bufferCache = null;

    private final ITreeIndexFrame frame;
    private final ITreeIndexTupleReference frameTuple;

    public TreeIndexDiskOrderScanCursor(ITreeIndexFrame frame) {
        this.frame = frame;
        this.frameTuple = frame.createTupleReference();
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        tupleIndex = 0;
        currentPageId = -1;
        maxPageId = -1;
        releasePage();
    }

    @Override
    public ITreeIndexTupleReference doGetTuple() {
        return frameTuple;
    }

    private boolean positionToNextLeaf(boolean skipCurrent) throws HyracksDataException {
        while (frame.getLevel() != 0 || skipCurrent || frame.getTupleCount() == 0) {
            if (++currentPageId > maxPageId) {
                break;
            }

            releasePage();
            ICachedPage nextPage = acquireNextPage();
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
    public boolean doHasNext() throws HyracksDataException {
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
    public void doNext() throws HyracksDataException {
        tupleIndex++;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        page = initialState.getPage();
        tupleIndex = 0;
        frame.setPage(page);
        positionToNextLeaf(false);
    }

    @Override
    public void doClose() throws HyracksDataException {
        tupleIndex = 0;
        currentPageId = -1;
        maxPageId = -1;
        releasePage();
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
    public boolean isExclusiveLatchNodes() {
        return false;
    }

    protected void releasePage() throws HyracksDataException {
        if (page != null) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            page = null;
        }
    }

    protected ICachedPage acquireNextPage() throws HyracksDataException {
        ICachedPage nextPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
        nextPage.acquireReadLatch();
        return nextPage;
    }
}
