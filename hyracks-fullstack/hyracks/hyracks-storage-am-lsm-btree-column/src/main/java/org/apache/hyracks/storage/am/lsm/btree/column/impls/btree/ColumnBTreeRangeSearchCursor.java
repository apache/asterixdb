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

package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTreeCursorInitialState;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class ColumnBTreeRangeSearchCursor extends EnforcedIndexCursor
        implements ITreeIndexCursor, IColumnReadMultiPageOp {

    protected final ColumnBTreeReadLeafFrame frame;
    protected final IColumnTupleIterator frameTuple;

    protected IBufferCache bufferCache = null;
    protected int fileId;

    protected int pageId;
    protected ICachedPage page0 = null;

    protected final RangePredicate reusablePredicate;
    protected MultiComparator originalKeyCmp;

    protected RangePredicate pred;
    protected ITupleReference lowKey;
    protected ITupleReference highKey;
    protected boolean firstNextCall;

    protected final IIndexCursorStats stats;

    public ColumnBTreeRangeSearchCursor(ColumnBTreeReadLeafFrame frame, IIndexCursorStats stats, int index) {
        this.frame = frame;
        this.frameTuple = frame.createTupleReference(index, this);
        this.reusablePredicate = new RangePredicate();
        this.stats = stats;
        fileId = -1;
        pageId = IBufferCache.INVALID_PAGEID;
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        // No Op all resources are released in the close call
    }

    @Override
    public ITupleReference doGetTuple() {
        return frameTuple;
    }

    private void fetchNextLeafPage(int leafPage) throws HyracksDataException {
        int nextLeafPage = leafPage;
        do {
            ICachedPage nextLeaf = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, nextLeafPage), false);
            stats.getPageCounter().update(1);
            bufferCache.unpin(page0);
            page0 = nextLeaf;
            frame.setPage(page0);
            frameTuple.reset(0);
            nextLeafPage = frame.getNextLeaf();
        } while (frame.getTupleCount() == 0 && nextLeafPage > 0);
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        int nextLeafPage;
        if (frameTuple.isConsumed() && !firstNextCall) {
            frameTuple.lastTupleReached();
            nextLeafPage = frame.getNextLeaf();
            if (nextLeafPage >= 0) {
                fetchNextLeafPage(nextLeafPage);
            } else {
                return false;
            }
        }
        return isNextIncluded();
    }

    @Override
    public void doNext() throws HyracksDataException {
        //NoOp
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // in case open is called multiple times without closing
        if (page0 != null) {
            releasePages();
        }
        originalKeyCmp = initialState.getOriginalKeyComparator();
        page0 = initialState.getPage();
        pageId = ((BTreeCursorInitialState) initialState).getPageId();
        frame.setPage(page0);
        frame.setMultiComparator(originalKeyCmp);
        frameTuple.reset(0);
        initCursorPosition(searchPred);
    }

    protected void initCursorPosition(ISearchPredicate searchPred) throws HyracksDataException {
        pred = (RangePredicate) searchPred;
        lowKey = pred.getLowKey();
        highKey = pred.getHighKey();

        reusablePredicate.setLowKeyComparator(originalKeyCmp);
        reusablePredicate.setHighKeyComparator(pred.getHighKeyComparator());
        reusablePredicate.setHighKey(pred.getHighKey(), pred.isHighKeyInclusive());
        firstNextCall = true;
        advanceTupleToLowKey();
    }

    protected boolean isNextIncluded() throws HyracksDataException {
        if (firstNextCall) {
            //The first call of frameTuple.next() was done during the opening of the cursor
            firstNextCall = false;
            return true;
        } else if (frameTuple.isConsumed()) {
            //All tuple were consumed
            return false;
        }
        //Next tuple
        frameTuple.next();
        //Check whether the frameTuple is not consumed and also include the search key
        return highKey == null || isLessOrEqual(frameTuple, highKey, pred.isHighKeyInclusive());
    }

    protected void advanceTupleToLowKey() throws HyracksDataException {
        if (highKey != null && isLessOrEqual(highKey, frame.getLeftmostTuple(), !pred.isHighKeyInclusive())) {
            /*
             * Lowest key from the frame is greater than the requested highKey. No tuple will satisfy the search
             * key. Consume the frameTuple to stop the search
             */
            firstNextCall = false;
            frameTuple.consume();
            return;
        } else if (lowKey == null) {
            //No range was specified.
            frameTuple.next();
            return;
        }

        //The lowKey is somewhere within the frame tuples
        boolean stop = false;
        int counter = 0;
        while (!stop && !frameTuple.isConsumed()) {
            frameTuple.next();
            stop = isLessOrEqual(lowKey, frameTuple, pred.isLowKeyInclusive());
            counter++;
        }
        //Advance all columns to the proper position
        frameTuple.skip(counter - 1);
    }

    protected void releasePages() throws HyracksDataException {
        //Unpin all column pages first
        frameTuple.unpinColumnsPages();
        if (page0 != null) {
            bufferCache.unpin(page0);
        }
    }

    private boolean isLessOrEqual(ITupleReference left, ITupleReference right, boolean inclusive)
            throws HyracksDataException {
        int cmp = originalKeyCmp.compare(left, right);
        return cmp < 0 || cmp == 0 && inclusive;
    }

    @Override
    public void doClose() throws HyracksDataException {
        releasePages();
        page0 = null;
        pred = null;
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

    /*
     * ***********************************************************
     * IColumnReadMultiPageOp
     * ***********************************************************
     */
    @Override
    public ICachedPage pin(int pageId) throws HyracksDataException {
        stats.getPageCounter().update(1);
        return bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        bufferCache.unpin(page);
    }

    @Override
    public int getPageSize() {
        return bufferCache.getPageSize();
    }
}
