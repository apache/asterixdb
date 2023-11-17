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
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
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

    protected FindTupleMode lowKeyFtm;
    protected FindTupleMode highKeyFtm;
    protected FindTupleNoExactMatchPolicy lowKeyFtp;
    protected FindTupleNoExactMatchPolicy highKeyFtp;
    protected boolean yieldFirstCall;

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
    public void doDestroy() {
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
            frameTuple.newPage();
            setCursorPosition();
            nextLeafPage = frame.getNextLeaf();
        } while (frame.getTupleCount() == 0 && nextLeafPage > 0);
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        int nextLeafPage;
        if (frameTuple.isConsumed() && !yieldFirstCall) {
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
        if (frame.getTupleCount() > 0) {
            frameTuple.newPage();
            initCursorPosition(searchPred);
        } else {
            yieldFirstCall = false;
            frameTuple.consume();
        }
    }

    protected void initCursorPosition(ISearchPredicate searchPred) throws HyracksDataException {
        setSearchPredicate(searchPred);
        reusablePredicate.setLowKeyComparator(originalKeyCmp);
        reusablePredicate.setHighKeyComparator(pred.getHighKeyComparator());
        reusablePredicate.setHighKey(pred.getHighKey(), pred.isHighKeyInclusive());
        yieldFirstCall = false;
        setCursorPosition();
    }

    private void setCursorPosition() throws HyracksDataException {
        int start = getLowKeyIndex();
        int end = getHighKeyIndex();
        if (end < start) {
            frameTuple.consume();
            return;
        }
        frameTuple.reset(start, end);
        yieldFirstCall = shouldYieldFirstCall();
    }

    protected boolean isNextIncluded() throws HyracksDataException {
        if (yieldFirstCall) {
            //The first call of frameTuple.next() was done during the opening of the cursor
            yieldFirstCall = false;
            return true;
        } else if (frameTuple.isConsumed()) {
            //All tuple were consumed
            return false;
        }
        //Next tuple
        frameTuple.next();
        //Check whether the frameTuple is not consumed and also include the search key
        return highKey == null
                || isLessOrEqual(frameTuple, highKey, pred.isHighKeyInclusive(), pred.getHighKeyComparator());
    }

    protected boolean shouldYieldFirstCall() throws HyracksDataException {
        // Proceed if the highKey is null or the current tuple's key is less than (or equal) the highKey
        return highKey == null
                || isLessOrEqual(frameTuple, highKey, pred.isHighKeyInclusive(), pred.getHighKeyComparator());
    }

    protected void releasePages() throws HyracksDataException {
        //Unpin all column pages first
        frameTuple.unpinColumnsPages();
        if (page0 != null) {
            bufferCache.unpin(page0);
        }
    }

    private boolean isLessOrEqual(ITupleReference left, ITupleReference right, boolean inclusive,
            MultiComparator comparator) throws HyracksDataException {
        int cmp = comparator.compare(left, right);
        return cmp < 0 || inclusive && cmp == 0;
    }

    protected int getLowKeyIndex() throws HyracksDataException {
        if (lowKey == null) {
            return 0;
        } else if (isLessOrEqual(frame.getRightmostTuple(), lowKey, !pred.isLowKeyInclusive(),
                pred.getLowKeyComparator())) {
            //The highest key from the frame is less than the requested lowKey
            return frame.getTupleCount();
        }

        int index = frameTuple.findTupleIndex(lowKey, pred.getLowKeyComparator(), lowKeyFtm, lowKeyFtp);
        if (pred.isLowKeyInclusive()) {
            index++;
        } else {
            if (index < 0) {
                index = frame.getTupleCount();
            }
        }

        return index;
    }

    protected int getHighKeyIndex() throws HyracksDataException {
        if (highKey == null) {
            return frame.getTupleCount() - 1;
        } else if (isLessOrEqual(highKey, frame.getLeftmostTuple(), !pred.isHighKeyInclusive(),
                pred.getHighKeyComparator())) {
            return -1;
        }

        int index = frameTuple.findTupleIndex(highKey, pred.getHighKeyComparator(), highKeyFtm, highKeyFtp);
        if (pred.isHighKeyInclusive()) {
            if (index < 0) {
                index = frame.getTupleCount() - 1;
            } else {
                index--;
            }
        }

        return index;
    }

    protected void setSearchPredicate(ISearchPredicate searchPred) {
        pred = (RangePredicate) searchPred;
        lowKey = pred.getLowKey();
        highKey = pred.getHighKey();

        lowKeyFtm = FindTupleMode.EXCLUSIVE;
        if (pred.isLowKeyInclusive()) {
            lowKeyFtp = FindTupleNoExactMatchPolicy.LOWER_KEY;
        } else {
            lowKeyFtp = FindTupleNoExactMatchPolicy.HIGHER_KEY;
        }

        highKeyFtm = FindTupleMode.EXCLUSIVE;
        if (pred.isHighKeyInclusive()) {
            highKeyFtp = FindTupleNoExactMatchPolicy.HIGHER_KEY;
        } else {
            highKeyFtp = FindTupleNoExactMatchPolicy.LOWER_KEY;
        }
    }

    @Override
    public void doClose() throws HyracksDataException {
        releasePages();
        frameTuple.close();
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
