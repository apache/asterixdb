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

package org.apache.hyracks.storage.am.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class BTreeRangeSearchCursor extends EnforcedIndexCursor implements ITreeIndexCursor {

    protected final IBTreeLeafFrame frame;
    protected final ITreeIndexTupleReference frameTuple;
    protected final boolean exclusiveLatchNodes;
    protected boolean isPageDirty;

    protected IBufferCache bufferCache = null;
    protected int fileId = -1;

    protected ICachedPage page = null;
    protected int pageId = -1; // This is used by the LSMRTree flush operation

    protected int tupleIndex = 0;
    protected int stopTupleIndex;

    protected final RangePredicate reusablePredicate;
    protected final ArrayTupleReference reconciliationTuple;
    protected IIndexAccessor accessor;
    protected ISearchOperationCallback searchCb;
    protected MultiComparator originalKeyCmp;
    protected ArrayTupleBuilder tupleBuilder;

    protected FindTupleMode lowKeyFtm;
    protected FindTupleMode highKeyFtm;
    protected FindTupleNoExactMatchPolicy lowKeyFtp;
    protected FindTupleNoExactMatchPolicy highKeyFtp;

    protected RangePredicate pred;
    protected MultiComparator lowKeyCmp;
    protected MultiComparator highKeyCmp;
    protected ITupleReference lowKey;
    protected ITupleReference highKey;

    protected final IIndexCursorStats stats;

    public BTreeRangeSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes) {
        this(frame, exclusiveLatchNodes, NoOpIndexCursorStats.INSTANCE);
    }

    public BTreeRangeSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes, IIndexCursorStats stats) {
        this.frame = frame;
        this.frameTuple = frame.createTupleReference();
        this.exclusiveLatchNodes = exclusiveLatchNodes;
        this.reusablePredicate = new RangePredicate();
        this.reconciliationTuple = new ArrayTupleReference();
        this.stats = stats;
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        // No Op all resources are released in the close call
    }

    @Override
    public ITupleReference doGetTuple() {
        return frameTuple;
    }

    public int getTupleOffset() {
        return frame.getTupleOffset(tupleIndex - 1);
    }

    public int getPageId() {
        return pageId;
    }

    protected void fetchNextLeafPage(int nextLeafPage) throws HyracksDataException {
        do {
            ICachedPage nextLeaf = acquirePage(nextLeafPage);
            releasePage();
            page = nextLeaf;
            isPageDirty = false;
            frame.setPage(page);
            pageId = nextLeafPage;
            nextLeafPage = frame.getNextLeaf();
        } while (frame.getTupleCount() == 0 && nextLeafPage > 0);
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        int nextLeafPage;
        if (tupleIndex >= frame.getTupleCount()) {
            nextLeafPage = frame.getNextLeaf();
            if (nextLeafPage >= 0) {
                fetchNextLeafPage(nextLeafPage);
                tupleIndex = 0;
                stopTupleIndex = getHighKeyIndex();
                if (stopTupleIndex < 0) {
                    return false;
                }
            } else {
                return false;
            }
        }

        if (tupleIndex > stopTupleIndex) {
            return false;
        }

        frameTuple.resetByTupleIndex(frame, tupleIndex);
        while (true) {
            if (searchCb.proceed(frameTuple)) {
                return true;
            } else {
                // copy the tuple before we unlatch/unpin
                if (tupleBuilder == null) {
                    tupleBuilder = new ArrayTupleBuilder(originalKeyCmp.getKeyFieldCount());
                }
                TupleUtils.copyTuple(tupleBuilder, frameTuple, originalKeyCmp.getKeyFieldCount());
                reconciliationTuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

                releasePage();
                page = null;
                isPageDirty = false;

                // reconcile
                searchCb.reconcile(reconciliationTuple);

                // retraverse the index looking for the reconciled key
                reusablePredicate.setLowKey(reconciliationTuple, true);
                // before re-using the cursor, we must close it
                close();
                // this search call will re-open the cursor
                accessor.search(this, reusablePredicate);
                if (stopTupleIndex < 0 || tupleIndex > stopTupleIndex) {
                    return false;
                }

                // see if we found the tuple we reconciled on
                frameTuple.resetByTupleIndex(frame, tupleIndex);
                if (originalKeyCmp.compare(reconciliationTuple, frameTuple) == 0) {
                    return true;
                } else {
                    searchCb.cancel(reconciliationTuple);
                }
            }
        }
    }

    @Override
    public void doNext() throws HyracksDataException {
        tupleIndex++;
    }

    protected int getLowKeyIndex() throws HyracksDataException {
        if (lowKey == null) {
            return 0;
        }

        int index = frame.findTupleIndex(lowKey, frameTuple, lowKeyCmp, lowKeyFtm, lowKeyFtp);
        if (pred.lowKeyInclusive) {
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
        }

        int index = frame.findTupleIndex(highKey, frameTuple, highKeyCmp, highKeyFtm, highKeyFtp);
        if (pred.highKeyInclusive) {
            if (index < 0) {
                index = frame.getTupleCount() - 1;
            } else {
                index--;
            }
        }

        return index;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // in case open is called multiple times without closing
        if (page != null) {
            resetBeforeOpen();
        }
        accessor = ((BTreeCursorInitialState) initialState).getAccessor();
        searchCb = initialState.getSearchOperationCallback();
        originalKeyCmp = initialState.getOriginalKeyComparator();
        pageId = ((BTreeCursorInitialState) initialState).getPageId();
        page = initialState.getPage();
        isPageDirty = false;
        frame.setPage(page);

        pred = (RangePredicate) searchPred;
        lowKeyCmp = pred.getLowKeyComparator();
        highKeyCmp = pred.getHighKeyComparator();
        lowKey = pred.getLowKey();
        highKey = pred.getHighKey();

        reusablePredicate.setLowKeyComparator(originalKeyCmp);
        reusablePredicate.setHighKeyComparator(pred.getHighKeyComparator());
        reusablePredicate.setHighKey(pred.getHighKey(), pred.isHighKeyInclusive());

        lowKeyFtm = FindTupleMode.EXCLUSIVE;
        if (pred.lowKeyInclusive) {
            lowKeyFtp = FindTupleNoExactMatchPolicy.LOWER_KEY;
        } else {
            lowKeyFtp = FindTupleNoExactMatchPolicy.HIGHER_KEY;
        }

        highKeyFtm = FindTupleMode.EXCLUSIVE;
        if (pred.highKeyInclusive) {
            highKeyFtp = FindTupleNoExactMatchPolicy.HIGHER_KEY;
        } else {
            highKeyFtp = FindTupleNoExactMatchPolicy.LOWER_KEY;
        }

        tupleIndex = getLowKeyIndex();
        stopTupleIndex = getHighKeyIndex();
    }

    protected void resetBeforeOpen() throws HyracksDataException {
        releasePage();
    }

    @Override
    public void doClose() throws HyracksDataException {
        if (page != null) {
            releasePage();
        }

        tupleIndex = 0;
        page = null;
        isPageDirty = false;
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
        return exclusiveLatchNodes;
    }

    protected void releasePage() throws HyracksDataException {
        if (exclusiveLatchNodes) {
            page.releaseWriteLatch(isPageDirty);
        } else {
            page.releaseReadLatch();
        }
        bufferCache.unpin(page);
    }

    protected ICachedPage acquirePage(int pageId) throws HyracksDataException {
        ICachedPage nextPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        if (exclusiveLatchNodes) {
            nextPage.acquireWriteLatch();
        } else {
            nextPage.acquireReadLatch();
        }
        stats.getPageCounter().update(1);
        return nextPage;
    }
}
