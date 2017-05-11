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
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class BTreeRangeSearchCursor implements ITreeIndexCursor {

    private final IBTreeLeafFrame frame;
    private final ITreeIndexTupleReference frameTuple;
    private final boolean exclusiveLatchNodes;
    private boolean isPageDirty;

    private IBufferCache bufferCache = null;
    private int fileId = -1;

    private ICachedPage page = null;
    private int pageId = -1; // This is used by the LSMRTree flush operation

    private int tupleIndex = 0;
    private int stopTupleIndex;

    private final RangePredicate reusablePredicate;
    private final ArrayTupleReference reconciliationTuple;
    private IIndexAccessor accessor;
    private ISearchOperationCallback searchCb;
    private MultiComparator originalKeyCmp;
    private ArrayTupleBuilder tupleBuilder;

    private FindTupleMode lowKeyFtm;
    private FindTupleMode highKeyFtm;
    private FindTupleNoExactMatchPolicy lowKeyFtp;
    private FindTupleNoExactMatchPolicy highKeyFtp;

    private RangePredicate pred;
    private MultiComparator lowKeyCmp;
    private MultiComparator highKeyCmp;
    protected ITupleReference lowKey;
    private ITupleReference highKey;

    public BTreeRangeSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes) {
        this.frame = frame;
        this.frameTuple = frame.createTupleReference();
        this.exclusiveLatchNodes = exclusiveLatchNodes;
        this.reusablePredicate = new RangePredicate();
        this.reconciliationTuple = new ArrayTupleReference();
    }

    @Override
    public void close() throws HyracksDataException {
        if (page != null) {
            if (exclusiveLatchNodes) {
                page.releaseWriteLatch(isPageDirty);
            } else {
                page.releaseReadLatch();
            }
            bufferCache.unpin(page);
        }

        tupleIndex = 0;
        page = null;
        isPageDirty = false;
        pred = null;
    }

    @Override
    public ITupleReference getTuple() {
        return frameTuple;
    }

    @Override
    public ITupleReference getFilterMinTuple() {
        return null;
    }

    @Override
    public ITupleReference getFilterMaxTuple() {
        return null;
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    public int getTupleOffset() {
        return frame.getTupleOffset(tupleIndex - 1);
    }

    public int getPageId() {
        return pageId;
    }

    private void fetchNextLeafPage(int nextLeafPage) throws HyracksDataException {
        do {
            ICachedPage nextLeaf = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, nextLeafPage), false);
            if (exclusiveLatchNodes) {
                nextLeaf.acquireWriteLatch();
                page.releaseWriteLatch(isPageDirty);
            } else {
                nextLeaf.acquireReadLatch();
                page.releaseReadLatch();
            }
            bufferCache.unpin(page);

            page = nextLeaf;
            isPageDirty = false;
            frame.setPage(page);
            pageId = nextLeafPage;
            nextLeafPage = frame.getNextLeaf();
        } while (frame.getTupleCount() == 0 && nextLeafPage > 0);
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
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

                // unlatch/unpin
                if (exclusiveLatchNodes) {
                    page.releaseWriteLatch(isPageDirty);
                } else {
                    page.releaseReadLatch();
                }
                bufferCache.unpin(page);
                page = null;
                isPageDirty = false;

                // reconcile
                searchCb.reconcile(reconciliationTuple);

                // retraverse the index looking for the reconciled key
                reusablePredicate.setLowKey(reconciliationTuple, true);
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
    public void next() throws HyracksDataException {
        tupleIndex++;
    }

    private int getLowKeyIndex() throws HyracksDataException {
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

    private int getHighKeyIndex() throws HyracksDataException {
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
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // in case open is called multiple times without closing
        if (page != null) {
            if (exclusiveLatchNodes) {
                page.releaseWriteLatch(isPageDirty);
            } else {
                page.releaseReadLatch();
            }
            bufferCache.unpin(page);
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

    @Override
    public void reset() throws HyracksDataException {
        close();
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
    public boolean exclusiveLatchNodes() {
        return exclusiveLatchNodes;
    }

    @Override
    public void markCurrentTupleAsUpdated() throws HyracksDataException {
        if (exclusiveLatchNodes) {
            isPageDirty = true;
        } else {
            throw new HyracksDataException("This cursor has not been created with the intention to allow updates.");
        }
    }

    public boolean isBloomFilterAware() {
        return false;
    }
}
