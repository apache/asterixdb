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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleMode;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class BTreeRangeSearchCursor implements ITreeIndexCursor {

    private final IBTreeLeafFrame frame;
    private final ITreeIndexTupleReference frameTuple;
    private final boolean exclusiveLatchNodes;

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
                page.releaseWriteLatch();
            } else {
                page.releaseReadLatch();
            }
            bufferCache.unpin(page);
        }

        tupleIndex = 0;
        page = null;
        pred = null;
    }

    public ITupleReference getTuple() {
        return frameTuple;
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
                page.releaseWriteLatch();
            } else {
                nextLeaf.acquireReadLatch();
                page.releaseReadLatch();
            }
            bufferCache.unpin(page);

            page = nextLeaf;
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
                    page.releaseWriteLatch();
                } else {
                    page.releaseReadLatch();
                }
                bufferCache.unpin(page);
                page = null;

                // reconcile
                searchCb.reconcile(reconciliationTuple);

                // retraverse the index looking for the reconciled key
                reusablePredicate.setLowKey(reconciliationTuple, true);
                try {
                    accessor.search(this, reusablePredicate);
                } catch (IndexException e) {
                    throw new HyracksDataException(e);
                }

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
                page.releaseWriteLatch();
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
}