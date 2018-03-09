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

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.api.ITupleAcceptor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;
import org.apache.hyracks.storage.common.arraylist.LongArrayList;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

public class BTreeOpContext implements IIndexOperationContext, IExtraPageBlockHelper {
    private static final int INIT_ARRAYLIST_SIZE = 6;

    private final IIndexAccessor accessor;
    private final MultiComparator cmp;
    private final ITreeIndexFrameFactory interiorFrameFactory;
    private final IBTreeInteriorFrame interiorFrame;
    private final IPageManager freePageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    private ITreeIndexFrameFactory leafFrameFactory;
    private IBTreeLeafFrame leafFrame;
    private IndexOperation op;
    private ITreeIndexCursor cursor;
    private BTreeCursorInitialState cursorInitialState;
    private RangePredicate pred;
    private BTreeSplitKey splitKey;
    private LongArrayList pageLsns;
    private IntArrayList smPages;
    private IntArrayList freePages;
    private int opRestarts = 0;
    private boolean exceptionHandled;
    private IModificationOperationCallback modificationCallback;
    private ISearchOperationCallback searchCallback;
    private ITupleAcceptor acceptor;
    private int smoCount;
    private boolean destroyed = false;

    // Debug
    private final Deque<PageValidationInfo> validationInfos;
    private final ITreeIndexTupleReference interiorFrameTuple;

    public BTreeOpContext(IIndexAccessor accessor, ITreeIndexFrameFactory leafFrameFactory,
            ITreeIndexFrameFactory interiorFrameFactory, IPageManager freePageManager,
            IBinaryComparatorFactory[] cmpFactories, IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        this.accessor = accessor;

        if (cmpFactories[0] != null) {
            //            this.cmp = MultiComparator.createIgnoreFieldLength(cmpFactories);
            this.cmp = MultiComparator.create(cmpFactories);
        } else {
            this.cmp = null;
        }

        this.setLeafFrameFactory(leafFrameFactory);
        this.leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        if (getLeafFrame() != null && this.getCmp() != null) {
            getLeafFrame().setMultiComparator(getCmp());
        }
        this.interiorFrameFactory = interiorFrameFactory;
        this.interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        if (getInteriorFrame() != null && this.getCmp() != null) {
            getInteriorFrame().setMultiComparator(getCmp());
        }
        this.freePageManager = freePageManager;
        this.metaFrame = freePageManager.createMetadataFrame();
        this.pageLsns = new LongArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
        this.smoCount = 0;
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;

        // Debug
        this.validationInfos = new ArrayDeque<>(INIT_ARRAYLIST_SIZE);
        this.interiorFrameTuple = getInteriorFrame().createTupleReference();
    }

    @Override
    public void reset() {
        if (pageLsns != null) {
            pageLsns.clear();
        }
        if (freePages != null) {
            freePages.clear();
        }
        if (smPages != null) {
            smPages.clear();
        }
        opRestarts = 0;
        smoCount = 0;
        exceptionHandled = false;
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        if (newOp == IndexOperation.SEARCH || newOp == IndexOperation.DISKORDERSCAN) {
            if (cursorInitialState == null) {
                cursorInitialState = new BTreeCursorInitialState(searchCallback, accessor);
            }
        } else {
            // Insert, delete, update or upsert operation.
            if (smPages == null) {
                smPages = new IntArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
            }
            if (freePages == null) {
                freePages = new IntArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
            }
            if (getPred() == null) {
                setPred(new RangePredicate(null, null, true, true, null, null));
            }
            if (splitKey == null) {
                splitKey = new BTreeSplitKey(getLeafFrame().getTupleWriter().createTupleReference());
            }
        }
        op = newOp;
        smoCount = 0;
        exceptionHandled = false;
    }

    public IBTreeLeafFrame createLeafFrame() {
        return (IBTreeLeafFrame) getLeafFrameFactory().createFrame();
    }

    public IBTreeInteriorFrame createInteriorFrame() {
        return (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
    }

    public PageValidationInfo createPageValidationInfo(PageValidationInfo parent) throws HyracksDataException {
        return new PageValidationInfo(parent);
    }

    public class PageValidationInfo {
        public final int numKeyFields;

        public final ArrayTupleBuilder lowRangeBuilder;
        public final ArrayTupleBuilder highRangeBuilder;
        public final ArrayTupleReference lowRangeTuple;
        public final ArrayTupleReference highRangeTuple;

        public boolean isLowRangeNull;
        public boolean isHighRangeNull;

        public PageValidationInfo() {
            this.numKeyFields = getCmp().getKeyFieldCount();
            this.lowRangeBuilder = new ArrayTupleBuilder(numKeyFields);
            this.highRangeBuilder = new ArrayTupleBuilder(numKeyFields);
            this.lowRangeTuple = new ArrayTupleReference();
            this.highRangeTuple = new ArrayTupleReference();
            this.isLowRangeNull = true;
            this.isHighRangeNull = true;
        }

        public PageValidationInfo(PageValidationInfo copy) throws HyracksDataException {
            this();
            if (copy != null) {
                propagateLowRangeKey(copy);
                propagateHighRangeKey(copy);
            }
        }

        public void propagateLowRangeKey(PageValidationInfo toPropagate) throws HyracksDataException {
            isLowRangeNull = toPropagate.isLowRangeNull;
            if (!isLowRangeNull) {
                adjustRangeKey(lowRangeBuilder, lowRangeTuple, toPropagate.lowRangeTuple);
            }
        }

        public void propagateHighRangeKey(PageValidationInfo toPropagate) throws HyracksDataException {
            isHighRangeNull = toPropagate.isHighRangeNull;
            if (!isHighRangeNull) {
                adjustRangeKey(highRangeBuilder, highRangeTuple, toPropagate.highRangeTuple);
            }
        }

        public void adjustLowRangeKey(ITupleReference newLowRangeKey) throws HyracksDataException {
            isLowRangeNull = newLowRangeKey == null ? true : false;
            if (!isLowRangeNull) {
                adjustRangeKey(lowRangeBuilder, lowRangeTuple, newLowRangeKey);
            }
        }

        public void adjustHighRangeKey(ITupleReference newHighRangeKey) throws HyracksDataException {
            isHighRangeNull = newHighRangeKey == null ? true : false;
            if (!isHighRangeNull) {
                adjustRangeKey(highRangeBuilder, highRangeTuple, newHighRangeKey);
            }
        }

        private void adjustRangeKey(ArrayTupleBuilder builder, ArrayTupleReference tuple, ITupleReference newRangeKey)
                throws HyracksDataException {
            TupleUtils.copyTuple(builder, newRangeKey, numKeyFields);
            tuple.reset(builder.getFieldEndOffsets(), builder.getByteArray());
        }
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    public void setCallbacks(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;
        if (cursorInitialState != null) {
            cursorInitialState.setSearchOperationCallback(searchCallback);
        }
    }

    @Override
    public int getFreeBlock(int size) throws HyracksDataException {
        return freePageManager.takeBlock(getMetaFrame(), size);
    }

    @Override
    public void returnFreePageBlock(int blockPageId, int size) throws HyracksDataException {
        freePageManager.releaseBlock(getMetaFrame(), blockPageId, size);
    }

    public ITreeIndexMetadataFrame getMetaFrame() {
        return metaFrame;
    }

    public ISearchOperationCallback getSearchCallback() {
        return searchCallback;
    }

    public Deque<PageValidationInfo> getValidationInfos() {
        return validationInfos;
    }

    public IBTreeLeafFrame getLeafFrame() {
        return leafFrame;
    }

    public ITreeIndexTupleReference getInteriorFrameTuple() {
        return interiorFrameTuple;
    }

    public RangePredicate getPred() {
        return pred;
    }

    public void setPred(RangePredicate pred) {
        this.pred = pred;
    }

    public ITreeIndexCursor getCursor() {
        return cursor;
    }

    public void setCursor(ITreeIndexCursor cursor) {
        this.cursor = cursor;
    }

    public int getOpRestarts() {
        return opRestarts;
    }

    public LongArrayList getPageLsns() {
        return pageLsns;
    }

    public void setPageLsns(LongArrayList pageLsns) {
        this.pageLsns = pageLsns;
    }

    public IntArrayList getSmPages() {
        return smPages;
    }

    public IBTreeInteriorFrame getInteriorFrame() {
        return interiorFrame;
    }

    public int getSmoCount() {
        return smoCount;
    }

    public BTreeSplitKey getSplitKey() {
        return splitKey;
    }

    public IModificationOperationCallback getModificationCallback() {
        return modificationCallback;
    }

    public MultiComparator getCmp() {
        return cmp;
    }

    public ITupleAcceptor getAcceptor() {
        return acceptor;
    }

    public void setOpRestarts(int opRestarts) {
        this.opRestarts = opRestarts;
    }

    public BTreeCursorInitialState getCursorInitialState() {
        return cursorInitialState;
    }

    public boolean isExceptionHandled() {
        return exceptionHandled;
    }

    public void setExceptionHandled(boolean exceptionHandled) {
        this.exceptionHandled = exceptionHandled;
    }

    public void setAcceptor(ITupleAcceptor acceptor) {
        this.acceptor = acceptor;
    }

    public void setSmoCount(int smoCount) {
        this.smoCount = smoCount;
    }

    public void setLeafFrame(IBTreeLeafFrame leafFrame) {
        this.leafFrame = leafFrame;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public void setLeafFrameFactory(ITreeIndexFrameFactory leafFrameFactory) {
        this.leafFrameFactory = leafFrameFactory;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        Throwable failure = CleanupUtils.destroy(null, accessor, cursor);
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }
}
