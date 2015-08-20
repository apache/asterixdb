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

import java.util.ArrayDeque;
import java.util.Deque;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.ITupleAcceptor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IntArrayList;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.LongArrayList;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class BTreeOpContext implements IIndexOperationContext {
    private final int INIT_ARRAYLIST_SIZE = 6;

    public IIndexAccessor accessor;
    public MultiComparator cmp;
    public ITreeIndexFrameFactory leafFrameFactory;
    public ITreeIndexFrameFactory interiorFrameFactory;
    public IBTreeLeafFrame leafFrame;
    public IBTreeInteriorFrame interiorFrame;
    public ITreeIndexMetaDataFrame metaFrame;
    public IndexOperation op;
    public ITreeIndexCursor cursor;
    public BTreeCursorInitialState cursorInitialState;
    public RangePredicate pred;
    public BTreeSplitKey splitKey;
    public LongArrayList pageLsns;
    public IntArrayList smPages;
    public IntArrayList freePages;
    public int opRestarts = 0;
    public boolean exceptionHandled;
    public IModificationOperationCallback modificationCallback;
    public ISearchOperationCallback searchCallback;
    public ITupleAcceptor acceptor;
    public int smoCount;

    // Debug
    public final Deque<PageValidationInfo> validationInfos;
    public final ITreeIndexTupleReference interiorFrameTuple;
    public final ITreeIndexTupleReference leafFrameTuple;

    public BTreeOpContext(IIndexAccessor accessor, ITreeIndexFrameFactory leafFrameFactory,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexMetaDataFrame metaFrame,
            IBinaryComparatorFactory[] cmpFactories, IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        this.accessor = accessor;

        if (cmpFactories[0] != null) {
            //            this.cmp = MultiComparator.createIgnoreFieldLength(cmpFactories);
            this.cmp = MultiComparator.create(cmpFactories);
        } else {
            this.cmp = null;
        }

        this.leafFrameFactory = leafFrameFactory;
        this.leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        if (leafFrame != null && this.cmp != null) {
            leafFrame.setMultiComparator(cmp);
        }
        this.interiorFrameFactory = interiorFrameFactory;
        this.interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();
        if (interiorFrame != null && this.cmp != null) {
            interiorFrame.setMultiComparator(cmp);
        }
        this.metaFrame = metaFrame;
        this.pageLsns = new LongArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
        this.smoCount = 0;
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;

        // Debug
        this.validationInfos = new ArrayDeque<PageValidationInfo>(INIT_ARRAYLIST_SIZE);
        this.interiorFrameTuple = interiorFrame.createTupleReference();
        this.leafFrameTuple = leafFrame.createTupleReference();
    }

    public void reset() {
        if (pageLsns != null)
            pageLsns.clear();
        if (freePages != null)
            freePages.clear();
        if (smPages != null)
            smPages.clear();
        opRestarts = 0;
        smoCount = 0;
        exceptionHandled = false;
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        if (newOp == IndexOperation.SEARCH || newOp == IndexOperation.DISKORDERSCAN) {
            if (cursorInitialState == null) {
                cursorInitialState = new BTreeCursorInitialState(null, searchCallback, accessor);
            }
        } else {
            // Insert, delete, update or upsert operation.
            if (smPages == null) {
                smPages = new IntArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
            }
            if (freePages == null) {
                freePages = new IntArrayList(INIT_ARRAYLIST_SIZE, INIT_ARRAYLIST_SIZE);
            }
            if (pred == null) {
                pred = new RangePredicate(null, null, true, true, null, null);
            }
            if (splitKey == null) {
                splitKey = new BTreeSplitKey(leafFrame.getTupleWriter().createTupleReference());
            }
        }
        op = newOp;
        smoCount = 0;
        exceptionHandled = false;
    }

    public IBTreeLeafFrame createLeafFrame() {
        return (IBTreeLeafFrame) leafFrameFactory.createFrame();
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
            this.numKeyFields = cmp.getKeyFieldCount();
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
}
