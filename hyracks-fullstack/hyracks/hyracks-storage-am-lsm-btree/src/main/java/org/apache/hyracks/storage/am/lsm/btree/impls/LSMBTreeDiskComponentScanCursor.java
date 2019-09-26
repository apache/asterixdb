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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;

public class LSMBTreeDiskComponentScanCursor extends LSMIndexSearchCursor {

    private static final IValueReference MATTER_TUPLE_FLAG = BooleanPointable.FACTORY.createPointable(false);
    private static final IValueReference ANTIMATTER_TUPLE_FLAG = BooleanPointable.FACTORY.createPointable(true);

    private BTreeAccessor[] btreeAccessors;

    private ArrayTupleBuilder tupleBuilder;
    private ArrayTupleBuilder antiMatterTupleBuilder;
    private final ArrayTupleReference outputTuple;
    private PermutingTupleReference originalTuple;

    private boolean foundNext;

    private IntegerPointable cursorIndexPointable;

    public LSMBTreeDiskComponentScanCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx, true, NoOpIndexCursorStats.INSTANCE);
        this.outputTuple = new ArrayTupleReference();
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        cmp = lsmInitialState.getOriginalKeyComparator();
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        includeMutableComponent = false;
        int numBTrees = operationalComponents.size();
        rangeCursors = new IIndexCursor[numBTrees];
        btreeAccessors = new BTreeAccessor[numBTrees];
        for (int i = 0; i < numBTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            BTree btree = (BTree) component.getIndex();
            btreeAccessors[i] = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            rangeCursors[i] = btreeAccessors[i].createSearchCursor(false);
        }
        IndexCursorUtils.open(btreeAccessors, rangeCursors, searchPred);
        try {
            cursorIndexPointable = new IntegerPointable();
            int length = IntegerPointable.TYPE_TRAITS.getFixedLength();
            cursorIndexPointable.set(new byte[length], 0, length);
            setPriorityQueueComparator();
            initPriorityQueue();
        } catch (Throwable th) { // NOSONAR: Must call this on
            for (int i = 0; i < numBTrees; i++) {
                IndexCursorUtils.close(rangeCursors[i], th);
            }
            throw HyracksDataException.create(th);
        }
    }

    @Override
    public void doNext() throws HyracksDataException {
        foundNext = false;
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        if (foundNext) {
            return true;
        }
        while (super.doHasNext()) {
            super.doNext();
            LSMBTreeTupleReference diskTuple = (LSMBTreeTupleReference) super.doGetTuple();
            if (diskTuple.isAntimatter()) {
                if (setAntiMatterTuple(diskTuple, outputElement.getCursorIndex())) {
                    foundNext = true;
                    return true;
                }
            } else {
                //matter tuple
                setMatterTuple(diskTuple, outputElement.getCursorIndex());
                foundNext = true;
                return true;
            }

        }

        return false;
    }

    @Override
    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB)
            throws HyracksDataException {
        // This method is used to check whether tupleA and tupleB (from different disk components) are identical.
        // If so, the tuple from the older component is ignored by default.
        // Here we use a simple trick so that tuples from different disk components are always not the same
        // so that they would be returned by the cursor anyway.
        return -1;
    }

    private void setMatterTuple(ITupleReference diskTuple, int cursorIndex) throws HyracksDataException {
        if (tupleBuilder == null) {
            tupleBuilder = new ArrayTupleBuilder(diskTuple.getFieldCount() + 2);
            antiMatterTupleBuilder = new ArrayTupleBuilder(diskTuple.getFieldCount() + 2);
            int[] permutation = new int[diskTuple.getFieldCount()];
            for (int i = 0; i < permutation.length; i++) {
                permutation[i] = i + 2;
            }
            originalTuple = new PermutingTupleReference(permutation);
        }
        //build the matter tuple
        buildTuple(tupleBuilder, diskTuple, cursorIndex, MATTER_TUPLE_FLAG);
        outputTuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        originalTuple.reset(outputTuple);
    }

    private boolean setAntiMatterTuple(ITupleReference diskTuple, int cursorIndex) throws HyracksDataException {
        if (originalTuple == null || cmp.compare(diskTuple, originalTuple) != 0) {
            // This could happen sometimes...
            // Consider insert tuple A into the memory component, and then delete it immediately.
            // We would have -A in the memory component, but there is no tuple A in the previous disk components.
            // But in this case, we can simply ignore it for the scan purpose
            return false;
        }
        buildTuple(antiMatterTupleBuilder, originalTuple, cursorIndex, ANTIMATTER_TUPLE_FLAG);
        outputTuple.reset(antiMatterTupleBuilder.getFieldEndOffsets(), antiMatterTupleBuilder.getByteArray());
        return true;
    }

    private void buildTuple(ArrayTupleBuilder builder, ITupleReference diskTuple, int cursorIndex,
            IValueReference tupleFlag) throws HyracksDataException {
        builder.reset();
        cursorIndexPointable.setInteger(cursorIndex);
        builder.addField(cursorIndexPointable);
        builder.addField(tupleFlag);
        for (int i = 0; i < diskTuple.getFieldCount(); i++) {
            builder.addField(diskTuple.getFieldData(i), diskTuple.getFieldStart(i), diskTuple.getFieldLength(i));
        }
    }

    @Override
    public ITupleReference doGetTuple() {
        return outputTuple;
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        Throwable failure = null;
        if (lsmHarness != null) {
            if (rangeCursors != null) {
                failure = CleanupUtils.destroy(failure, rangeCursors);
                rangeCursors = null;
            }
            try {
                lsmHarness.endScanDiskComponents(opCtx);
            } catch (Throwable th) { // NOSONAR. Don't lose the root cause
                failure = ExceptionUtils.suppress(failure, th);
            }
        }
        foundNext = false;
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    @Override
    protected void setPriorityQueueComparator() {
        if (pqCmp == null || cmp != pqCmp.getMultiComparator()) {
            pqCmp = new PriorityQueueScanComparator(cmp);
        }
    }

    private class PriorityQueueScanComparator extends PriorityQueueComparator {
        public PriorityQueueScanComparator(MultiComparator cmp) {
            super(cmp);
        }

        @Override
        public int compare(PriorityQueueElement elementA, PriorityQueueElement elementB) {
            int result;
            try {
                result = cmp.compare(elementA.getTuple(), elementB.getTuple());
                if (result != 0) {
                    return result;
                }
            } catch (HyracksDataException e) {
                throw new IllegalArgumentException(e);
            }
            // the components in the component list are in descending order of creation time
            // we want older components to be returned first
            return elementA.getCursorIndex() > elementB.getCursorIndex() ? -1 : 1;

        }
    }

}
