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

package org.apache.hyracks.storage.am.lsm.rtree.impls;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;

public class LSMRTreeWithAntiMatterTuplesFlushCursor extends EnforcedIndexCursor implements ILSMIndexCursor {
    private final TreeTupleSorter rTreeTupleSorter;
    private final TreeTupleSorter bTreeTupleSorter;
    private final int[] comparatorFields;
    private final MultiComparator cmp;
    private ITupleReference frameTuple;
    private ITupleReference leftOverTuple;
    private ITupleReference rtreeTuple;
    private ITupleReference btreeTuple;
    private boolean foundNext = false;

    public LSMRTreeWithAntiMatterTuplesFlushCursor(TreeTupleSorter rTreeTupleSorter, TreeTupleSorter bTreeTupleSorter,
            int[] comparatorFields, IBinaryComparatorFactory[] comparatorFactories) {
        this.rTreeTupleSorter = rTreeTupleSorter;
        this.bTreeTupleSorter = bTreeTupleSorter;
        this.comparatorFields = comparatorFields;
        cmp = MultiComparator.create(comparatorFactories);
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        try {
            rTreeTupleSorter.open(initialState, searchPred);
            bTreeTupleSorter.open(initialState, searchPred);
        } catch (Throwable th) { // NOSONAR: Must catch all failures
            IndexCursorUtils.close(bTreeTupleSorter, th);
            IndexCursorUtils.close(rTreeTupleSorter, th);
            throw HyracksDataException.create(th);
        }
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        if (foundNext) {
            return true;
        }
        while (true) {
            if (leftOverTuple != null && leftOverTuple == rtreeTuple) {
                if (bTreeTupleSorter.hasNext()) {
                    bTreeTupleSorter.next();
                    btreeTuple = bTreeTupleSorter.getTuple();
                } else {
                    frameTuple = rtreeTuple;
                    foundNext = true;
                    leftOverTuple = null;
                    return true;
                }
            } else if (leftOverTuple != null && leftOverTuple == btreeTuple) {
                if (rTreeTupleSorter.hasNext()) {
                    rTreeTupleSorter.next();
                    rtreeTuple = rTreeTupleSorter.getTuple();
                } else {
                    frameTuple = btreeTuple;
                    foundNext = true;
                    leftOverTuple = null;
                    return true;
                }
            } else {
                if (rTreeTupleSorter.hasNext() && bTreeTupleSorter.hasNext()) {
                    rTreeTupleSorter.next();
                    bTreeTupleSorter.next();
                    rtreeTuple = rTreeTupleSorter.getTuple();
                    btreeTuple = bTreeTupleSorter.getTuple();
                } else if (rTreeTupleSorter.hasNext()) {
                    rTreeTupleSorter.next();
                    rtreeTuple = rTreeTupleSorter.getTuple();
                    frameTuple = rtreeTuple;
                    leftOverTuple = null;
                    foundNext = true;
                    return true;
                } else if (bTreeTupleSorter.hasNext()) {
                    bTreeTupleSorter.next();
                    btreeTuple = bTreeTupleSorter.getTuple();
                    frameTuple = btreeTuple;
                    leftOverTuple = null;
                    foundNext = true;
                    return true;
                } else {
                    return false;
                }
            }

            int c = cmp.selectiveFieldCompare(rtreeTuple, btreeTuple, comparatorFields);
            if (c == 0) {
                frameTuple = rtreeTuple;
                leftOverTuple = null;
                foundNext = true;
                return true;
            } else if (c < 0) {
                frameTuple = rtreeTuple;
                leftOverTuple = btreeTuple;
                foundNext = true;
                return true;
            } else {
                frameTuple = btreeTuple;
                leftOverTuple = rtreeTuple;
                foundNext = true;
                return true;
            }
        }
    }

    @Override
    public void doNext() throws HyracksDataException {
        foundNext = false;

    }

    @Override
    public void doDestroy() throws HyracksDataException {
        try {
            rTreeTupleSorter.destroy();
        } finally {
            bTreeTupleSorter.destroy();
        }
    }

    @Override
    public void doClose() throws HyracksDataException {
        try {
            rTreeTupleSorter.close();
        } finally {
            bTreeTupleSorter.close();
        }
    }

    @Override
    public ITupleReference doGetTuple() {
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
    public boolean getSearchOperationCallbackProceedResult() {
        return false;
    }

}
