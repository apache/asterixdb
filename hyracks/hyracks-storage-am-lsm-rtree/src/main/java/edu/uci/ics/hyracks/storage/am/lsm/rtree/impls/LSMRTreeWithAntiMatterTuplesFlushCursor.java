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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMRTreeWithAntiMatterTuplesFlushCursor implements ITreeIndexCursor {
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
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {

    }

    @Override
    public boolean hasNext() throws HyracksDataException {
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
                leftOverTuple = null;
                continue;
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
    public void next() throws HyracksDataException {
        foundNext = false;

    }

    @Override
    public void close() throws HyracksDataException {
    }

    @Override
    public void reset() throws HyracksDataException {

    }

    @Override
    public ITupleReference getTuple() {
        return frameTuple;
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFileId(int fileId) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean exclusiveLatchNodes() {
        // TODO Auto-generated method stub
        return false;
    }

}
