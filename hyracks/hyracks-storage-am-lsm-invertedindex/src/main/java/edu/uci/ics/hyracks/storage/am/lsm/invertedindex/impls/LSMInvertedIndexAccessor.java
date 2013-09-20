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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;

public class LSMInvertedIndexAccessor implements ILSMIndexAccessorInternal, IInvertedIndexAccessor {

    protected final ILSMHarness lsmHarness;
    protected final ILSMIndexOperationContext ctx;

    public LSMInvertedIndexAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx) {
        this.lsmHarness = lsmHarness;
        this.ctx = ctx;
    }

    @Override
    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.INSERT);
        lsmHarness.modify(ctx, false, tuple);
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.DELETE);
        lsmHarness.modify(ctx, false, tuple);
    }

    @Override
    public boolean tryInsert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.INSERT);
        return lsmHarness.modify(ctx, true, tuple);
    }

    @Override
    public boolean tryDelete(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.DELETE);
        return lsmHarness.modify(ctx, true, tuple);
    }

    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.SEARCH);
        lsmHarness.search(ctx, cursor, searchPred);
    }

    public IIndexCursor createSearchCursor() {
        return new LSMInvertedIndexSearchCursor();
    }

    @Override
    public void scheduleFlush(ILSMIOOperationCallback callback) throws HyracksDataException {
        ctx.setOperation(IndexOperation.FLUSH);
        lsmHarness.scheduleFlush(ctx, callback);
    }

    @Override
    public void flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        lsmHarness.flush(ctx, operation);
    }

    @Override
    public void scheduleMerge(ILSMIOOperationCallback callback, List<ILSMComponent> components)
            throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.MERGE);
        ctx.getComponentsToBeMerged().clear();
        ctx.getComponentsToBeMerged().addAll(components);
        lsmHarness.scheduleMerge(ctx, callback);
    }

    @Override
    public void scheduleFullMerge(ILSMIOOperationCallback callback) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.FULL_MERGE);
        lsmHarness.scheduleFullMerge(ctx, callback);
    }

    @Override
    public void merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        lsmHarness.merge(ctx, operation);
    }

    @Override
    public IIndexCursor createRangeSearchCursor() {
        return new LSMInvertedIndexRangeSearchCursor(ctx);
    }

    @Override
    public void rangeSearch(IIndexCursor cursor, ISearchPredicate searchPred) throws IndexException,
            HyracksDataException {
        search(cursor, searchPred);
    }

    @Override
    public void forcePhysicalDelete(ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Physical delete not supported by lsm inverted index.");
    }

    @Override
    public void forceInsert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.INSERT);
        lsmHarness.forceModify(ctx, tuple);
    }

    @Override
    public void forceDelete(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.DELETE);
        lsmHarness.forceModify(ctx, tuple);
    }

    @Override
    public void physicalDelete(ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Physical delete not supported by lsm inverted index.");
    }

    @Override
    public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Update not supported by lsm inverted index.");
    }

    @Override
    public void upsert(ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Upsert not supported by lsm inverted index.");
    }

    @Override
    public boolean tryUpdate(ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Update not supported by lsm inverted index.");
    }

    @Override
    public boolean tryUpsert(ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Upsert not supported by lsm inverted index.");
    }

    @Override
    public IInvertedListCursor createInvertedListCursor() {
        throw new UnsupportedOperationException("Cannot create inverted list cursor on lsm inverted index.");
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference searchKey)
            throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Cannot open inverted list cursor on lsm inverted index.");
    }
}
