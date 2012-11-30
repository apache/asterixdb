/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;

public class LSMInvertedIndexAccessor implements ILSMIndexAccessor, IInvertedIndexAccessor {

    protected final ILSMHarness lsmHarness;
    protected final ILSMIndexFileManager fileManager;
    protected final ILSMIndexOperationContext ctx;
    protected final LSMInvertedIndex invIndex;

    public LSMInvertedIndexAccessor(LSMInvertedIndex invIndex, ILSMHarness lsmHarness,
            ILSMIndexFileManager fileManager, ILSMIndexOperationContext ctx) {
        this.lsmHarness = lsmHarness;
        this.fileManager = fileManager;
        this.ctx = ctx;
        this.invIndex = invIndex;
    }

    @Override
    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.INSERT);
        lsmHarness.insertUpdateOrDelete(tuple, ctx, false);
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.DELETE);
        lsmHarness.insertUpdateOrDelete(tuple, ctx, false);
    }

    @Override
    public boolean tryInsert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.INSERT);
        return lsmHarness.insertUpdateOrDelete(tuple, ctx, true);
    }

    @Override
    public boolean tryDelete(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.DELETE);
        return lsmHarness.insertUpdateOrDelete(tuple, ctx, true);
    }

    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.SEARCH);
        lsmHarness.search(cursor, searchPred, ctx, true);
    }

    public IIndexCursor createSearchCursor() {
        return new LSMInvertedIndexSearchCursor();
    }

    @Override
    public ILSMIOOperation createFlushOperation(ILSMIOOperationCallback callback) {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return new LSMInvertedIndexFlushOperation(lsmHarness.getIndex(),
                componentFileRefs.getInsertIndexFileReference(), componentFileRefs.getDeleteIndexFileReference(),
                callback);
    }

    @Override
    public void flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        lsmHarness.flush(operation);
    }

    @Override
    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException,
            IndexException {
        return lsmHarness.createMergeOperation(callback);
    }

    @Override
    public void merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        lsmHarness.merge(operation);
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
    public boolean tryNoOp() throws HyracksDataException {
        return lsmHarness.noOp(ctx, true);
    }

    @Override
    public void noOp() throws HyracksDataException {
        lsmHarness.noOp(ctx, false);
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
