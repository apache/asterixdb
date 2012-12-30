/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public abstract class LSMTreeIndexAccessor implements ILSMIndexAccessorInternal {
    protected ILSMHarness lsmHarness;
    protected ILSMIndexOperationContext ctx;

    public LSMTreeIndexAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx) {
        this.lsmHarness = lsmHarness;
        this.ctx = ctx;
    }

    @Override
    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.INSERT);
        lsmHarness.insertUpdateOrDelete(tuple, ctx, false);
    }

    @Override
    public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
        // Update is the same as insert.
        ctx.setOperation(IndexOperation.UPDATE);
        lsmHarness.insertUpdateOrDelete(tuple, ctx, false);
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.DELETE);
        lsmHarness.insertUpdateOrDelete(tuple, ctx, false);
    }

    @Override
    public void upsert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.UPSERT);
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

    @Override
    public boolean tryUpdate(ITupleReference tuple) throws HyracksDataException, IndexException {
        // Update is the same as insert.
        ctx.setOperation(IndexOperation.UPDATE);
        return lsmHarness.insertUpdateOrDelete(tuple, ctx, true);
    }

    @Override
    public boolean tryUpsert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.UPSERT);
        return lsmHarness.insertUpdateOrDelete(tuple, ctx, true);
    }

    @Override
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.SEARCH);
        lsmHarness.search(cursor, searchPred, ctx, true);
    }

    @Override
    public void flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        lsmHarness.flush(operation);
    }

    @Override
    public void merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        lsmHarness.merge(operation);
    }

    @Override
    public void physicalDelete(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.setOperation(IndexOperation.PHYSICALDELETE);
        lsmHarness.insertUpdateOrDelete(tuple, ctx, false);
    }

    @Override
    public void scheduleMerge(ILSMIOOperationCallback callback) throws HyracksDataException, IndexException {
        ILSMIOOperation op = lsmHarness.createMergeOperation(callback);
        if (op != null) {
            lsmHarness.getIOScheduler().scheduleOperation(op);
        }
    }

    @Override
    public boolean tryNoOp() throws HyracksDataException {
        return lsmHarness.noOp(ctx, true);
    }

    @Override
    public void noOp() throws HyracksDataException {
        lsmHarness.noOp(ctx, false);
    }
}