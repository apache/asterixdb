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
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public abstract class LSMTreeIndexAccessor implements ILSMIndexAccessor {
    protected LSMHarness lsmHarness;
    protected IIndexOpContext ctx;

    public LSMTreeIndexAccessor(LSMHarness lsmHarness, IIndexOpContext ctx) {
        this.lsmHarness = lsmHarness;
        this.ctx = ctx;
    }

    @Override
    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.reset(IndexOp.INSERT);
        lsmHarness.insertUpdateOrDelete(tuple, ctx);
    }

    @Override
    public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
        // Update is the same as insert.
        ctx.reset(IndexOp.UPDATE);
        lsmHarness.insertUpdateOrDelete(tuple, ctx);
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.reset(IndexOp.DELETE);
        lsmHarness.insertUpdateOrDelete(tuple, ctx);
    }
    
    @Override
    public void upsert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.reset(IndexOp.UPSERT);
        lsmHarness.insertUpdateOrDelete(tuple, ctx);
    }

    @Override
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        ctx.reset(IndexOp.SEARCH);
        lsmHarness.search(cursor, searchPred, ctx, true);
    }

    @Override
    public void flush() throws HyracksDataException, IndexException {
        lsmHarness.flush();
    }

    @Override
    public void merge() throws HyracksDataException, IndexException {
        lsmHarness.merge();
    }
    
    @Override
    public void physicalDelete(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.reset(IndexOp.PHYSICALDELETE);
        lsmHarness.insertUpdateOrDelete(tuple, ctx);
    }
}