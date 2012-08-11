package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;

public class LSMInvertedIndexAccessor implements ILSMIndexAccessor {

    protected LSMHarness lsmHarness;
    protected IIndexOpContext ctx;

    public LSMInvertedIndexAccessor(LSMHarness lsmHarness, IIndexOpContext ctx) {
        this.lsmHarness = lsmHarness;
        this.ctx = ctx;
    }

    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.reset(IndexOp.INSERT);
        lsmHarness.insertUpdateOrDelete(tuple, ctx);
    }

    public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
        //not supported yet
    }

    public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
        //not supported yet
    }

    public IIndexCursor createSearchCursor() {
        return new LSMInvertedIndexSearchCursor(); 
    }

    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        ctx.reset(IndexOp.SEARCH);
        //search include in-memory components
        lsmHarness.search(cursor, searchPred, ctx, true);
    }

    public void flush() throws HyracksDataException, IndexException {
        lsmHarness.flush();
    }

    public void merge() throws HyracksDataException, IndexException {
        lsmHarness.merge();
    }
}
