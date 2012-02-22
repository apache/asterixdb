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
	public void insert(ITupleReference tuple) throws HyracksDataException,
			IndexException {
		ctx.reset(IndexOp.INSERT);
		lsmHarness.insertUpdateOrDelete(tuple, ctx);
	}

	@Override
	public void update(ITupleReference tuple) throws HyracksDataException,
			IndexException {
		// Update is the same as insert.
		ctx.reset(IndexOp.INSERT);
		lsmHarness.insertUpdateOrDelete(tuple, ctx);
	}

	@Override
	public void delete(ITupleReference tuple) throws HyracksDataException,
			IndexException {
		ctx.reset(IndexOp.DELETE);
		lsmHarness.insertUpdateOrDelete(tuple, ctx);
	}
	
	@Override
	public void search(IIndexCursor cursor, ISearchPredicate searchPred)
			throws HyracksDataException, IndexException {
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
}