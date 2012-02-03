package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMTreeIndexAccessor;

public abstract class LSMTreeIndexAccessor implements ILSMTreeIndexAccessor {
	protected LSMHarness lsmHarness;
	protected IIndexOpContext ctx;

	public LSMTreeIndexAccessor(LSMHarness lsmHarness, IIndexOpContext ctx) {
		this.lsmHarness = lsmHarness;
		this.ctx = ctx;
	}

	@Override
	public void insert(ITupleReference tuple) throws HyracksDataException,
			TreeIndexException {
		ctx.reset(IndexOp.INSERT);
		lsmHarness.insertUpdateOrDelete(tuple, ctx);
	}

	@Override
	public void update(ITupleReference tuple) throws HyracksDataException,
			TreeIndexException {
		// Update is the same as insert.
		ctx.reset(IndexOp.INSERT);
		lsmHarness.insertUpdateOrDelete(tuple, ctx);
	}

	@Override
	public void delete(ITupleReference tuple) throws HyracksDataException,
			TreeIndexException {
		ctx.reset(IndexOp.DELETE);
		lsmHarness.insertUpdateOrDelete(tuple, ctx);
	}
	
	@Override
	public void search(ITreeIndexCursor cursor, ISearchPredicate searchPred)
			throws HyracksDataException, TreeIndexException {
		ctx.reset(IndexOp.SEARCH);
		lsmHarness.search(cursor, searchPred, ctx, true);
	}

	@Override
	public ITreeIndexCursor createDiskOrderScanCursor() {
		// Disk-order scan doesn't make sense for the LSMBTree because it cannot
		// correctly resolve deleted tuples.
		throw new UnsupportedOperationException(
				"DiskOrderScan not supported by LSMTree.");
	}

	@Override
	public void diskOrderScan(ITreeIndexCursor cursor)
			throws HyracksDataException {
		// Disk-order scan doesn't make sense for the LSMBTree because it cannot
		// correctly resolve deleted tuples.
		throw new UnsupportedOperationException(
				"DiskOrderScan not supported by LSMTree.");
	}
	
	@Override
	public void flush() throws HyracksDataException, TreeIndexException {
		lsmHarness.flush();
	}

	@Override
	public void merge() throws HyracksDataException, TreeIndexException {
		lsmHarness.merge();
	}
}