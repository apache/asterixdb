package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class NullTupleReferenceFactory implements ITupleReferenceFactory {
	
	private static final long serialVersionUID = 1L;

	@Override
	public ITupleReference createTuple(IHyracksContext ctx) {
		return null;
	}

}
