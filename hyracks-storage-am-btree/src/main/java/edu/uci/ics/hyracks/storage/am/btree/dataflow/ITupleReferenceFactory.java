package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ITupleReferenceFactory extends Serializable {
	ITupleReference createTuple(IHyracksContext ctx);
}
