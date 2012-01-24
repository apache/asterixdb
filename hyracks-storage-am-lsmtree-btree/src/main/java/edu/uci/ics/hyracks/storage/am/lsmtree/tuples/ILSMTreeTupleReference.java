package edu.uci.ics.hyracks.storage.am.lsmtree.tuples;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;

public interface ILSMTreeTupleReference extends ITreeIndexTupleReference {
	public boolean isDelete();
}
