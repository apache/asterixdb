package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;

public interface IIndexOpContext {
	void reset();
	void reset(IndexOp newOp);
}
