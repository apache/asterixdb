package edu.uci.ics.hyracks.storage.am.invertedindex.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IInvertedIndexSearcher {			
	public void search(ITupleReference queryTuple, int queryFieldIndex) throws Exception;
	public IInvertedIndexResultCursor getResultCursor();
}
