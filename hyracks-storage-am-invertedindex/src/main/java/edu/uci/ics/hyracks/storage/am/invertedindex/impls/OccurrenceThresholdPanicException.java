package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

public class OccurrenceThresholdPanicException extends InvertedIndexException {
	private static final long serialVersionUID = 1L;
	
	public OccurrenceThresholdPanicException(String msg) {
		super(msg);
	}
}
