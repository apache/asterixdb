package edu.uci.ics.hyracks.storage.am.lsm.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class LSMPriorityQueueElement {
	private ITupleReference tuple;
	private int cursorIndex;
	
	public LSMPriorityQueueElement(ITupleReference tuple, int cursorIndex) {
		reset(tuple, cursorIndex);
	}

	public ITupleReference getTuple() {
		return tuple;
	}

	public int getCursorIndex() {
		return cursorIndex;
	}
	
	public void reset(ITupleReference tuple, int cursorIndex) {
		this.tuple = tuple;
		this.cursorIndex = cursorIndex;
	}
}
