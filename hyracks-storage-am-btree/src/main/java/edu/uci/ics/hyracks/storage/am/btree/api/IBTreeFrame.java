package edu.uci.ics.hyracks.storage.am.btree.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public interface IBTreeFrame extends ITreeIndexFrame {
	public int findUpdateTupleIndex(ITupleReference tuple, MultiComparator cmp) throws TreeIndexException;
	public int findInsertTupleIndex(ITupleReference tuple, MultiComparator cmp) throws TreeIndexException;
	public int findDeleteTupleIndex(ITupleReference tuple, MultiComparator cmp) throws TreeIndexException;
	public void insertSorted(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException;
    public boolean getSmFlag();
    public void setSmFlag(boolean smFlag);
    public void setMultiComparator(MultiComparator cmp);
}
