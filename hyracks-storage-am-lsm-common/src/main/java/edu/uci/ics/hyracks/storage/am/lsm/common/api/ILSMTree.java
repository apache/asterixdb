package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;

public interface ILSMTree extends ITreeIndex {
    public void merge() throws HyracksDataException, TreeIndexException;

    public void flush() throws HyracksDataException, TreeIndexException;
}
