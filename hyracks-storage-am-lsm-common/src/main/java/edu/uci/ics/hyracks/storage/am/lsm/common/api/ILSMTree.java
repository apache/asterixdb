package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;

public interface ILSMTree extends ITreeIndex {
    public void merge() throws Exception;
    
    public void flush() throws Exception;
}
