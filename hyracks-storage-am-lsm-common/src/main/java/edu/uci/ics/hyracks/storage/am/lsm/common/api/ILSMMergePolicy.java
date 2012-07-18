package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;


public interface ILSMMergePolicy {
    public void diskComponentAdded(ILSMIndex index, int totalNumDiskComponents) throws HyracksDataException;
}
