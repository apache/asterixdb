package edu.uci.ics.hyracks.storage.am.lsm.common.api;


public interface ILSMMergePolicy {
    public void diskComponentAdded(ILSMIndex index, int totalNumDiskComponents);
}
