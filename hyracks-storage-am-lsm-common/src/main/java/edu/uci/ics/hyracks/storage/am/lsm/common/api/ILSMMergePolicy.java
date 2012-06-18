package edu.uci.ics.hyracks.storage.am.lsm.common.api;


public interface ILSMMergePolicy {
    public void componentAdded(ILSMIndex index, int totalNumDiskComponents, boolean mergeInProgress);
}
