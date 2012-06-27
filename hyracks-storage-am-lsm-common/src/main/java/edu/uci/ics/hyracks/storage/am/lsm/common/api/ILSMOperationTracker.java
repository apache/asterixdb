package edu.uci.ics.hyracks.storage.am.lsm.common.api;


public interface ILSMOperationTracker {
    public void threadEnter(ILSMIndex index);

    public void threadExit(ILSMIndex index);
}
