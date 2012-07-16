package edu.uci.ics.hyracks.storage.am.lsm.common.api;


public interface ILSMIOOperationScheduler {
    public void scheduleOperation(ILSMIOOperation operation);
}
