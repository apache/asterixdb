package edu.uci.ics.hyracks.storage.am.lsm.common.api;

public interface ILSMIOOperationCallbackProvider {
    public ILSMIOOperationCallback getIOOperationCallback(ILSMIndex index);
}
