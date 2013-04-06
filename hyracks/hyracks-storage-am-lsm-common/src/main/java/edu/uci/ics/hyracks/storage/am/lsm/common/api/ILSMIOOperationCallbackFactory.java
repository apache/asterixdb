package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;

public interface ILSMIOOperationCallbackFactory extends Serializable {
    public ILSMIOOperationCallback createIOOperationCallback(Object syncObj);
}
