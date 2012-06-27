package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;


public interface ILSMOperationTrackerProvider extends Serializable {
    public ILSMOperationTracker getOperationTracker();
}
