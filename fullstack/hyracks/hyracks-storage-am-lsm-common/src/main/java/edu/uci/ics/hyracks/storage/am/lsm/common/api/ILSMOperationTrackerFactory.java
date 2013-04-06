package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;

public interface ILSMOperationTrackerFactory extends Serializable {
    public ILSMOperationTracker createOperationTracker(ILSMIndex index);
}
