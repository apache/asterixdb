package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;


public interface ILSMOperationTrackerProvider extends Serializable {
    public ILSMOperationTracker getOperationTracker(IHyracksTaskContext ctx);
}
