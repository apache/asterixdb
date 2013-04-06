package edu.uci.ics.hyracks.storage.am.common.api;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public interface IIndexLifecycleManagerProvider extends Serializable {
    public IIndexLifecycleManager getLifecycleManager(IHyracksTaskContext ctx);
}
