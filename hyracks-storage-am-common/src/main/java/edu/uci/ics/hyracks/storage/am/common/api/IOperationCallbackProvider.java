package edu.uci.ics.hyracks.storage.am.common.api;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public interface IOperationCallbackProvider extends Serializable {
    public IModificationOperationCallback getModificationOperationCallback(long resourceId, IHyracksTaskContext ctx);

    public ISearchOperationCallback getSearchOperationCallback(long resourceId, IHyracksTaskContext ctx);
}
