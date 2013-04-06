package edu.uci.ics.hyracks.storage.am.common.impls;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;

/**
 * Dummy NoOp callback factory used primarily for testing. Always returns the {@link NoOpOperationCallback} instance.
 * Implemented as an enum to preserve singleton model while being serializable
 */
public enum NoOpOperationCallbackFactory implements ISearchOperationCallbackFactory,
        IModificationOperationCallbackFactory {
    INSTANCE;

    @Override
    public IModificationOperationCallback createModificationOperationCallback(long resourceId, Object resource, IHyracksTaskContext ctx) {
        return NoOpOperationCallback.INSTANCE;
    }

    @Override
    public ISearchOperationCallback createSearchOperationCallback(long resourceId, IHyracksTaskContext ctx) {
        return NoOpOperationCallback.INSTANCE;
    }
}
