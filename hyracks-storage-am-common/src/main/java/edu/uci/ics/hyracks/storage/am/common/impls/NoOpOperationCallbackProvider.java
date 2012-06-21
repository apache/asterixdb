package edu.uci.ics.hyracks.storage.am.common.impls;

import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;

/**
 * Dummy NoOp callback provider used primarily for testing. Always returns the 
 * {@link NoOpOperationCallback} instance. 
 *
 * Implemented as an enum to preserve singleton model while being serializable
 */
public enum NoOpOperationCallbackProvider implements IOperationCallbackProvider {
    INSTANCE;

    @Override
    public IModificationOperationCallback getModificationOperationCallback() {
        return NoOpOperationCallback.INSTANCE;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return NoOpOperationCallback.INSTANCE;
    }
}
