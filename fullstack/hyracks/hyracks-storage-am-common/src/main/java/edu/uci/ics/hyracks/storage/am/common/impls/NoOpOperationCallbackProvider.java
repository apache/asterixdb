package edu.uci.ics.hyracks.storage.am.common.impls;

import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallbackProvider;

/**
 * Dummy NoOp callback provider used primarily for testing. Always returns the 
 * {@link NoOpOperationCallback} instance. 
 *
 * Implemented as an enum to preserve singleton model while being serializable
 */
public enum NoOpOperationCallbackProvider implements IOperationCallbackProvider {
    INSTANCE;

    @Override
    public IOperationCallback getOperationCallback() {
        return NoOpOperationCallback.INSTANCE;
    }
}
