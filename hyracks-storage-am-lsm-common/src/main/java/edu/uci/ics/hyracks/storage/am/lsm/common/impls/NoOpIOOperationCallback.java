package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;

public enum NoOpIOOperationCallback implements ILSMIOOperationCallback {
    INSTANCE;

    @Override
    public void callback() {
        // Do nothing.
    }
}
