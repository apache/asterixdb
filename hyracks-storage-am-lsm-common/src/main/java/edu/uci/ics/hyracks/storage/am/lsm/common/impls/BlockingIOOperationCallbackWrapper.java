package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;

public class BlockingIOOperationCallbackWrapper implements ILSMIOOperationCallback {

    private boolean notified = false;

    private final ILSMIOOperationCallback wrappedCallback;

    public BlockingIOOperationCallbackWrapper(ILSMIOOperationCallback callback) {
        this.wrappedCallback = callback;
    }

    public synchronized void waitForIO() throws InterruptedException {
        if (!notified) {
            this.wait();
        }
        notified = false;
    }

    @Override
    public void beforeOperation(ILSMIOOperation operation) throws HyracksDataException {
        wrappedCallback.beforeOperation(operation);
    }

    @Override
    public void afterOperation(ILSMIOOperation operation, List<ILSMComponent> oldComponents, ILSMComponent newComponent)
            throws HyracksDataException {
        wrappedCallback.afterOperation(operation, oldComponents, newComponent);
    }

    @Override
    public synchronized void afterFinalize(ILSMIOOperation operation, ILSMComponent newComponent)
            throws HyracksDataException {
        wrappedCallback.afterFinalize(operation, newComponent);
        this.notifyAll();
        notified = true;
    }
}
