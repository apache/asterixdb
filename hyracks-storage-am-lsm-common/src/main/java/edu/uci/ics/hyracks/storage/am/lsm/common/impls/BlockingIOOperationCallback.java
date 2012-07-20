package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;

public class BlockingIOOperationCallback implements ILSMIOOperationCallback {

    private boolean notified = false;

    @Override
    public synchronized void callback() {
        this.notifyAll();
        notified = true;
    }

    public synchronized void waitForIO() throws InterruptedException {
        if (!notified) {
            this.wait();
        }
    }

    public synchronized void reset() {
        notified = false;
    }

}
