package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;

public class BlockingIOOperationCallback implements ILSMIOOperationCallback {

    @Override
    public void callback() {
        this.notifyAll();
    }

    public void block() throws InterruptedException {
        this.wait();
    }

}
