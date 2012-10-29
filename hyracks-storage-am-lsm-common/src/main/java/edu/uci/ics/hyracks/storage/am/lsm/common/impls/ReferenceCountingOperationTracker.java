package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;

public class ReferenceCountingOperationTracker implements ILSMOperationTracker {

    private int threadRefCount = 0;
    private final ILSMIndex index;
    private final FlushOperationCallback FLUSHCALLBACK_INSTANCE = new FlushOperationCallback();

    public ReferenceCountingOperationTracker(ILSMIndex index) {
        this.index = index;
    }

    @Override
    public synchronized void beforeOperation(IndexOperation op) throws HyracksDataException {
        // Wait for pending flushes to complete.
        // If flushFlag is set, then the flush is queued to occur by the last exiting thread.
        // This operation should wait for that flush to occur before proceeding.
        if (index.getFlushController().getFlushStatus(index)) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        threadRefCount++;
    }

    @Override
    public void afterOperation(IndexOperation op) throws HyracksDataException {
        // The operation is considered inactive, immediately after leaving the index.
        completeOperation(op);
    }

    @Override
    public synchronized void completeOperation(IndexOperation op) throws HyracksDataException {
        threadRefCount--;

        // Flush will only be handled by last exiting thread.
        if (index.getFlushController().getFlushStatus(index) && threadRefCount == 0) {
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            index.getIOScheduler().scheduleOperation(accessor.createFlushOperation(FLUSHCALLBACK_INSTANCE));
        }
    }

    private class FlushOperationCallback implements ILSMIOOperationCallback {
        @Override
        public void callback() {
            ReferenceCountingOperationTracker.this.notifyAll();
        }
    }
}
