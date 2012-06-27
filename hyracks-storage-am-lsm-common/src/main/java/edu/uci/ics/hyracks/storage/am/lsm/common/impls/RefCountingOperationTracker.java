package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;

public class RefCountingOperationTracker implements ILSMOperationTracker {

    private int threadRefCount = 0;

    @Override
    public void threadEnter(ILSMIndex index) {
        boolean waitForFlush = true;
        do {
            synchronized (this) {
                // flushFlag may be set to true even though the flush has not occurred yet.
                // If flushFlag is set, then the flush is queued to occur by the last exiting thread.
                // This operation should wait for that flush to occur before proceeding.
                if (!index.getFlushController().getFlushStatus(index)) {
                    // Increment the threadRefCount in order to block the possibility of a concurrent flush.
                    // The corresponding threadExit() call is in LSMTreeRangeSearchCursor.close()
                    threadRefCount++;

                    // A flush is not pending, so proceed with the operation.
                    waitForFlush = false;
                }
            }
        } while (waitForFlush);
    }

    @Override
    public void threadExit(ILSMIndex index) {
        synchronized (this) {
            threadRefCount--;

            // Flush will only be handled by last exiting thread.
            if (index.getFlushController().getFlushStatus(index) && threadRefCount == 0) {
                index.getIOScheduler().scheduleFlush(index);
            }
        }
    }

}
