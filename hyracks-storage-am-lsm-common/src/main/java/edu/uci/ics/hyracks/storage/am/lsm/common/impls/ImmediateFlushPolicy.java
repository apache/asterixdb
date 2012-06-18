package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushPolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;

public class ImmediateFlushPolicy implements ILSMFlushPolicy {

    private final ILSMIOScheduler ioScheduler;

    public ImmediateFlushPolicy(ILSMIOScheduler ioScheduler) {
        this.ioScheduler = ioScheduler;
    }

    @Override
    public void memoryComponentExceededThreshold(final ILSMIndex index) {
        // Schedule a flush immediately when the memory component is full
        ioScheduler.scheduleFlush(index);
    }
}
