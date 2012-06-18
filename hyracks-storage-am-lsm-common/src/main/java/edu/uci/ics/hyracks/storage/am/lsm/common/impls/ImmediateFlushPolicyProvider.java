package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushPolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushPolicyProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOSchedulerProvider;

public class ImmediateFlushPolicyProvider implements ILSMFlushPolicyProvider {

    private static final long serialVersionUID = 1L;

    private final ILSMIOSchedulerProvider schedulerProvider;

    public ImmediateFlushPolicyProvider(ILSMIOSchedulerProvider schedulerProvider) {
        this.schedulerProvider = schedulerProvider;
    }

    @Override
    public ILSMFlushPolicy getFlushPolicy() {
        return new ImmediateFlushPolicy(schedulerProvider.getIOScheduler());
    }

}
