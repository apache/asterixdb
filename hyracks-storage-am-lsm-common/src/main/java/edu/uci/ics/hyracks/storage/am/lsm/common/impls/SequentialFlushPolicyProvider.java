package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushPolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushPolicyProvider;

public class SequentialFlushPolicyProvider implements ILSMFlushPolicyProvider {

    private static final long serialVersionUID = 1L;

    @Override
    public ILSMFlushPolicy getFlushPolicy() {
        return SequentialFlushPolicy.INSTANCE;
    }

}
