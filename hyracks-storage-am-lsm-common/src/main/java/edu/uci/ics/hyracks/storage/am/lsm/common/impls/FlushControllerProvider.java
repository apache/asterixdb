package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushControllerProvider;

public class FlushControllerProvider implements ILSMFlushControllerProvider {

    private static final long serialVersionUID = 1L;

    @Override
    public ILSMFlushController getFlushController(IHyracksTaskContext ctx) {
        return new FlushController();
    }

}
