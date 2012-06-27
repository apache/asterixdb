package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;

public class FlushController implements ILSMFlushController {

    private boolean needsFlush = false;

    @Override
    public void setFlushStatus(ILSMIndex index, boolean needsFlush) {
        this.needsFlush = needsFlush;
    }

    @Override
    public boolean getFlushStatus(ILSMIndex index) {
        return needsFlush;
    }
}
