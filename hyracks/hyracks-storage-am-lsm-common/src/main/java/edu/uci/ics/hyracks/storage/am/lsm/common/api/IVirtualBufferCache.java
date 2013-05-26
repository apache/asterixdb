package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public interface IVirtualBufferCache extends IBufferCache {
    public void open();
}
