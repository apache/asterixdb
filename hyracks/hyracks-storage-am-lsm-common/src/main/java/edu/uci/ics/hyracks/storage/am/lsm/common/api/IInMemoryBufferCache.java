package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCacheInternal;

public interface IInMemoryBufferCache extends IBufferCacheInternal {
    public void open();
}
