package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public interface IVirtualBufferCache extends IBufferCache {
    public void open();

    public IFileMapManager getFileMapProvider();
}
