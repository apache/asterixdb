package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public interface ILSMComponentFactory {
    public ILSMComponent createLSMComponentInstance(LSMComponentFileReferences cfr) throws IndexException;

    public IBufferCache getBufferCache();
}
