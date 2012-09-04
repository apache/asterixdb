package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;

public interface IIndexLifecycleManager {
    public IIndex getIndex(long resourceID);

    public void register(long resourceID, IIndex index) throws HyracksDataException;

    public void unregister(long resourceID) throws HyracksDataException;

    public void open(long resourceID) throws HyracksDataException;

    public void close(long resourceID);
}