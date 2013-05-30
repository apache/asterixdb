package edu.uci.ics.hyracks.storage.am.common.api;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IIndexLifecycleManager {
    public IIndex getIndex(long resourceID) throws HyracksDataException;

    public void register(long resourceID, IIndex index) throws HyracksDataException;

    public void unregister(long resourceID) throws HyracksDataException;

    public void open(long resourceID) throws HyracksDataException;

    public void close(long resourceID) throws HyracksDataException;

    public List<IIndex> getOpenIndexes();
}