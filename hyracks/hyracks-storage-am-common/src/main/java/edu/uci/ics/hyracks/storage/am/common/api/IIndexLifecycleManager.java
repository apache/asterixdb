package edu.uci.ics.hyracks.storage.am.common.api;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;

public interface IIndexLifecycleManager extends ILifeCycleComponent {
    public IIndex getIndex(long resourceID);

    public void register(long resourceID, IIndex index) throws HyracksDataException;

    public void unregister(long resourceID) throws HyracksDataException;

    public void open(long resourceID) throws HyracksDataException;

    public void close(long resourceID);

    public List<IIndex> getOpenIndexes();
}