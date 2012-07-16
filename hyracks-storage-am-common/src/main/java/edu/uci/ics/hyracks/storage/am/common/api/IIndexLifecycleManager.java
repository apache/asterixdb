package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;

public interface IIndexLifecycleManager {
    public void create(IIndexDataflowHelper helper) throws HyracksDataException;

    public void destroy(IIndexDataflowHelper helper) throws HyracksDataException;

    public IIndex open(IIndexDataflowHelper helper) throws HyracksDataException;

    public void close(IIndexDataflowHelper helper) throws HyracksDataException;
}
