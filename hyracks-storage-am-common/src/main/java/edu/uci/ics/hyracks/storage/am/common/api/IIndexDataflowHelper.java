package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;

public interface IIndexDataflowHelper {
    public IIndex getIndexInstance() throws HyracksDataException;

    public FileReference getFileReference();

    public long getResourceID();

    public IIndexOperatorDescriptor getOperatorDescriptor();

    public IHyracksTaskContext getHyracksTaskContext();
}
