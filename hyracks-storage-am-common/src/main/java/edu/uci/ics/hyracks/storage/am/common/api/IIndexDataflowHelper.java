package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;

public interface IIndexDataflowHelper {
    public IIndex getIndexInstance() throws HyracksDataException;

    public FileReference getFileReference();

    public long getResourceID();
}
