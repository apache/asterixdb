package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;

public interface IIndexDataflowHelper {
    public void create() throws HyracksDataException;

    public void close() throws HyracksDataException;

    public void open() throws HyracksDataException;

    public void destroy() throws HyracksDataException;

    public IIndex getIndexInstance();

    public FileReference getFileReference();

    public long getResourceID() throws HyracksDataException;
    
    public IHyracksTaskContext getTaskContext();
}
