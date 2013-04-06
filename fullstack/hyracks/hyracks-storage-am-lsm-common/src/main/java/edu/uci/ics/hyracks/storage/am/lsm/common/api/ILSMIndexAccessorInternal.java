package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;

public interface ILSMIndexAccessorInternal extends ILSMIndexAccessor {

    /**
     * Force a flush of the in-memory component.
     * 
     * @throws HyracksDataException
     * @throws TreeIndexException
     */
    public void flush(ILSMIOOperation operation) throws HyracksDataException, IndexException;

    /**
     * Merge all on-disk components.
     * 
     * @throws HyracksDataException
     * @throws TreeIndexException
     */
    public void merge(ILSMIOOperation operation) throws HyracksDataException, IndexException;
}
