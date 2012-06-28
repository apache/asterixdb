package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IIndexBulkLoader {
    /**
     * Append a tuple to the index in the context of a bulk load.
     * 
     * @param tuple
     *            Tuple to be inserted.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     */
    public void add(ITupleReference tuple) throws HyracksDataException;

    /**
     * Finalize the bulk loading operation in the given context.
     * 
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     */
    public void end() throws HyracksDataException;

}
