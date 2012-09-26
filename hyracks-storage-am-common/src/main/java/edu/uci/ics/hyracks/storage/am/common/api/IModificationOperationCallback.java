package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * This operation callback allows for arbitrary actions to be taken while traversing 
 * an index structure. The {@link IModificationOperationCallback} will be called on 
 * all modifying operations (e.g. insert, update, delete...) for all indexes.
 * 
 * @author zheilbron
 */
public interface IModificationOperationCallback {

    /**
     * This method is called on a tuple that is about to traverse an index's structure 
     * (i.e. before any pages are pinned or latched). 
     *
     * The format of the tuple is the format that would be stored in the index itself.
     * 
     * @param tuple the tuple that is about to be operated on
     */
    public void before(ITupleReference tuple) throws HyracksDataException;

    /**
     * This method is called on a tuple when a tuple with a matching key is found for the 
     * current operation. It is possible that tuple is null, in which case no tuple with a 
     * matching key was found.
     * 
     * When found is called, the leaf page where the tuple resides will be pinned and latched, 
     * so blocking operations should be avoided.
     * 
     * @param tuple a tuple with a matching key, otherwise null if none exists
     */
    public void found(ITupleReference tuple) throws HyracksDataException;
}
