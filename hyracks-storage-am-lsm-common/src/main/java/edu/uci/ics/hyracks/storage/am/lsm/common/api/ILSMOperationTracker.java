package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;

/**
 * This interface exposes methods for tracking when operations (e.g. insert, search) 
 * enter and exit an {@link ILSMIndex}.
 * 
 * Note that 'operation' below refers to {@link IIndexAccessor} methods.
 *  
 * @author zheilbron
 */
public interface ILSMOperationTracker {

    /**
     * This method is guaranteed to be called whenever an operation first begins on the index, 
     * before any traversals of the index structures (more specifically, before any 
     * pages are pinned/latched).
     * 
     * @param index the index for which the operation entered
     */
    public void threadEnter(ILSMIndex index) throws HyracksDataException;

    /**
     * This method is guaranteed to be called just before an operation is finished on the index.
     * 
     * @param index the index for which the operation exited
     */
    public void threadExit(ILSMIndex index) throws HyracksDataException;
}
