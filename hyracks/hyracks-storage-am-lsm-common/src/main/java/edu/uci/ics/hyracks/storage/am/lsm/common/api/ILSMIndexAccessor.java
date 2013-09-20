/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

/**
 * Client handle for performing operations
 * (insert/delete/update/search/diskorderscan/merge/flush) on an {@link ILSMHarness}.
 * An {@link ILSMIndexAccessor} is not thread safe, but different {@link ILSMIndexAccessor}s
 * can concurrently operate on the same {@link ILSMIndex} (i.e., the {@link ILSMIndex} must allow
 * concurrent operations).
 */
public interface ILSMIndexAccessor extends IIndexAccessor {
    public void scheduleFlush(ILSMIOOperationCallback callback) throws HyracksDataException;

    public void scheduleMerge(ILSMIOOperationCallback callback, List<ILSMComponent> components)
            throws HyracksDataException, IndexException;

    public void scheduleFullMerge(ILSMIOOperationCallback callback) throws HyracksDataException, IndexException;

    /**
     * Deletes the tuple from the memory component only.
     * 
     * @throws HyracksDataException
     * @throws IndexException
     */
    public void physicalDelete(ITupleReference tuple) throws HyracksDataException, IndexException;

    /**
     * Attempts to insert the given tuple.
     * If the insert would have to wait for a flush to complete, then this method returns false to
     * allow the caller to avoid potential deadlock situations.
     * Otherwise, returns true (insert was successful).
     * 
     * @param tuple
     *            Tuple to be inserted.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws IndexException
     *             If an index-specific constraint is violated, e.g., the key
     *             already exists.
     */
    public boolean tryInsert(ITupleReference tuple) throws HyracksDataException, IndexException;

    /**
     * Attempts to delete the given tuple.
     * If the delete would have to wait for a flush to complete, then this method returns false to
     * allow the caller to avoid potential deadlock situations.
     * Otherwise, returns true (delete was successful).
     * 
     * @param tuple
     *            Tuple to be deleted.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws IndexException
     *             If there is no matching tuple in the index.
     */
    public boolean tryDelete(ITupleReference tuple) throws HyracksDataException, IndexException;

    /**
     * Attempts to update the given tuple.
     * If the update would have to wait for a flush to complete, then this method returns false to
     * allow the caller to avoid potential deadlock situations.
     * Otherwise, returns true (update was successful).
     * 
     * @param tuple
     *            Tuple whose match in the index is to be update with the given
     *            tuples contents.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws IndexException
     *             If there is no matching tuple in the index.
     */
    public boolean tryUpdate(ITupleReference tuple) throws HyracksDataException, IndexException;

    /**
     * This operation is only supported by indexes with the notion of a unique key.
     * If tuple's key already exists, then this operation attempts to performs an update.
     * Otherwise, it attempts to perform an insert.
     * If the operation would have to wait for a flush to complete, then this method returns false to
     * allow the caller to avoid potential deadlock situations.
     * Otherwise, returns true (insert/update was successful).
     * 
     * @param tuple
     *            Tuple to be deleted.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws IndexException
     *             If there is no matching tuple in the index.
     */
    public boolean tryUpsert(ITupleReference tuple) throws HyracksDataException, IndexException;

    public void forcePhysicalDelete(ITupleReference tuple) throws HyracksDataException, IndexException;

    public void forceInsert(ITupleReference tuple) throws HyracksDataException, IndexException;

    public void forceDelete(ITupleReference tuple) throws HyracksDataException, IndexException;
}
