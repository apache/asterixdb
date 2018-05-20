/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.lsm.common.api;

import java.util.List;
import java.util.function.Predicate;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;

/**
 * Client handle for performing operations
 * (insert/delete/update/search/diskorderscan/merge/flush) on an {@link ILSMHarness}.
 * An {@link ILSMIndexAccessor} is not thread safe, but different {@link ILSMIndexAccessor}s
 * can concurrently operate on the same {@link ILSMIndex} (i.e., the {@link ILSMIndex} must allow
 * concurrent operations).
 */
public interface ILSMIndexAccessor extends IIndexAccessor {

    /**
     * @return the operation context associated with the accessor
     */
    ILSMIndexOperationContext getOpContext();

    /**
     * Schedule a flush operation
     *
     * @throws HyracksDataException
     */
    ILSMIOOperation scheduleFlush() throws HyracksDataException;

    /**
     * Schedule a merge operation
     *
     * @param components
     *            the components to be merged
     * @throws HyracksDataException
     * @throws IndexException
     */
    ILSMIOOperation scheduleMerge(List<ILSMDiskComponent> components) throws HyracksDataException;

    /**
     * Schedule a full merge
     *
     * @throws HyracksDataException
     * @throws IndexException
     */
    ILSMIOOperation scheduleFullMerge() throws HyracksDataException;

    /**
     * Delete the tuple from the memory component only. Don't replace with antimatter tuple
     *
     * @param tuple
     *            the tuple to be deleted
     * @throws HyracksDataException
     * @throws IndexException
     */
    void physicalDelete(ITupleReference tuple) throws HyracksDataException;

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
    boolean tryInsert(ITupleReference tuple) throws HyracksDataException;

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
    boolean tryDelete(ITupleReference tuple) throws HyracksDataException;

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
    boolean tryUpdate(ITupleReference tuple) throws HyracksDataException;

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
    boolean tryUpsert(ITupleReference tuple) throws HyracksDataException;

    /**
     * Delete the tuple from the memory component only. Don't replace with antimatter tuple
     * Perform operation even if the memory component is full
     *
     * @param tuple
     *            the tuple to delete
     * @throws HyracksDataException
     * @throws IndexException
     */
    void forcePhysicalDelete(ITupleReference tuple) throws HyracksDataException;

    /**
     * Insert a new tuple (failing if duplicate key entry is found)
     *
     * @param tuple
     *            the tuple to insert
     * @throws HyracksDataException
     * @throws IndexException
     */
    void forceInsert(ITupleReference tuple) throws HyracksDataException;

    /**
     * Force deleting an index entry even if the memory component is full
     * replace the entry if found with an antimatter tuple, otherwise, simply insert the antimatter tuple
     *
     * @param tuple
     *            tuple to delete
     * @throws HyracksDataException
     * @throws IndexException
     */
    void forceDelete(ITupleReference tuple) throws HyracksDataException;

    /**
     * Force upserting the tuple into the memory component even if it is full
     *
     * @param tuple
     * @throws HyracksDataException
     * @throws IndexException
     */
    void forceUpsert(ITupleReference tuple) throws HyracksDataException;

    /**
     * Schedule a replication for disk components
     *
     * @param diskComponents
     *            the components to be replicated
     * @param opType
     *            the operation type
     * @throws HyracksDataException
     */
    void scheduleReplication(List<ILSMDiskComponent> diskComponents, LSMOperationType opType)
            throws HyracksDataException;

    /**
     * Flush an in-memory component.
     *
     * @throws HyracksDataException
     * @throws TreeIndexException
     */
    void flush(ILSMIOOperation operation) throws HyracksDataException;

    /**
     * Merge all on-disk components.
     *
     * @throws HyracksDataException
     * @throws TreeIndexException
     */
    void merge(ILSMIOOperation operation) throws HyracksDataException;

    /**
     * Update the metadata of the memory component, wait for the new component if the current one is UNWRITABLE
     *
     * @param key
     *            the key
     * @param value
     *            the value
     * @throws HyracksDataException
     */
    void updateMeta(IValueReference key, IValueReference value) throws HyracksDataException;

    /**
     * Force update the metadata of the current memory component even if it is UNWRITABLE
     *
     * @param key
     * @param value
     * @throws HyracksDataException
     */
    void forceUpdateMeta(IValueReference key, IValueReference value) throws HyracksDataException;

    /**
     * Open the given cursor for scanning all disk components of the primary index.
     * The returned tuple has the format of [(int) disk_component_position, (boolean) anti-matter flag,
     * primary key, payload].
     * The returned tuples are first ordered on primary key, and then ordered on the descending order of
     * disk_component_position (older components get returned first)
     *
     * If this method returns successfully, then the cursor has been opened. If an exception is thrown then
     * the cursor was not opened
     *
     * @param icursor
     *            Cursor over the index entries satisfying searchPred.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     */
    void scanDiskComponents(IIndexCursor cursor) throws HyracksDataException;

    /**
     * Delete components that match the passed predicate
     * NOTE: This call can only be made when the caller knows that data modification has been stopped
     *
     * @param filter
     * @throws HyracksDataException
     */
    void deleteComponents(Predicate<ILSMComponent> predicate) throws HyracksDataException;

    /**
     * Update the filter of an LSM index
     *
     * @param tuple
     * @throws HyracksDataException
     */
    void updateFilter(ITupleReference tuple) throws HyracksDataException;
}
