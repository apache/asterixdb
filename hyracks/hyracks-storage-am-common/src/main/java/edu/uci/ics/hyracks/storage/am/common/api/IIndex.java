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

package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

/**
 * This interface describes the operations common to all indexes. Indexes
 * implementing this interface can easily reuse existing index operators for
 * dataflow. Users must perform operations on an via an {@link IIndexAccessor}.
 * During dataflow, the lifecycle of IIndexes are handled through an {@link IIndexLifecycleManager}.
 */
public interface IIndex {

    /**
     * Initializes the persistent state of an index.
     * An index cannot be created if it is in the activated state.
     * Calling create on an index that is deactivated has the effect of clearing the index.
     * 
     * @throws HyracksDataException
     *             if there is an error in the BufferCache while (un)pinning pages, (un)latching pages,
     *             creating files, or deleting files
     *             if the index is in the activated state
     */
    public void create() throws HyracksDataException;

    /**
     * Initializes the index's operational state. An index in the activated state may perform
     * operations via an {@link IIndexAccessor}.
     * 
     * @throws HyracksDataException
     *             if there is a problem in the BufferCache while (un)pinning pages, (un)latching pages,
     *             creating files, or deleting files
     */
    public void activate() throws HyracksDataException;

    /**
     * Resets the operational state of the index. Calling clear has the same logical effect
     * as calling deactivate(), destroy(), create(), then activate(), but not necessarily the
     * same physical effect.
     * 
     * @throws HyracksDataException
     *             if there is a problem in the BufferCache while (un)pinning pages, (un)latching pages,
     *             creating files, or deleting files
     *             if the index is not in the activated state
     */
    public void clear() throws HyracksDataException;

    /**
     * Deinitializes the index's operational state. An index in the deactivated state may not
     * perform operations.
     * 
     * @throws HyracksDataException
     *             if there is a problem in the BufferCache while (un)pinning pages, (un)latching pages,
     *             creating files, or deleting files
     */
    public void deactivate() throws HyracksDataException;

    /**
     * Removes the persistent state of an index.
     * An index cannot be destroyed if it is in the activated state.
     * 
     * @throws HyracksDataException
     *             if there is an error in the BufferCache while (un)pinning pages, (un)latching pages,
     *             creating files, or deleting files
     *             if the index is already activated
     */
    public void destroy() throws HyracksDataException;

    /**
     * Creates an {@link IIndexAccessor} for performing operations on this index.
     * An IIndexAccessor is not thread safe, but different IIndexAccessors can concurrently operate
     * on the same {@link IIndex}.
     * 
     * @returns IIndexAccessor an accessor for this {@link IIndex}
     * @param modificationCallback
     *            the callback to be used for modification operations
     * @param searchCallback
     *            the callback to be used for search operations
     * @throws HyracksDataException
     */
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) throws HyracksDataException;

    /**
     * Ensures that all pages (and tuples) of the index are logically consistent.
     * An assertion error is thrown if validation fails.
     * 
     * @throws HyracksDataException
     *             if there is an error performing validation
     */
    public void validate() throws HyracksDataException;

    /**
     * @return the {@link IBufferCache} underlying this index.
     */
    public IBufferCache getBufferCache();

    /**
     * @return the size, in bytes, of pre-allocated memory space that this index was allotted.
     */
    public long getMemoryAllocationSize();

    /**
     * @param fillFactor
     * @param verifyInput
     * @throws IndexException
     */
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws IndexException;
}
