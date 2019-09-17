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

package org.apache.hyracks.storage.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;

/**
 * This interface describes the operations common to all indexes. Indexes
 * implementing this interface can easily reuse existing index operators for
 * dataflow. Users must perform operations on an via an {@link IIndexAccessor}.
 * During dataflow, the lifecycle of IIndexes are handled through an {@link IResourceLifecycleManager}.
 */
public interface IIndex {

    /**
     * Initializes the persistent state of an index.
     * An index cannot be created if it is in the activated state.
     * Calling create on an index that is deactivated has the effect of clearing the index.
     * This method is atomic. If an exception is thrown, then the call had no effect.
     *
     * @throws HyracksDataException
     *             if there is an error in the BufferCache while (un)pinning pages, (un)latching pages,
     *             creating files, or deleting files
     *             if the index is in the activated state
     */
    void create() throws HyracksDataException;

    /**
     * Initializes the index's operational state. An index in the activated state may perform
     * operations via an {@link IIndexAccessor}.
     *
     * @throws HyracksDataException
     *             if there is a problem in the BufferCache while (un)pinning pages, (un)latching pages,
     *             creating files, or deleting files
     */
    void activate() throws HyracksDataException;

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
    void clear() throws HyracksDataException;

    /**
     * Deinitializes the index's operational state. An index in the deactivated state may not
     * perform operations.
     *
     * @throws HyracksDataException
     *             if there is a problem in the BufferCache while (un)pinning pages, (un)latching pages,
     *             creating files, or deleting files
     */
    void deactivate() throws HyracksDataException;

    /**
     * Removes the persistent state of an index.
     * An index cannot be destroyed if it is in the activated state.
     *
     * @throws HyracksDataException
     *             if there is an error in the BufferCache while (un)pinning pages, (un)latching pages,
     *             creating files, or deleting files
     *             if the index is already activated
     */
    void destroy() throws HyracksDataException;

    /**
     * Purge the index files out of the buffer cache.
     * Can only be called if the caller is absolutely sure the files don't contain dirty pages
     *
     * @throws HyracksDataException
     *             if the index is active
     */
    void purge() throws HyracksDataException;

    /**
     * Creates an {@link IIndexAccessor} for performing operations on this index.
     * An IIndexAccessor is not thread safe, but different IIndexAccessors can concurrently operate
     * on the same {@link IIndex}.
     *
     * @returns IIndexAccessor an accessor for this {@link IIndex}
     * @param iap
     *            an instance of the index access parameter class that contains modification callback,
     *            search operation callback, etc
     * @throws HyracksDataException
     */
    IIndexAccessor createAccessor(IIndexAccessParameters iap) throws HyracksDataException;

    /**
     * TODO: Get rid of this method
     *
     * Strictly a test method
     *
     * Ensures that all pages (and tuples) of the index are logically consistent.
     * An assertion error is thrown if validation fails.
     *
     * @throws HyracksDataException
     *             if there is an error performing validation
     */
    void validate() throws HyracksDataException;

    /**
     * @return the {@link IBufferCache} underlying this index.
     */
    IBufferCache getBufferCache();

    /**
     * @return the size, in bytes, of pre-allocated memory space that this index was allotted.
     */
    public long getMemoryAllocationSize();

    /**
     * @param fillFactor
     * @param verifyInput
     * @throws HyracksDataException
     */
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException;

    /**
     * TODO: This should be moved to ILSMIndex since filters don't make sense in non LSM context
     *
     * @return the number of filter fields
     */
    int getNumOfFilterFields();
}
