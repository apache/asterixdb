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
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

public interface ILSMHarness {

    /**
     * Force modification even if memory component is full
     *
     * @param ctx
     *            the operation context
     * @param tuple
     *            the operation tuple
     * @throws HyracksDataException
     * @throws IndexException
     */
    void forceModify(ILSMIndexOperationContext ctx, ITupleReference tuple) throws HyracksDataException;

    /**
     * Modify the index if the memory component is not full, wait for a new memory component if the current one is full
     *
     * @param ctx
     *            the operation context
     * @param tryOperation
     *            true if IO operation
     * @param tuple
     *            the operation tuple
     * @return
     * @throws HyracksDataException
     * @throws IndexException
     */
    boolean modify(ILSMIndexOperationContext ctx, boolean tryOperation, ITupleReference tuple)
            throws HyracksDataException;

    /**
     * Search the index
     *
     * @param ctx
     *            the search operation context
     * @param cursor
     *            the index cursor
     * @param pred
     *            the search predicate
     * @throws HyracksDataException
     * @throws IndexException
     */
    void search(ILSMIndexOperationContext ctx, IIndexCursor cursor, ISearchPredicate pred) throws HyracksDataException;

    /**
     * End the search
     *
     * @param ctx
     * @throws HyracksDataException
     */
    void endSearch(ILSMIndexOperationContext ctx) throws HyracksDataException;

    /**
     * Scan all disk components of the index
     *
     * @param ctx
     *            the search operation context
     * @param cursor
     *            the index cursor
     * @throws HyracksDataException
     */
    void scanDiskComponents(ILSMIndexOperationContext ctx, IIndexCursor cursor) throws HyracksDataException;

    /**
     * End the scan
     *
     * @param ctx
     * @throws HyracksDataException
     */
    void endScanDiskComponents(ILSMIndexOperationContext ctx) throws HyracksDataException;

    /**
     * Schedule a merge
     *
     * @param ctx
     * @param callback
     * @throws HyracksDataException
     * @throws IndexException
     */
    ILSMIOOperation scheduleMerge(ILSMIndexOperationContext ctx) throws HyracksDataException;

    /**
     * Schedule full merge
     *
     * @param ctx
     * @param callback
     * @throws HyracksDataException
     * @throws IndexException
     */
    ILSMIOOperation scheduleFullMerge(ILSMIndexOperationContext ctx) throws HyracksDataException;

    /**
     * Perform a merge operation
     *
     * @param operation
     * @throws HyracksDataException
     * @throws IndexException
     */
    void merge(ILSMIOOperation operation) throws HyracksDataException;

    /**
     * Schedule a flush
     *
     * @param ctx
     * @param callback
     * @throws HyracksDataException
     */
    ILSMIOOperation scheduleFlush(ILSMIndexOperationContext ctx) throws HyracksDataException;

    /**
     * Perform a flush
     *
     * @param operation
     * @throws HyracksDataException
     * @throws IndexException
     */
    void flush(ILSMIOOperation operation) throws HyracksDataException;

    /**
     * Add bulk loaded component
     *
     * @param ioOperation
     *            the io operation that added the new component
     * @throws HyracksDataException
     * @throws IndexException
     */
    void addBulkLoadedComponent(ILSMIOOperation ioOperation) throws HyracksDataException;

    /**
     * Get index operation tracker
     */
    ILSMOperationTracker getOperationTracker();

    /**
     * Schedule replication
     *
     * @param ctx
     *            the operation context
     * @param diskComponents
     *            the disk component to be replicated
     * @param opType
     *            The operation type
     * @throws HyracksDataException
     */
    void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMDiskComponent> diskComponents,
            LSMOperationType opType) throws HyracksDataException;

    /**
     * End a replication operation
     *
     * @param ctx
     *            the operation context
     * @throws HyracksDataException
     */
    void endReplication(ILSMIndexOperationContext ctx) throws HyracksDataException;

    /**
     * Update the metadata of the memory component of the index. Waiting for a new memory component if
     * the current memory component is full
     *
     * @param ctx
     *            the operation context
     * @param key
     *            the meta key
     * @param value
     *            the meta value
     * @throws HyracksDataException
     */
    void updateMeta(ILSMIndexOperationContext ctx, IValueReference key, IValueReference value)
            throws HyracksDataException;

    /**
     * Force updating the metadata of the memory component of the index even if memory component is full
     *
     * @param ctx
     *            the operation context
     * @param key
     *            the meta key
     * @param value
     *            the meta value
     * @throws HyracksDataException
     */
    void forceUpdateMeta(ILSMIndexOperationContext ctx, IValueReference key, IValueReference value)
            throws HyracksDataException;

    /**
     * Update the filter with the value in the passed tuple
     *
     * @param ctx
     * @throws HyracksDataException
     */
    void updateFilter(ILSMIndexOperationContext ctx, ITupleReference tuple) throws HyracksDataException;

    /**
     * Perform batch operation on all tuples in the passed frame tuple accessor
     *
     * @param ctx
     *            the operation ctx
     * @param accessor
     *            the frame tuple accessor
     * @param tuple
     *            the mutable tuple used to pass the tuple to the processor
     * @param processor
     *            the tuple processor
     * @param frameOpCallback
     *            the callback at the end of the frame
     * @throws HyracksDataException
     */
    void batchOperate(ILSMIndexOperationContext ctx, FrameTupleAccessor accessor, FrameTupleReference tuple,
            IFrameTupleProcessor processor, IFrameOperationCallback frameOpCallback) throws HyracksDataException;

    /**
     * Rollback components that match the passed predicate
     *
     * @param ctx
     * @param predicate
     * @throws HyracksDataException
     */
    void deleteComponents(ILSMIndexOperationContext ctx, Predicate<ILSMComponent> predicate)
            throws HyracksDataException;

    /**
     * Replace the memory components in this operation context with their corresponding disk
     * components if possible
     *
     * @param ctx
     *            the operation context
     * @param startIndex
     *            the index of the first component to switch
     * @throws HyracksDataException
     */
    void replaceMemoryComponentsWithDiskComponents(ILSMIndexOperationContext ctx, int startIndex)
            throws HyracksDataException;
}
