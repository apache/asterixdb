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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
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
    void search(ILSMIndexOperationContext ctx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException;

    /**
     * End the search
     *
     * @param ctx
     * @throws HyracksDataException
     */
    void endSearch(ILSMIndexOperationContext ctx) throws HyracksDataException;

    /**
     * Schedule a merge
     *
     * @param ctx
     * @param callback
     * @throws HyracksDataException
     * @throws IndexException
     */
    void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException;

    /**
     * Schedule full merge
     *
     * @param ctx
     * @param callback
     * @throws HyracksDataException
     * @throws IndexException
     */
    void scheduleFullMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException;

    /**
     * Perform a merge operation
     *
     * @param ctx
     * @param operation
     * @throws HyracksDataException
     * @throws IndexException
     */
    void merge(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException;

    /**
     * Schedule a flush
     *
     * @param ctx
     * @param callback
     * @throws HyracksDataException
     */
    void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback) throws HyracksDataException;

    /**
     * Perform a flush
     *
     * @param ctx
     * @param operation
     * @throws HyracksDataException
     * @throws IndexException
     */
    void flush(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException;

    /**
     * Add bulk loaded component
     *
     * @param index
     *            the new component
     * @throws HyracksDataException
     * @throws IndexException
     */
    void addBulkLoadedComponent(ILSMDiskComponent index) throws HyracksDataException;

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
     * @param bulkload
     *            true if the components were bulk loaded, false otherwise
     * @param opType
     *            The operation type
     * @throws HyracksDataException
     */
    void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMDiskComponent> diskComponents, boolean bulkload,
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
}
