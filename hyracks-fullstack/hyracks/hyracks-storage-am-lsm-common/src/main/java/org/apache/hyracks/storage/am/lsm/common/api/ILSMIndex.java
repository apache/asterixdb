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
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMHarness;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;

/**
 * Methods to be implemented by an LSM index, which are called from {@link LSMHarness}.
 * The implementations of the methods below should be thread agnostic.
 * Synchronization of LSM operations like updates/searches/flushes/merges are
 * done by the {@link LSMHarness}. For example, a flush() implementation should only
 * create and return the new on-disk component, ignoring the fact that
 * concurrent searches/updates/merges may be ongoing.
 */
public interface ILSMIndex extends IIndex {

    void deactivate(boolean flush) throws HyracksDataException;

    @Override
    ILSMIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) throws HyracksDataException;

    ILSMOperationTracker getOperationTracker();

    ILSMIOOperationScheduler getIOScheduler();

    ILSMIOOperationCallback getIOOperationCallback();

    /**
     * components with lower indexes are newer than components with higher index
     */
    List<ILSMDiskComponent> getImmutableComponents();

    boolean isPrimaryIndex();

    void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException;

    void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred) throws HyracksDataException;

    public void scanDiskComponents(ILSMIndexOperationContext ctx, IIndexCursor cursor) throws HyracksDataException;

    void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback) throws HyracksDataException;

    ILSMDiskComponent flush(ILSMIOOperation operation) throws HyracksDataException;

    void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback) throws HyracksDataException;

    ILSMDiskComponent merge(ILSMIOOperation operation) throws HyracksDataException;

    void addDiskComponent(ILSMDiskComponent index) throws HyracksDataException;

    void subsumeMergedComponents(ILSMDiskComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException;

    void changeMutableComponent();

    void changeFlushStatusForCurrentMutableCompoent(boolean needsFlush);

    boolean hasFlushRequestForCurrentMutableComponent();

    /**
     * Populates the context's component holder with a snapshot of the components involved in the operation.
     *
     * @param ctx
     *            - the operation's context
     * @throws HyracksDataException
     */
    void getOperationalComponents(ILSMIndexOperationContext ctx) throws HyracksDataException;

    List<ILSMDiskComponent> getInactiveDiskComponents();

    void addInactiveDiskComponent(ILSMDiskComponent diskComponent);

    /**
     * Persist the LSM component
     *
     * @param lsmComponent
     *            , the component to be persistent
     * @throws HyracksDataException
     */
    void markAsValid(ILSMDiskComponent lsmComponent) throws HyracksDataException;

    boolean isCurrentMutableComponentEmpty() throws HyracksDataException;

    void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMDiskComponent> diskComponents, boolean bulkload,
            ReplicationOperation operation, LSMOperationType opType) throws HyracksDataException;

    boolean isMemoryComponentsAllocated();

    /**
     * Allocates the memory components of an LSM index in the buffer cache.
     *
     * @throws HyracksDataException
     */
    void allocateMemoryComponents() throws HyracksDataException;

    ILSMMemoryComponent getCurrentMemoryComponent();

    int getCurrentMemoryComponentIndex();

    List<ILSMMemoryComponent> getMemoryComponents();

    /**
     * @return true if the index is durable. Otherwise false.
     */
    boolean isDurable();

    /**
     * Update the filter with the passed tuple
     *
     * @param ictx
     * @param tuple
     * @throws HyracksDataException
     */
    void updateFilter(ILSMIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException;

    /**
     * Create a component bulk loader for the given component
     *
     * @param component
     * @param fillFactor
     * @param verifyInput
     * @param numElementsHint
     * @param checkIfEmptyIndex
     * @param withFilter
     * @param cleanupEmptyComponent
     * @return
     * @throws HyracksDataException
     */
    ILSMDiskComponentBulkLoader createComponentBulkLoader(ILSMDiskComponent component, float fillFactor,
            boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter,
            boolean cleanupEmptyComponent) throws HyracksDataException;

}
