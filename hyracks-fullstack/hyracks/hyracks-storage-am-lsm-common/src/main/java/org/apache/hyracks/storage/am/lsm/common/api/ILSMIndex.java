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
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMHarness;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
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
    ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) throws HyracksDataException;

    ILSMOperationTracker getOperationTracker();

    ILSMIOOperationCallback getIOOperationCallback();

    /**
     * components with lower indexes are newer than components with higher index
     */
    List<ILSMDiskComponent> getDiskComponents();

    boolean isPrimaryIndex();

    void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException;

    /**
     * If this method returns successfully, then the cursor has been opened, and need to be closed
     * Otherwise, it has not been opened
     *
     * @param ictx
     * @param cursor
     * @param pred
     * @throws HyracksDataException
     */
    void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred) throws HyracksDataException;

    public void scanDiskComponents(ILSMIndexOperationContext ctx, IIndexCursor cursor) throws HyracksDataException;

    /**
     * Create a flush operation.
     * This is an atomic operation. If an exception is thrown, no partial effect is left
     *
     * @return the flush operation
     *
     * @param ctx
     *            the operation context
     * @param callback
     *            the IO callback
     * @throws HyracksDataException
     */
    ILSMIOOperation createFlushOperation(ILSMIndexOperationContext ctx) throws HyracksDataException;

    ILSMDiskComponent flush(ILSMIOOperation operation) throws HyracksDataException;

    /**
     * Create a merge operation.
     * This is an atomic operation. If an exception is thrown, no partial effect is left
     *
     * @param ctx
     *            the operation context
     * @param callback
     *            the IO callback
     * @throws HyracksDataException
     */
    ILSMIOOperation createMergeOperation(ILSMIndexOperationContext ctx) throws HyracksDataException;

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

    List<ILSMMemoryComponent> getInactiveMemoryComponents();

    void addInactiveMemoryComponent(ILSMMemoryComponent memoryComponent);

    boolean isCurrentMutableComponentEmpty() throws HyracksDataException;

    void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMDiskComponent> diskComponents,
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
     * Creates a disk component for the bulk load operation
     *
     * @return
     * @throws HyracksDataException
     */
    ILSMDiskComponent createBulkLoadTarget() throws HyracksDataException;

    /**
     * @return The number of all memory components (active and inactive)
     */
    int getNumberOfAllMemoryComponents();

    /**
     * @return the {@link ILSMHarness} of the index
     */
    ILSMHarness getHarness();

    /**
     * @return the absolute path of the index
     */
    String getIndexIdentifier();

    /**
     * Create a bulk loader
     *
     * @param fillFactor
     * @param verifyInput
     * @param numElementsHint
     * @param checkIfEmptyIndex
     * @param parameters
     * @return
     * @throws HyracksDataException
     */
    IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, Map<String, Object> parameters) throws HyracksDataException;

    /**
     * Reset the current memory component id to 0.
     */
    void resetCurrentComponentIndex();

}
