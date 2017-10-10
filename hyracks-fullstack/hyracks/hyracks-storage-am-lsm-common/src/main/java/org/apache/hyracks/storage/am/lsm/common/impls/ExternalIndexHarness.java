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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class ExternalIndexHarness extends LSMHarness {
    private static final Logger LOGGER = Logger.getLogger(ExternalIndexHarness.class.getName());

    public ExternalIndexHarness(ILSMIndex lsmIndex, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            boolean replicationEnabled) {
        super(lsmIndex, mergePolicy, opTracker, replicationEnabled, null);
    }

    @Override
    protected boolean getAndEnterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType,
            boolean isTryOperation) throws HyracksDataException {
        validateOperationEnterComponentsState(ctx);
        synchronized (opTracker) {
            while (true) {
                lsmIndex.getOperationalComponents(ctx);
                // Before entering the components, prune those corner cases that indeed should not proceed.
                switch (opType) {
                    case MERGE:
                        if (ctx.getComponentHolder().size() < 2) {
                            // There is only a single component. There is nothing to merge.
                            return false;
                        }
                    default:
                        break;
                }
                if (enterComponents(ctx, opType)) {
                    return true;
                } else if (isTryOperation) {
                    return false;
                }
            }
        }
    }

    @Override
    protected boolean enterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType)
            throws HyracksDataException {
        validateOperationEnterComponentsState(ctx);
        List<ILSMComponent> components = ctx.getComponentHolder();
        int numEntered = 0;
        boolean entranceSuccessful = false;
        try {
            for (ILSMComponent c : components) {
                if (!c.threadEnter(opType, false)) {
                    break;
                }
                numEntered++;
            }
            entranceSuccessful = numEntered == components.size();
        } finally {
            if (!entranceSuccessful) {
                for (ILSMComponent c : components) {
                    if (numEntered == 0) {
                        break;
                    }
                    c.threadExit(opType, true, false);
                    numEntered--;
                }
                return false;
            }
            ctx.setAccessingComponents(true);
        }
        // Check if there is any action that is needed to be taken based on the operation type
        switch (opType) {
            case MERGE:
                lsmIndex.getIOOperationCallback().beforeOperation(LSMOperationType.MERGE);
            default:
                break;
        }
        opTracker.beforeOperation(lsmIndex, opType, ctx.getSearchOperationCallback(), ctx.getModificationCallback());
        return true;
    }

    private void exitComponents(ILSMIndexOperationContext ctx, LSMOperationType opType, ILSMDiskComponent newComponent,
            boolean failedOperation) throws HyracksDataException {
        /**
         * FLUSH and MERGE operations should always exit the components
         * to notify waiting threads.
         */
        if (!ctx.isAccessingComponents() && opType != LSMOperationType.FLUSH && opType != LSMOperationType.MERGE) {
            return;
        }
        synchronized (opTracker) {
            try {
                // First check if there is any action that is needed to be taken based on the state of each component.
                for (ILSMComponent c : ctx.getComponentHolder()) {
                    c.threadExit(opType, failedOperation, false);
                    switch (c.getState()) {
                        case INACTIVE:
                            if (replicationEnabled) {
                                componentsToBeReplicated.clear();
                                componentsToBeReplicated.add((ILSMDiskComponent) c);
                                lsmIndex.scheduleReplication(null, componentsToBeReplicated, false,
                                        ReplicationOperation.DELETE, opType);
                            }
                            ((ILSMDiskComponent) c).destroy();
                            break;
                        default:
                            break;
                    }
                }
                ctx.setAccessingComponents(false);
                // Then, perform any action that is needed to be taken based on the operation type.
                switch (opType) {
                    case MERGE:
                        // newComponent is null if the merge op. was not performed.
                        if (newComponent != null) {
                            beforeSubsumeMergedComponents(newComponent, ctx.getComponentHolder());
                            lsmIndex.subsumeMergedComponents(newComponent, ctx.getComponentHolder());
                            if (replicationEnabled) {
                                componentsToBeReplicated.clear();
                                componentsToBeReplicated.add(newComponent);
                                triggerReplication(componentsToBeReplicated, false, opType);
                            }
                            mergePolicy.diskComponentAdded(lsmIndex, fullMergeIsRequested.get());
                        }
                        break;
                    default:
                        break;
                }
            } finally {
                opTracker.afterOperation(lsmIndex, opType, ctx.getSearchOperationCallback(),
                        ctx.getModificationCallback());
            }
        }
    }

    @Override
    public void forceModify(ILSMIndexOperationContext ctx, ITupleReference tuple) throws HyracksDataException {
        throw HyracksDataException.create(ErrorCode.MODIFY_NOT_SUPPORTED_IN_EXTERNAL_INDEX);
    }

    @Override
    public boolean modify(ILSMIndexOperationContext ctx, boolean tryOperation, ITupleReference tuple)
            throws HyracksDataException {
        throw HyracksDataException.create(ErrorCode.MODIFY_NOT_SUPPORTED_IN_EXTERNAL_INDEX);
    }

    @Override
    public void search(ILSMIndexOperationContext ctx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        LSMOperationType opType = LSMOperationType.SEARCH;
        getAndEnterComponents(ctx, opType, false);
        try {
            lsmIndex.search(ctx, cursor, pred);
        } catch (Exception e) {
            exitComponents(ctx, opType, null, true);
            throw e;
        }
    }

    @Override
    public void endSearch(ILSMIndexOperationContext ctx) throws HyracksDataException {
        if (ctx.getOperation() == IndexOperation.SEARCH) {
            try {
                exitComponents(ctx, LSMOperationType.SEARCH, null, false);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        if (!getAndEnterComponents(ctx, LSMOperationType.MERGE, true)) {
            callback.afterFinalize(LSMOperationType.MERGE, null);
            return;
        }
        lsmIndex.scheduleMerge(ctx, callback);
    }

    @Override
    public void scheduleFullMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        fullMergeIsRequested.set(true);
        if (!getAndEnterComponents(ctx, LSMOperationType.MERGE, true)) {
            // If the merge cannot be scheduled because there is already an ongoing merge on subset/all of the components, then
            // whenever the current merge has finished, it will schedule the full merge again.
            callback.afterFinalize(LSMOperationType.MERGE, null);
            return;
        }
        fullMergeIsRequested.set(false);
        lsmIndex.scheduleMerge(ctx, callback);
    }

    @Override
    public void merge(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Started a merge operation for index: " + lsmIndex + " ...");
        }

        ILSMDiskComponent newComponent = null;
        try {
            newComponent = lsmIndex.merge(operation);
            operation.getCallback().afterOperation(LSMOperationType.MERGE, ctx.getComponentHolder(), newComponent);
            newComponent.markAsValid(lsmIndex.isDurable());
        } finally {
            exitComponents(ctx, LSMOperationType.MERGE, newComponent, false);
            operation.getCallback().afterFinalize(LSMOperationType.MERGE, newComponent);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Finished the merge operation for index: " + lsmIndex);
        }
    }

    @Override
    public void addBulkLoadedComponent(ILSMDiskComponent c) throws HyracksDataException {
        c.markAsValid(lsmIndex.isDurable());
        synchronized (opTracker) {
            lsmIndex.addDiskComponent(c);
            if (replicationEnabled) {
                componentsToBeReplicated.clear();
                componentsToBeReplicated.add(c);
                triggerReplication(componentsToBeReplicated, true, LSMOperationType.MERGE);
            }
            // Enter the component
            enterComponent(c);
            mergePolicy.diskComponentAdded(lsmIndex, false);
        }
    }

    // Three differences from  addBulkLoadedComponent
    // 1. this needs synchronization since others might be accessing the index (specifically merge operations that might change the lists of components)
    // 2. the actions taken by the index itself are different
    // 3. the component has already been marked valid by the bulk update operation
    public void addTransactionComponents(ILSMDiskComponent newComponent) throws HyracksDataException {
        ITwoPCIndex index = (ITwoPCIndex) lsmIndex;
        synchronized (opTracker) {
            List<ILSMDiskComponent> newerList;
            List<ILSMDiskComponent> olderList;
            if (index.getCurrentVersion() == 0) {
                newerList = index.getFirstComponentList();
                olderList = index.getSecondComponentList();
            } else {
                newerList = index.getSecondComponentList();
                olderList = index.getFirstComponentList();
            }
            // Exit components in old version of the index so they are ready to be
            // deleted if they are not needed anymore
            for (ILSMDiskComponent c : olderList) {
                exitComponent(c);
            }
            // Enter components in the newer list
            for (ILSMDiskComponent c : newerList) {
                enterComponent(c);
            }
            if (newComponent != null) {
                // Enter new component
                enterComponent(newComponent);
            }
            index.commitTransactionDiskComponent(newComponent);
            mergePolicy.diskComponentAdded(lsmIndex, fullMergeIsRequested.get());
        }
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        callback.afterFinalize(LSMOperationType.FLUSH, null);
    }

    @Override
    public void flush(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException {
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return opTracker;
    }

    public void beforeSubsumeMergedComponents(ILSMComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException {
        ITwoPCIndex index = (ITwoPCIndex) lsmIndex;
        // check if merge will affect the first list
        if (index.getFirstComponentList().containsAll(mergedComponents)) {
            // exit un-needed components
            for (ILSMComponent c : mergedComponents) {
                exitComponent((ILSMDiskComponent) c);
            }
            // enter new component
            enterComponent(newComponent);
        }
        // check if merge will affect the second list
        if (index.getSecondComponentList().containsAll(mergedComponents)) {
            // exit un-needed components
            for (ILSMComponent c : mergedComponents) {
                exitComponent((ILSMDiskComponent) c);
            }
            // enter new component
            enterComponent(newComponent);
        }
    }

    // The two methods: enterComponent and exitComponent are used to control
    // when components are to be deleted from disk
    private void enterComponent(ILSMComponent diskComponent) throws HyracksDataException {
        diskComponent.threadEnter(LSMOperationType.SEARCH, false);
    }

    private void exitComponent(ILSMDiskComponent diskComponent) throws HyracksDataException {
        diskComponent.threadExit(LSMOperationType.SEARCH, false, false);
        if (diskComponent.getState() == ILSMComponent.ComponentState.INACTIVE) {
            if (replicationEnabled) {
                componentsToBeReplicated.clear();
                componentsToBeReplicated.add(diskComponent);
                lsmIndex.scheduleReplication(null, componentsToBeReplicated, false, ReplicationOperation.DELETE, null);
            }
            diskComponent.destroy();
        }
    }

    public void indexFirstTimeActivated() throws HyracksDataException {
        ITwoPCIndex index = (ITwoPCIndex) lsmIndex;
        // Enter disk components <-- To avoid deleting them when they are
        // still needed-->
        for (ILSMComponent c : index.getFirstComponentList()) {
            enterComponent(c);
        }
        for (ILSMComponent c : index.getSecondComponentList()) {
            enterComponent(c);
        }
    }

    public void indexClear() throws HyracksDataException {
        ITwoPCIndex index = (ITwoPCIndex) lsmIndex;
        for (ILSMDiskComponent c : index.getFirstComponentList()) {
            exitComponent(c);
        }
        for (ILSMDiskComponent c : index.getSecondComponentList()) {
            exitComponent(c);
        }
    }

}
