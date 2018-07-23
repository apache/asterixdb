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

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.util.trace.ITracer;

public class ExternalIndexHarness extends LSMHarness {
    public ExternalIndexHarness(ILSMIndex lsmIndex, ILSMIOOperationScheduler ioScheduler, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, boolean replicationEnabled) {
        super(lsmIndex, ioScheduler, mergePolicy, opTracker, replicationEnabled, ITracer.NONE);
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
                                lsmIndex.scheduleReplication(null, componentsToBeReplicated,
                                        ReplicationOperation.DELETE, opType);
                            }
                            ((ILSMDiskComponent) c).deactivateAndDestroy();
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
                                triggerReplication(componentsToBeReplicated, opType);
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
                throw HyracksDataException.create(e);
            }
        }
    }

    @SuppressWarnings("squid:S1181")
    @Override
    public void addBulkLoadedComponent(ILSMIOOperation ioOperation) throws HyracksDataException {
        ILSMDiskComponent c = ioOperation.getNewComponent();
        try {
            c.markAsValid(lsmIndex.isDurable(), ioOperation);
        } catch (Throwable th) {
            ioOperation.setFailure(th);
        }
        if (ioOperation.hasFailed()) {
            throw HyracksDataException.create(ioOperation.getFailure());
        }
        synchronized (opTracker) {
            lsmIndex.addDiskComponent(c);
            if (replicationEnabled) {
                componentsToBeReplicated.clear();
                componentsToBeReplicated.add(c);
                triggerReplication(componentsToBeReplicated, LSMOperationType.LOAD);
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
    public ILSMIOOperation scheduleFlush(ILSMIndexOperationContext ctx) throws HyracksDataException {
        return NoOpIoOperation.INSTANCE;
    }

    @Override
    public void flush(ILSMIOOperation operation) throws HyracksDataException {
        throw new UnsupportedOperationException();
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
                lsmIndex.scheduleReplication(null, componentsToBeReplicated, ReplicationOperation.DELETE, null);
            }
            diskComponent.deactivateAndDestroy();
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
