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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameTupleProcessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.util.IOOperationUtils;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.ITracer.Scope;

public class LSMHarness implements ILSMHarness {
    private static final Logger LOGGER = Logger.getLogger(LSMHarness.class.getName());

    protected final ILSMIndex lsmIndex;
    protected final ILSMMergePolicy mergePolicy;
    protected final ILSMOperationTracker opTracker;
    protected final AtomicBoolean fullMergeIsRequested;
    protected final boolean replicationEnabled;
    protected List<ILSMDiskComponent> componentsToBeReplicated;
    protected ITracer tracer;
    protected long traceCategory;

    public LSMHarness(ILSMIndex lsmIndex, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            boolean replicationEnabled, ITracer tracer) {
        this.lsmIndex = lsmIndex;
        this.opTracker = opTracker;
        this.mergePolicy = mergePolicy;
        this.tracer = tracer;
        this.traceCategory = tracer.getRegistry().get("release-memory-component");
        fullMergeIsRequested = new AtomicBoolean();
        //only durable indexes are replicated
        this.replicationEnabled = replicationEnabled && lsmIndex.isDurable();
        if (replicationEnabled) {
            this.componentsToBeReplicated = new ArrayList<>();
        }
    }

    protected boolean getAndEnterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType,
            boolean isTryOperation) throws HyracksDataException {
        validateOperationEnterComponentsState(ctx);
        synchronized (opTracker) {
            while (true) {
                lsmIndex.getOperationalComponents(ctx);
                // Before entering the components, prune those corner cases that indeed should not proceed.
                switch (opType) {
                    case FLUSH:
                        // if the lsm index does not have memory components allocated, then nothing to flush
                        if (!lsmIndex.isMemoryComponentsAllocated()) {
                            return false;
                        }
                        ILSMMemoryComponent flushingComponent = (ILSMMemoryComponent) ctx.getComponentHolder().get(0);
                        if (!flushingComponent.isModified()) {
                            if (flushingComponent.getState() == ComponentState.READABLE_UNWRITABLE) {
                                //The mutable component has not been modified by any writer. There is nothing to flush.
                                //since the component is empty, set its state back to READABLE_WRITABLE only when it's
                                //state has been set to READABLE_UNWRITABLE
                                flushingComponent.setState(ComponentState.READABLE_WRITABLE);
                                opTracker.notifyAll();

                                // Call recycled only when we change it's state is reset back to READABLE_WRITABLE
                                // Otherwise, if the component is in other state, e.g., INACTIVE, or
                                // READABLE_UNWRITABLE_FLUSHING, it's not considered as being recycled here.
                                lsmIndex.getIOOperationCallback().recycled(flushingComponent);
                            }
                            return false;
                        }
                        if (flushingComponent.getWriterCount() > 0) {
                            /*
                             * This case is a case where even though FLUSH log was flushed to disk and scheduleFlush is triggered,
                             * the current in-memory component (whose state was changed to READABLE_WRITABLE (RW)
                             * from READABLE_UNWRITABLE(RU) before FLUSH log was written to log tail (which is memory buffer of log file)
                             * and then the state was changed back to RW (as shown in the following scenario)) can have writers
                             * based on the current code base/design.
                             * Thus, the writer count of the component may be greater than 0.
                             * if this happens, intead of throwing exception, scheduleFlush() deal with this situation by not flushing
                             * the component.
                             * Please see issue 884 for more detail information:
                             * https://code.google.com/p/asterixdb/issues/detail?id=884&q=owner%3Akisskys%40gmail.com&colspec=ID%20Type%20Status%20Priority%20Milestone%20Owner%20Summary%20ETA%20Severity
                             *
                             */
                            return false;
                        }
                        break;
                    case MERGE:
                        if (ctx.getComponentHolder().size() < 2
                                && ctx.getOperation() != IndexOperation.DELETE_DISK_COMPONENTS) {
                            // There is only a single component. There is nothing to merge.
                            return false;
                        }
                        break;
                    default:
                        break;
                }
                if (enterComponents(ctx, opType)) {
                    return true;
                } else if (isTryOperation) {
                    return false;
                }
                try {
                    // Flush and merge operations should never reach this wait call, because they are always try operations.
                    // If they fail to enter the components, then it means that there are an ongoing flush/merge operation on
                    // the same components, so they should not proceed.
                    if (opType == LSMOperationType.MODIFICATION) {
                        // before waiting, make sure the index is in a modifiable state to avoid waiting forever.
                        ensureIndexModifiable();
                    }
                    opTracker.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
            }
        }
    }

    protected boolean enterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType)
            throws HyracksDataException {
        validateOperationEnterComponentsState(ctx);
        List<ILSMComponent> components = ctx.getComponentHolder();
        int numEntered = 0;
        boolean entranceSuccessful = false;
        try {
            for (ILSMComponent c : components) {
                boolean isMutableComponent = numEntered == 0 && c.getType() == LSMComponentType.MEMORY ? true : false;
                if (!c.threadEnter(opType, isMutableComponent)) {
                    break;
                }
                numEntered++;
            }
            entranceSuccessful = numEntered == components.size();
        } catch (Throwable e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.log(Level.SEVERE, opType.name() + " failed to enter components on " + lsmIndex, e);
            }
            throw e;
        } finally {
            if (!entranceSuccessful) {
                int i = 0;
                for (ILSMComponent c : components) {
                    if (numEntered == 0) {
                        break;
                    }
                    boolean isMutableComponent = i == 0 && c.getType() == LSMComponentType.MEMORY ? true : false;
                    c.threadExit(opType, true, isMutableComponent);
                    i++;
                    numEntered--;
                }
                return false;
            }
            ctx.setAccessingComponents(true);
        }
        // Check if there is any action that is needed to be taken based on the operation type
        switch (opType) {
            case FLUSH:
                lsmIndex.getIOOperationCallback().beforeOperation(LSMIOOperationType.FLUSH);
                // Changing the flush status should *always* precede changing the mutable component.
                lsmIndex.changeFlushStatusForCurrentMutableCompoent(false);
                lsmIndex.changeMutableComponent();
                // Notify all waiting threads whenever a flush has been scheduled since they will check
                // again if they can grab and enter the mutable component.
                opTracker.notifyAll();
                break;
            case MERGE:
                lsmIndex.getIOOperationCallback().beforeOperation(LSMIOOperationType.MERGE);
                break;
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
        List<ILSMDiskComponent> inactiveDiskComponents = null;
        List<ILSMDiskComponent> inactiveDiskComponentsToBeDeleted = null;
        try {
            synchronized (opTracker) {
                try {
                    /**
                     * [flow control]
                     * If merge operations are lagged according to the merge policy,
                     * flushing in-memory components are hold until the merge operation catches up.
                     * See PrefixMergePolicy.isMergeLagging() for more details.
                     */
                    if (opType == LSMOperationType.FLUSH) {
                        opTracker.notifyAll();
                        if (!failedOperation) {
                            waitForLaggingMerge();
                        }
                    } else if (opType == LSMOperationType.MERGE) {
                        opTracker.notifyAll();
                    }

                    int i = 0;
                    // First check if there is any action that is needed to be taken based on the state of each component.
                    for (ILSMComponent c : ctx.getComponentHolder()) {
                        boolean isMutableComponent = i == 0 && c.getType() == LSMComponentType.MEMORY ? true : false;
                        c.threadExit(opType, failedOperation, isMutableComponent);
                        if (c.getType() == LSMComponentType.MEMORY) {
                            switch (c.getState()) {
                                case READABLE_UNWRITABLE:
                                    if (isMutableComponent && (opType == LSMOperationType.MODIFICATION
                                            || opType == LSMOperationType.FORCE_MODIFICATION)) {
                                        lsmIndex.changeFlushStatusForCurrentMutableCompoent(true);
                                    }
                                    break;
                                case INACTIVE:
                                    tracer.instant(c.toString(), traceCategory, Scope.p, lsmIndex.toString());
                                    ((AbstractLSMMemoryComponent) c).reset();
                                    // Notify all waiting threads whenever the mutable component's state has changed to
                                    // inactive. This is important because even though we switched the mutable
                                    // components, it is possible that the component that we just switched to is still
                                    // busy flushing its data to disk. Thus, the notification that was issued upon
                                    // scheduling the flush is not enough.
                                    opTracker.notifyAll();
                                    break;
                                default:
                                    break;
                            }
                        } else {
                            switch (c.getState()) {
                                case INACTIVE:
                                    lsmIndex.addInactiveDiskComponent((AbstractLSMDiskComponent) c);
                                    break;
                                default:
                                    break;
                            }
                        }
                        i++;
                    }
                    ctx.setAccessingComponents(false);
                    // Then, perform any action that is needed to be taken based on the operation type.
                    switch (opType) {
                        case FLUSH:
                            // newComponent is null if the flush op. was not performed.
                            if (!failedOperation && newComponent != null) {
                                lsmIndex.addDiskComponent(newComponent);
                                if (replicationEnabled) {
                                    componentsToBeReplicated.clear();
                                    componentsToBeReplicated.add(newComponent);
                                    triggerReplication(componentsToBeReplicated, false, opType);
                                }
                                mergePolicy.diskComponentAdded(lsmIndex, false);
                            }
                            break;
                        case MERGE:
                            // newComponent is null if the merge op. was not performed.
                            if (!failedOperation && newComponent != null) {
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
                } catch (Throwable e) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.log(Level.SEVERE, e.getMessage(), e);
                    }
                    throw e;
                } finally {
                    if (failedOperation && (opType == LSMOperationType.MODIFICATION
                            || opType == LSMOperationType.FORCE_MODIFICATION)) {
                        //When the operation failed, completeOperation() method must be called
                        //in order to decrement active operation count which was incremented in beforeOperation() method.
                        opTracker.completeOperation(lsmIndex, opType, ctx.getSearchOperationCallback(),
                                ctx.getModificationCallback());
                    } else {
                        opTracker.afterOperation(lsmIndex, opType, ctx.getSearchOperationCallback(),
                                ctx.getModificationCallback());
                    }

                    /*
                     * = Inactive disk components lazy cleanup if any =
                     * Prepare to cleanup inactive diskComponents which were old merged components
                     * and not anymore accessed.
                     * This cleanup is done outside of optracker synchronized block.
                     */
                    inactiveDiskComponents = lsmIndex.getInactiveDiskComponents();
                    if (!inactiveDiskComponents.isEmpty()) {
                        for (ILSMDiskComponent inactiveComp : inactiveDiskComponents) {
                            if (inactiveComp.getFileReferenceCount() == 1) {
                                if (inactiveDiskComponentsToBeDeleted == null) {
                                    inactiveDiskComponentsToBeDeleted = new LinkedList<>();
                                }
                                inactiveDiskComponentsToBeDeleted.add(inactiveComp);
                            }
                        }
                        if (inactiveDiskComponentsToBeDeleted != null) {
                            inactiveDiskComponents.removeAll(inactiveDiskComponentsToBeDeleted);
                        }
                    }
                }
            }
        } finally {
            /*
             * cleanup inactive disk components if any
             */
            if (inactiveDiskComponentsToBeDeleted != null) {
                try {
                    //schedule a replication job to delete these inactive disk components from replicas
                    if (replicationEnabled) {
                        lsmIndex.scheduleReplication(null, inactiveDiskComponentsToBeDeleted, false,
                                ReplicationOperation.DELETE, opType);
                    }
                    for (ILSMDiskComponent c : inactiveDiskComponentsToBeDeleted) {
                        c.deactivateAndDestroy();
                    }
                } catch (Throwable e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.log(Level.WARNING, "Failure scheduling replication or destroying merged component", e);
                    }
                    throw e;
                }
            }
        }

    }

    @Override
    public void forceModify(ILSMIndexOperationContext ctx, ITupleReference tuple) throws HyracksDataException {
        LSMOperationType opType = LSMOperationType.FORCE_MODIFICATION;
        modify(ctx, false, tuple, opType);
    }

    @Override
    public boolean modify(ILSMIndexOperationContext ctx, boolean tryOperation, ITupleReference tuple)
            throws HyracksDataException {
        LSMOperationType opType = LSMOperationType.MODIFICATION;
        return modify(ctx, tryOperation, tuple, opType);
    }

    @Override
    public void updateMeta(ILSMIndexOperationContext ctx, IValueReference key, IValueReference value)
            throws HyracksDataException {
        if (!lsmIndex.isMemoryComponentsAllocated()) {
            lsmIndex.allocateMemoryComponents();
        }
        getAndEnterComponents(ctx, LSMOperationType.MODIFICATION, false);
        try {
            AbstractLSMMemoryComponent c = (AbstractLSMMemoryComponent) ctx.getComponentHolder().get(0);
            c.getMetadata().put(key, value);
            c.setModified();
        } finally {
            exitAndComplete(ctx, LSMOperationType.MODIFICATION);
        }
    }

    private void exitAndComplete(ILSMIndexOperationContext ctx, LSMOperationType op) throws HyracksDataException {
        try {
            exitComponents(ctx, op, null, false);
        } finally {
            opTracker.completeOperation(null, op, null, ctx.getModificationCallback());
        }
    }

    @Override
    public void forceUpdateMeta(ILSMIndexOperationContext ctx, IValueReference key, IValueReference value)
            throws HyracksDataException {
        if (!lsmIndex.isMemoryComponentsAllocated()) {
            lsmIndex.allocateMemoryComponents();
        }
        getAndEnterComponents(ctx, LSMOperationType.FORCE_MODIFICATION, false);
        try {
            AbstractLSMMemoryComponent c = (AbstractLSMMemoryComponent) ctx.getComponentHolder().get(0);
            c.getMetadata().put(key, value);
            c.setModified();
        } finally {
            exitAndComplete(ctx, LSMOperationType.FORCE_MODIFICATION);
        }
    }

    private boolean modify(ILSMIndexOperationContext ctx, boolean tryOperation, ITupleReference tuple,
            LSMOperationType opType) throws HyracksDataException {
        if (!lsmIndex.isMemoryComponentsAllocated()) {
            lsmIndex.allocateMemoryComponents();
        }
        boolean failedOperation = false;
        if (!getAndEnterComponents(ctx, opType, tryOperation)) {
            return false;
        }
        try {
            lsmIndex.modify(ctx, tuple);
            // The mutable component is always in the first index.
            AbstractLSMMemoryComponent mutableComponent = (AbstractLSMMemoryComponent) ctx.getComponentHolder().get(0);
            mutableComponent.setModified();
        } catch (Exception e) {
            failedOperation = true;
            throw e;
        } finally {
            exitComponents(ctx, opType, null, failedOperation);
        }
        return true;
    }

    @Override
    public void search(ILSMIndexOperationContext ctx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        LSMOperationType opType = LSMOperationType.SEARCH;
        ctx.setSearchPredicate(pred);
        getAndEnterComponents(ctx, opType, false);
        try {
            ctx.getSearchOperationCallback().before(pred.getLowKey());
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

    @Override
    public void scanDiskComponents(ILSMIndexOperationContext ctx, IIndexCursor cursor) throws HyracksDataException {
        if (!lsmIndex.isPrimaryIndex()) {
            throw HyracksDataException.create(ErrorCode.DISK_COMPONENT_SCAN_NOT_ALLOWED_FOR_SECONDARY_INDEX);
        }
        LSMOperationType opType = LSMOperationType.DISK_COMPONENT_SCAN;
        getAndEnterComponents(ctx, opType, false);
        try {
            ctx.getSearchOperationCallback().before(null);
            lsmIndex.scanDiskComponents(ctx, cursor);
        } catch (Exception e) {
            exitComponents(ctx, opType, null, true);
            throw e;
        }
    }

    @Override
    public void endScanDiskComponents(ILSMIndexOperationContext ctx) throws HyracksDataException {
        if (ctx.getOperation() == IndexOperation.DISK_COMPONENT_SCAN) {
            try {
                exitComponents(ctx, LSMOperationType.DISK_COMPONENT_SCAN, null, false);
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        if (!getAndEnterComponents(ctx, LSMOperationType.FLUSH, true)) {
            callback.afterFinalize(LSMIOOperationType.FLUSH, null);
            return;
        }
        lsmIndex.scheduleFlush(ctx, callback);
    }

    @Override
    public void flush(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Started a flush operation for index: " + lsmIndex + " ...");
        }

        ILSMDiskComponent newComponent = null;
        boolean failedOperation = false;
        try {
            newComponent = lsmIndex.flush(operation);
            operation.getCallback().afterOperation(LSMIOOperationType.FLUSH, null, newComponent);
            newComponent.markAsValid(lsmIndex.isDurable());
        } catch (Throwable e) {
            failedOperation = true;
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.log(Level.SEVERE, "Flush failed on " + lsmIndex, e);
            }
            throw e;
        } finally {
            exitComponents(ctx, LSMOperationType.FLUSH, newComponent, failedOperation);
            operation.getCallback().afterFinalize(LSMIOOperationType.FLUSH, newComponent);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Finished the flush operation for index: " + lsmIndex);
        }
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        if (!getAndEnterComponents(ctx, LSMOperationType.MERGE, true)) {
            callback.afterFinalize(LSMIOOperationType.MERGE, null);
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
            callback.afterFinalize(LSMIOOperationType.MERGE, null);
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
        boolean failedOperation = false;
        try {
            newComponent = lsmIndex.merge(operation);
            operation.getCallback().afterOperation(LSMIOOperationType.MERGE, ctx.getComponentHolder(), newComponent);
            newComponent.markAsValid(lsmIndex.isDurable());
        } catch (Throwable e) {
            failedOperation = true;
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.log(Level.SEVERE, "Failed merge operation on " + lsmIndex, e);
            }
            throw e;
        } finally {
            exitComponents(ctx, LSMOperationType.MERGE, newComponent, failedOperation);
            // Completion of the merge operation is called here to and not on afterOperation because
            // Deletion of the old components comes after afterOperation is called and the number of
            // io operation should not be decremented before the operation is complete to avoid
            // index destroy from competing with the merge on deletion of the files.
            // The order becomes:
            // 1. scheduleMerge
            // 2. enterComponents
            // 3. beforeOperation (increment the numOfIoOperations)
            // 4. merge
            // 5. exitComponents
            // 6. afterOperation (no op)
            // 7. delete components
            // 8. completeOperation (decrement the numOfIoOperations)
            opTracker.completeOperation(lsmIndex, LSMOperationType.MERGE, ctx.getSearchOperationCallback(),
                    ctx.getModificationCallback());
            operation.getCallback().afterFinalize(LSMIOOperationType.MERGE, newComponent);
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
            mergePolicy.diskComponentAdded(lsmIndex, false);
        }
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return opTracker;
    }

    protected void triggerReplication(List<ILSMDiskComponent> lsmComponents, boolean bulkload, LSMOperationType opType)
            throws HyracksDataException {
        ILSMIndexAccessor accessor = lsmIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        accessor.scheduleReplication(lsmComponents, bulkload, opType);
    }

    @Override
    public void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMDiskComponent> lsmComponents,
            boolean bulkload, LSMOperationType opType) throws HyracksDataException {
        //enter the LSM components to be replicated to prevent them from being deleted until they are replicated
        if (!getAndEnterComponents(ctx, LSMOperationType.REPLICATE, false)) {
            return;
        }
        lsmIndex.scheduleReplication(ctx, lsmComponents, bulkload, ReplicationOperation.REPLICATE, opType);
    }

    @Override
    public void endReplication(ILSMIndexOperationContext ctx) throws HyracksDataException {
        exitComponents(ctx, LSMOperationType.REPLICATE, null, false);
    }

    protected void validateOperationEnterComponentsState(ILSMIndexOperationContext ctx) {
        if (ctx.isAccessingComponents()) {
            throw new IllegalStateException("Operation already has access to components of index " + lsmIndex);
        }
    }

    @Override
    public void updateFilter(ILSMIndexOperationContext ctx, ITupleReference tuple) throws HyracksDataException {
        if (!lsmIndex.isMemoryComponentsAllocated()) {
            lsmIndex.allocateMemoryComponents();
        }
        lsmIndex.updateFilter(ctx, tuple);
    }

    private void enter(ILSMIndexOperationContext ctx) throws HyracksDataException {
        if (!lsmIndex.isMemoryComponentsAllocated()) {
            lsmIndex.allocateMemoryComponents();
        }
        getAndEnterComponents(ctx, LSMOperationType.MODIFICATION, false);
    }

    private void exit(ILSMIndexOperationContext ctx) throws HyracksDataException {
        getAndExitComponentsAndComplete(ctx, LSMOperationType.MODIFICATION);
    }

    private void getAndExitComponentsAndComplete(ILSMIndexOperationContext ctx, LSMOperationType op)
            throws HyracksDataException {
        validateOperationEnterComponentsState(ctx);
        synchronized (opTracker) {
            lsmIndex.getOperationalComponents(ctx);
            ctx.setAccessingComponents(true);
            exitAndComplete(ctx, op);
        }
    }

    @Override
    public void batchOperate(ILSMIndexOperationContext ctx, FrameTupleAccessor accessor, FrameTupleReference tuple,
            IFrameTupleProcessor processor, IFrameOperationCallback frameOpCallback) throws HyracksDataException {
        processor.start();
        enter(ctx);
        try {
            try {
                processFrame(accessor, tuple, processor);
                frameOpCallback.frameCompleted();
            } finally {
                processor.finish();
            }
        } catch (HyracksDataException e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.log(Level.SEVERE, "Failed to process frame", e);
            }
            throw e;
        } finally {
            exit(ctx);
        }
    }

    /***
     * Ensures the index is in a modifiable state (no failed flushes)
     *
     * @throws HyracksDataException
     *             if the index is not in a modifiable state
     */
    private void ensureIndexModifiable() throws HyracksDataException {
        // if current memory component has a flush request, it means that flush didn't start for it
        if (lsmIndex.hasFlushRequestForCurrentMutableComponent()) {
            return;
        }
        // find if there is any memory component which is in a writable state or eventually will be in a writable state
        for (ILSMMemoryComponent memoryComponent : lsmIndex.getMemoryComponents()) {
            switch (memoryComponent.getState()) {
                case INACTIVE:
                    // will be activated on next modification
                case UNREADABLE_UNWRITABLE:
                    // flush completed successfully but readers are still inside
                case READABLE_WRITABLE:
                    // writable
                case READABLE_UNWRITABLE_FLUSHING:
                    // flush is ongoing
                    return;
                default:
                    // continue to the next component
            }
        }
        throw HyracksDataException.create(ErrorCode.CANNOT_MODIFY_INDEX_DISK_IS_FULL);
    }

    /**
     * Waits for any lagging merge operations to finish to avoid breaking
     * the merge policy (i.e. adding a new disk component can make the
     * number of mergable immutable components > maxToleranceComponentCount
     * by the merge policy)
     *
     * @throws HyracksDataException
     */
    private void waitForLaggingMerge() throws HyracksDataException {
        synchronized (opTracker) {
            while (mergePolicy.isMergeLagging(lsmIndex)) {
                try {
                    opTracker.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.log(Level.WARNING, "Ignoring interrupt while waiting for lagging merge on " + lsmIndex,
                                e);
                    }
                }
            }
        }
    }

    @Override
    public void deleteComponents(ILSMIndexOperationContext ctx, Predicate<ILSMComponent> predicate)
            throws HyracksDataException {
        BlockingIOOperationCallbackWrapper ioCallback =
                new BlockingIOOperationCallbackWrapper(lsmIndex.getIOOperationCallback());
        boolean deleteMemoryComponent;
        synchronized (opTracker) {
            waitForFlushesAndMerges();
            ensureNoFailedFlush();
            // We always start with the memory component
            ILSMMemoryComponent memComponent = lsmIndex.getCurrentMemoryComponent();
            deleteMemoryComponent = predicate.test(memComponent);
            if (deleteMemoryComponent) {
                // schedule a delete for flushed component
                ctx.reset();
                ctx.setOperation(IndexOperation.DELETE_MEMORY_COMPONENT);
                // ScheduleFlush is actually a try operation
                scheduleFlush(ctx, ioCallback);
            }
        }
        // Here, we are releasing the opTracker to allow other operations:
        // (searches, delete flush we will schedule, delete merge we will schedule).
        if (deleteMemoryComponent) {
            IOOperationUtils.waitForIoOperation(ioCallback);
        }
        ctx.reset();
        ioCallback = new BlockingIOOperationCallbackWrapper(lsmIndex.getIOOperationCallback());
        ctx.setOperation(IndexOperation.DELETE_DISK_COMPONENTS);
        List<ILSMDiskComponent> toBeDeleted;
        synchronized (opTracker) {
            waitForFlushesAndMerges();
            // Ensure that current memory component is empty and that no failed flushes happened so far
            // This is a workaround until ASTERIXDB-2106 is fixed
            ensureNoFailedFlush();
            List<ILSMDiskComponent> diskComponents = lsmIndex.getDiskComponents();
            for (ILSMDiskComponent component : diskComponents) {
                if (predicate.test(component)) {
                    ctx.getComponentsToBeMerged().add(component);
                }
            }
            if (ctx.getComponentsToBeMerged().isEmpty()) {
                return;
            }
            toBeDeleted = new ArrayList<>(ctx.getComponentsToBeMerged());
            // ScheduleMerge is actually a try operation
            scheduleMerge(ctx, ioCallback);
        }
        IOOperationUtils.waitForIoOperation(ioCallback);
        // ensure that merge has succeeded
        for (ILSMDiskComponent component : toBeDeleted) {
            if (lsmIndex.getDiskComponents().contains(component)) {
                throw HyracksDataException.create(ErrorCode.A_MERGE_OPERATION_HAS_FAILED);
            }
        }
    }

    /**
     * This can only be called in the steady state where:
     * 1. no scheduled flushes
     * 2. no incoming data
     *
     * @throws HyracksDataException
     */
    private void ensureNoFailedFlush() throws HyracksDataException {
        for (ILSMMemoryComponent memoryComponent : lsmIndex.getMemoryComponents()) {
            if (memoryComponent.getState() == ComponentState.READABLE_UNWRITABLE) {
                throw HyracksDataException.create(ErrorCode.A_FLUSH_OPERATION_HAS_FAILED);
            }
        }
    }

    private void waitForFlushesAndMerges() throws HyracksDataException {
        while (flushingOrMerging()) {
            try {
                opTracker.wait(); // NOSONAR: OpTracker is always synchronized here
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING, "Interrupted while attempting component level delete", e);
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            }
        }
    }

    private boolean flushingOrMerging() {
        // check if flushes are taking place
        for (ILSMMemoryComponent memComponent : lsmIndex.getMemoryComponents()) {
            if (memComponent.getState() == ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                return true;
            }
        }
        // check if merges are taking place
        for (ILSMDiskComponent diskComponent : lsmIndex.getDiskComponents()) {
            if (diskComponent.getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }

    private static void processFrame(FrameTupleAccessor accessor, FrameTupleReference tuple,
            IFrameTupleProcessor processor) throws HyracksDataException {
        int tupleCount = accessor.getTupleCount();
        int i = 0;
        while (i < tupleCount) {
            tuple.reset(accessor, i);
            processor.process(tuple, i);
            i++;
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + lsmIndex;
    }
}
