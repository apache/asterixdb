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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.util.annotations.CriticalPath;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.ITracer.Scope;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LSMHarness implements ILSMHarness {
    private static final Logger LOGGER = LogManager.getLogger();

    protected final ILSMIndex lsmIndex;
    protected final ILSMIOOperationScheduler ioScheduler;
    protected final ComponentReplacementContext componentReplacementCtx;
    protected final ILSMMergePolicy mergePolicy;
    protected final ILSMOperationTracker opTracker;
    protected final AtomicBoolean fullMergeIsRequested;
    protected final boolean replicationEnabled;
    protected List<ILSMDiskComponent> componentsToBeReplicated;
    protected ITracer tracer;
    protected long traceCategory;

    public LSMHarness(ILSMIndex lsmIndex, ILSMIOOperationScheduler ioScheduler, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, boolean replicationEnabled, ITracer tracer) {
        this.lsmIndex = lsmIndex;
        this.ioScheduler = ioScheduler;
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
        componentReplacementCtx = new ComponentReplacementContext(lsmIndex);
    }

    protected boolean getAndEnterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType,
            boolean isTryOperation) throws HyracksDataException {
        long before = 0L;
        if (ctx.isTracingEnabled()) {
            before = System.nanoTime();
        }
        try {
            validateOperationEnterComponentsState(ctx);
            synchronized (opTracker) {
                while (true) {
                    lsmIndex.getOperationalComponents(ctx);
                    if (enterComponents(ctx, opType)) {
                        return true;
                    } else if (isTryOperation) {
                        return false;
                    }
                    try {
                        opTracker.wait(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw HyracksDataException.create(e);
                    }
                }
            }
        } finally {
            if (ctx.isTracingEnabled()) {
                ctx.incrementEnterExitTime(System.nanoTime() - before);
            }
        }
    }

    @CriticalPath
    protected boolean enterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType)
            throws HyracksDataException {
        validateOperationEnterComponentsState(ctx);
        List<ILSMComponent> components = ctx.getComponentHolder();
        int numEntered = 0;
        boolean entranceSuccessful = false;
        try {
            final int componentsCount = components.size();
            for (int i = 0; i < componentsCount; i++) {
                final ILSMComponent component = components.get(i);
                boolean isMutableComponent = numEntered == 0 && component.getType() == LSMComponentType.MEMORY;
                if (!component.threadEnter(opType, isMutableComponent)) {
                    break;
                }
                numEntered++;
            }
            entranceSuccessful = numEntered == components.size();
        } catch (Throwable e) { // NOSONAR: Log and re-throw
            LOGGER.warn("{} failed to enter components on {}", opType.name(), lsmIndex, e);
            throw e;
        } finally {
            if (!entranceSuccessful) {
                final int componentsCount = components.size();
                for (int i = 0; i < componentsCount; i++) {
                    final ILSMComponent component = components.get(i);
                    if (numEntered == 0) {
                        break;
                    }
                    boolean isMutableComponent = i == 0 && component.getType() == LSMComponentType.MEMORY;
                    component.threadExit(opType, true, isMutableComponent);
                    numEntered--;
                }
            }
        }
        if (!entranceSuccessful) {
            return false;
        }
        ctx.setAccessingComponents(true);
        opTracker.beforeOperation(lsmIndex, opType, ctx.getSearchOperationCallback(), ctx.getModificationCallback());
        return true;
    }

    @CriticalPath
    private void doExitComponents(ILSMIndexOperationContext ctx, LSMOperationType opType,
            ILSMDiskComponent newComponent, boolean failedOperation) throws HyracksDataException {
        /*
         * FLUSH and MERGE operations should always exit the components
         * to notify waiting threads.
         */
        if (!ctx.isAccessingComponents() && opType != LSMOperationType.FLUSH && opType != LSMOperationType.MERGE) {
            return;
        }
        List<ILSMDiskComponent> inactiveDiskComponentsToBeDeleted = null;
        List<ILSMMemoryComponent> inactiveMemoryComponentsToBeCleanedUp = null;
        try {
            synchronized (opTracker) {
                try {
                    /*
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
                    exitOperationalComponents(ctx, opType, failedOperation);
                    ctx.setAccessingComponents(false);
                    exitOperation(ctx, opType, newComponent, failedOperation);
                } catch (Throwable e) { // NOSONAR: Log and re-throw
                    LOGGER.warn("Failure exiting components", e);
                    throw e;
                } finally {
                    if (failedOperation && (opType == LSMOperationType.MODIFICATION
                            || opType == LSMOperationType.FORCE_MODIFICATION)) {
                        //When the operation failed, completeOperation() method must be called
                        //in order to decrement active operation count which was incremented
                        // in beforeOperation() method.
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
                    List<ILSMDiskComponent> inactiveDiskComponents = lsmIndex.getInactiveDiskComponents();
                    if (!inactiveDiskComponents.isEmpty()) {
                        for (ILSMDiskComponent inactiveComp : inactiveDiskComponents) {
                            if (inactiveComp.getFileReferenceCount() == 1) {
                                inactiveDiskComponentsToBeDeleted = inactiveDiskComponentsToBeDeleted == null
                                        ? new ArrayList<>() : inactiveDiskComponentsToBeDeleted;
                                inactiveDiskComponentsToBeDeleted.add(inactiveComp);
                            }
                        }
                        if (inactiveDiskComponentsToBeDeleted != null) {
                            inactiveDiskComponents.removeAll(inactiveDiskComponentsToBeDeleted);
                        }
                    }
                    List<ILSMMemoryComponent> inactiveMemoryComponents = lsmIndex.getInactiveMemoryComponents();
                    if (!inactiveMemoryComponents.isEmpty()) {
                        inactiveMemoryComponentsToBeCleanedUp = new ArrayList<>(inactiveMemoryComponents);
                        inactiveMemoryComponents.clear();
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
                        lsmIndex.scheduleReplication(null, inactiveDiskComponentsToBeDeleted,
                                ReplicationOperation.DELETE, opType);
                    }
                    for (ILSMDiskComponent c : inactiveDiskComponentsToBeDeleted) {
                        c.deactivateAndDestroy();
                    }
                } catch (Throwable e) { // NOSONAR Log and re-throw
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.log(Level.WARN, "Failure scheduling replication or destroying merged component", e);
                    }
                    throw e; // NOSONAR: The last call in the finally clause
                }
            }
            if (inactiveMemoryComponentsToBeCleanedUp != null) {
                for (ILSMMemoryComponent c : inactiveMemoryComponentsToBeCleanedUp) {
                    tracer.instant(c.toString(), traceCategory, Scope.p, lsmIndex.toString());
                    c.cleanup();
                    synchronized (opTracker) {
                        c.reset();
                        // Notify all waiting threads whenever the mutable component's state
                        // has changed to inactive. This is important because even though we switched
                        // the mutable components, it is possible that the component that we just
                        // switched to is still busy flushing its data to disk. Thus, the notification
                        // that was issued upon scheduling the flush is not enough.
                        opTracker.notifyAll(); // NOSONAR: Always called inside synchronized block
                    }
                }
            }
            if (opType == LSMOperationType.FLUSH) {
                ILSMMemoryComponent flushingComponent = (ILSMMemoryComponent) ctx.getComponentHolder().get(0);
                // We must call flushed without synchronizing on opTracker to avoid deadlocks
                flushingComponent.flushed();
            }
        }
    }

    private void exitOperation(ILSMIndexOperationContext ctx, LSMOperationType opType, ILSMDiskComponent newComponent,
            boolean failedOperation) throws HyracksDataException {
        // Then, perform any action that is needed to be taken based on the operation type.
        switch (opType) {
            case FLUSH:
                // newComponent is null if the flush op. was not performed.
                if (!failedOperation && newComponent != null) {
                    lsmIndex.addDiskComponent(newComponent);
                    // TODO: The following should also replicate component Id
                    // even if empty component
                    if (replicationEnabled && newComponent != EmptyComponent.INSTANCE) {
                        componentsToBeReplicated.clear();
                        componentsToBeReplicated.add(newComponent);
                        triggerReplication(componentsToBeReplicated, opType);
                    }
                    mergePolicy.diskComponentAdded(lsmIndex, false);
                }
                break;
            case MERGE:
                // newComponent is null if the merge op. was not performed.
                if (!failedOperation && newComponent != null) {
                    lsmIndex.subsumeMergedComponents(newComponent, ctx.getComponentHolder());
                    if (replicationEnabled && newComponent != EmptyComponent.INSTANCE) {
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
    }

    @CriticalPath
    private void exitOperationalComponents(ILSMIndexOperationContext ctx, LSMOperationType opType,
            boolean failedOperation) throws HyracksDataException {
        // First check if there is any action that is needed to be taken
        // based on the state of each component.
        final List<ILSMComponent> componentHolder = ctx.getComponentHolder();
        final int componentsCount = componentHolder.size();
        for (int i = 0; i < componentsCount; i++) {
            final ILSMComponent c = componentHolder.get(i);
            boolean isMutableComponent = i == 0 && c.getType() == LSMComponentType.MEMORY;
            boolean needsCleanup = c.threadExit(opType, failedOperation, isMutableComponent);
            if (c.getType() == LSMComponentType.MEMORY) {
                if (c.getState() == ComponentState.READABLE_UNWRITABLE) {
                    if (isMutableComponent && (opType == LSMOperationType.MODIFICATION
                            || opType == LSMOperationType.FORCE_MODIFICATION)) {
                        lsmIndex.changeFlushStatusForCurrentMutableCompoent(true);
                    }
                }
                if (needsCleanup) {
                    lsmIndex.addInactiveMemoryComponent((ILSMMemoryComponent) c);
                }
            } else if (c.getState() == ComponentState.INACTIVE) {
                lsmIndex.addInactiveDiskComponent((AbstractLSMDiskComponent) c);
            }
        }
    }

    private void exitComponents(ILSMIndexOperationContext ctx, LSMOperationType opType, ILSMDiskComponent newComponent,
            boolean failedOperation) throws HyracksDataException {
        long before = 0L;
        if (ctx.isTracingEnabled()) {
            before = System.nanoTime();
        }
        try {
            doExitComponents(ctx, opType, newComponent, failedOperation);
        } finally {
            if (ctx.isTracingEnabled()) {
                ctx.incrementEnterExitTime(System.nanoTime() - before);
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
        // lock should be acquired before entering LSM components.
        // Otherwise, if a writer activates a new memory component, its updates may be ignored by subsequent readers
        // if the readers have entered components first. However, based on the order of acquiring locks,
        // the updates made by the writer should be seen by these subsequent readers.
        ctx.getSearchOperationCallback().before(pred.getLowKey());
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
    public ILSMIOOperation scheduleFlush(ILSMIndexOperationContext ctx) throws HyracksDataException {
        ILSMIOOperation flush;
        LOGGER.debug("Flush is being scheduled on {}", lsmIndex);
        if (!lsmIndex.isMemoryComponentsAllocated()) {
            lsmIndex.allocateMemoryComponents();
        }
        synchronized (opTracker) {
            try {
                flush = lsmIndex.createFlushOperation(ctx);
            } finally {
                // Notify all waiting threads whenever a flush has been scheduled since they will check
                // again if they can grab and enter the mutable component.
                opTracker.notifyAll();
            }
        }
        ioScheduler.scheduleOperation(flush);
        return flush;
    }

    @SuppressWarnings("squid:S2142")
    @Override
    public void flush(ILSMIOOperation operation) throws HyracksDataException {
        LOGGER.debug("Started a flush operation for index: {}", lsmIndex);
        synchronized (opTracker) {
            while (!enterComponents(operation.getAccessor().getOpContext(), LSMOperationType.FLUSH)) {
                try {
                    opTracker.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
            }
        }
        try {
            doIo(operation);
        } finally {
            exitComponents(operation.getAccessor().getOpContext(), LSMOperationType.FLUSH, operation.getNewComponent(),
                    operation.getStatus() == LSMIOOperationStatus.FAILURE);
            opTracker.completeOperation(lsmIndex, LSMOperationType.FLUSH,
                    operation.getAccessor().getOpContext().getSearchOperationCallback(),
                    operation.getAccessor().getOpContext().getModificationCallback());
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Finished the flush operation for index: {}. Result: {}", lsmIndex, operation.getStatus());
        }
    }

    public void doIo(ILSMIOOperation operation) {
        try {
            operation.getCallback().beforeOperation(operation);
            ILSMDiskComponent newComponent = operation.getIOOpertionType() == LSMIOOperationType.FLUSH
                    ? lsmIndex.flush(operation) : lsmIndex.merge(operation);
            operation.setNewComponent(newComponent);
            operation.getCallback().afterOperation(operation);
            if (newComponent != null) {
                newComponent.markAsValid(lsmIndex.isDurable(), operation);
            }
        } catch (Throwable e) { // NOSONAR Must catch all
            operation.setStatus(LSMIOOperationStatus.FAILURE);
            operation.setFailure(e);
            if (LOGGER.isErrorEnabled()) {
                LOGGER.log(Level.ERROR, "{} operation failed on {}", operation.getIOOpertionType(), lsmIndex, e);
            }
        } finally {
            try {
                operation.getCallback().afterFinalize(operation);
            } catch (Throwable th) {// NOSONAR Must catch all
                operation.setStatus(LSMIOOperationStatus.FAILURE);
                operation.setFailure(th);
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.log(Level.ERROR, "{} operation.afterFinalize failed on {}", operation.getIOOpertionType(),
                            lsmIndex, th);
                }
            }
        }
        // if the operation failed, we need to cleanup files
        if (operation.getStatus() == LSMIOOperationStatus.FAILURE) {
            operation.cleanup(lsmIndex.getBufferCache());
        }
    }

    @Override
    public void merge(ILSMIOOperation operation) throws HyracksDataException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Started a merge operation for index: {}", lsmIndex);
        }
        synchronized (opTracker) {
            enterComponents(operation.getAccessor().getOpContext(), LSMOperationType.MERGE);
        }
        try {
            doIo(operation);
        } finally {
            exitComponents(operation.getAccessor().getOpContext(), LSMOperationType.MERGE, operation.getNewComponent(),
                    operation.getStatus() == LSMIOOperationStatus.FAILURE);
            opTracker.completeOperation(lsmIndex, LSMOperationType.MERGE,
                    operation.getAccessor().getOpContext().getSearchOperationCallback(),
                    operation.getAccessor().getOpContext().getModificationCallback());
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Finished the merge operation for index: {}. Result: {}", lsmIndex, operation.getStatus());
        }
    }

    @Override
    public ILSMIOOperation scheduleMerge(ILSMIndexOperationContext ctx) throws HyracksDataException {
        ILSMIOOperation operation;
        synchronized (opTracker) {
            operation = lsmIndex.createMergeOperation(ctx);
        }
        ioScheduler.scheduleOperation(operation);
        return operation;
    }

    @Override
    public ILSMIOOperation scheduleFullMerge(ILSMIndexOperationContext ctx) throws HyracksDataException {
        ILSMIOOperation operation;
        synchronized (opTracker) {
            fullMergeIsRequested.set(true);
            ctx.getComponentsToBeMerged().addAll(lsmIndex.getDiskComponents());
            operation = lsmIndex.createMergeOperation(ctx);
            if (operation != NoOpIoOperation.INSTANCE) {
                fullMergeIsRequested.set(false);
            }
            // If the merge cannot be scheduled because there is already an ongoing merge on
            // subset/all of the components, then whenever the current merge has finished,
            // it will schedule the full merge again.
        }
        ioScheduler.scheduleOperation(operation);
        return operation;
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
            mergePolicy.diskComponentAdded(lsmIndex, false);
        }
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return opTracker;
    }

    protected void triggerReplication(List<ILSMDiskComponent> lsmComponents, LSMOperationType opType)
            throws HyracksDataException {
        ILSMIndexAccessor accessor = lsmIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        accessor.scheduleReplication(lsmComponents, opType);
    }

    @Override
    public void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMDiskComponent> lsmComponents,
            LSMOperationType opType) throws HyracksDataException {
        //enter the LSM components to be replicated to prevent them from being deleted until they are replicated
        if (!getAndEnterComponents(ctx, LSMOperationType.REPLICATE, false)) {
            return;
        }
        lsmIndex.scheduleReplication(ctx, lsmComponents, ReplicationOperation.REPLICATE, opType);
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
            } catch (Throwable th) {
                processor.fail(th);
                throw th;
            } finally {
                processor.finish();
            }
        } catch (HyracksDataException e) {
            LOGGER.warn("Failed to process frame", e);
            throw e;
        } finally {
            exit(ctx);
            ctx.logPerformanceCounters(accessor.getTupleCount());
        }
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
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.log(Level.WARN, "Ignoring interrupt while waiting for lagging merge on " + lsmIndex, e);
                    }
                }
            }
        }
    }

    @SuppressWarnings("squid:S2142")
    @Override
    public void deleteComponents(ILSMIndexOperationContext ctx, Predicate<ILSMComponent> predicate)
            throws HyracksDataException {
        ILSMIOOperation ioOperation = null;
        // We need to always start the component delete from current memory component.
        // This will ensure Primary and secondary component id still matches after component delete
        if (!lsmIndex.isMemoryComponentsAllocated()) {
            lsmIndex.allocateMemoryComponents();
        }
        synchronized (opTracker) {
            waitForFlushesAndMerges();
            // We always start with the memory component
            ILSMMemoryComponent memComponent = lsmIndex.getCurrentMemoryComponent();
            if (predicate.test(memComponent)) {
                // schedule a delete for flushed component
                ctx.reset();
                ctx.setOperation(IndexOperation.DELETE_COMPONENTS);
                ioOperation = scheduleFlush(ctx);
            } else {
                // since we're not deleting the memory component, we can't delete any previous component
                return;
            }
        }
        // Here, we are releasing the opTracker to allow other operations:
        // (searches, delete flush we will schedule, delete merge we will schedule).
        try {
            ioOperation.sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
        if (ioOperation.getStatus() == LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(ioOperation.getFailure());
        }
        ctx.reset();
        ctx.setOperation(IndexOperation.DELETE_COMPONENTS);
        List<ILSMDiskComponent> toBeDeleted;
        synchronized (opTracker) {
            waitForFlushesAndMerges();
            List<ILSMDiskComponent> diskComponents = lsmIndex.getDiskComponents();
            for (ILSMDiskComponent component : diskComponents) {
                if (predicate.test(component)) {
                    ctx.getComponentsToBeMerged().add(component);
                } else {
                    // Can't delete older components when newer one is still there
                    break;
                }
            }
            if (ctx.getComponentsToBeMerged().isEmpty()) {
                return;
            }
            toBeDeleted = new ArrayList<>(ctx.getComponentsToBeMerged());
            // ScheduleMerge is actually a try operation
            ioOperation = scheduleMerge(ctx);
        }
        try {
            ioOperation.sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
        if (ioOperation.getStatus() == LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(ioOperation.getFailure());
        }
        synchronized (opTracker) {
            // ensure that merge has succeeded
            for (ILSMDiskComponent component : toBeDeleted) {
                if (lsmIndex.getDiskComponents().contains(component)) {
                    throw HyracksDataException.create(ErrorCode.A_MERGE_OPERATION_HAS_FAILED, component.toString());
                }
            }
        }
    }

    private void waitForFlushesAndMerges() throws HyracksDataException {
        while (flushingOrMerging()) {
            try {
                opTracker.wait(); // NOSONAR: OpTracker is always synchronized here
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARN, "Interrupted while attempting component level delete", e);
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

    @Override
    public void replaceMemoryComponentsWithDiskComponents(ILSMIndexOperationContext ctx, int startIndex)
            throws HyracksDataException {
        synchronized (opTracker) {
            componentReplacementCtx.reset();
            for (int i = 0; i < ctx.getComponentHolder().size(); i++) {
                if (i >= startIndex) {
                    ILSMComponent next = ctx.getComponentHolder().get(i);
                    if (next.getType() == LSMComponentType.MEMORY
                            && next.getState() == ComponentState.UNREADABLE_UNWRITABLE) {
                        componentReplacementCtx.getComponentHolder().add(next);
                        componentReplacementCtx.swapIndex(i);
                    }
                }
            }
            if (componentReplacementCtx.getComponentHolder().isEmpty()) {
                throw new IllegalStateException(
                        "replaceMemoryComponentsWithDiskComponents called with no potential components");
            }
            // before we exit, we should keep the replaced component ids
            // we should also ensure that exact disk component replacement exist
            if (componentReplacementCtx.proceed(lsmIndex.getDiskComponents())) {
                // exit old component
                exitComponents(componentReplacementCtx, LSMOperationType.SEARCH, null, false);
                // enter new component
                componentReplacementCtx.prepareToEnter();
                enterComponents(componentReplacementCtx, LSMOperationType.SEARCH);
                componentReplacementCtx.replace(ctx);
            }
        }
    }
}
