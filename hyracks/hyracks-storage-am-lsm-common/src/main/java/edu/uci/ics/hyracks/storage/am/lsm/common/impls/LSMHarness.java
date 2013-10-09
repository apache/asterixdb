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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;

public class LSMHarness implements ILSMHarness {
    private static final Logger LOGGER = Logger.getLogger(LSMHarness.class.getName());

    private final ILSMIndexInternal lsmIndex;
    private final ILSMMergePolicy mergePolicy;
    private final ILSMOperationTracker opTracker;
    private final AtomicBoolean fullMergeIsRequested;

    public LSMHarness(ILSMIndexInternal lsmIndex, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker) {
        this.lsmIndex = lsmIndex;
        this.opTracker = opTracker;
        this.mergePolicy = mergePolicy;
        fullMergeIsRequested = new AtomicBoolean();
    }

    private boolean getAndEnterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType, boolean isTryOperation)
            throws HyracksDataException {
        synchronized (opTracker) {
            while (true) {
                lsmIndex.getOperationalComponents(ctx);
                // Before entering the components, prune those corner cases that indeed should not proceed.
                switch (opType) {
                    case FLUSH:
                        ILSMComponent flushingComponent = ctx.getComponentHolder().get(0);
                        if (!((AbstractMemoryLSMComponent) flushingComponent).isModified()) {
                            // The mutable component has not been modified by any writer. There is nothing to flush.
                            return false;
                        }
                        break;
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
                try {
                    // Flush and merge operations should never reach this wait call, because they are always try operations.
                    // If they fail to enter the components, then it means that there are an ongoing flush/merge operation on 
                    // the same components, so they should not proceed.
                    opTracker.wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    private boolean enterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType) throws HyracksDataException {
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
        }
        // Check if there is any action that is needed to be taken based on the operation type
        switch (opType) {
            case FLUSH:
                lsmIndex.getIOOperationCallback().beforeOperation(LSMOperationType.FLUSH);
                // Changing the flush status should *always* precede changing the mutable component.
                lsmIndex.changeFlushStatusForCurrentMutableCompoent(false);
                lsmIndex.changeMutableComponent();
                // Notify all waiting threads whenever a flush has been scheduled since they will check 
                // again if they can grab and enter the mutable component.
                opTracker.notifyAll();
                break;
            case MERGE:
                lsmIndex.getIOOperationCallback().beforeOperation(LSMOperationType.MERGE);
            default:
                break;
        }
        opTracker.beforeOperation(lsmIndex, opType, ctx.getSearchOperationCallback(), ctx.getModificationCallback());
        return true;
    }

    private void exitComponents(ILSMIndexOperationContext ctx, LSMOperationType opType, ILSMComponent newComponent,
            boolean failedOperation) throws HyracksDataException, IndexException {
        synchronized (opTracker) {
            try {
                int i = 0;
                // First check if there is any action that is needed to be taken based on the state of each component.
                for (ILSMComponent c : ctx.getComponentHolder()) {
                    boolean isMutableComponent = i == 0 && c.getType() == LSMComponentType.MEMORY ? true : false;
                    c.threadExit(opType, failedOperation, isMutableComponent);
                    if (c.getType() == LSMComponentType.MEMORY) {
                        switch (c.getState()) {
                            case READABLE_UNWRITABLE:
                                if (isMutableComponent
                                        && (opType == LSMOperationType.MODIFICATION || opType == LSMOperationType.FORCE_MODIFICATION)) {
                                    lsmIndex.changeFlushStatusForCurrentMutableCompoent(true);
                                }
                                break;
                            case INACTIVE:
                                ((AbstractMemoryLSMComponent) c).reset();
                                // Notify all waiting threads whenever the mutable component's has change to inactive. This is important because
                                // even though we switched the mutable components, it is possible that the component that we just switched
                                // to is still busy flushing its data to disk. Thus, the notification that was issued upon scheduling the flush
                                // is not enough. 
                                opTracker.notifyAll();
                                break;
                            default:
                                break;
                        }
                    } else {
                        switch (c.getState()) {
                            case INACTIVE:
                                ((AbstractDiskLSMComponent) c).destroy();
                                break;
                            default:
                                break;
                        }
                    }
                    i++;
                }
                // Then, perform any action that is needed to be taken based on the operation type.
                switch (opType) {
                    case FLUSH:
                        // newComponent is null if the flush op. was not performed.
                        if (newComponent != null) {
                            lsmIndex.addComponent(newComponent);
                            mergePolicy.diskComponentAdded(lsmIndex, false);
                        }
                        break;
                    case MERGE:
                        // newComponent is null if the merge op. was not performed.
                        if (newComponent != null) {
                            lsmIndex.subsumeMergedComponents(newComponent, ctx.getComponentHolder());
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
    public void forceModify(ILSMIndexOperationContext ctx, ITupleReference tuple) throws HyracksDataException,
            IndexException {
        LSMOperationType opType = LSMOperationType.FORCE_MODIFICATION;
        modify(ctx, false, tuple, opType);
    }

    @Override
    public boolean modify(ILSMIndexOperationContext ctx, boolean tryOperation, ITupleReference tuple)
            throws HyracksDataException, IndexException {
        LSMOperationType opType = LSMOperationType.MODIFICATION;
        return modify(ctx, tryOperation, tuple, opType);
    }

    private boolean modify(ILSMIndexOperationContext ctx, boolean tryOperation, ITupleReference tuple,
            LSMOperationType opType) throws HyracksDataException, IndexException {
        if (!getAndEnterComponents(ctx, opType, tryOperation)) {
            return false;
        }
        try {
            lsmIndex.modify(ctx, tuple);
            // The mutable component is always in the first index.
            AbstractMemoryLSMComponent mutableComponent = (AbstractMemoryLSMComponent) ctx.getComponentHolder().get(0);
            mutableComponent.setIsModified();
        } finally {
            exitComponents(ctx, opType, null, false);
        }
        return true;
    }

    @Override
    public void search(ILSMIndexOperationContext ctx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        LSMOperationType opType = LSMOperationType.SEARCH;
        getAndEnterComponents(ctx, opType, false);
        try {
            lsmIndex.search(ctx, cursor, pred);
        } catch (HyracksDataException | IndexException e) {
            exitComponents(ctx, opType, null, true);
            throw e;
        }
    }

    @Override
    public void endSearch(ILSMIndexOperationContext ctx) throws HyracksDataException {
        if (ctx.getOperation() == IndexOperation.SEARCH) {
            try {
                exitComponents(ctx, LSMOperationType.SEARCH, null, false);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        if (!getAndEnterComponents(ctx, LSMOperationType.FLUSH, true)) {
            callback.afterFinalize(LSMOperationType.FLUSH, null);
            return;
        }
        lsmIndex.scheduleFlush(ctx, callback);
    }

    @Override
    public void flush(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException,
            IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Started a flush operation for index: " + lsmIndex + " ...");
        }

        ILSMComponent newComponent = null;
        try {
            newComponent = lsmIndex.flush(operation);
            operation.getCallback().afterOperation(LSMOperationType.FLUSH, null, newComponent);
            lsmIndex.markAsValid(newComponent);
        } finally {
            exitComponents(ctx, LSMOperationType.FLUSH, newComponent, false);
            operation.getCallback().afterFinalize(LSMOperationType.FLUSH, newComponent);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Finished the flush operation for index: " + lsmIndex);
        }
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        if (!getAndEnterComponents(ctx, LSMOperationType.MERGE, true)) {
            callback.afterFinalize(LSMOperationType.MERGE, null);
            return;
        }
        lsmIndex.scheduleMerge(ctx, callback);
    }

    @Override
    public void scheduleFullMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
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
    public void merge(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException,
            IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Started a merge operation for index: " + lsmIndex + " ...");
        }

        ILSMComponent newComponent = null;
        try {
            newComponent = lsmIndex.merge(operation);
            operation.getCallback().afterOperation(LSMOperationType.MERGE, ctx.getComponentHolder(), newComponent);
            lsmIndex.markAsValid(newComponent);
        } finally {
            exitComponents(ctx, LSMOperationType.MERGE, newComponent, false);
            operation.getCallback().afterFinalize(LSMOperationType.MERGE, newComponent);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Finished the merge operation for index: " + lsmIndex);
        }
    }

    @Override
    public void addBulkLoadedComponent(ILSMComponent c) throws HyracksDataException, IndexException {
        lsmIndex.markAsValid(c);
        lsmIndex.addComponent(c);
        mergePolicy.diskComponentAdded(lsmIndex, false);
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return opTracker;
    }
}