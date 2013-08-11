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

    public LSMHarness(ILSMIndexInternal lsmIndex, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker) {
        this.lsmIndex = lsmIndex;
        this.opTracker = opTracker;
        this.mergePolicy = mergePolicy;
    }

    private boolean getAndEnterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType, boolean tryOperation)
            throws HyracksDataException {
        boolean entranceSuccessful = false;

        while (!entranceSuccessful) {
            synchronized (opTracker) {
                lsmIndex.getOperationalComponents(ctx);
                entranceSuccessful = enterComponents(ctx, opType);
                if (tryOperation && !entranceSuccessful) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean enterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType) throws HyracksDataException {
        boolean entranceSuccessful = false;
        int numEntered = 0;
        List<ILSMComponent> components = ctx.getComponentHolder();
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
        opTracker.beforeOperation(lsmIndex, opType, ctx.getSearchOperationCallback(), ctx.getModificationCallback());

        // Check if there is any action that is needed to be taken based on the operation type
        switch (opType) {
            case FLUSH:
                // Changing the flush status should *always* precede changing the mutable component.
                lsmIndex.changeFlushStatusForCurrentMutableCompoent(false);
                lsmIndex.changeMutableComponent();
                break;
            default:
                break;
        }

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
                            mergePolicy.diskComponentAdded(lsmIndex);
                        }
                        break;
                    case MERGE:
                        // newComponent is null if the merge op. was not performed.
                        if (newComponent != null) {
                            lsmIndex.subsumeMergedComponents(newComponent, ctx.getComponentHolder());
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
        LSMOperationType opType = LSMOperationType.FLUSH;
        if (!getAndEnterComponents(ctx, opType, true)) {
            return;
        }
        ILSMComponent flushingComponent = ctx.getComponentHolder().get(0);
        if (!((AbstractMemoryLSMComponent) flushingComponent).isModified()) {
            callback.beforeOperation();
            callback.afterOperation(null, null);
            try {
                exitComponents(ctx, opType, null, false);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            callback.afterFinalize(null);
        } else {
            lsmIndex.scheduleFlush(ctx, callback);
        }
    }

    @Override
    public void flush(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException,
            IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(lsmIndex + ": Flushing ...");
        }

        ILSMComponent newComponent = null;
        try {
            operation.getCallback().beforeOperation();
            newComponent = lsmIndex.flush(operation);
            operation.getCallback().afterOperation(null, newComponent);
            lsmIndex.markAsValid(newComponent);
        } finally {
            exitComponents(ctx, LSMOperationType.FLUSH, newComponent, false);
            operation.getCallback().afterFinalize(newComponent);
        }
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        LSMOperationType opType = LSMOperationType.MERGE;
        if (!getAndEnterComponents(ctx, opType, true)) {
            return;
        }
        if (ctx.getComponentHolder().size() < 2) {
            exitComponents(ctx, opType, null, true);
        } else {
            lsmIndex.scheduleMerge(ctx, callback);
        }
    }

    @Override
    public void merge(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException,
            IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(lsmIndex + ": Merging ...");
        }

        ILSMComponent newComponent = null;
        try {
            operation.getCallback().beforeOperation();
            newComponent = lsmIndex.merge(operation);
            operation.getCallback().afterOperation(ctx.getComponentHolder(), newComponent);
            lsmIndex.markAsValid(newComponent);
        } finally {
            exitComponents(ctx, LSMOperationType.MERGE, newComponent, false);
            operation.getCallback().afterFinalize(newComponent);
        }
    }

    @Override
    public void addBulkLoadedComponent(ILSMComponent c) throws HyracksDataException, IndexException {
        lsmIndex.markAsValid(c);
        lsmIndex.addComponent(c);
        mergePolicy.diskComponentAdded(lsmIndex);
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return opTracker;
    }
}