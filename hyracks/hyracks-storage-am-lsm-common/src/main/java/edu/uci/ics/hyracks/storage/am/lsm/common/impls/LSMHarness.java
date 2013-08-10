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

import java.util.ArrayList;
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

    private void threadExit(ILSMIndexOperationContext opCtx, LSMOperationType opType) throws HyracksDataException {
        opTracker.afterOperation(lsmIndex, opType, opCtx.getSearchOperationCallback(), opCtx.getModificationCallback());
    }

    private boolean getAndEnterComponents(ILSMIndexOperationContext ctx, LSMOperationType opType, boolean tryOperation)
            throws HyracksDataException {
        boolean entranceSuccessful = false;

        while (!entranceSuccessful) {
            synchronized (opTracker) {
                int numEntered = 0;
                lsmIndex.getOperationalComponents(ctx);
                List<ILSMComponent> components = ctx.getComponentHolder();
                try {
                    for (ILSMComponent c : components) {
                        if (!c.threadEnter(opType, numEntered == 0 ? true : false)) {
                            break;
                        }
                        numEntered++;
                    }
                    entranceSuccessful = numEntered == components.size();
                    if (entranceSuccessful) {
                        opTracker.beforeOperation(lsmIndex, opType, ctx.getSearchOperationCallback(),
                                ctx.getModificationCallback());
                    }
                } finally {
                    if (!entranceSuccessful) {
                        int i = 0;
                        for (ILSMComponent c : components) {
                            if (numEntered == 0) {
                                break;
                            }
                            c.threadExit(opType, true, i == 0 ? true : false);
                            i++;
                            numEntered--;
                        }
                    }
                }
                if (tryOperation && !entranceSuccessful) {
                    return false;
                }
            }
        }
        return true;
    }

    private void exitComponents(ILSMIndexOperationContext ctx, LSMOperationType opType, boolean failedOperation)
            throws HyracksDataException {
        synchronized (opTracker) {
            try {
                int i = 0;
                for (ILSMComponent c : ctx.getComponentHolder()) {
                    c.threadExit(opType, failedOperation, i == 0 ? true : false);
                    i++;
                }
            } finally {
                threadExit(ctx, opType);
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
        } finally {
            exitComponents(ctx, opType, false);
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
            exitComponents(ctx, opType, true);
            throw e;
        }
    }

    @Override
    public void endSearch(ILSMIndexOperationContext ctx) throws HyracksDataException {
        if (ctx.getOperation() == IndexOperation.SEARCH) {
            exitComponents(ctx, LSMOperationType.SEARCH, false);
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
        if (!((AbstractMutableLSMComponent) flushingComponent).isModified()) {
            callback.beforeOperation();
            callback.afterOperation(null, null);
            exitComponents(ctx, opType, false);
            callback.afterFinalize(null);
        } else {
            lsmIndex.scheduleFlush(ctx, callback);
        }
    }

    @Override
    public void flush(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException,
            IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(lsmIndex + ": flushing");
        }

        operation.getCallback().beforeOperation();
        ILSMComponent newComponent = lsmIndex.flush(operation);
        operation.getCallback().afterOperation(null, newComponent);
        lsmIndex.markAsValid(newComponent);

        synchronized (opTracker) {
            lsmIndex.addComponent(newComponent);
            mergePolicy.diskComponentAdded(lsmIndex);
            exitComponents(ctx, LSMOperationType.FLUSH, false);
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
            exitComponents(ctx, opType, true);
        } else {
            lsmIndex.scheduleMerge(ctx, callback);
        }
    }

    @Override
    public void merge(ILSMIndexOperationContext ctx, ILSMIOOperation operation) throws HyracksDataException,
            IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(lsmIndex + ": merging");
        }

        List<ILSMComponent> mergedComponents = new ArrayList<ILSMComponent>();
        operation.getCallback().beforeOperation();
        ILSMComponent newComponent = lsmIndex.merge(mergedComponents, operation);
        operation.getCallback().afterOperation(mergedComponents, newComponent);
        lsmIndex.markAsValid(newComponent);
        lsmIndex.subsumeMergedComponents(newComponent, mergedComponents);
        exitComponents(ctx, LSMOperationType.MERGE, false);
        operation.getCallback().afterFinalize(newComponent);
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