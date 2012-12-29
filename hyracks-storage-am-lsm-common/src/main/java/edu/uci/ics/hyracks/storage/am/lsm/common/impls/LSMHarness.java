/*
 * Copyright 2009-2012 by The Regents of the University of California
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
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;

/**
 * Common code for synchronizing LSM operations like
 * updates/searches/flushes/merges on any {@link ILSMIndex}. This class only deals with
 * synchronizing LSM operations, and delegates the concrete implementations of
 * actual operations to {@link ILSMIndex} (passed in the constructor).
 * Concurrency behavior:
 * All operations except merge (insert/update/delete/search) are blocked during a flush.
 * During a merge, all operations (except another merge) can proceed concurrently.
 * A merge and a flush can proceed concurrently.
 */
public class LSMHarness implements ILSMHarness {
    protected final Logger LOGGER = Logger.getLogger(LSMHarness.class.getName());

    private ILSMIndexInternal lsmIndex;

    // All accesses to the LSM-Tree's on-disk components are synchronized on diskComponentsSync.
    private Object diskComponentsSync = new Object();

    private final ILSMMergePolicy mergePolicy;
    private final ILSMOperationTracker opTracker;
    private final ILSMIOOperationScheduler ioScheduler;

    public LSMHarness(ILSMIndexInternal lsmIndex, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler) {
        this.lsmIndex = lsmIndex;
        this.opTracker = opTracker;
        this.mergePolicy = mergePolicy;
        this.ioScheduler = ioScheduler;
    }

    private void threadExit(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        if (!lsmIndex.getFlushStatus(lsmIndex) && lsmIndex.getInMemoryFreePageManager().isFull()) {
            lsmIndex.setFlushStatus(lsmIndex, true);
        }
        opTracker.afterOperation(opCtx.getSearchOperationCallback(), opCtx.getModificationCallback());
    }

    @Override
    public boolean insertUpdateOrDelete(ITupleReference tuple, ILSMIndexOperationContext ctx, boolean tryOperation)
            throws HyracksDataException, IndexException {
        if (!opTracker.beforeOperation(ctx.getSearchOperationCallback(), ctx.getModificationCallback(), tryOperation)) {
            return false;
        }
        try {
            lsmIndex.insertUpdateOrDelete(tuple, ctx);
        } finally {
            threadExit(ctx);
        }

        return true;
    }

    @Override
    public boolean noOp(ILSMIndexOperationContext ctx, boolean tryOperation) throws HyracksDataException {
        if (!opTracker.beforeOperation(ctx.getSearchOperationCallback(), ctx.getModificationCallback(), tryOperation)) {
            return false;
        }
        threadExit(ctx);
        return true;
    }

    @Override
    public void flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Flushing LSM-Index: " + lsmIndex);
        }
        ILSMComponent newComponent = null;
        try {
            operation.getCallback().beforeOperation(operation);
            newComponent = lsmIndex.flush(operation);
            operation.getCallback().afterOperation(operation, null, newComponent);

            // The implementation of this call must take any necessary steps to make
            // the new component permanent, and mark it as valid (usually this means
            // forcing all pages of the tree to disk, possibly with some extra
            // information to mark the tree as valid).
            lsmIndex.markAsValid(newComponent);
        } finally {
            operation.getCallback().afterFinalize(operation, newComponent);
        }
        lsmIndex.resetMutableComponent();
        synchronized (diskComponentsSync) {
            lsmIndex.addComponent(newComponent);
            mergePolicy.diskComponentAdded(lsmIndex, lsmIndex.getImmutableComponents().size());
        }

        // Unblock entering threads waiting for the flush
        lsmIndex.setFlushStatus(lsmIndex, false);
    }

    @Override
    public List<ILSMComponent> search(IIndexCursor cursor, ISearchPredicate pred, ILSMIndexOperationContext ctx,
            boolean includeMutableComponent) throws HyracksDataException, IndexException {
        // If the search doesn't include the in-memory component, then we don't have
        // to synchronize with a flush.
        if (includeMutableComponent) {
            opTracker.beforeOperation(ctx.getSearchOperationCallback(), ctx.getModificationCallback(), false);
        }

        // Get a snapshot of the current on-disk Trees.
        // If includeMutableComponent is true, then no concurrent
        // flush can add another on-disk Tree (due to threadEnter());
        // If includeMutableComponent is false, then it is possible that a concurrent
        // flush adds another on-disk Tree.
        // Since this mode is only used for merging trees, it doesn't really
        // matter if the merge excludes the new on-disk Tree.
        List<ILSMComponent> operationalComponents;
        synchronized (diskComponentsSync) {
            operationalComponents = lsmIndex.getOperationalComponents(ctx);
        }
        for (ILSMComponent c : operationalComponents) {
            c.threadEnter();
        }

        lsmIndex.search(cursor, operationalComponents, pred, ctx, includeMutableComponent);
        return operationalComponents;
    }

    @Override
    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException,
            IndexException {
        ILSMIOOperation mergeOp = lsmIndex.createMergeOperation(callback);
        return mergeOp;
    }

    @Override
    public void merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Merging LSM-Index: " + lsmIndex);
        }

        List<ILSMComponent> mergedComponents = new ArrayList<ILSMComponent>();
        ILSMComponent newComponent = null;
        try {
            operation.getCallback().beforeOperation(operation);
            newComponent = lsmIndex.merge(mergedComponents, operation);
            operation.getCallback().afterOperation(operation, mergedComponents, newComponent);

            // No merge happened.
            if (newComponent == null) {
                return;
            }

            // TODO: Move this to just before the merge cleanup and remove latching on disk components
            // The implementation of this call must take any necessary steps to make
            // the new component permanent, and mark it as valid (usually this means
            // forcing all pages of the tree to disk, possibly with some extra
            // information to mark the tree as valid).
            lsmIndex.markAsValid(newComponent);
        } finally {
            operation.getCallback().afterFinalize(operation, newComponent);
        }

        // Remove the old components from the list, and add the new merged component(s).
        try {
            synchronized (diskComponentsSync) {
                lsmIndex.subsumeMergedComponents(newComponent, mergedComponents);
            }
        } finally {
            // Cleanup merged components in case there are no more searchers accessing them.
            for (ILSMComponent c : mergedComponents) {
                c.setState(LSMComponentState.DONE_MERGING);
                if (c.getThreadReferenceCount() == 0) {
                    c.destroy();
                }
            }
        }
    }

    @Override
    public void closeSearchCursor(List<ILSMComponent> operationalComponents, boolean includeMutableComponent,
            ILSMIndexOperationContext ctx) throws HyracksDataException {
        // TODO: we should not worry about the mutable component.
        if (includeMutableComponent) {
            threadExit(ctx);
        }
        try {
            for (ILSMComponent c : operationalComponents) {
                c.threadExit();
            }
        } finally {
            for (ILSMComponent c : operationalComponents) {
                if (c.getState() == LSMComponentState.DONE_MERGING && c.getThreadReferenceCount() == 0) {
                    c.destroy();
                }
            }
        }
    }

    @Override
    public void addBulkLoadedComponent(ILSMComponent index) throws HyracksDataException, IndexException {
        // The implementation of this call must take any necessary steps to make
        // the new component permanent, and mark it as valid (usually this means
        // forcing all pages of the tree to disk, possibly with some extra
        // information to mark the tree as valid).
        lsmIndex.markAsValid(index);
        synchronized (diskComponentsSync) {
            lsmIndex.addComponent(index);
            mergePolicy.diskComponentAdded(lsmIndex, lsmIndex.getImmutableComponents().size());
        }
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return opTracker;
    }

    @Override
    public ILSMIOOperationScheduler getIOScheduler() {
        return ioScheduler;
    }

    @Override
    public ILSMIndex getIndex() {
        return lsmIndex;
    }
}
