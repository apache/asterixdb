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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
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
    protected static final long AFTER_MERGE_CLEANUP_SLEEP = 100;

    private ILSMIndexInternal lsmIndex;

    // All accesses to the LSM-Tree's on-disk components are synchronized on diskComponentsSync.
    private Object diskComponentsSync = new Object();

    // For synchronizing searchers with a concurrent merge.
    private AtomicBoolean isMerging = new AtomicBoolean(false);
    private AtomicInteger searcherRefCountA = new AtomicInteger(0);
    private AtomicInteger searcherRefCountB = new AtomicInteger(0);

    // Represents the current number of searcher threads that are operating on
    // the unmerged on-disk Trees.
    // We alternate between searcherRefCountA and searcherRefCountB.
    private AtomicInteger searcherRefCount = searcherRefCountA;

    // Flush and Merge Policies
    private final ILSMFlushController flushController;
    private final ILSMMergePolicy mergePolicy;
    private final ILSMOperationTracker opTracker;
    private final ILSMIOOperationScheduler ioScheduler;

    public LSMHarness(ILSMIndexInternal lsmIndex, ILSMFlushController flushController, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler) {
        this.lsmIndex = lsmIndex;
        this.opTracker = opTracker;
        this.flushController = flushController;
        this.mergePolicy = mergePolicy;
        this.ioScheduler = ioScheduler;
    }

    private void threadExit(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        if (!lsmIndex.getFlushController().getFlushStatus(lsmIndex) && lsmIndex.getInMemoryFreePageManager().isFull()) {
            lsmIndex.getFlushController().setFlushStatus(lsmIndex, true);
        }
        opTracker.afterOperation(opCtx);
    }

    public void insertUpdateOrDelete(ITupleReference tuple, ILSMIndexOperationContext ctx) throws HyracksDataException,
            IndexException {
        opTracker.beforeOperation(ctx);
        // It is possible, due to concurrent execution of operations, that an operation will 
        // fail. In such a case, simply retry the operation. Refer to the specific LSMIndex code 
        // to see exactly why an operation might fail.
        try {
            lsmIndex.insertUpdateOrDelete(tuple, ctx);
        } finally {
            threadExit(ctx);
        }
    }

    public void flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Flushing LSM-Index: " + lsmIndex);
        }
        Object newComponent = null;
        try {
            operation.getCallback().beforeOperation(operation);
            newComponent = lsmIndex.flush(operation);
            operation.getCallback().afterOperation(operation, newComponent);
            
            // The implementation of this call must take any necessary steps to make
            // the new component permanent, and mark it as valid (usually this means
            // forcing all pages of the tree to disk, possibly with some extra
            // information to mark the tree as valid).
            lsmIndex.getComponentFinalizer().finalize(newComponent);
        } finally {
            operation.getCallback().afterFinalize(operation, newComponent);
        }
        lsmIndex.resetInMemoryComponent();
        synchronized (diskComponentsSync) {
            lsmIndex.addFlushedComponent(newComponent);
            mergePolicy.diskComponentAdded(lsmIndex, lsmIndex.getDiskComponents().size());
        }

        // Unblock entering threads waiting for the flush
        flushController.setFlushStatus(lsmIndex, false);
    }

    public List<Object> search(IIndexCursor cursor, ISearchPredicate pred, ILSMIndexOperationContext ctx,
            boolean includeMemComponent) throws HyracksDataException, IndexException {
        // If the search doesn't include the in-memory component, then we don't have
        // to synchronize with a flush.
        if (includeMemComponent) {
            opTracker.beforeOperation(ctx);
        }

        // Get a snapshot of the current on-disk Trees.
        // If includeMemComponent is true, then no concurrent
        // flush can add another on-disk Tree (due to threadEnter());
        // If includeMemComponent is false, then it is possible that a concurrent
        // flush adds another on-disk Tree.
        // Since this mode is only used for merging trees, it doesn't really
        // matter if the merge excludes the new on-disk Tree.
        List<Object> diskComponentSnapshot = new ArrayList<Object>();
        AtomicInteger localSearcherRefCount = null;
        synchronized (diskComponentsSync) {
            diskComponentSnapshot.addAll(lsmIndex.getDiskComponents());
            localSearcherRefCount = searcherRefCount;
            localSearcherRefCount.incrementAndGet();
        }

        lsmIndex.search(cursor, diskComponentSnapshot, pred, ctx, includeMemComponent, localSearcherRefCount);
        return diskComponentSnapshot;
    }

    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException,
            IndexException {
        if (!isMerging.compareAndSet(false, true)) {
            throw new LSMMergeInProgressException(
                    "Merge already in progress. Only one merge process allowed at a time.");
        }

        ILSMIOOperation mergeOp = lsmIndex.createMergeOperation(callback);
        if (mergeOp == null) {
            isMerging.set(false);
        }
        return mergeOp;
    }

    public void merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Merging LSM-Index: " + lsmIndex);
        }

        // Point to the current searcher ref count, so we can wait for it later
        // (after we swap the searcher ref count).
        AtomicInteger localSearcherRefCount = searcherRefCount;

        List<Object> mergedComponents = new ArrayList<Object>();
        Object newComponent = null;
        try {
            operation.getCallback().beforeOperation(operation);
            newComponent = lsmIndex.merge(mergedComponents, operation);
            operation.getCallback().afterOperation(operation, newComponent);
            
            // No merge happened.
            if (newComponent == null) {
                isMerging.set(false);
                return;
            }

            // TODO: Move this to just before the merge cleanup and remove latching on disk components
            // The implementation of this call must take any necessary steps to make
            // the new component permanent, and mark it as valid (usually this means
            // forcing all pages of the tree to disk, possibly with some extra
            // information to mark the tree as valid).
            lsmIndex.getComponentFinalizer().finalize(newComponent);
        } finally {
            operation.getCallback().afterFinalize(operation, newComponent);
        }
        
        // Remove the old Trees from the list, and add the new merged Tree(s).
        // Also, swap the searchRefCount.
        synchronized (diskComponentsSync) {
            lsmIndex.addMergedComponent(newComponent, mergedComponents);
            // Swap the searcher ref count reference, and reset it to zero.    
            if (searcherRefCount == searcherRefCountA) {
                searcherRefCount = searcherRefCountB;
            } else {
                searcherRefCount = searcherRefCountA;
            }
            searcherRefCount.set(0);
        }
        

        // Wait for all searchers that are still accessing the old on-disk
        // Trees, then perform the final cleanup of the old Trees.
        while (localSearcherRefCount.get() > 0) {
            try {
                Thread.sleep(AFTER_MERGE_CLEANUP_SLEEP);
            } catch (InterruptedException e) {
                // Propagate the exception to the caller, so that an appropriate
                // cleanup action can be taken.
                throw new HyracksDataException(e);
            }
        }

        // Cleanup. At this point we have guaranteed that no searchers are
        // touching the old on-disk Trees (localSearcherRefCount == 0).
        lsmIndex.cleanUpAfterMerge(mergedComponents);
        isMerging.set(false);
    }

    @Override
    public void closeSearchCursor(AtomicInteger searcherRefCount, boolean includeMemComponent,
            ILSMIndexOperationContext ctx) throws HyracksDataException {
        // If the in-memory Tree was not included in the search, then we don't
        // need to synchronize with a flush.
        if (includeMemComponent) {
            threadExit(ctx);
        }
        // A merge may be waiting on this searcher to finish searching the on-disk components.
        // Decrement the searcherRefCount so that the merge process is able to cleanup any old
        // on-disk components.
        searcherRefCount.decrementAndGet();
    }

    @Override
    public void addBulkLoadedComponent(Object index) throws HyracksDataException, IndexException {
        // The implementation of this call must take any necessary steps to make
        // the new component permanent, and mark it as valid (usually this means
        // forcing all pages of the tree to disk, possibly with some extra
        // information to mark the tree as valid).
        lsmIndex.getComponentFinalizer().finalize(index);
        synchronized (diskComponentsSync) {
            lsmIndex.addFlushedComponent(index);
            mergePolicy.diskComponentAdded(lsmIndex, lsmIndex.getDiskComponents().size());
        }
    }

    public ILSMFlushController getFlushController() {
        return flushController;
    }

    public ILSMOperationTracker getOperationTracker() {
        return opTracker;
    }

    public ILSMIOOperationScheduler getIOScheduler() {
        return ioScheduler;
    }

    public ILSMIndex getIndex() {
        return lsmIndex;
    }

}
