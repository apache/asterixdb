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
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushPolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;

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
public class LSMHarness {
    protected final Logger LOGGER = Logger.getLogger(LSMHarness.class.getName());
    protected static final long AFTER_MERGE_CLEANUP_SLEEP = 100;

    private ILSMIndex lsmIndex;

    // All accesses to the LSM-Tree's on-disk components are synchronized on diskComponentsSync.
    private Object diskComponentsSync = new Object();

    // For synchronizing all operations with flushes.
    // Currently, all operations block during a flush.
    private int threadRefCount;
    private boolean flushFlag;

    // For synchronizing searchers with a concurrent merge.
    private AtomicBoolean isMerging = new AtomicBoolean(false);
    private AtomicInteger searcherRefCountA = new AtomicInteger(0);
    private AtomicInteger searcherRefCountB = new AtomicInteger(0);

    // Represents the current number of searcher threads that are operating on
    // the unmerged on-disk Trees.
    // We alternate between searcherRefCountA and searcherRefCountB.
    private AtomicInteger searcherRefCount = searcherRefCountA;

    // Flush and Merge Policies
    private final ILSMFlushPolicy flushPolicy;

    public LSMHarness(ILSMIndex lsmIndex, ILSMFlushPolicy flushPolicy) {
        this.lsmIndex = lsmIndex;
        this.threadRefCount = 0;
        this.flushPolicy = flushPolicy;
        this.flushFlag = false;
    }

    public void threadEnter() {
        threadRefCount++;
    }

    public void threadExit() throws HyracksDataException, IndexException {
        synchronized (this) {
            threadRefCount--;

            // Check if we've reached or exceeded the maximum number of pages.
            if (!flushFlag && lsmIndex.getInMemoryFreePageManager().isFull()) {
                flushFlag = true;
            }

            // Flush will only be handled by last exiting thread.
            if (flushFlag && threadRefCount == 0) {
                flushPolicy.shouldFlush(lsmIndex);
            }
        }
    }

    public void insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ctx) throws HyracksDataException,
            IndexException {
        boolean waitForFlush = true;
        do {
            synchronized (this) {
                // flushFlag may be set to true even though the flush has not occurred yet.
                // If flushFlag is set, then the flush is queued to occur by the last exiting thread.
                // This operation should wait for that flush to occur before proceeding.
                if (!flushFlag) {
                    // Increment the threadRefCount in order to block the possibility of a concurrent flush.
                    // The corresponding threadExit() call is in LSMTreeRangeSearchCursor.close()
                    threadEnter();

                    // A flush is not pending, so proceed with the operation.
                    waitForFlush = false;
                }
            }
        } while (waitForFlush);

        // It is possible, due to concurrent execution of operations, that an operation will 
        // fail. In such a case, simply retry the operation. Refer to the specific LSMIndex code 
        // to see exactly why an operation might fail.
        boolean operationComplete = true;
        try {
            do {
                operationComplete = lsmIndex.insertUpdateOrDelete(tuple, ctx);
            } while (!operationComplete);
        } finally {
            threadExit();
        }
    }

    public void flush() throws HyracksDataException, IndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Flushing LSM-Tree.");
        }
        Object newComponent = lsmIndex.flush();

        // The implementation of this call must take any necessary steps to make
        // the new component permanent, and mark it as valid (usually this means
        // forcing all pages of the tree to disk, possibly with some extra
        // information to mark the tree as valid).
        lsmIndex.getComponentFinalizer().finalize(newComponent);

        lsmIndex.resetInMemoryComponent();
        synchronized (diskComponentsSync) {
            lsmIndex.addFlushedComponent(newComponent);
        }

        // Unblock entering threads waiting for the flush
        flushFlag = false;
    }

    public List<Object> search(IIndexCursor cursor, ISearchPredicate pred, IIndexOpContext ctx,
            boolean includeMemComponent) throws HyracksDataException, IndexException {
        // If the search doesn't include the in-memory component, then we don't have
        // to synchronize with a flush.
        if (includeMemComponent) {
            boolean waitForFlush = true;
            do {
                synchronized (this) {
                    // flushFlag may be set to true even though the flush has not occurred yet.
                    // If flushFlag is set, then the flush is queued to occur by the last exiting thread.
                    // This operation should wait for that flush to occur before proceeding.
                    if (!flushFlag) {
                        // Increment the threadRefCount in order to block the possibility of a concurrent flush.
                        // The corresponding threadExit() call is in LSMTreeRangeSearchCursor.close()
                        threadEnter();

                        // A flush is not pending, so proceed with the operation.
                        waitForFlush = false;
                    }
                }
            } while (waitForFlush);
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

    public void merge() throws HyracksDataException, IndexException {
        if (!isMerging.compareAndSet(false, true)) {
            throw new LSMMergeInProgressException(
                    "Merge already in progress. Only one merge process allowed at a time.");
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Merging LSM-Tree.");
        }

        // Point to the current searcher ref count, so we can wait for it later
        // (after we swap the searcher ref count).
        AtomicInteger localSearcherRefCount = searcherRefCount;

        List<Object> mergedComponents = new ArrayList<Object>();
        Object newComponent = lsmIndex.merge(mergedComponents);

        // No merge happened.
        if (newComponent == null) {
            isMerging.set(false);
            return;
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

        // The implementation of this call must take any necessary steps to make
        // the new component permanent, and mark it as valid (usually this means
        // forcing all pages of the tree to disk, possibly with some extra
        // information to mark the tree as valid).
        lsmIndex.getComponentFinalizer().finalize(newComponent);

        // Cleanup. At this point we have guaranteed that no searchers are
        // touching the old on-disk Trees (localSearcherRefCount == 0).
        lsmIndex.cleanUpAfterMerge(mergedComponents);
        isMerging.set(false);
    }

    public void closeSearchCursor(AtomicInteger searcherRefCount, boolean includeMemComponent)
            throws HyracksDataException {
        // If the in-memory Tree was not included in the search, then we don't
        // need to synchronize with a flush.
        if (includeMemComponent) {
            try {
                threadExit();
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }
        // A merge may be waiting on this searcher to finish searching the on-disk components.
        // Decrement the searcherRefCount so that the merge process is able to cleanup any old
        // on-disk components.
        searcherRefCount.decrementAndGet();
    }

    public void addBulkLoadedComponent(Object index) throws HyracksDataException {
        // The implementation of this call must take any necessary steps to make
        // the new component permanent, and mark it as valid (usually this means
        // forcing all pages of the tree to disk, possibly with some extra
        // information to mark the tree as valid).
        lsmIndex.getComponentFinalizer().finalize(index);
        synchronized (diskComponentsSync) {
            lsmIndex.addFlushedComponent(index);
        }
    }
}
