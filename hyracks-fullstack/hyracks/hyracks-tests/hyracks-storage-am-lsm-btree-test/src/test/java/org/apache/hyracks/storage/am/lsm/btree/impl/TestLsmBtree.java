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
package org.apache.hyracks.storage.am.lsm.btree.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class TestLsmBtree extends LSMBTree {

    // Semaphores are used to control operations
    private final Semaphore modifySemaphore = new Semaphore(0);
    private final Semaphore searchSemaphore = new Semaphore(0);
    private final Semaphore flushSemaphore = new Semaphore(0);
    private final Semaphore mergeSemaphore = new Semaphore(0);
    private final List<ITestOpCallback<Semaphore>> modifyCallbacks = new ArrayList<>();
    private final List<ITestOpCallback<Semaphore>> searchCallbacks = new ArrayList<>();
    private final List<ITestOpCallback<Semaphore>> flushCallbacks = new ArrayList<>();
    private final List<ITestOpCallback<Semaphore>> mergeCallbacks = new ArrayList<>();

    private final List<ITestOpCallback<ILSMMemoryComponent>> ioAllocateCallbacks = new ArrayList<>();
    private final List<ITestOpCallback<ILSMMemoryComponent>> ioRecycleCallbacks = new ArrayList<>();
    private final List<ITestOpCallback<Void>> ioBeforeCallbacks = new ArrayList<>();
    private final List<ITestOpCallback<Void>> ioAfterOpCallbacks = new ArrayList<>();
    private final List<ITestOpCallback<Void>> ioAfterFinalizeCallbacks = new ArrayList<>();
    private final List<ITestOpCallback<Void>> allocateComponentCallbacks = new ArrayList<>();

    private volatile int numScheduledFlushes;
    private volatile int numStartedFlushes;
    private volatile int numFinishedFlushes;
    private volatile int numScheduledMerges;
    private volatile int numFinishedMerges;
    private volatile int numStartedMerges;

    public TestLsmBtree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOperationCallbackFactory, boolean needKeyDupCheck, int[] btreeFields,
            int[] filterFields, boolean durable, boolean updateAware, ITracer tracer) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, interiorFrameFactory, insertLeafFrameFactory, deleteLeafFrameFactory,
                diskBufferCache, fileManager, componentFactory, bulkLoadComponentFactory, filterHelper,
                filterFrameFactory, filterManager, bloomFilterFalsePositiveRate, fieldCount, cmpFactories, mergePolicy,
                opTracker, ioScheduler, ioOperationCallbackFactory, needKeyDupCheck, btreeFields, filterFields, durable,
                updateAware, tracer);
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        synchronized (modifyCallbacks) {
            for (ITestOpCallback<Semaphore> callback : modifyCallbacks) {
                callback(callback, modifySemaphore);
            }
        }
        acquire(modifySemaphore);
        super.modify(ictx, tuple);
        synchronized (modifyCallbacks) {
            for (ITestOpCallback<Semaphore> callback : modifyCallbacks) {
                callback.after();
            }
        }
    }

    public static <T> void callback(ITestOpCallback<T> callback, T t) {
        if (callback != null) {
            callback.before(t);
        }
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        super.scheduleFlush(ctx, callback);
        numScheduledFlushes++;
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        super.scheduleMerge(ctx, callback);
        numScheduledMerges++;
    }

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        numStartedFlushes++;
        synchronized (flushCallbacks) {
            for (ITestOpCallback<Semaphore> callback : flushCallbacks) {
                callback(callback, flushSemaphore);
            }
        }
        acquire(flushSemaphore);
        ILSMDiskComponent c = super.doFlush(operation);
        numFinishedFlushes++;
        synchronized (flushCallbacks) {
            for (ITestOpCallback<Semaphore> callback : flushCallbacks) {
                callback.after();
            }
        }
        return c;
    }

    @Override
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        numStartedMerges++;
        synchronized (mergeCallbacks) {
            for (ITestOpCallback<Semaphore> callback : mergeCallbacks) {
                callback(callback, mergeSemaphore);
            }
        }
        acquire(mergeSemaphore);
        ILSMDiskComponent c = super.doMerge(operation);
        numFinishedMerges++;
        synchronized (mergeCallbacks) {
            for (ITestOpCallback<Semaphore> callback : mergeCallbacks) {
                callback.after();
            }
        }
        return c;
    }

    private void acquire(Semaphore semaphore) throws HyracksDataException {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
    }

    public void allowModify(int permits) {
        modifySemaphore.release(permits);
    }

    public void allowSearch(int permits) {
        searchSemaphore.release(permits);
    }

    public void allowFlush(int permits) {
        flushSemaphore.release(permits);
    }

    public void allowMerge(int permits) {
        mergeSemaphore.release(permits);
    }

    @Override
    public ILSMIndexAccessor createAccessor(AbstractLSMIndexOperationContext opCtx) {
        return new LSMTreeIndexAccessor(getHarness(), opCtx, ctx -> new TestLsmBtreeSearchCursor(ctx, this));
    }

    public int getNumScheduledFlushes() {
        return numScheduledFlushes;
    }

    public int getNumStartedFlushes() {
        return numStartedFlushes;
    }

    public int getNumScheduledMerges() {
        return numScheduledMerges;
    }

    public int getNumStartedMerges() {
        return numStartedMerges;
    }

    public int getNumFinishedFlushes() {
        return numFinishedFlushes;
    }

    public int getNumFinishedMerges() {
        return numFinishedMerges;
    }

    public List<ITestOpCallback<Semaphore>> getModifyCallbacks() {
        return modifyCallbacks;
    }

    public void addModifyCallback(ITestOpCallback<Semaphore> modifyCallback) {
        synchronized (mergeCallbacks) {
            modifyCallbacks.add(modifyCallback);
        }
    }

    public void clearModifyCallbacks() {
        synchronized (mergeCallbacks) {
            modifyCallbacks.clear();
        }
    }

    public void addIoRecycleCallback(ITestOpCallback<ILSMMemoryComponent> callback) {
        synchronized (ioRecycleCallbacks) {
            ioRecycleCallbacks.add(callback);
        }
    }

    public void clearIoRecycleCallback() {
        synchronized (ioRecycleCallbacks) {
            ioRecycleCallbacks.clear();
        }
    }

    public void addIoAllocateCallback(ITestOpCallback<ILSMMemoryComponent> callback) {
        synchronized (ioAllocateCallbacks) {
            ioAllocateCallbacks.add(callback);
        }
    }

    public void clearIoAllocateCallback() {
        synchronized (ioAllocateCallbacks) {
            ioAllocateCallbacks.clear();
        }
    }

    public List<ITestOpCallback<Semaphore>> getSearchCallbacks() {
        return searchCallbacks;
    }

    public void clearSearchCallbacks() {
        synchronized (searchCallbacks) {
            searchCallbacks.clear();
        }
    }

    public void addSearchCallback(ITestOpCallback<Semaphore> searchCallback) {
        synchronized (searchCallbacks) {
            searchCallbacks.add(searchCallback);
        }
    }

    public void addFlushCallback(ITestOpCallback<Semaphore> flushCallback) {
        synchronized (flushCallbacks) {
            flushCallbacks.add(flushCallback);
        }
    }

    public void clearFlushCallbacks() {
        synchronized (flushCallbacks) {
            flushCallbacks.clear();
        }
    }

    public void addMergeCallback(ITestOpCallback<Semaphore> mergeCallback) {
        synchronized (mergeCallbacks) {
            mergeCallbacks.add(mergeCallback);
        }
    }

    public void clearMergeCallbacks() {
        synchronized (mergeCallbacks) {
            mergeCallbacks.clear();
        }
    }

    public Semaphore getSearchSemaphore() {
        return searchSemaphore;
    }

    public void addAllocateCallback(ITestOpCallback<Void> callback) {
        synchronized (allocateComponentCallbacks) {
            allocateComponentCallbacks.add(callback);
        }
    }

    public void clearAllocateCallbacks() {
        synchronized (allocateComponentCallbacks) {
            allocateComponentCallbacks.clear();
        }
    }

    public void addVirtuablBufferCacheCallback(IVirtualBufferCacheCallback callback) {
        for (IVirtualBufferCache vbc : virtualBufferCaches) {
            ((TestVirtualBufferCache) vbc).addCallback(callback);
        }
    }

    public void clearVirtuablBufferCacheCallbacks() {
        for (IVirtualBufferCache vbc : virtualBufferCaches) {
            ((TestVirtualBufferCache) vbc).clearCallbacks();
        }
    }

    @Override
    public void allocateMemoryComponents() throws HyracksDataException {
        synchronized (allocateComponentCallbacks) {
            for (ITestOpCallback<Void> callback : allocateComponentCallbacks) {
                callback(callback, null);
            }
        }
        super.allocateMemoryComponents();
        synchronized (allocateComponentCallbacks) {
            for (ITestOpCallback<Void> callback : allocateComponentCallbacks) {
                callback.after();
            }
        }
    }

    public void beforeIoOperationCalled() {
        synchronized (ioBeforeCallbacks) {
            for (ITestOpCallback<Void> callback : ioBeforeCallbacks) {
                callback.before(null);
            }
        }
    }

    public void beforeIoOperationReturned() {
        synchronized (ioBeforeCallbacks) {
            for (ITestOpCallback<Void> callback : ioBeforeCallbacks) {
                callback.after();
            }
        }
    }

    public void afterIoOperationCalled() {
        synchronized (ioAfterOpCallbacks) {
            for (ITestOpCallback<Void> callback : ioAfterOpCallbacks) {
                callback.before(null);
            }
        }
    }

    public void afterIoOperationReturned() {
        synchronized (ioAfterOpCallbacks) {
            for (ITestOpCallback<Void> callback : ioAfterOpCallbacks) {
                callback.after();
            }
        }
    }

    public void afterIoFinalizeCalled() {
        synchronized (ioAfterFinalizeCallbacks) {
            for (ITestOpCallback<Void> callback : ioAfterFinalizeCallbacks) {
                callback.before(null);
            }
        }
    }

    public void afterIoFinalizeReturned() {
        synchronized (ioAfterFinalizeCallbacks) {
            for (ITestOpCallback<Void> callback : ioAfterFinalizeCallbacks) {
                callback.after();
            }
        }
    }

    public void recycledCalled(ILSMMemoryComponent component) {
        synchronized (ioRecycleCallbacks) {
            for (ITestOpCallback<ILSMMemoryComponent> callback : ioRecycleCallbacks) {
                callback.before(component);
            }
        }
    }

    public void recycledReturned(ILSMMemoryComponent component) {
        synchronized (ioRecycleCallbacks) {
            for (ITestOpCallback<ILSMMemoryComponent> callback : ioRecycleCallbacks) {
                callback.after();
            }
        }
    }

    public void allocatedCalled(ILSMMemoryComponent component) {
        synchronized (ioAllocateCallbacks) {
            for (ITestOpCallback<ILSMMemoryComponent> callback : ioAllocateCallbacks) {
                callback.before(component);
            }
        }
    }

    public void allocatedReturned(ILSMMemoryComponent component) {
        synchronized (ioAllocateCallbacks) {
            for (ITestOpCallback<ILSMMemoryComponent> callback : ioAllocateCallbacks) {
                callback.after();
            }
        }
    }

}
