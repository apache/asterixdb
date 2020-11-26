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
package org.apache.asterix.common.context;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.api.replication.IIOReplicationManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;
import org.apache.hyracks.storage.common.buffercache.VirtualPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapManager;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class GlobalVirtualBufferCache implements IVirtualBufferCache, ILifeCycleComponent {
    private static final Logger LOGGER = LogManager.getLogger();

    // keep track of the memory usage of each filtered memory component
    private final Map<ILSMMemoryComponent, AtomicInteger> memoryComponentUsageMap =
            Collections.synchronizedMap(new HashMap<>());
    private final Map<FileReference, AtomicInteger> fileRefUsageMap = Collections.synchronizedMap(new HashMap<>());
    private final Int2ObjectMap<AtomicInteger> fileIdUsageMap =
            Int2ObjectMaps.synchronize(new Int2ObjectOpenHashMap<>());

    private final int maxConcurrentFlushes;
    private final List<ILSMIndex> primaryIndexes = new ArrayList<>();

    private final Set<ILSMIndex> flushingIndexes = Collections.synchronizedSet(new HashSet<>());
    private volatile int flushPtr;

    private final int filteredMemoryComponentMaxNumPages;
    private final int flushPageBudget;
    private final VirtualBufferCache vbc;
    private final AtomicBoolean isOpen = new AtomicBoolean(false);
    private final FlushThread flushThread = new FlushThread();

    public GlobalVirtualBufferCache(ICacheMemoryAllocator allocator, StorageProperties storageProperties,
            int maxConcurrentFlushes) {
        this.vbc = new VirtualBufferCache(allocator, storageProperties.getBufferCachePageSize(),
                (int) (storageProperties.getMemoryComponentGlobalBudget()
                        / storageProperties.getMemoryComponentPageSize()));
        this.flushPageBudget = (int) (storageProperties.getMemoryComponentGlobalBudget()
                / storageProperties.getMemoryComponentPageSize()
                * storageProperties.getMemoryComponentFlushThreshold());
        this.filteredMemoryComponentMaxNumPages = storageProperties.getFilteredMemoryComponentMaxNumPages();
        this.maxConcurrentFlushes = maxConcurrentFlushes;
    }

    @Override
    public int getPageSize() {
        return vbc.getPageSize();
    }

    @Override
    public int getPageSizeWithHeader() {
        return vbc.getPageSizeWithHeader();
    }

    @Override
    public void register(ILSMMemoryComponent memoryComponent) {
        ILSMIndex index = memoryComponent.getLsmIndex();
        if (index.isPrimaryIndex()) {
            synchronized (this) {
                if (!primaryIndexes.contains(index)) {
                    // make sure only add index once
                    primaryIndexes.add(index);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Registered {} index {} to the global VBC",
                                isMetadataIndex(index) ? "metadata" : "primary", index.toString());
                    }
                }
                if (index.getNumOfFilterFields() > 0) {
                    // handle filtered primary index
                    AtomicInteger usage = new AtomicInteger();
                    memoryComponentUsageMap.put(memoryComponent, usage);
                    for (FileReference ref : memoryComponent.getComponentFileRefs().getFileReferences()) {
                        if (ref != null) {
                            fileRefUsageMap.put(ref, usage);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void unregister(ILSMMemoryComponent memoryComponent) {
        ILSMIndex index = memoryComponent.getLsmIndex();
        if (index.isPrimaryIndex()) {
            synchronized (this) {
                int pos = primaryIndexes.indexOf(index);
                if (pos >= 0) {
                    primaryIndexes.remove(index);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Unregistered {} index {} to the global VBC",
                                isMetadataIndex(index) ? "metadata" : "primary", index.toString());
                    }
                    if (primaryIndexes.isEmpty()) {
                        flushPtr = 0;
                    } else if (flushPtr > pos) {
                        // If the removed index is before flushPtr, we should decrement flushPtr by 1 so that
                        // it still points to the same index.
                        flushPtr = (flushPtr - 1) % primaryIndexes.size();
                    }
                }
                if (index.getNumOfFilterFields() > 0) {
                    memoryComponentUsageMap.remove(memoryComponent);
                    for (FileReference ref : memoryComponent.getComponentFileRefs().getFileReferences()) {
                        if (ref != null) {
                            fileRefUsageMap.remove(ref);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void flushed(ILSMMemoryComponent memoryComponent) throws HyracksDataException {
        if (flushingIndexes.remove(memoryComponent.getLsmIndex())) {
            LOGGER.info("Completed flushing {}.", memoryComponent.getIndex());
            // After the flush operation is completed, we may have 2 cases:
            // 1. there is no active reader on this memory component and memory is reclaimed;
            // 2. there are still some active readers and memory cannot be reclaimed.
            // But for both cases, we will notify all primary index op trackers to let their writers retry,
            // if they have been blocked. Moreover, we will check whether more flushes are needed.
            synchronized (this) {
                final int size = primaryIndexes.size();
                for (int i = 0; i < size; i++) {
                    ILSMOperationTracker opTracker = primaryIndexes.get(i).getOperationTracker();
                    synchronized (opTracker) {
                        opTracker.notifyAll();
                    }
                }
            }
            checkAndNotifyFlushThread();
        }
        if (memoryComponent.getLsmIndex().getNumOfFilterFields() > 0
                && memoryComponent.getLsmIndex().isPrimaryIndex()) {
            AtomicInteger usage = memoryComponentUsageMap.get(memoryComponent);
            if (usage != null) {
                // reset usage to 0 after the memory component is flushed
                usage.set(0);
            }
        }
    }

    @Override
    public int getPageBudget() {
        return vbc.getPageBudget();
    }

    @Override
    public boolean isFull() {
        return vbc.isFull();
    }

    @Override
    public boolean isFull(ILSMMemoryComponent memoryComponent) {
        return flushingIndexes.contains(memoryComponent.getLsmIndex())
                || isFilteredMemoryComponentFull(memoryComponent);
    }

    private boolean isFilteredMemoryComponentFull(ILSMMemoryComponent memoryComponent) {
        if (filteredMemoryComponentMaxNumPages <= 0 || memoryComponent.getLsmIndex().getNumOfFilterFields() == 0
                || !memoryComponent.getLsmIndex().isPrimaryIndex()) {
            return false;
        }
        AtomicInteger usage = memoryComponentUsageMap.get(memoryComponent);
        return usage.get() >= filteredMemoryComponentMaxNumPages;
    }

    @Override
    public int createFile(FileReference fileRef) throws HyracksDataException {
        int fileId = vbc.createFile(fileRef);
        updateFileIdUsageMap(fileRef, fileId);
        return fileId;
    }

    @Override
    public int openFile(FileReference fileRef) throws HyracksDataException {
        int fileId = vbc.openFile(fileRef);
        updateFileIdUsageMap(fileRef, fileId);
        return fileId;
    }

    private void updateFileIdUsageMap(FileReference fileRef, int fileId) {
        AtomicInteger usage = fileRefUsageMap.get(fileRef);
        if (usage != null) {
            fileIdUsageMap.put(fileId, usage);
        }
    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
        vbc.openFile(fileId);
    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
        vbc.closeFile(fileId);
    }

    @Override
    public void deleteFile(FileReference fileRef) throws HyracksDataException {
        vbc.deleteFile(fileRef);
    }

    @Override
    public void deleteFile(int fileId) throws HyracksDataException {
        vbc.deleteFile(fileId);
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException {
        ICachedPage page = vbc.pin(dpid, newPage);
        if (newPage) {
            incrementFilteredMemoryComponentUsage(dpid, 1);
            checkAndNotifyFlushThread();
        }
        return page;
    }

    private void incrementFilteredMemoryComponentUsage(long dpid, int pages) {
        if (filteredMemoryComponentMaxNumPages > 0) {
            // update memory usage of filtered index
            AtomicInteger usage = fileIdUsageMap.get(BufferedFileHandle.getFileId(dpid));
            if (usage != null) {
                usage.addAndGet(pages);
                // We do not need extra code to flush this filtered memory component when it becomes full.
                // This method is only called when there are active writers on this memory component.
                // When the writer exits, it'll automatically flush this memory component when it finds out
                // that this memory component becomes full.
            }
        }
    }

    private void checkAndNotifyFlushThread() {
        if (vbc.getUsage() < flushPageBudget) {
            return;
        }
        // Notify the flush thread to schedule flushes. This is used to avoid deadlocks because page pins can be
        // called while synchronizing on op trackers.
        synchronized (flushThread.flushLock) {
            flushThread.flushLock.notifyAll();
        }
    }

    @Override
    public void resizePage(ICachedPage cPage, int multiplier, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException {
        vbc.resizePage(cPage, multiplier, extraPageBlockHelper);
        int delta = multiplier - cPage.getFrameSizeMultiplier();
        incrementFilteredMemoryComponentUsage(((VirtualPage) cPage).dpid(), delta);
        if (delta > 0) {
            checkAndNotifyFlushThread();
        }
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        vbc.unpin(page);
    }

    @Override
    public void flush(ICachedPage page) throws HyracksDataException {
        vbc.flush(page);
    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
        vbc.force(fileId, metadata);
    }

    @Override
    public void open() throws HyracksDataException {

    }

    @Override
    public void close() throws HyracksDataException {
        // no op
    }

    @Override
    public void start() {
        if (isOpen.compareAndSet(false, true)) {
            try {
                vbc.open();
            } catch (HyracksDataException e) {
                throw new IllegalStateException("Fail to open virtual buffer cache ", e);
            }
            flushThread.start();
        }
    }

    @Override
    public void stop(boolean dumpState, OutputStream ouputStream) throws IOException {
        if (isOpen.compareAndSet(true, false)) {
            if (dumpState) {
                dumpState(ouputStream);
            }
            vbc.close();
            synchronized (flushThread.flushLock) {
                flushThread.flushLock.notifyAll();
            }
            try {
                flushThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            }
        }
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        os.write(vbc.toString().getBytes());
    }

    @Override
    public IFileMapManager getFileMapProvider() {
        return vbc.getFileMapProvider();
    }

    @Override
    public int getNumPagesOfFile(int fileId) throws HyracksDataException {
        return vbc.getNumPagesOfFile(fileId);
    }

    @Override
    public void returnPage(ICachedPage page) {
        vbc.returnPage(page);
    }

    @Override
    public IFIFOPageWriter createFIFOWriter(IPageWriteCallback callback, IPageWriteFailureCallback failureCallback) {
        return vbc.createFIFOWriter(callback, failureCallback);
    }

    @Override
    public ICachedPage confiscatePage(long dpid) throws HyracksDataException {
        return vbc.confiscatePage(dpid);
    }

    @Override
    public ICachedPage confiscateLargePage(long dpid, int multiplier, int extraBlockPageId)
            throws HyracksDataException {
        return vbc.confiscateLargePage(dpid, multiplier, extraBlockPageId);
    }

    @Override
    public void returnPage(ICachedPage page, boolean reinsert) {
        vbc.returnPage(page, reinsert);
    }

    @Override
    public int getFileReferenceCount(int fileId) {
        return vbc.getFileReferenceCount(fileId);
    }

    @Override
    public boolean isReplicationEnabled() {
        return vbc.isReplicationEnabled();
    }

    @Override
    public IIOReplicationManager getIOReplicationManager() {
        return vbc.getIOReplicationManager();
    }

    @Override
    public void purgeHandle(int fileId) throws HyracksDataException {
        vbc.purgeHandle(fileId);
    }

    @Override
    public String toString() {
        return vbc.toString();
    }

    @Override
    public void closeFileIfOpen(FileReference fileRef) {
        vbc.closeFileIfOpen(fileRef);
    }

    @Override
    public int getUsage() {
        return vbc.getUsage();
    }

    private boolean isMetadataIndex(ILSMIndex index) {
        BaseOperationTracker opTracker = (BaseOperationTracker) index.getOperationTracker();
        return MetadataIndexImmutableProperties.isMetadataDataset(opTracker.getDatasetInfo().getDatasetID());
    }

    /**
     * We use a dedicated thread to schedule flushes to avoid deadlock. We cannot schedule flushes directly during
     * page pins because page pins can be called while synchronized on op trackers (e.g., when resetting a
     * memory component).
     */
    private class FlushThread extends Thread {
        private final Object flushLock = new Object();

        @Override
        public void run() {
            while (isOpen.get()) {
                synchronized (flushLock) {
                    try {
                        flushLock.wait();
                    } catch (InterruptedException e) {
                        LOGGER.error("Flushing thread is interrupted unexpectedly.", e);
                    }
                }
                if (isOpen.get()) {
                    try {
                        scheduleFlush();
                    } catch (Throwable e) {
                        LOGGER.error("Unexpected exception when trying to schedule flushes.", e);
                        ExitUtil.halt(ExitUtil.EC_IO_SCHEDULER_FAILED);
                    }
                }
            }
        }

        private void scheduleFlush() throws HyracksDataException {
            ILSMIndex selectedIndex = null;
            synchronized (GlobalVirtualBufferCache.this) {
                while (flushingIndexes.size() < maxConcurrentFlushes
                        && ((selectedIndex = selectFlushIndex()) != null)) {
                    LOGGER.debug("Waiting for flushing primary index {} to complete...", selectedIndex);
                    flushingIndexes.add(selectedIndex);
                }
            }
        }

        private ILSMIndex selectFlushIndex() throws HyracksDataException {
            int cycles = 0;
            while (vbc.getUsage() >= flushPageBudget && cycles <= primaryIndexes.size()) {
                // find the first modified memory component while avoiding infinite loops
                ILSMIndex primaryIndex = primaryIndexes.get(flushPtr);
                flushPtr = (flushPtr + 1) % primaryIndexes.size();
                cycles++;
                if (!primaryIndex.isCurrentMutableComponentEmpty() && !flushingIndexes.contains(primaryIndex)) {
                    // we need to manually flush this memory component because it may be idle at this point
                    // note that this is different from flushing a filtered memory component
                    PrimaryIndexOperationTracker opTracker =
                            (PrimaryIndexOperationTracker) primaryIndex.getOperationTracker();
                    synchronized (opTracker) {
                        boolean flushable = !primaryIndex.isCurrentMutableComponentEmpty();
                        if (flushable && !opTracker.isFlushLogCreated()) {
                            // if the flush log has already been created, then we can simply wait for
                            // that flush to complete
                            ILSMMemoryComponent memoryComponent = primaryIndex.getCurrentMemoryComponent();
                            if (memoryComponent.getState() == ComponentState.READABLE_WRITABLE) {
                                // before we schedule the flush, mark the memory component as unwritable to prevent
                                // future writers
                                memoryComponent.setUnwritable();
                            }

                            opTracker.setFlushOnExit(true);
                            opTracker.flushIfNeeded();
                            // If the flush cannot be scheduled at this time, then there must be active writers.
                            // The flush will be eventually scheduled when writers exit
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Requested flushing {} index {}",
                                        isMetadataIndex(primaryIndex) ? "metadata" : "primary",
                                        primaryIndex.toString());
                            }
                        }
                        if ((flushable || opTracker.isFlushLogCreated()) && !isMetadataIndex(primaryIndex)) {
                            // global vbc cannot wait on metadata indexes because metadata indexes support full
                            // ACID transactions. Waiting on metadata indexes can introduce deadlocks.
                            return primaryIndex;
                        }
                    }
                }
            }
            return null;
        }

    }

}
