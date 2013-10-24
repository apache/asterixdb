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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractLSMIndex implements ILSMIndexInternal {
    protected final ILSMHarness lsmHarness;

    protected final ILSMIOOperationScheduler ioScheduler;
    protected final ILSMIOOperationCallback ioOpCallback;

    // In-memory components.   
    protected final List<ILSMComponent> memoryComponents;
    protected final List<IVirtualBufferCache> virtualBufferCaches;
    protected AtomicInteger currentMutableComponentId;

    // On-disk components.    
    protected final IBufferCache diskBufferCache;
    protected final ILSMIndexFileManager fileManager;
    protected final IFileMapProvider diskFileMapProvider;
    protected final List<ILSMComponent> diskComponents;
    protected final double bloomFilterFalsePositiveRate;

    protected boolean isActivated;

    protected final AtomicBoolean[] flushRequests;

    public AbstractLSMIndex(List<IVirtualBufferCache> virtualBufferCaches, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, IFileMapProvider diskFileMapProvider,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback) {
        this.virtualBufferCaches = virtualBufferCaches;
        this.diskBufferCache = diskBufferCache;
        this.diskFileMapProvider = diskFileMapProvider;
        this.fileManager = fileManager;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioScheduler = ioScheduler;
        this.ioOpCallback = ioOpCallback;
        this.ioOpCallback.setNumOfMutableComponents(virtualBufferCaches.size());
        lsmHarness = new LSMHarness(this, mergePolicy, opTracker);
        isActivated = false;
        diskComponents = new LinkedList<ILSMComponent>();
        memoryComponents = new ArrayList<ILSMComponent>();
        currentMutableComponentId = new AtomicInteger();
        flushRequests = new AtomicBoolean[virtualBufferCaches.size()];
        for (int i = 0; i < virtualBufferCaches.size(); i++) {
            flushRequests[i] = new AtomicBoolean();
        }
    }

    protected void forceFlushDirtyPages(ITreeIndex treeIndex) throws HyracksDataException {
        int fileId = treeIndex.getFileId();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        // Flush all dirty pages of the tree. 
        // By default, metadata and data are flushed asynchronously in the buffercache.
        // This means that the flush issues writes to the OS, but the data may still lie in filesystem buffers.
        ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory().createFrame();
        int startPage = 0;
        int maxPage = treeIndex.getFreePageManager().getMaxPage(metadataFrame);
        forceFlushDirtyPages(bufferCache, fileId, startPage, maxPage);
    }

    protected void forceFlushDirtyPages(IBufferCache bufferCache, int fileId, int startPageId, int endPageId)
            throws HyracksDataException {
        for (int i = startPageId; i <= endPageId; i++) {
            ICachedPage page = bufferCache.tryPin(BufferedFileHandle.getDiskPageId(fileId, i));
            // If tryPin returns null, it means the page is not cached, and therefore cannot be dirty.
            if (page == null) {
                continue;
            }
            try {
                bufferCache.flushDirtyPage(page);
            } finally {
                bufferCache.unpin(page);
            }
        }
        // Forces all pages of given file to disk. This guarantees the data makes it to disk.
        bufferCache.force(fileId, true);
    }

    protected void markAsValidInternal(ITreeIndex treeIndex) throws HyracksDataException {
        int fileId = treeIndex.getFileId();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory().createFrame();
        // Mark the component as a valid component by flushing the metadata page to disk
        int metadataPageId = treeIndex.getFreePageManager().getFirstMetadataPage();
        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metadataPageId), false);
        metadataPage.acquireWriteLatch();
        try {
            metadataFrame.setPage(metadataPage);
            metadataFrame.setValid(true);

            // Flush the single modified page to disk.
            bufferCache.flushDirtyPage(metadataPage);

            // Force modified metadata page to disk.
            bufferCache.force(fileId, true);
        } finally {
            metadataPage.releaseWriteLatch();
            bufferCache.unpin(metadataPage);
        }
    }

    @Override
    public void addComponent(ILSMComponent c) {
        diskComponents.add(0, c);
    }

    @Override
    public void subsumeMergedComponents(ILSMComponent newComponent, List<ILSMComponent> mergedComponents) {
        int swapIndex = diskComponents.indexOf(mergedComponents.get(0));
        diskComponents.removeAll(mergedComponents);
        diskComponents.add(swapIndex, newComponent);
    }

    @Override
    public void changeMutableComponent() {
        currentMutableComponentId.set((currentMutableComponentId.get() + 1) % memoryComponents.size());
        ((AbstractMemoryLSMComponent) memoryComponents.get(currentMutableComponentId.get())).setActive();
    }

    @Override
    public List<ILSMComponent> getImmutableComponents() {
        return diskComponents;
    }

    @Override
    public void changeFlushStatusForCurrentMutableCompoent(boolean needsFlush) {
        flushRequests[currentMutableComponentId.get()].set(needsFlush);
    }

    @Override
    public boolean hasFlushRequestForCurrentMutableComponent() {
        return flushRequests[currentMutableComponentId.get()].get();
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return lsmHarness.getOperationTracker();
    }

    @Override
    public ILSMIOOperationScheduler getIOScheduler() {
        return ioScheduler;
    }

    @Override
    public ILSMIOOperationCallback getIOOperationCallback() {
        return ioOpCallback;
    }

    @Override
    public IBufferCache getBufferCache() {
        return diskBufferCache;
    }

    public boolean isEmptyIndex() throws HyracksDataException {
        boolean isModified = false;
        for (ILSMComponent c : memoryComponents) {
            AbstractMemoryLSMComponent mutableComponent = (AbstractMemoryLSMComponent) c;
            if (mutableComponent.isModified()) {
                isModified = true;
                break;
            }
        }
        return diskComponents.isEmpty() && !isModified;
    }

    @Override
    public String toString() {
        return "LSMIndex [" + fileManager.getBaseDir() + "]";
    }
}
