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
import java.util.concurrent.atomic.AtomicReference;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
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
    protected final ILSMIOOperationCallbackProvider ioOpCallbackProvider;

    // In-memory components.   
    protected final List<IVirtualBufferCache> virtualBufferCaches;
    protected AtomicInteger currentMutableComponentId;

    // On-disk components.    
    protected final IBufferCache diskBufferCache;
    protected final ILSMIndexFileManager fileManager;
    protected final IFileMapProvider diskFileMapProvider;
    protected final AtomicReference<List<ILSMComponent>> componentsRef;
    protected final double bloomFilterFalsePositiveRate;

    protected boolean isActivated;

    protected final AtomicBoolean[] flushRequests;

    public AbstractLSMIndex(List<IVirtualBufferCache> virtualBufferCaches, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, IFileMapProvider diskFileMapProvider,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackProvider ioOpCallbackProvider) {
        this.virtualBufferCaches = virtualBufferCaches;
        this.diskBufferCache = diskBufferCache;
        this.diskFileMapProvider = diskFileMapProvider;
        this.fileManager = fileManager;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioScheduler = ioScheduler;
        this.ioOpCallbackProvider = ioOpCallbackProvider;
        lsmHarness = new LSMHarness(this, mergePolicy, opTracker);
        isActivated = false;
        componentsRef = new AtomicReference<List<ILSMComponent>>();
        componentsRef.set(new LinkedList<ILSMComponent>());
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
        List<ILSMComponent> oldList = componentsRef.get();
        List<ILSMComponent> newList = new ArrayList<ILSMComponent>();
        newList.add(c);
        for (ILSMComponent oc : oldList) {
            newList.add(oc);
        }
        componentsRef.set(newList);
    }

    @Override
    public void subsumeMergedComponents(ILSMComponent newComponent, List<ILSMComponent> mergedComponents) {
        List<ILSMComponent> oldList = componentsRef.get();
        List<ILSMComponent> newList = new ArrayList<ILSMComponent>();
        int swapIndex = oldList.indexOf(mergedComponents.get(0));
        int swapSize = mergedComponents.size();
        for (int i = 0; i < oldList.size(); i++) {
            if (i < swapIndex || i >= swapIndex + swapSize) {
                newList.add(oldList.get(i));
            } else if (i == swapIndex) {
                newList.add(newComponent);
            }
        }
        componentsRef.set(newList);
    }

    @Override
    public List<ILSMComponent> getImmutableComponents() {
        return componentsRef.get();
    }

    @Override
    public boolean getFlushStatus() {
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
    public IBufferCache getBufferCache() {
        return diskBufferCache;
    }

    @Override
    public int getCurrentMutableComponentId() {
        return currentMutableComponentId.get();
    }

    @Override
    public String toString() {
        return "LSMIndex [" + fileManager.getBaseDir() + "]";
    }
}
