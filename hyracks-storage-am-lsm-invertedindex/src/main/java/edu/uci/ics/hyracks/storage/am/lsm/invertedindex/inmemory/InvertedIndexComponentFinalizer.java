/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class InvertedIndexComponentFinalizer implements ILSMComponentFinalizer {
    
    protected final IFileMapProvider fileMapProvider;
    
    public InvertedIndexComponentFinalizer(IFileMapProvider fileMapProvider) {
        this.fileMapProvider = fileMapProvider;
    }
    
    @Override
    public boolean isValid(Object lsmComponent) throws HyracksDataException {
        InvertedIndex index = (InvertedIndex) lsmComponent;
        ITreeIndex treeIndex = index.getBTree();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        FileReference fileRef = new FileReference(file);
        bufferCache.createFile(fileRef);
        int fileId = fileMapProvider.lookupFileId(fileRef);
        bufferCache.openFile(fileId);
        treeIndex.open(fileId);
        try {
            int metadataPage = treeIndex.getFreePageManager().getFirstMetadataPage();
            ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory().createFrame();
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(treeIndex.getFileId(), metadataPage), false);
            page.acquireReadLatch();
            try {
                metadataFrame.setPage(page);
                return metadataFrame.isValid();
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        } finally {
            treeIndex.close();
            bufferCache.closeFile(fileId);
            bufferCache.deleteFile(fileId, false);
        }
    }

    @Override
    public void finalize(Object lsmComponent) throws HyracksDataException {
        InvertedIndex index = (InvertedIndex) lsmComponent;
        ITreeIndex treeIndex = index.getBTree();
        int fileId = treeIndex.getFileId();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        // Flush all dirty pages of the tree. 
        // By default, metadata and data are flushed async in the buffercache.
        // This means that the flush issues writes to the OS, but the data may still lie in filesystem buffers.
        ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory().createFrame();        
        int startPage = 0;
        int maxPage = treeIndex.getFreePageManager().getMaxPage(metadataFrame);
        for (int i = startPage; i <= maxPage; i++) {
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
}
