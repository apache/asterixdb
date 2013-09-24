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

package edu.uci.ics.asterix.common.ioopcallbacks;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public abstract class AbstractLSMIOOperationCallback implements ILSMIOOperationCallback {

    protected long firstLSN;
    protected long lastLSN;
    protected long[] immutableLastLSNs;
    protected int readIndex;
    protected int writeIndex;

    public AbstractLSMIOOperationCallback() {
        resetLSNs();
    }

    @Override
    public void setNumOfMutableComponents(int count) {
        immutableLastLSNs = new long[count];
        readIndex = 0;
        writeIndex = 0;
    }

    @Override
    public void beforeOperation(LSMOperationType opType) {
        if (opType == LSMOperationType.FLUSH) {
            synchronized (this) {
                immutableLastLSNs[writeIndex] = lastLSN;
                writeIndex = (writeIndex + 1) % immutableLastLSNs.length;
                resetLSNs();
            }
        }
    }

    @Override
    public void afterFinalize(LSMOperationType opType, ILSMComponent newComponent) {
        // Do nothing.
    }

    public abstract long getComponentLSN(List<ILSMComponent> oldComponents) throws HyracksDataException;

    protected void putLSNIntoMetadata(ITreeIndex treeIndex, List<ILSMComponent> oldComponents)
            throws HyracksDataException {
        long componentLSN = getComponentLSN(oldComponents);
        int fileId = treeIndex.getFileId();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory().createFrame();
        int metadataPageId = treeIndex.getFreePageManager().getFirstMetadataPage();
        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metadataPageId), false);
        metadataPage.acquireWriteLatch();
        try {
            metadataFrame.setPage(metadataPage);
            metadataFrame.setLSN(componentLSN);
        } finally {
            metadataPage.releaseWriteLatch();
            bufferCache.unpin(metadataPage);
        }
    }

    protected long getTreeIndexLSN(ITreeIndex treeIndex) throws HyracksDataException {
        int fileId = treeIndex.getFileId();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory().createFrame();
        int metadataPageId = treeIndex.getFreePageManager().getFirstMetadataPage();
        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metadataPageId), false);
        metadataPage.acquireReadLatch();
        try {
            metadataFrame.setPage(metadataPage);
            return metadataFrame.getLSN();
        } finally {
            metadataPage.releaseReadLatch();
            bufferCache.unpin(metadataPage);
        }
    }

    protected void resetLSNs() {
        firstLSN = -1;
        lastLSN = -1;
    }

    public void updateLastLSN(long lastLSN) {
        if (firstLSN == -1) {
            firstLSN = lastLSN;
        }
        this.lastLSN = Math.max(this.lastLSN, lastLSN);
    }

    public long getFirstLSN() {
        return firstLSN;
    }

    public long getLastLSN() {
        return lastLSN;
    }

}
