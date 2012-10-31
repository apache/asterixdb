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

package edu.uci.ics.asterix.transaction.management.ioopcallbacks;

import edu.uci.ics.asterix.transaction.management.opcallbacks.IndexOperationTracker;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public abstract class AbstractLSMIOOperationCallback implements ILSMIOOperationCallback {

    // Object on which blocked LSMIndex operations are waiting.
    private final IndexOperationTracker opTracker;

    public AbstractLSMIOOperationCallback(IndexOperationTracker opTracker) {
        this.opTracker = opTracker;
    }

    @Override
    public void beforeOperation(ILSMIOOperation operation) {
        // Do nothing.
    }

    @Override
    public synchronized void afterFinalize(ILSMIOOperation operation, Object newComponent) {
        // Wake up all blocked index operations that were waiting for this io operation to complete.
        opTracker.notifyAll();
    }

    protected void putLSNIntoMetadata(ITreeIndex treeIndex) throws HyracksDataException {
        int fileId = treeIndex.getFileId();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory().createFrame();
        // Mark the component as a valid component by flushing the metadata page to disk
        int metadataPageId = treeIndex.getFreePageManager().getFirstMetadataPage();
        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metadataPageId), false);
        metadataPage.acquireWriteLatch();
        try {
            metadataFrame.setPage(metadataPage);
            metadataFrame.setLSN(opTracker.getLastLSN());
        } finally {
            metadataPage.releaseWriteLatch();
            bufferCache.unpin(metadataPage);
        }
    }

}
