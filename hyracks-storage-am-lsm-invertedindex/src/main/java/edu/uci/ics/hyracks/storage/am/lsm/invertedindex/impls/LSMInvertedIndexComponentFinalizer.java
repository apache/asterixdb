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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndex.LSMInvertedIndexComponent;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMInvertedIndexComponentFinalizer extends TreeIndexComponentFinalizer {

    public LSMInvertedIndexComponentFinalizer(IFileMapProvider fileMapProvider) {
        super(fileMapProvider);
    }

    @Override
    public boolean isValid(Object lsmComponent) throws HyracksDataException {
        LSMInvertedIndexComponent invIndexComponent = (LSMInvertedIndexComponent) lsmComponent;
        OnDiskInvertedIndex invIndex = (OnDiskInvertedIndex) invIndexComponent.getInvIndex();
        // Use the dictionary BTree for validation.
        return super.isValid(invIndex.getBTree());
    }

    @Override
    public void finalize(Object lsmComponent) throws HyracksDataException {
        LSMInvertedIndexComponent invIndexComponent = (LSMInvertedIndexComponent) lsmComponent;
        OnDiskInvertedIndex invIndex = (OnDiskInvertedIndex) invIndexComponent.getInvIndex();
        ITreeIndex treeIndex = invIndex.getBTree();
        // Flush inverted index first.
        forceFlushDirtyPages(treeIndex);
        forceFlushInvListsFileDirtyPages(invIndex);
        // Flush deleted keys BTree.
        forceFlushDirtyPages(invIndexComponent.getDeletedKeysBTree());
        // We use the dictionary BTree for marking the inverted index as valid.
        markAsValid(treeIndex);
    }

    protected void forceFlushInvListsFileDirtyPages(OnDiskInvertedIndex invIndex) throws HyracksDataException {
        int fileId = invIndex.getInvListsFileId();
        IBufferCache bufferCache = invIndex.getBufferCache();
        int startPageId = 0;
        int maxPageId = invIndex.getInvListsMaxPageId();
        forceFlushDirtyPages(bufferCache, fileId, startPageId, maxPageId);
    }
}
