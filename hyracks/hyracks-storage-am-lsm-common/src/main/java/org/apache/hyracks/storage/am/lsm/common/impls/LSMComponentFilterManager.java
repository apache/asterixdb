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
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrame;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFilterManager;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class LSMComponentFilterManager implements ILSMComponentFilterManager {

    private final IBufferCache bufferCache;
    private final ILSMComponentFilterFrameFactory filterFrameFactory;

    public LSMComponentFilterManager(IBufferCache bufferCache, ILSMComponentFilterFrameFactory filterFrameFactory) {
        this.bufferCache = bufferCache;
        this.filterFrameFactory = filterFrameFactory;
    }

    @Override
    public void updateFilterInfo(ILSMComponentFilter filter, List<ITupleReference> filterTuples)
            throws HyracksDataException {
        MultiComparator filterCmp = MultiComparator.create(filter.getFilterCmpFactories());
        for (ITupleReference tuple : filterTuples) {
            filter.update(tuple, filterCmp);
        }
    }

    @Override
    public void writeFilterInfo(ILSMComponentFilter filter, ITreeIndex treeIndex) throws HyracksDataException {
        int fileId = treeIndex.getFileId();
        ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory().createFrame();

        // Read the filter page from the first metadata page of the tree.
        // If it is has not been created yet, then create a new one.
        int metadataPageId = treeIndex.getFreePageManager().getFirstMetadataPage();
        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metadataPageId), false);
        metadataPage.acquireWriteLatch();
        int componentFilterPageId;
        try {
            metadataFrame.setPage(metadataPage);
            componentFilterPageId = metadataFrame.getLSMComponentFilterPageId();
            if (componentFilterPageId == -1) {
                componentFilterPageId = treeIndex.getFreePageManager().getFreePage(metadataFrame);
                metadataFrame.setLSMComponentFilterPageId(componentFilterPageId);
            }
        } finally {
            metadataPage.releaseWriteLatch(true);
            bufferCache.unpin(metadataPage);
        }

        ICachedPage filterPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, componentFilterPageId), true);
        filterPage.acquireWriteLatch();
        try {
            ILSMComponentFilterFrame filterFrame = filterFrameFactory.createFrame();
            filterFrame.setPage(filterPage);
            filterFrame.initBuffer();
            if (filter.getMinTuple() != null) {
                filterFrame.writeMinTuple(filter.getMinTuple());
            }
            if (filter.getMaxTuple() != null) {
                filterFrame.writeMaxTuple(filter.getMaxTuple());
            }

        } finally {
            filterPage.releaseWriteLatch(true);
            bufferCache.unpin(filterPage);
        }
    }

    @Override
    public boolean readFilterInfo(ILSMComponentFilter filter, ITreeIndex treeIndex) throws HyracksDataException {
        int fileId = treeIndex.getFileId();
        ITreeIndexMetaDataFrame metadataFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory().createFrame();

        int metadataPageId = treeIndex.getFreePageManager().getFirstMetadataPage();
        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metadataPageId), false);
        metadataPage.acquireReadLatch();
        int componentFilterPageId;
        try {
            metadataFrame.setPage(metadataPage);
            componentFilterPageId = metadataFrame.getLSMComponentFilterPageId();
            if (componentFilterPageId == -1) {
                return false;
            }
        } finally {
            metadataPage.releaseReadLatch();
            bufferCache.unpin(metadataPage);
        }

        ICachedPage filterPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, componentFilterPageId), true);
        filterPage.acquireReadLatch();
        try {
            ILSMComponentFilterFrame filterFrame = filterFrameFactory.createFrame();
            filterFrame.setPage(filterPage);

            if (!filterFrame.isMinTupleSet() || !filterFrame.isMaxTupleSet()) {
                return false;
            }
            List<ITupleReference> filterTuples = new ArrayList<ITupleReference>();
            filterTuples.add(filterFrame.getMinTuple());
            filterTuples.add(filterFrame.getMaxTuple());
            updateFilterInfo(filter, filterTuples);

        } finally {
            filterPage.releaseReadLatch();
            bufferCache.unpin(filterPage);
        }
        return true;
    }
}
