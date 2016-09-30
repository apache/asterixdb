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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetaDataPageManager;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrame;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

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
        IMetaDataPageManager treeMetaManager = treeIndex.getMetaManager();
        ICachedPage filterPage = null;
        int componentFilterPageId = treeMetaManager.getFilterPageId();
        boolean appendOnly = false;
        int fileId = treeIndex.getFileId();
        if (componentFilterPageId == LinkedMetaDataPageManager.NO_FILTER_IN_PLACE) { //in-place mode, no filter page yet
            ITreeIndexMetaDataFrame metadataFrame = treeIndex.getMetaManager().getMetaDataFrameFactory().createFrame();
            int metaPageId = treeMetaManager.getFirstMetadataPage();
            ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metaPageId), false);
            metadataPage.acquireWriteLatch();
            try {
                metadataFrame.setPage(metadataPage);
                componentFilterPageId = treeIndex.getMetaManager().getFreePage(metadataFrame);
                metadataFrame.setLSMComponentFilterPageId(componentFilterPageId);
            } finally {
                metadataPage.releaseWriteLatch(true);
                bufferCache.unpin(metadataPage);
            }
        } else if (componentFilterPageId <= LinkedMetaDataPageManager.NO_FILTER_APPEND_ONLY) {
            appendOnly = true;
            filterPage = treeMetaManager.getFilterPage();
            if (filterPage == null) {
                treeMetaManager.setFilterPage(bufferCache.confiscatePage(IBufferCache.INVALID_DPID));
                filterPage = treeMetaManager.getFilterPage();
            }
        } else { // in place, not a new filter page
            filterPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, componentFilterPageId), true);
        }

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
            if (!appendOnly) {
                bufferCache.unpin(filterPage);
                filterPage.releaseWriteLatch(true);
            } else {
                filterPage.releaseWriteLatch(false);
            }
        }
    }

    @Override
    public boolean readFilterInfo(ILSMComponentFilter filter, ITreeIndex treeIndex) throws HyracksDataException {
        int fileId = treeIndex.getFileId();

        IMetaDataPageManager treeMetaManager = treeIndex.getMetaManager();

        int componentFilterPageId = treeMetaManager.getFilterPageId();
        if (componentFilterPageId < 0)
            return false;

        ICachedPage filterPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, componentFilterPageId),
                false);

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
