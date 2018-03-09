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
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterReference;
import org.apache.hyracks.storage.common.MultiComparator;

public class LSMComponentFilterManager implements ILSMComponentFilterManager {

    public static final MutableArrayValueReference FILTER_KEY = new MutableArrayValueReference("Filter".getBytes());
    private final ILSMComponentFilterFrameFactory filterFrameFactory;

    public LSMComponentFilterManager(ILSMComponentFilterFrameFactory filterFrameFactory) {
        this.filterFrameFactory = filterFrameFactory;
    }

    @Override
    public void updateFilter(ILSMComponentFilter filter, List<ITupleReference> filterTuples,
            IExtendedModificationOperationCallback opCallback) throws HyracksDataException {
        MultiComparator filterCmp = MultiComparator.create(filter.getFilterCmpFactories());
        for (ITupleReference tuple : filterTuples) {
            filter.update(tuple, filterCmp, opCallback);
        }
    }

    @Override
    public void writeFilter(ILSMComponentFilter filter, ITreeIndex treeIndex) throws HyracksDataException {
        IMetadataPageManager treeMetaManager = (IMetadataPageManager) treeIndex.getPageManager();
        ILSMComponentFilterReference filterFrame = filterFrameFactory.createFrame();
        try {
            if (filter.getMinTuple() != null) {
                filterFrame.writeMinTuple(filter.getMinTuple());
            }
            if (filter.getMaxTuple() != null) {
                filterFrame.writeMaxTuple(filter.getMaxTuple());
            }
        } finally {
            treeMetaManager.put(treeMetaManager.createMetadataFrame(), FILTER_KEY, filterFrame);
        }
    }

    @Override
    public boolean readFilter(ILSMComponentFilter filter, ITreeIndex treeIndex) throws HyracksDataException {
        IMetadataPageManager treeMetaManager = (IMetadataPageManager) treeIndex.getPageManager();
        ILSMComponentFilterReference filterFrame = filterFrameFactory.createFrame();
        treeMetaManager.get(treeMetaManager.createMetadataFrame(), FILTER_KEY, filterFrame);
        // TODO: Filters never have one of min/max set and the other not
        if (!filterFrame.isMinTupleSet() || !filterFrame.isMaxTupleSet()) {
            return false;
        }
        List<ITupleReference> filterTuples = new ArrayList<>();
        filterTuples.add(filterFrame.getMinTuple());
        filterTuples.add(filterFrame.getMaxTuple());
        updateFilter(filter, filterTuples, NoOpOperationCallback.INSTANCE);
        return true;
    }

}
