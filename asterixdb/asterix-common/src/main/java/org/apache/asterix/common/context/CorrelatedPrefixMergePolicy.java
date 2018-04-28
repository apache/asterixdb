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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId.IdCompareResult;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.PrefixMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.util.LSMComponentIdUtils;

public class CorrelatedPrefixMergePolicy extends PrefixMergePolicy {

    private final IDatasetLifecycleManager datasetLifecycleManager;
    private final int datasetId;

    public CorrelatedPrefixMergePolicy(IDatasetLifecycleManager datasetLifecycleManager, int datasetId) {
        this.datasetLifecycleManager = datasetLifecycleManager;
        this.datasetId = datasetId;
    }

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException {
        // This merge policy will only look at primary indexes in order to evaluate if a merge operation is needed. If it decides that
        // a merge operation is needed, then it will merge *all* the indexes that belong to the dataset. The criteria to decide if a merge
        // is needed is the same as the one that is used in the prefix merge policy:
        // 1.  Look at the candidate components for merging in oldest-first order.  If one exists, identify the prefix of the sequence of
        // all such components for which the sum of their sizes exceeds MaxMrgCompSz.  Schedule a merge of those components into a new component.
        // 2.  If a merge from 1 doesn't happen, see if the set of candidate components for merging exceeds MaxTolCompCnt.  If so, schedule
        // a merge all of the current candidates into a new single component.

        if (fullMergeIsRequested || index.isPrimaryIndex()) {
            super.diskComponentAdded(index, fullMergeIsRequested);
        }
    }

    /**
     * Adopts the similar logic to decide merge lagging based on {@link PrefixMergePolicy}
     *
     * @throws HyracksDataException
     */
    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        if (index.isPrimaryIndex()) {
            return super.isMergeLagging(index);
        } else {
            return false;
        }

    }

    @Override
    protected boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        // Reverse the components order so that we look at components from oldest to newest.
        Collections.reverse(immutableComponents);

        Pair<Integer, Integer> mergeableIndexes = getMergableComponentsIndex(immutableComponents);
        if (mergeableIndexes == null) {
            //nothing to merge
            return false;
        }
        ILSMComponent leftComponent = immutableComponents.get(mergeableIndexes.getLeft());
        ILSMComponent rightComponent = immutableComponents.get(mergeableIndexes.getRight());
        ILSMComponentId targetId = LSMComponentIdUtils.union(leftComponent.getId(), rightComponent.getId());
        int partition = ((PrimaryIndexOperationTracker) index.getOperationTracker()).getPartition();
        Set<ILSMIndex> indexes =
                datasetLifecycleManager.getDatasetInfo(datasetId).getDatasetPartitionOpenIndexes(partition);
        triggerScheduledMerge(targetId, indexes);
        return true;
    }

    /**
     * Submit merge requests for all disk components within the range specified by targetId
     * of all indexes of a given dataset in the given partition
     *
     * @param targetId
     * @param indexInfos
     * @throws HyracksDataException
     */
    private void triggerScheduledMerge(ILSMComponentId targetId, Set<ILSMIndex> indexes) throws HyracksDataException {
        for (ILSMIndex lsmIndex : indexes) {
            List<ILSMDiskComponent> immutableComponents = new ArrayList<>(lsmIndex.getDiskComponents());
            if (isMergeOngoing(immutableComponents)) {
                continue;
            }
            List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
            for (ILSMDiskComponent component : immutableComponents) {
                ILSMComponentId id = component.getId();
                IdCompareResult cmp = targetId.compareTo(id);
                if (cmp == IdCompareResult.INCLUDE) {
                    mergableComponents.add(component);
                } else if (cmp == IdCompareResult.GREATER_THAN) {
                    //disk components are ordered from latest (with largest IDs) to oldest (with smallest IDs)
                    // if targetId>component.Id, we can safely skip the rest disk components in the list
                    break;
                }
            }
            ILSMIndexAccessor accessor = lsmIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleMerge(mergableComponents);
        }
    }
}
