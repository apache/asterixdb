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
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.context.DatasetLifecycleManager.DatasetInfo;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManager;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractDiskLSMComponent;

public class CorrelatedPrefixMergePolicy implements ILSMMergePolicy {

    private long maxMergableComponentSize;
    private int maxToleranceComponentCount;

    private final DatasetLifecycleManager datasetLifecycleManager;
    private final int datasetID;
    
    public CorrelatedPrefixMergePolicy(IIndexLifecycleManager datasetLifecycleManager, int datasetID) {
        this.datasetLifecycleManager = (DatasetLifecycleManager) datasetLifecycleManager;
        this.datasetID = datasetID;
    }

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException,
            IndexException {
        // This merge policy will only look at primary indexes in order to evaluate if a merge operation is needed. If it decides that
        // a merge operation is needed, then it will merge *all* the indexes that belong to the dataset. The criteria to decide if a merge
        // is needed is the same as the one that is used in the prefix merge policy:
        // 1.  Look at the candidate components for merging in oldest-first order.  If one exists, identify the prefix of the sequence of
        // all such components for which the sum of their sizes exceeds MaxMrgCompSz.  Schedule a merge of those components into a new component.
        // 2.  If a merge from 1 doesn't happen, see if the set of candidate components for merging exceeds MaxTolCompCnt.  If so, schedule
        // a merge all of the current candidates into a new single component.
        List<ILSMComponent> immutableComponents = new ArrayList<ILSMComponent>(index.getImmutableComponents());
        // Reverse the components order so that we look at components from oldest to newest.
        Collections.reverse(immutableComponents);

        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return;
            }
        }
        if (fullMergeIsRequested) {
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleFullMerge(index.getIOOperationCallback());
            return;
        }
        if (!index.isPrimaryIndex()) {
            return;
        }
        long totalSize = 0;
        int startIndex = -1;
        int minNumComponents = Integer.MAX_VALUE;

        Set<ILSMIndex> indexes = datasetLifecycleManager.getDatasetInfo(datasetID).getDatasetIndexes();
        for (ILSMIndex lsmIndex : indexes) {
            minNumComponents = Math.min(minNumComponents, lsmIndex.getImmutableComponents().size());
        }

        for (int i = 0; i < minNumComponents; i++) {
            ILSMComponent c = immutableComponents.get(i);
            long componentSize = ((AbstractDiskLSMComponent) c).getComponentSize();
            if (componentSize > maxMergableComponentSize) {
                startIndex = i;
                totalSize = 0;
                continue;
            }
            totalSize += componentSize;
            boolean isLastComponent = i + 1 == minNumComponents ? true : false;
            if (totalSize > maxMergableComponentSize
                    || (isLastComponent && i - startIndex >= maxToleranceComponentCount)) {

                for (ILSMIndex lsmIndex : indexes) {
                    List<ILSMComponent> reversedImmutableComponents = new ArrayList<ILSMComponent>(
                            lsmIndex.getImmutableComponents());
                    // Reverse the components order so that we look at components from oldest to newest.
                    Collections.reverse(reversedImmutableComponents);

                    List<ILSMComponent> mergableComponents = new ArrayList<ILSMComponent>();
                    for (int j = startIndex + 1; j <= i; j++) {
                        mergableComponents.add(reversedImmutableComponents.get(j));
                    }
                    // Reverse the components order back to its original order
                    Collections.reverse(mergableComponents);

                    ILSMIndexAccessor accessor = (ILSMIndexAccessor) lsmIndex.createAccessor(
                            NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                    accessor.scheduleMerge(lsmIndex.getIOOperationCallback(), mergableComponents);
                }
                break;
            }
        }
    }

    @Override
    public void configure(Map<String, String> properties) {
        maxMergableComponentSize = Long.parseLong(properties.get("max-mergable-component-size"));
        maxToleranceComponentCount = Integer.parseInt(properties.get("max-tolerance-component-count"));
    }
}
