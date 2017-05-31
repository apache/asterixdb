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
import java.util.stream.Collectors;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.impls.PrefixMergePolicy;

public class CorrelatedPrefixMergePolicy implements ILSMMergePolicy {

    private long maxMergableComponentSize;
    private int maxToleranceComponentCount;

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

        if (fullMergeIsRequested) {
            //full merge request is handled by each index separately, since it is possible that
            //when a primary index wants to send full merge requests for all secondaries,
            //one secondary index is being merged and the request cannot be scheduled
            List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getImmutableComponents());
            if (!areComponentsReadableUnwritableState(immutableComponents)) {
                return;
            }

            ILSMIndexAccessor accessor =
                    index.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            accessor.scheduleFullMerge(index.getIOOperationCallback());
            return;
        }

        if (!index.isPrimaryIndex()) {
            return;
        }
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getImmutableComponents());
        if (!areComponentsReadableUnwritableState(immutableComponents)) {
            return;
        }
        scheduleMerge(index);
    }

    @Override
    public void configure(Map<String, String> properties) {
        maxMergableComponentSize =
                Long.parseLong(properties.get(CorrelatedPrefixMergePolicyFactory.KEY_MAX_COMPONENT_SIZE));
        maxToleranceComponentCount =
                Integer.parseInt(properties.get(CorrelatedPrefixMergePolicyFactory.KEY_MAX_COMPONENT_COUNT));
    }

    /**
     * Adopts the similar logic to decide merge lagging based on {@link PrefixMergePolicy}
     *
     * @throws HyracksDataException
     */
    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        /**
         * case 1.
         * if mergableImmutableCommponentCount < threshold,
         * merge operation is not lagged ==> return false.
         * case 2.
         * if a) mergableImmutableCommponentCount >= threshold && b) there is an ongoing merge,
         * merge operation is lagged. ==> return true.
         * case 3. *SPECIAL CASE*
         * if a) mergableImmutableCommponentCount >= threshold && b) there is *NO* ongoing merge,
         * merge operation is lagged. ==> *schedule a merge operation* and then return true.
         * This is a special case that requires to schedule a merge operation.
         * Otherwise, all flush operations will be hung.
         * This case can happen in a following situation:
         * The system may crash when
         * condition 1) the mergableImmutableCommponentCount >= threshold and
         * condition 2) merge operation is going on.
         * After the system is recovered, still condition 1) is true.
         * If there are flush operations in the same dataset partition after the recovery,
         * all these flush operations may not proceed since there is no ongoing merge and
         * there will be no new merge either in this situation.
         * Note for case 3, we only let the primary index to schedule merge operations on behalf
         * of all indexes.
         */

        List<ILSMDiskComponent> immutableComponents = index.getImmutableComponents();
        int mergableImmutableComponentCount = getMergableImmutableComponentCount(immutableComponents);

        // [case 1]
        if (mergableImmutableComponentCount < maxToleranceComponentCount) {
            return false;
        }

        boolean isMergeOngoing = isMergeOngoing(immutableComponents);

        if (isMergeOngoing) {
            // [case 2]
            return true;
        }

        if (index.isPrimaryIndex()) {
            // [case 3]
            // make sure that all components are of READABLE_UNWRITABLE state.
            if (!areComponentsReadableUnwritableState(immutableComponents)) {
                throw new IllegalStateException();
            }
            // schedule a merge operation
            boolean isMergeTriggered = scheduleMerge(index);
            if (!isMergeTriggered) {
                throw new IllegalStateException();
            }
            return true;
        } else {
            //[case 3]
            //if the index is secondary then ignore the merge request (since the merge should be
            //triggered by the primary) and here we simply treat it as not lagged.
            return false;
        }
    }

    private boolean scheduleMerge(ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getImmutableComponents());
        Collections.reverse(immutableComponents);

        long totalSize = 0;
        int startIndex = -1;

        int numComponents = immutableComponents.size();

        for (int i = 0; i < numComponents; i++) {
            ILSMComponent c = immutableComponents.get(i);
            long componentSize = ((ILSMDiskComponent) c).getComponentSize();
            if (componentSize > maxMergableComponentSize || ((ILSMDiskComponent) c).getComponentId().notFound()) {
                startIndex = i;
                totalSize = 0;
                continue;
            }
            totalSize += componentSize;
            boolean isLastComponent = i + 1 == numComponents ? true : false;
            if (totalSize > maxMergableComponentSize
                    || (isLastComponent && i - startIndex >= maxToleranceComponentCount)) {
                //merge disk components from startIndex+1 to i
                long minID = Long.MAX_VALUE;
                long maxID = Long.MIN_VALUE;
                for (int j = startIndex + 1; j <= i; j++) {
                    ILSMDiskComponentId id = immutableComponents.get(j).getComponentId();
                    if (minID > id.getMinId()) {
                        minID = id.getMinId();
                    }
                    if (maxID < id.getMaxId()) {
                        maxID = id.getMaxId();
                    }
                }
                Set<IndexInfo> indexInfos = datasetLifecycleManager.getDatasetInfo(datasetId).getDatsetIndexInfos();
                int partition = getIndexPartition(index, indexInfos);
                triggerScheduledMerge(minID, maxID, indexInfos.stream().filter(info -> info.getPartition() == partition)
                        .collect(Collectors.toSet()));
                return true;
            }
        }
        return false;
    }

    /**
     * Submit merge requests for all disk components within [minID, maxID]
     * of all indexes of a given dataset in the given partition
     *
     * @param minID
     * @param maxID
     * @param partition
     * @param indexInfos
     * @throws HyracksDataException
     */
    private void triggerScheduledMerge(long minID, long maxID, Set<IndexInfo> indexInfos) throws HyracksDataException {
        for (IndexInfo info : indexInfos) {
            ILSMIndex lsmIndex = info.getIndex();

            List<ILSMDiskComponent> immutableComponents = new ArrayList<>(lsmIndex.getImmutableComponents());
            if (isMergeOngoing(immutableComponents)) {
                continue;
            }
            List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
            for (ILSMDiskComponent component : immutableComponents) {
                ILSMDiskComponentId id = component.getComponentId();
                if (!id.notFound()) {
                    if (id.getMinId() >= minID && id.getMaxId() <= maxID) {
                        mergableComponents.add(component);
                    }
                    if (id.getMaxId() < minID) {
                        //disk components are ordered from latest (with largest IDs) to oldest (with smallest IDs)
                        //if the component.maxID < minID, we can safely skip the rest disk components in the list
                        break;
                    }
                }
            }
            ILSMIndexAccessor accessor =
                    lsmIndex.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            accessor.scheduleMerge(lsmIndex.getIOOperationCallback(), mergableComponents);
        }
    }

    /**
     * This method returns the number of mergable components among the given list
     * of immutable components that are ordered from the latest component to order ones. A caller
     * need to make sure the order in the list.
     *
     * @param immutableComponents
     * @return the number of mergable component
     * @throws HyracksDataException
     */
    private int getMergableImmutableComponentCount(List<ILSMDiskComponent> immutableComponents)
            throws HyracksDataException {
        int count = 0;
        for (ILSMComponent c : immutableComponents) {
            long componentSize = ((ILSMDiskComponent) c).getComponentSize();
            //stop when the first non-mergable component is found.
            if (c.getState() != ComponentState.READABLE_UNWRITABLE || componentSize > maxMergableComponentSize
                    || ((ILSMDiskComponent) c).getComponentId().notFound()) {
                break;
            }
            ++count;
        }
        return count;
    }

    /**
     * This method returns whether there is an ongoing merge operation or not by checking
     * each component state of given components.
     *
     * @param immutableComponents
     * @return true if there is an ongoing merge operation, false otherwise.
     */
    private boolean isMergeOngoing(List<ILSMDiskComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }

    /**
     * checks whether all given components are of READABLE_UNWRITABLE state
     *
     * @param immutableComponents
     * @return true if all components are of READABLE_UNWRITABLE state, false otherwise.
     */
    private boolean areComponentsReadableUnwritableState(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    private int getIndexPartition(ILSMIndex index, Set<IndexInfo> indexInfos) {
        for (IndexInfo info : indexInfos) {
            if (info.getIndex() == index) {
                return info.getPartition();
            }
        }
        return -1;
    }
}
