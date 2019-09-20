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

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class ConcurrentMergePolicy implements ILSMMergePolicy {
    /**
     * The minimum number of disk components per merge
     */
    private int minMergeComponentCount;

    /**
     * The maximum number of disk components per merge
     */
    private int maxMergeComponentCount;

    /**
     * The maximum number of disk component allowed before stopping flushes
     */
    private int maxComponentCount;

    /**
     * This parameter is used to avoid merging a big component with a sequence of small components.
     * If a component is larger than ratio * all younger disk components in the merge list, then
     * this old (large) component is ignored in this round.
     */
    private double sizeRatio;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException {

        if (fullMergeIsRequested) {
            List<ILSMDiskComponent> diskComponents = index.getDiskComponents();
            if (!diskComponents.stream().allMatch(d -> d.getState() != ComponentState.READABLE_UNWRITABLE)) {
                return;
            }
            index.createAccessor(NoOpIndexAccessParameters.INSTANCE).scheduleFullMerge();
            return;
        }

        scheduleMerge(index);
    }

    @Override
    public void configure(Map<String, String> properties) {
        minMergeComponentCount =
                Integer.parseInt(properties.get(ConcurrentMergePolicyFactory.MIN_MERGE_COMPONENT_COUNT));
        maxMergeComponentCount =
                Integer.parseInt(properties.get(ConcurrentMergePolicyFactory.MAX_MERGE_COMPONENT_COUNT));
        sizeRatio = Double.parseDouble(properties.get(ConcurrentMergePolicyFactory.SIZE_RATIO));
        maxComponentCount = Integer.parseInt(properties.get(ConcurrentMergePolicyFactory.MAX_COMPONENT_COUNT));
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> diskComponents = index.getDiskComponents();
        if (diskComponents.size() < maxComponentCount) {
            // not reach the component threshold, simply return false
            return false;
        } else {
            if (diskComponents.stream().anyMatch(d -> d.getState() == ComponentState.READABLE_MERGING)) {
                // reach the component threshold and some components are being merged, return true (stop flushing)
                return true;
            } else {
                // reach the component threshold but no components are being merged
                // this can happen in two cases: (1) the system just recovers; (2) maxComponentCount is too small
                if (!diskComponents.stream().allMatch(d -> d.getState() == ComponentState.READABLE_UNWRITABLE)) {
                    throw new IllegalStateException("Illegal disk component states in isMergeLagging");
                }
                // try to schedule a merge operation first
                boolean isMergeTriggered = scheduleMerge(index);
                if (!isMergeTriggered) {
                    // no merge is scheduled => maxComponentCount is too small
                    // Ideally, this should NEVER happen. But we add a guard here to simply schedule a full merge
                    // so that the system can still proceed (otherwise, writes will be blocked forever)
                    ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                    accessor.scheduleFullMerge();
                }
                return true;
            }
        }
    }

    /**
     * schedule a merge operation according to this merge policy
     *
     * @param index
     * @return true if merge is scheduled, false otherwise.
     * @throws HyracksDataException
     * @throws IndexException
     */
    protected boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> diskComponents = index.getDiskComponents();
        Pair<Integer, Integer> mergeableIndexes = getMergableComponentsIndex(diskComponents);
        if (mergeableIndexes != null) {
            triggerScheduleMerge(index, diskComponents, mergeableIndexes.getLeft(), mergeableIndexes.getRight());
            return true;
        } else {
            return false;
        }
    }

    private void triggerScheduleMerge(ILSMIndex index, List<ILSMDiskComponent> diskComponents, int startIndex,
            int endIndex) throws HyracksDataException {
        List<ILSMDiskComponent> mergableComponents = diskComponents.subList(startIndex, endIndex + 1);
        index.createAccessor(NoOpIndexAccessParameters.INSTANCE).scheduleMerge(mergableComponents);
    }

    /**
     * Given a list of disk components (ordered from newest to oldest), this function
     * identify a sequence of components to be merged. It works logically as follows:
     * 1. Find the longest prefix of the component sequence (i.e., youngest) that are not being merged
     * 2. Look at the candidate components for merging in oldest-first order. If one exists, identify the
     * suffix of the component sequence so that size_ratio * younger component size >= oldest component size
     * and the number of merging components is between [minMergeComponentCount, maxMergeComponentCount]
     *
     * @param diskComponents
     * @return a pair of indexes indicating the start (inclusive) and end (exclusive) position of the sequence
     *         otherwise, return null if no sequence is found
     */
    protected Pair<Integer, Integer> getMergableComponentsIndex(List<ILSMDiskComponent> diskComponents)
            throws HyracksDataException {
        int numComponents = diskComponents.size();
        // find the longest suffix of the component sequence that are not being merged
        int count = 0;
        for (; count < numComponents; count++) {
            if (diskComponents.get(count).getState() != ComponentState.READABLE_UNWRITABLE) {
                break;
            }
        }
        if (count < minMergeComponentCount) {
            return null;
        }
        // build the prefix sum of sizes of the these components
        long[] componentSizeSum = new long[count];
        for (int i = 0; i < count; i++) {
            componentSizeSum[i] = diskComponents.get(i).getComponentSize() + (i > 0 ? componentSizeSum[i - 1] : 0);
        }

        for (int end = count - 1; end >= minMergeComponentCount - 1; end--) {
            long componentSize = componentSizeSum[end] - componentSizeSum[end - 1];
            int start = Math.max(end - maxMergeComponentCount + 1, 0);
            if (componentSize <= sizeRatio
                    * (componentSizeSum[end - 1] - (start > 0 ? componentSizeSum[start - 1] : 0))) {
                return Pair.of(start, end);
            }
        }
        return null;
    }

}
