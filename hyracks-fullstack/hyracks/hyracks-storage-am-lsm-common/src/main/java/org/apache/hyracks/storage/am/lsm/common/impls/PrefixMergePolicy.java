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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class PrefixMergePolicy implements ILSMMergePolicy {
    protected long maxMergableComponentSize;
    protected int maxToleranceComponentCount;

    /**
     * This parameter is used to avoid merging a big component with a sequence of small components.
     * If a component is larger than ratio * all younger disk components in the merge list, then
     * this old (large) component is ignored in this round.
     * Since it's a temporary fix, we don't set this parameter as configurable in order not to
     * disturb the users.
     * This number is taken from HBase compaction policy
     * see https://www.ngdata.com/visualizing-hbase-flushes-and-compactions/
     */
    private final static double MAX_MERGABLE_COMPONENT_SIZE_RATIO = 1.2;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException {

        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());

        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }

        if (fullMergeIsRequested) {
            index.createAccessor(NoOpIndexAccessParameters.INSTANCE).scheduleFullMerge();
            return;
        }

        scheduleMerge(index);
    }

    @Override
    public void configure(Map<String, String> properties) {
        maxMergableComponentSize = Long.parseLong(properties.get("max-mergable-component-size"));
        maxToleranceComponentCount = Integer.parseInt(properties.get("max-tolerance-component-count"));
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {

        /**
         * [for flow-control purpose]
         * when merge operations are lagged, threads which flushed components will be blocked
         * until merge operations catch up, i.e, until the number of mergable immutable components <= maxToleranceComponentCount
         * example:
         * suppose that maxToleranceComponentCount = 3 and maxMergableComponentSize = 1GB
         * The following shows a set of events occurred in time ti with a brief description.
         * time
         * t40: c32-1(1GB, RU) c38-33(192MB, RU) c39-39(32MB, RU) c40-40(32MB, RU)
         * --> a thread which added c40-40 will trigger a merge including c38-33,c39-39,c40-40
         * t41: c32-1(1GB, RU) c38-33(192MB, RUM) c39-39(32MB, RUM) c40-40(32MB, RUM) c41-41(32MB, RU)
         * --> a thread which added c41-41 will not be blocked
         * t42: c32-1(1GB, RU) c38-33(192MB, RUM) c39-39(32MB, RUM) c40-40(32MB, RUM) c41-41(32MB, RU) c42-42(32MB, RU)
         * --> a thread which added c42-42 will not be blocked
         * t43: c32-1(1GB, RU) c38-33(192MB, RUM) c39-39(32MB, RUM) c40-40(32MB, RUM) c41-41(32MB, RU) c42-42(32MB, RU) c43-43(32MB, RU)
         * --> a thread which added c43-43 will not be blocked and will not trigger a merge since there is an ongoing merge triggered in t1.
         * t44: c32-1(1GB, RU) c38-33(192MB, RUM) c39-39(32MB, RUM) c40-40(32MB, RUM) c41-41(32MB, RU) c42-42(32MB, RU) c43-43(32MB, RU) 'c44-44(32MB, RU)'
         * --> a thread which will add c44-44 (even if the disk component is created, but not added to index instance disk components yet)
         * will be blocked until the number of RU components < maxToleranceComponentCount
         * t45: c32-1(1GB, RU) *c40-33(256MB, RU)* c41-41(32MB, RU) c42-42(32MB, RU) c43-43(32MB, RU) 'c44-44(32MB, RU)'
         * --> a thread which completed the merge triggered in t1 added c40-33 and will go ahead and trigger the next merge with c40-33,c41-41,c42-42,c43-43.
         * Still, the blocked thread will continue being blocked and the c44-44 was not included in the merge since it's not added yet.
         * t46: c32-1(1GB, RU) c40-33(256MB, RUM) c41-41(32MB, RUM) c42-42(32MB, RUM) c43-43(32MB, RUM) c44-44(32MB, RUM)
         * --> the merge triggered in t45 is going on and the merge unblocked the blocked thread, so c44-44 was added.
         * t47: c32-1(1GB, RU) *c43-33(320MB, RU)* c44-44(32MB, RUM)
         * --> a thread completed the merge triggered in t45 and added c43-33.
         * t48: c32-1(1GB, RU) c43-33(320MB, RU) c44-44(32MB, RUM) c48-48(32MB, RU)
         * --> a thread added c48-48 and will not be blocked and will trigger a merge with c43-44, c44-44, c48-48.
         * ... continues ...
         * ----------------------------------------
         * legend:
         * For example, C32-1 represents a disk component, more specifically, disk component name, where 32-1 represents a timestamp range from t1 to time t32.
         * This means that the component C32-1 is a component resulting from a merge operation that merged components C1-1 to C32-32.
         * This also implies that if two timestamps in a component name are equal, the component has not been merged yet after it was created.
         * RU and RUM are possible state of disk components, where RU represents READABLE_UNWRITABLE and RUM represents READABLE_UNWRITABLE_MERGING.
         * Now, c32-1(1GB, RU) represents a disk component resulted from merging c1-1 ~ c32-32 and the component size is 1GB.
         * ----------------------------------------
         * The flow control allows at most maxToleranceComponentCount mergable components,
         * where the mergable components are disk components whose i) state == RU and ii) size < maxMergableComponentSize.
         */

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
         */

        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        // reverse the list so that we look from the oldest to the newest components
        Collections.reverse(immutableComponents);
        int mergableImmutableComponentCount = getMergableImmutableComponentCount(immutableComponents);

        // [case 1]
        if (mergableImmutableComponentCount < maxToleranceComponentCount) {
            return false;
        }

        boolean isMergeOngoing = isMergeOngoing(immutableComponents);

        // here, implicitly (mergableImmutableComponentCount >= maxToleranceComponentCount) is true by passing case 1.
        if (isMergeOngoing) {
            // [case 2]
            return true;
        } else {
            // [case 3]
            // make sure that all components are of READABLE_UNWRITABLE state.
            if (!areComponentsReadableWritableState(immutableComponents)) {
                throw new IllegalStateException();
            }
            // schedule a merge operation
            boolean isMergeTriggered = scheduleMerge(index);
            if (!isMergeTriggered) {
                throw new IllegalStateException();
            }
            return true;
        }
    }

    /**
     * This method returns whether there is an ongoing merge operation or not by checking
     * each component state of given components.
     *
     * @param immutableComponents
     * @return true if there is an ongoing merge operation, false otherwise.
     */
    protected boolean isMergeOngoing(List<ILSMDiskComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method returns the number of mergable components among the given list
     * of immutable components that are ordered from the oldest component to newer ones. A caller
     * need to make sure the order in the list.
     *
     * @param immutableComponents
     * @return the number of mergable component
     */
    protected int getMergableImmutableComponentCount(List<ILSMDiskComponent> immutableComponents) {
        Pair<Integer, Integer> mergableIndexes = getMergableComponentsIndex(immutableComponents);
        return mergableIndexes == null ? 0 : mergableIndexes.getRight() - mergableIndexes.getLeft() + 1;
    }

    /**
     * checks whether all given components are of READABLE_UNWRITABLE state
     *
     * @param immutableComponents
     * @return true if all components are of READABLE_UNWRITABLE state, false otherwise.
     */
    protected boolean areComponentsReadableWritableState(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    /**
     * schedule a merge operation according to this prefix merge policy
     *
     * @param index
     * @return true if merge is scheduled, false otherwise.
     * @throws HyracksDataException
     * @throws IndexException
     */
    protected boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        // Reverse the components order so that we look at components from oldest to newest.
        Collections.reverse(immutableComponents);

        Pair<Integer, Integer> mergeableIndexes = getMergableComponentsIndex(immutableComponents);
        if (mergeableIndexes != null) {
            triggerScheduleMerge(index, immutableComponents, mergeableIndexes.getLeft(), mergeableIndexes.getRight());
            return true;
        } else {
            return false;
        }
    }

    private void triggerScheduleMerge(ILSMIndex index, List<ILSMDiskComponent> immutableComponents, int startIndex,
            int endIndex) throws HyracksDataException {
        List<ILSMDiskComponent> mergableComponents =
                new ArrayList<>(immutableComponents.subList(startIndex, endIndex + 1));

        // Reverse the components order back to its original order
        Collections.reverse(mergableComponents);
        index.createAccessor(NoOpIndexAccessParameters.INSTANCE).scheduleMerge(mergableComponents);
    }

    /**
     * Given a list of disk components (ordered from oldest to newest), this function
     * identify a sequence of components to be merged. It works logically as follows:
     * 1. Look at the candidate components for merging in oldest-first order. If one exists, identify the
     * prefix of the sequence of all such components for which the sum of their sizes exceeds MaxMrgCompSz.
     * 2. If a merge from 1 doesn't happen, see if the set of candidate components for merging exceeds
     * MaxTolCompCnt.
     * 3. If we find a sequence from 1 or 2, and the first (oldest) component in the sequence is smaller than
     * ratio * total size of the younger components in the sequence, schedule a merge of all sequences.
     * Otherwise, go back to step 1 with the next component.
     *
     * @param immutableComponents
     * @return a pair of indexes indicating the start and end position of the sequence
     *         otherwise, return null if no sequence is found
     */
    protected Pair<Integer, Integer> getMergableComponentsIndex(List<ILSMDiskComponent> immutableComponents) {
        int numComponents = immutableComponents.size();
        for (int i = 0; i < numComponents; i++) {
            if (immutableComponents.get(i).getComponentSize() > maxMergableComponentSize
                    || immutableComponents.get(i).getState() != ComponentState.READABLE_UNWRITABLE) {
                continue;
            }
            long startComponentSize = immutableComponents.get(i).getComponentSize();

            long totalSize = startComponentSize;
            int j = i + 1;
            boolean mergeable = true;
            for (; j < numComponents; j++) {
                long componentSize = immutableComponents.get(j).getComponentSize();
                if (componentSize > maxMergableComponentSize
                        || immutableComponents.get(j).getState() != ComponentState.READABLE_UNWRITABLE) {
                    // skip unmergeable components if any
                    break;
                }
                totalSize += componentSize;
                mergeable = startComponentSize < MAX_MERGABLE_COMPONENT_SIZE_RATIO * (totalSize - startComponentSize);
                if (totalSize > maxMergableComponentSize && mergeable) {
                    // If the ratio check passes and total size exceeds the threshold, we return this sequence
                    return Pair.of(i, j);
                }
                // Stops search if the ratio threshold cannot be met.
                // Since components are ordered from older to newer, newer (later) components
                // would have smaller sizes than the current component size.
                if (startComponentSize >= MAX_MERGABLE_COMPONENT_SIZE_RATIO
                        * (totalSize + componentSize * (numComponents - j - 1) - startComponentSize)) {
                    break;
                }
            }
            if (j != numComponents) {
                continue;
            }
            if (numComponents - i >= maxToleranceComponentCount && mergeable) {
                // If it's the last component, component count exceeds the threshold, the ratio check passes,
                // then we return this sequence.
                return Pair.of(i, numComponents - 1);
            }
            // if we reach the last component, but there are not enough components to merge,
            // then we don't need to check i+1 th component
            if (numComponents - i < maxToleranceComponentCount) {
                return null;
            }
        }
        return null;
    }

}
