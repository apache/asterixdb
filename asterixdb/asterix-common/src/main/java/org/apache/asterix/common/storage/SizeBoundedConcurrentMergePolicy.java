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
package org.apache.asterix.common.storage;

import static org.apache.asterix.common.storage.SizeBoundedConcurrentMergePolicy.Range.isRangeMergable;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

/**
 * This merge policy deals with the constraint that the resultant merged component
 * should not exceed the specified maxResultantComponentSize.
 */
public class SizeBoundedConcurrentMergePolicy implements ILSMMergePolicy {
    private final long maxComponentSize;
    private int minMergeComponentCount;
    private int maxMergeComponentCount;
    private int maxComponentCount;
    private double sizeRatio;

    public SizeBoundedConcurrentMergePolicy(long maxComponentSize) {
        this.maxComponentSize = maxComponentSize;
    }

    @Override
    public void diskComponentAdded(ILSMIndex index, boolean fullMergeIsRequested) throws HyracksDataException {
        if (fullMergeIsRequested) {
            throw new IllegalArgumentException("SizeBoundedConcurrentMergePolicy does not support fullMerge.");
        }

        scheduleMerge(index, false);
    }

    @Override
    public void configure(Map<String, String> properties) {
        minMergeComponentCount =
                Integer.parseInt(properties.get(SizeBoundedConcurrentMergePolicyFactory.MIN_MERGE_COMPONENT_COUNT));
        maxMergeComponentCount =
                Integer.parseInt(properties.get(SizeBoundedConcurrentMergePolicyFactory.MAX_MERGE_COMPONENT_COUNT));
        sizeRatio = Double.parseDouble(properties.get(SizeBoundedConcurrentMergePolicyFactory.SIZE_RATIO));
        maxComponentCount =
                Integer.parseInt(properties.get(SizeBoundedConcurrentMergePolicyFactory.MAX_COMPONENT_COUNT));
    }

    // hold flush when the componentCount reaches maxComponentCount, and try to find the suitable range
    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> diskComponents = index.getDiskComponents();
        MergableSolution mergableSolutionWithComponentCount =
                getMergableIndexesRange(index.getDiskComponents(), minMergeComponentCount, true);
        if (mergableSolutionWithComponentCount.getComponentsCount() < maxComponentCount) {
            // not reach the component threshold, simply return false
            return false;
        } else {
            if (diskComponents.stream().anyMatch(d -> d.getState() == ILSMComponent.ComponentState.READABLE_MERGING)) {
                // reach the component threshold and some components are being merged, return true (stop flushing)
                return true;
            } else {
                // reach the component threshold but no components are being merged
                // this can happen in two cases: (1) the system just recovers; (2) maxComponentCount is too small
                if (!diskComponents.stream()
                        .allMatch(d -> d.getState() == ILSMComponent.ComponentState.READABLE_UNWRITABLE)) {
                    throw new IllegalStateException("Illegal disk component states in isMergeLagging");
                }

                if (scheduleMerge(index, false)) {
                    return true;
                }
                // desperate request to merge any two disk components, while satisfying the maxComponentSize criterion
                // if unable to schedule a merge, don't block the component flush.
                return scheduleMerge(index, true);
            }
        }
    }

    // In case of no-force merge, we try to find the largestIndexRange which satisfies the merge-policy rules.
    // but in case of force, there is desperate request for merge, hence try finding as low as 2 components to merge.
    private boolean scheduleMerge(ILSMIndex index, boolean force) throws HyracksDataException {
        List<ILSMDiskComponent> diskComponents = index.getDiskComponents();
        MergableSolution mergableSolutionWithRange;
        if (force) {
            mergableSolutionWithRange = getMergableIndexesRange(diskComponents, 2, false);
        } else {
            mergableSolutionWithRange = getMergableIndexesRange(diskComponents, minMergeComponentCount, false);
        }
        if (mergableSolutionWithRange.getRange() != null) {
            triggerScheduleMerge(index, diskComponents, mergableSolutionWithRange.getRange().getStartIndex(),
                    mergableSolutionWithRange.getRange().getEndIndex());
            return true;
        } else {
            return false;
        }
    }

    /**
     * <p>
     * getMergableIndexesRange gives the [startComponentIndex, endComponentIndex] which can be merged based on the
     * provided condition.
     * </p>
     * <ol>
     *     <li>localMinMergeComponentCount <= range(startComponentIndex, endComponentIndex) <= maxMergeComponentCount</li>
     *     <li>sumOfComponents(startComponentIndex, endComponentIndex) < maxComponentSize</li>
     * </ol>
     *
     *<p>
     * In order to satisfy range(startComponentIndex, endComponentIndex) <= maxMergeComponentCount,
     * search range is bounded by leftBoundary, and if we reach the maxComponentSize before reaching the leftBoundary,
     * we get our boundary where startIndex > leftBoundary.
     *</p>
     * <p>
     * but in case the components in the range does not contribute enough to exceed maxComponentSize then the candidate
     * range will be [leftBoundary, endComponentIndex] which satisfies both 1 & 2.
     *</p>
     * @param diskComponents The disk components within an Index
     * @param localMinMergeComponentCount The min count of contiguous components required to call a mergable range.
     * @param countFlag if enabled, will count all the components that can be merged, else will return on first found range
     * @return MergableSolution
     */
    private MergableSolution getMergableIndexesRange(List<ILSMDiskComponent> diskComponents,
            int localMinMergeComponentCount, boolean countFlag) {
        int numComponents = diskComponents.size();
        int candidateComponentsCount = 0;
        for (; candidateComponentsCount < numComponents; candidateComponentsCount++) {
            if (diskComponents.get(candidateComponentsCount)
                    .getState() != ILSMComponent.ComponentState.READABLE_UNWRITABLE) {
                break;
            }
        }

        if (candidateComponentsCount < localMinMergeComponentCount) {
            return MergableSolution.NO_SOLUTION;
        }

        // count the total number of non-overlapping components that can be merged.
        // eg: if there are two possible ranges, total non-overlapping components will be [0,2],[4,7] = 3 + 4 = 7
        int totalMergableComponentsCount = 0;

        int endComponentIndex = candidateComponentsCount - 1;
        while (endComponentIndex >= localMinMergeComponentCount - 1) {
            // acting as a branching condition for advancing the endComponentIndex to prevRange's startIndex - 1.
            // thus allowing to get non-overlapping ranges.
            // TODO(merge-policy): Need refactoring for better readability
            boolean aRangeFound = false;

            long lastComponentSize = diskComponents.get(endComponentIndex).getComponentSize();
            long resultantComponentSize = lastComponentSize;

            int probableStartIndex = endComponentIndex - 1;
            int leftBoundaryIndex = Math.max(endComponentIndex - maxMergeComponentCount, 0);

            for (; probableStartIndex >= leftBoundaryIndex; probableStartIndex--) {
                long currentComponentSize = diskComponents.get(probableStartIndex).getComponentSize();
                resultantComponentSize += currentComponentSize;

                if (resultantComponentSize >= maxComponentSize) {
                    // since the resultComponentSize after merging index from [probableStartIndex, endComponentIndex]
                    // exceeds the maxComponentSize, the appropriate range should be (probableStartIndex, endComponentIndex]
                    if (isRangeMergable(probableStartIndex + 1, endComponentIndex, localMinMergeComponentCount,
                            (resultantComponentSize - currentComponentSize - lastComponentSize), sizeRatio,
                            lastComponentSize)) {
                        int mergableRangeCount = endComponentIndex - probableStartIndex;
                        if (countFlag) {
                            aRangeFound = true;
                            endComponentIndex = probableStartIndex;
                            totalMergableComponentsCount += mergableRangeCount;
                        } else {
                            return new MergableSolution(new Range(probableStartIndex + 1, endComponentIndex),
                                    mergableRangeCount);
                        }
                    }
                    // break as we already exceeded the maxComponentSize, and still haven't found the suitable range,
                    break;
                }
            }

            // since we exceeded the leftBoundary, and still we are within maxComponentCount
            // [leftBoundaryIndex, endComponentIndex] can be a probable solution
            if (probableStartIndex == leftBoundaryIndex - 1) {
                if (isRangeMergable(leftBoundaryIndex, endComponentIndex, localMinMergeComponentCount,
                        (resultantComponentSize - lastComponentSize), sizeRatio, lastComponentSize)) {
                    int mergableRangeCount = endComponentIndex - leftBoundaryIndex + 1;
                    if (countFlag) {
                        aRangeFound = true;
                        endComponentIndex = probableStartIndex;
                        totalMergableComponentsCount += mergableRangeCount;
                    } else {
                        return new MergableSolution(new Range(leftBoundaryIndex, endComponentIndex),
                                mergableRangeCount);
                    }
                }
            }

            if (!aRangeFound) {
                --endComponentIndex;
            }
        }

        return new MergableSolution(totalMergableComponentsCount);
    }

    private void triggerScheduleMerge(ILSMIndex index, List<ILSMDiskComponent> diskComponents, int startIndex,
            int endIndex) throws HyracksDataException {
        List<ILSMDiskComponent> mergableComponents = diskComponents.subList(startIndex, endIndex + 1);
        index.createAccessor(NoOpIndexAccessParameters.INSTANCE).scheduleMerge(mergableComponents);
    }

    static class MergableSolution {
        public static final MergableSolution NO_SOLUTION = new MergableSolution(null, 0);
        private final Range range;
        private final int componentsCount;

        MergableSolution(int componentsCount) {
            range = null;
            this.componentsCount = componentsCount;
        }

        MergableSolution(Range range, int componentsCount) {
            this.range = range;
            this.componentsCount = componentsCount;
        }

        public Range getRange() {
            return range;
        }

        public int getComponentsCount() {
            return componentsCount;
        }
    }

    static class Range {
        private final int startIndex;
        private final int endIndex;

        public Range(int startIndex, int endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        public int getStartIndex() {
            return startIndex;
        }

        public int getEndIndex() {
            return endIndex;
        }

        static boolean isRangeMergable(int startIndexInclusive, int endIndexInclusive, int localMinMergeComponentCount,
                long resultantComponentSizeExcludingEndComponent, double sizeRatio, long endComponentSize) {
            int mergableRangeCount = endIndexInclusive - startIndexInclusive + 1;
            return mergableRangeCount >= localMinMergeComponentCount
                    && ((resultantComponentSizeExcludingEndComponent * sizeRatio) >= endComponentSize);
        }
    }
}
