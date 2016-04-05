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
package org.apache.hyracks.dataflow.common.data.partition.range;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryRangeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.storage.IGrowableIntArray;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangePartitionType.RangePartitioningType;

public class FieldRangePartitionComputerFactory implements ITupleRangePartitionComputerFactory {
    private static final long serialVersionUID = 1L;
    private final int[] rangeFields;
    private IRangeMap rangeMap;
    private IBinaryRangeComparatorFactory[] comparatorFactories;
    private RangePartitioningType rangeType;

    public FieldRangePartitionComputerFactory(int[] rangeFields, IBinaryRangeComparatorFactory[] comparatorFactories,
            IRangeMap rangeMap, RangePartitioningType rangeType) {
        this.rangeFields = rangeFields;
        this.comparatorFactories = comparatorFactories;
        this.rangeMap = rangeMap;
        this.rangeType = rangeType;
    }

    public ITupleRangePartitionComputer createPartitioner() {
        final IBinaryComparator[] minComparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            minComparators[i] = comparatorFactories[i].createMinBinaryComparator();
        }
        final IBinaryComparator[] maxComparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            maxComparators[i] = comparatorFactories[i].createMaxBinaryComparator();
        }
        return new ITupleRangePartitionComputer() {
            private int partionCount;
            private double rangesPerPart = 1;

            public void partition(IFrameTupleAccessor accessor, int tIndex, int nParts, IGrowableIntArray map)
                    throws HyracksDataException {
                if (nParts == 1) {
                    map.add(0);
                    return;
                }
                // Map range partition to node partitions.
                if (partionCount != nParts) {
                    partionCount = nParts;
                    if (rangeMap.getSplitCount() + 1 > nParts) {
                        rangesPerPart = ((double) rangeMap.getSplitCount() + 1) / nParts;
                    }
                }
                getRangePartitions(accessor, tIndex, map);
            }

            /*
             * Determine the range partitions.
             */
            private void getRangePartitions(IFrameTupleAccessor accessor, int tIndex, IGrowableIntArray map)
                    throws HyracksDataException {
                switch (rangeType) {
                    case PROJECT: {
                        int minPartition = getPartitionMap(binarySearchRangePartition(accessor, tIndex, minComparators));
                        addPartition(minPartition, map);
                        break;
                    }
                    case PROJECT_END: {
                        int maxPartition = getPartitionMap(
                                binarySearchRangePartition(accessor, tIndex, maxComparators));
                        addPartition(maxPartition, map);
                        break;
                    }
                    case REPLICATE: {
                        int minPartition = getPartitionMap(binarySearchRangePartition(accessor, tIndex, minComparators));
                        int maxPartition = getPartitionMap(rangeMap.getSplitCount() + 1);
                        for (int pid = minPartition; pid < maxPartition; ++pid) {
                            addPartition(pid, map);
                        }
                        break;
                    }
                    case SPLIT: {
                        int minPartition = getPartitionMap(binarySearchRangePartition(accessor, tIndex, minComparators));
                        int maxPartition = getPartitionMap(
                                binarySearchRangePartition(accessor, tIndex, maxComparators));
                        for (int pid = minPartition; pid <= maxPartition; ++pid) {
                            addPartition(pid, map);
                        }
                        break;
                    }
                }
            }

            private void addPartition(int partition, IGrowableIntArray map) {
                if (!hasPartition(partition, map)) {
                    map.add(partition);
                }
            }

            private int getPartitionMap(int partition) {
                return (int) Math.floor(partition / rangesPerPart);
            }

            private boolean hasPartition(int pid, IGrowableIntArray map) {
                for (int i = 0; i < map.size(); ++i) {
                    if (map.get(i) == pid) {
                        return true;
                    }
                }
                return false;
            }

            /*
             * Return first match or suggested index.
             */
            private int binarySearchRangePartition(IFrameTupleAccessor accessor, int tIndex,
                    IBinaryComparator[] comparators) throws HyracksDataException {
                int searchIndex = 0;
                int left = 0;
                int right = rangeMap.getSplitCount() - 1;
                int cmp = 0;
                while (left <= right) {
                    searchIndex = (left + right) / 2;
                    cmp = compareSlotAndFields(accessor, tIndex, searchIndex, comparators);
                    if (cmp > 0) {
                        left = searchIndex + 1;
                        searchIndex += 1;
                    } else if (cmp < 0) {
                        right = searchIndex - 1;
                    } else {
                        return searchIndex + 1;
                    }
                }
                return searchIndex;
            }

            private int compareSlotAndFields(IFrameTupleAccessor accessor, int tIndex, int mapIndex,
                    IBinaryComparator[] comparators) throws HyracksDataException {
                int c = 0;
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int slotLength = accessor.getFieldSlotsLength();
                for (int f = 0; f < comparators.length; ++f) {
                    int fIdx = rangeFields[f];
                    int fStart = accessor.getFieldStartOffset(tIndex, fIdx);
                    int fEnd = accessor.getFieldEndOffset(tIndex, fIdx);
                    c = comparators[f].compare(accessor.getBuffer().array(), startOffset + slotLength + fStart,
                            fEnd - fStart, rangeMap.getByteArray(f, mapIndex), rangeMap.getStartOffset(f, mapIndex),
                            rangeMap.getLength(f, mapIndex));
                    if (c != 0) {
                        return c;
                    }
                }
                return c;
            }

        };
    }
}
