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
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.storage.IGrowableIntArray;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangePartitionType.RangePartitioningType;

public class FieldRangePartitionComputerFactory implements ITupleRangePartitionComputerFactory {
    private static final long serialVersionUID = 1L;
    private final int[] rangeFields;
    private IRangeMap rangeMap;
    private IBinaryComparatorFactory[] comparatorFactories;
    private RangePartitioningType rangeType;

    public FieldRangePartitionComputerFactory(int[] rangeFields, IBinaryComparatorFactory[] comparatorFactories,
            IRangeMap rangeMap, RangePartitioningType rangeType) {
        this.rangeFields = rangeFields;
        this.comparatorFactories = comparatorFactories;
        this.rangeMap = rangeMap;
        this.rangeType = rangeType;
    }

    public ITupleRangePartitionComputer createPartitioner() {
        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
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
                int suggestedPartition = binarySearchRangePartition(accessor, tIndex);
                addPartition(suggestedPartition, map);
                if (rangeType != RangePartitioningType.PROJECT) {
                    // Check range values above an below to ensure match partitions are included.
                    int partitionIndex = suggestedPartition - 1;
                    while (partitionIndex >= 0 && compareSlotAndFields(accessor, tIndex, partitionIndex) == 0) {
                        addPartition(partitionIndex, map);
                        --partitionIndex;
                    }
                    partitionIndex = suggestedPartition + 1;
                    while (partitionIndex < rangeMap.getSplitCount()
                            && compareSlotAndFields(accessor, tIndex, partitionIndex) == 0) {
                        addPartition(partitionIndex, map);
                        ++partitionIndex;
                    }
                    partitionIndex = rangeMap.getSplitCount();
                    if (compareSlotAndFields(accessor, tIndex, partitionIndex - 1) == 0) {
                        addPartition(partitionIndex, map);
                    }
                }
            }

            private void addPartition(int partition, IGrowableIntArray map) {
                map.add((int) Math.floor(partition / rangesPerPart));
            }

            /*
             * Return first match or suggested index.
             */
            private int binarySearchRangePartition(IFrameTupleAccessor accessor, int tIndex)
                    throws HyracksDataException {
                int searchIndex = 0;
                int left = 0;
                int right = rangeMap.getSplitCount() - 1;
                int cmp = 0;
                while (left <= right) {
                    searchIndex = (left + right) / 2;
                    cmp = compareSlotAndFields(accessor, tIndex, searchIndex);
                    if (cmp > 0) {
                        left = searchIndex + 1;
                        searchIndex = left;
                    } else if (cmp < 0) {
                        right = searchIndex - 1;
                        searchIndex = left;
                    } else {
                        return searchIndex + 1;
                    }
                }
                return searchIndex;
            }

            private int compareSlotAndFields(IFrameTupleAccessor accessor, int tIndex, int fieldIndex)
                    throws HyracksDataException {
                int c = 0;
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int slotLength = accessor.getFieldSlotsLength();
                for (int f = 0; f < comparators.length; ++f) {
                    int fIdx = rangeFields[f];
                    int fStart = accessor.getFieldStartOffset(tIndex, fIdx);
                    int fEnd = accessor.getFieldEndOffset(tIndex, fIdx);
                    c = comparators[f].compare(accessor.getBuffer().array(), startOffset + slotLength + fStart,
                            fEnd - fStart, rangeMap.getByteArray(fieldIndex, f), rangeMap.getStartOffset(fieldIndex, f),
                            rangeMap.getLength(fieldIndex, f));
                    if (c != 0) {
                        return c;
                    }
                }
                return c;
            }

        };
    }
}
