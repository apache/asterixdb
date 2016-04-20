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
package org.apache.asterix.runtime.operators.joins;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangePartitionType.RangePartitioningType;

public class OverlappingIntervalMergeJoinCheckerFactory extends AbstractIntervalMergeJoinCheckerFactory {
    private static final long serialVersionUID = 1L;
    private IRangeMap rangeMap;

    public OverlappingIntervalMergeJoinCheckerFactory(IRangeMap rangeMap) {
        this.rangeMap = rangeMap;
    }

    @Override
    public IIntervalMergeJoinChecker createMergeJoinChecker(int[] keys0, int[] keys1, int partition)
            throws HyracksDataException {
        int fieldIndex = 0;
        if (ATypeTag.INT64.serialize() != rangeMap.getTag(0, 0)) {
            throw new HyracksDataException("Invalid range map type for interval merge join checker.");
        }
        int slot = partition - 1;
        long partitionStart = 0;
        // All lookups are on typed values.
        if (partition <= 0) {
            partitionStart = LongPointable.getLong(rangeMap.getMinByteArray(fieldIndex),
                    rangeMap.getMinStartOffset(fieldIndex) + 1);
        } else if (partition <= rangeMap.getSplitCount()) {
            partitionStart = LongPointable.getLong(rangeMap.getByteArray(fieldIndex, slot),
                    rangeMap.getStartOffset(fieldIndex, slot) + 1);
        } else if (partition > rangeMap.getSplitCount()) {
            partitionStart = LongPointable.getLong(rangeMap.getMaxByteArray(fieldIndex),
                    rangeMap.getMaxStartOffset(fieldIndex) + 1);
        }
        return new OverlappingIntervalMergeJoinChecker(keys0, keys1, partitionStart);
    }

    @Override
    public RangePartitioningType getRightPartitioningType() {
        return RangePartitioningType.SPLIT;
    }

}