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

import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalLogic;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalPartitionLogic;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class OverlappingIntervalMergeJoinChecker extends AbstractIntervalMergeJoinChecker {
    private static final long serialVersionUID = 1L;
    private final long partitionStart;

    public OverlappingIntervalMergeJoinChecker(int[] keysLeft, int[] keysRight, long partitionStart) {
        super(keysLeft[0], keysRight[0]);
        this.partitionStart = partitionStart;
    }

    @Override
    public boolean checkToSaveInResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long end0 = IntervalJoinUtil.getIntervalEnd(accessorLeft, leftTupleIndex, idLeft);

        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        long end1 = IntervalJoinUtil.getIntervalEnd(accessorRight, rightTupleIndex, idRight);

        if (start0 < partitionStart && start1 < partitionStart) {
            // Both tuples will match in a different partition.
            return false;
        }
        return compareInterval(start0, end0, start1, end1);
    }

    public <T extends Comparable<T>> boolean compareInterval(T start0, T end0, T start1, T end1) {
        return IntervalLogic.overlapping(start0, end0, start1, end1);
    }

    public <T extends Comparable<T>> boolean compareIntervalPartition(T start0, T end0, T start1, T end1) {
        return IntervalPartitionLogic.overlapping(start0, end0, start1, end1);
    }

}