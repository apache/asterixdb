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

public class BeforeIntervalMergeJoinChecker extends AbstractIntervalMergeJoinChecker {
    private static final long serialVersionUID = 1L;

    public BeforeIntervalMergeJoinChecker(int[] keysLeft, int[] keysRight) {
        super(keysLeft[0], keysRight[0]);
    }

    @Override
    public boolean checkToSaveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start0 = IntervalPartitionUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long start1 = IntervalPartitionUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        return (start0 < start1);
    }

    @Override
    public boolean checkToRemoveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long end0 = IntervalPartitionUtil.getIntervalEnd(accessorLeft, leftTupleIndex, idLeft);
        long start1 = IntervalPartitionUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        return (start1 < end0);
    }

    @Override
    public boolean checkToLoadNextRightTuple(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start0 = IntervalPartitionUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long start1 = IntervalPartitionUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        return (start0 < start1);
    }

    public <T extends Comparable<T>> boolean compareInterval(T start0, T end0, T start1, T end1) {
        return IntervalLogic.before(start0, end0, start1, end1);
    }

    public <T extends Comparable<T>> boolean compareIntervalPartition(T start0, T end0, T start1, T end1) {
        return IntervalPartitionLogic.before(start0, end0, start1, end1);
    }

}