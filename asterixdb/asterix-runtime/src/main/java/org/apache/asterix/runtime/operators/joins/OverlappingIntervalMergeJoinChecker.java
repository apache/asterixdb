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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalPartitionLogic;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

public class OverlappingIntervalMergeJoinChecker extends AbstractIntervalMergeJoinChecker {
    private static final long serialVersionUID = 1L;
    private final long partitionStart;

    public OverlappingIntervalMergeJoinChecker(int[] keysLeft, int[] keysRight, long partitionStart) {
        super(keysLeft[0], keysRight[0]);
        this.partitionStart = partitionStart;
    }

    @Override
    public boolean checkToSaveInResult(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, idLeft);
        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, idRight);
        if (start0 < partitionStart && start1 < partitionStart) {
            // Both tuples will match in a different partition.
            return false;
        }
        try {
            IntervalJoinUtil.getIntervalPointable(accessorLeft, idLeft, tvp, ipLeft);
            IntervalJoinUtil.getIntervalPointable(accessorRight, idRight, tvp, ipRight);
            return compareInterval(ipLeft, ipRight);
        } catch (AsterixException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public boolean checkToSaveInResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        if (start0 < partitionStart && start1 < partitionStart) {
            // Both tuples will match in a different partition.
            return false;
        }
        try {
            IntervalJoinUtil.getIntervalPointable(accessorLeft, leftTupleIndex, idLeft, tvp, ipLeft);
            IntervalJoinUtil.getIntervalPointable(accessorRight, rightTupleIndex, idRight, tvp, ipRight);
            return compareInterval(ipLeft, ipRight);
        } catch (AsterixException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public boolean compareInterval(AIntervalPointable ipLeft, AIntervalPointable ipRight) throws AsterixException {
        return il.overlapping(ipLeft, ipRight);
    }

    @Override
    public boolean compareIntervalPartition(int s1, int e1, int s2, int e2) {
        return IntervalPartitionLogic.overlapping(s1, e1, s2, e2);
    }

}