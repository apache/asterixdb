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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

public abstract class AbstractIntervalInverseMergeJoinChecker extends AbstractIntervalMergeJoinChecker {

    private static final long serialVersionUID = 1L;

    public AbstractIntervalInverseMergeJoinChecker(int idLeft, int idRight) {
        super(idLeft, idRight);
    }

    @Override
    public boolean checkToSaveInMemory(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, idLeft);
        long end1 = IntervalJoinUtil.getIntervalEnd(accessorRight, idRight);
        return (start0 <= end1);
    }

    @Override
    public boolean checkToRemoveInMemory(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        return !checkToSaveInMemory( accessorLeft,  accessorRight);
    }

    @Override
    public boolean checkToSaveInResult(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, idLeft);
        long end0 = IntervalJoinUtil.getIntervalEnd(accessorLeft, idLeft);

        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, idRight);
        long end1 = IntervalJoinUtil.getIntervalEnd(accessorRight, idRight);

        return compareInterval(start0, end0, start1, end1);
    }

    @Override
    public boolean checkToSaveInResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long end0 = IntervalJoinUtil.getIntervalEnd(accessorLeft, leftTupleIndex, idLeft);

        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        long end1 = IntervalJoinUtil.getIntervalEnd(accessorRight, rightTupleIndex, idRight);

        return compareInterval(start0, end0, start1, end1);
    }
}