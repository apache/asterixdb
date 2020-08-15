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
package org.apache.asterix.runtime.operators.joins.interval.utils;

import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalLogic;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.IntervalJoinUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public abstract class AbstractIntervalJoinUtil implements IIntervalJoinUtil {

    protected final int idLeft;
    protected final int idRight;

    protected final IntervalLogic il = new IntervalLogic();

    protected final TaggedValuePointable tvp = TaggedValuePointable.FACTORY.createPointable();
    protected final AIntervalPointable ipLeft = (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();
    protected final AIntervalPointable ipRight = (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();

    protected final IBinaryComparator ch = BinaryComparatorFactoryProvider.INSTANCE
            .getBinaryComparatorFactory(BuiltinType.ANY, BuiltinType.ANY, true).createBinaryComparator();
    protected final IPointable startLeft = VoidPointable.FACTORY.createPointable();
    protected final IPointable startRight = VoidPointable.FACTORY.createPointable();

    public AbstractIntervalJoinUtil(int idLeft, int idRight) {
        this.idLeft = idLeft;
        this.idRight = idRight;
    }

    /**
     * Right (second argument) interval starts before left (first argument) interval ends.
     */
    @Override
    public boolean checkToSaveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) {
        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        return start0 <= start1;
    }

    /**
     * Left (first argument) interval starts after the Right (second argument) interval ends.
     */
    @Override
    public boolean checkToRemoveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) {
        long start0 = IntervalJoinUtil.getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        return start0 > start1;
    }

    /**
     * Left (first argument) interval starts after the Right (second argument) interval ends.
     */
    @Override
    public boolean checkIfMoreMatches(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) {
        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        long end0 = IntervalJoinUtil.getIntervalEnd(accessorLeft, leftTupleIndex, idLeft);
        return end0 > start1;
    }

    @Override
    public boolean checkToSaveInResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex, boolean reversed) throws HyracksDataException {
        if (reversed) {
            IntervalJoinUtil.getIntervalPointable(accessorLeft, leftTupleIndex, idLeft, ipRight);
            IntervalJoinUtil.getIntervalPointable(accessorRight, rightTupleIndex, idRight, ipLeft);
        } else {
            IntervalJoinUtil.getIntervalPointable(accessorLeft, leftTupleIndex, idLeft, ipLeft);
            IntervalJoinUtil.getIntervalPointable(accessorRight, rightTupleIndex, idRight, ipRight);
        }
        return compareInterval(ipLeft, ipRight);
    }

    /**
     * Right (second argument) interval starts before left (first argument) interval ends.
     */
    @Override
    public boolean checkForEarlyExit(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) {
        long start1 = IntervalJoinUtil.getIntervalStart(accessorRight, rightTupleIndex, idRight);
        long end0 = IntervalJoinUtil.getIntervalEnd(accessorLeft, leftTupleIndex, idLeft);
        return end0 < start1;
    }

    @Override
    public abstract boolean compareInterval(AIntervalPointable ipLeft, AIntervalPointable ipRight)
            throws HyracksDataException;

}
