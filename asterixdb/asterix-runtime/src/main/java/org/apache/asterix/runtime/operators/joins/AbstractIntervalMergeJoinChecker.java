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
import org.apache.asterix.runtime.evaluators.comparisons.ComparisonHelper;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalLogic;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;

public abstract class AbstractIntervalMergeJoinChecker implements IIntervalMergeJoinChecker {

    private static final long serialVersionUID = 1L;

    protected final int idLeft;
    protected final int idRight;

    protected final IntervalLogic il = new IntervalLogic();

    protected final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    protected final AIntervalPointable ipLeft = (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();
    protected final AIntervalPointable ipRight = (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();

    protected final ComparisonHelper ch = new ComparisonHelper();
    protected final IPointable startLeft = VoidPointable.FACTORY.createPointable();
    protected final IPointable endLeft = VoidPointable.FACTORY.createPointable();
    protected final IPointable startRight = VoidPointable.FACTORY.createPointable();
    protected final IPointable endRight = VoidPointable.FACTORY.createPointable();

    public AbstractIntervalMergeJoinChecker(int idLeft, int idRight) {
        this.idLeft = idLeft;
        this.idRight = idRight;
    }

    public boolean checkToRemoveLeftActive() {
        return true;
    }

    public boolean checkToRemoveRightActive() {
        return true;
    }

    public boolean checkToSaveInMemory(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        try {
            IntervalJoinUtil.getIntervalPointable(accessorLeft, idLeft, tvp, ipLeft);
            IntervalJoinUtil.getIntervalPointable(accessorRight, idRight, tvp, ipRight);
            ipLeft.getEnd(endLeft);
            ipRight.getStart(startRight);
            return ch.compare(ipLeft.getTypeTag(), ipRight.getTypeTag(), endLeft, startRight) >= 0;
        } catch (AsterixException e) {
            throw new HyracksDataException(e);
        }
    }

    public boolean checkToRemoveInMemory(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        try {
            IntervalJoinUtil.getIntervalPointable(accessorLeft, idLeft, tvp, ipLeft);
            IntervalJoinUtil.getIntervalPointable(accessorRight, idRight, tvp, ipRight);
            ipLeft.getStart(startLeft);
            ipRight.getEnd(endRight);
            return !(ch.compare(ipLeft.getTypeTag(), ipRight.getTypeTag(), startLeft, endRight) <= 0);
        } catch (AsterixException e) {
            throw new HyracksDataException(e);
        }
    }

    public boolean checkToLoadNextRightTuple(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        return checkToSaveInMemory(accessorLeft, accessorRight);
    }

    public boolean checkToSaveInResult(ITupleAccessor accessorLeft, ITupleAccessor accessorRight)
            throws HyracksDataException {
        try {
            IntervalJoinUtil.getIntervalPointable(accessorLeft, idLeft, tvp, ipLeft);
            IntervalJoinUtil.getIntervalPointable(accessorRight, idRight, tvp, ipRight);
            return compareInterval(ipLeft, ipRight);
        } catch (AsterixException e) {
            throw new HyracksDataException(e);
        }
    }

    public boolean checkToSaveInResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        try {
            IntervalJoinUtil.getIntervalPointable(accessorLeft, leftTupleIndex, idLeft, tvp, ipLeft);
            IntervalJoinUtil.getIntervalPointable(accessorRight, rightTupleIndex, idRight, tvp, ipRight);
            return compareInterval(ipLeft, ipRight);
        } catch (AsterixException e) {
            throw new HyracksDataException(e);
        }
    }

    public abstract boolean compareInterval(AIntervalPointable ipLeft, AIntervalPointable ipRight)
            throws AsterixException;

    public abstract boolean compareIntervalPartition(int s1, int e1, int s2, int e2);

}