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

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.join.IMergeJoinChecker;

public abstract class AbstractIntervalMergeJoinChecker implements IMergeJoinChecker {

    private static final long serialVersionUID = 1L;
    protected final int idLeft;
    protected final int idRight;

    public AbstractIntervalMergeJoinChecker(int idLeft, int idRight) {
        this.idLeft = idLeft;
        this.idRight = idRight;
    }

    protected long getIntervalStart(IFrameTupleAccessor accessor, int tupleId, int fieldId)
            throws HyracksDataException {
        int start = accessor.getTupleStartOffset(tupleId) + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(tupleId, fieldId) + 1;
        return AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), start);
    }

    protected long getIntervalEnd(IFrameTupleAccessor accessor, int tupleId, int fieldId) throws HyracksDataException {
        int start = accessor.getTupleStartOffset(tupleId) + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(tupleId, fieldId) + 1;
        return AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), start);
    }

    public boolean checkToSaveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long end0 = getIntervalEnd(accessorLeft, leftTupleIndex, idLeft);
        long start1 = getIntervalStart(accessorRight, rightTupleIndex, idRight);
        return (start1 < end0);
    }

    public boolean checkToRemoveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long start0 = getIntervalStart(accessorLeft, leftTupleIndex, idLeft);
        long end1 = getIntervalEnd(accessorRight, rightTupleIndex, idRight);
        return (end1 < start0);
    }

    public boolean checkToLoadNextRightTuple(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        long end0 = getIntervalEnd(accessorLeft, leftTupleIndex, idLeft);
        long start1 = getIntervalStart(accessorRight, rightTupleIndex, idRight);
        return (start1 < end0);
    }

}