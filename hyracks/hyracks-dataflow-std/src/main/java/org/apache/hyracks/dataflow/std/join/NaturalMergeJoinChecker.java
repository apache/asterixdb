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
package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

public class NaturalMergeJoinChecker implements IMergeJoinChecker {
    private static final long serialVersionUID = 1L;
    FrameTuplePairComparator comparator;

    public NaturalMergeJoinChecker(FrameTuplePairComparator comparator) {
        this.comparator = comparator;
    }

    public boolean checkToSaveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        int c = comparator.compare(accessorLeft, leftTupleIndex, accessorRight, rightTupleIndex);
        return (c == 0);
    }

    public boolean checkToRemoveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        int c = comparator.compare(accessorLeft, leftTupleIndex, accessorRight, rightTupleIndex);
        return (c < 0);
    }

    public boolean checkToLoadNextRightTuple(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        int c = comparator.compare(accessorLeft, leftTupleIndex, accessorRight, rightTupleIndex);
        return (c <= 0);
    }

    @Override
    public boolean checkToSaveInResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException {
        int c = comparator.compare(accessorLeft, leftTupleIndex, accessorRight, rightTupleIndex);
        return (c == 0);
    }

}