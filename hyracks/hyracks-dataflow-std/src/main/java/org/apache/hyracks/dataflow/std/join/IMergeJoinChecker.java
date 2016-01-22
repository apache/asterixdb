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

import java.io.Serializable;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IMergeJoinChecker extends Serializable {

    /**
     * Check to see if the right tuple should be added to memory during the merge join.
     *
     * The memory is used to check the right tuple with the remaining left tuples.
     * The check is true if the next left tuple could still match with this right tuple.
     *
     * @param accessorLeft
     * @param leftTupleIndex
     * @param accessorRight
     * @param rightTupleIndex
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkToSaveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex) throws HyracksDataException;

    /**
     * Check to see if the right tuple should be removed from memory during the merge join.
     *
     * The memory is used to check the right tuple with the remaining left tuples.
     * The check is true if the next left tuple is NOT able match with this right tuple.
     *
     * @param accessorLeft
     * @param leftTupleIndex
     * @param accessorRight
     * @param rightTupleIndex
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkToRemoveInMemory(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException;

    /**
     * Check to see if the next right tuple should be loaded during the merge join.
     *
     * The check is true if the left tuple could match with the next right tuple.
     * Once the left tuple can no long match, the check returns false.
     *
     * @param accessorLeft
     * @param leftTupleIndex
     * @param accessorRight
     * @param rightTupleIndex
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkToLoadNextRightTuple(IFrameTupleAccessor accessorLeft, int leftTupleIndex,
            IFrameTupleAccessor accessorRight, int rightTupleIndex) throws HyracksDataException;

    boolean checkToSaveInResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex) throws HyracksDataException;
}