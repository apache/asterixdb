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

import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IIntervalJoinUtil {

    /**
     * Check to see if the right tuple should be added to memory during the merge join.
     * The memory is used to check the right tuple with the remaining left tuples.
     * The check is true if the next left tuple could still match with this right tuple.
     *
     * @param buildAccessor
     * @param probeAccessor
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkToSaveInMemory(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex);

    /**
     * Check to see if the right tuple should be removed from memory during the merge join.
     * The memory is used to check the right tuple with the remaining left tuples.
     * The check is true if the next left tuple is NOT able match with this right tuple.
     *
     * @param buildAccessor
     * @param probeAccessor
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkToRemoveInMemory(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex);

    /**
     * Check to see if tuples match join condition
     * The check is true if they match.
     *
     * @param buildAccessor
     * @param buildTupleIndex
     * @param probeAccessor
     * @param probeTupleIndex
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkToSaveInResult(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) throws HyracksDataException;

    /**
     * Check to see if the interval matches the join condition.
     * The check is true if it matches.
     *
     * @param ipBuild
     * @param ipProbe
     * @return boolean
     * @throws HyracksDataException
     */
    boolean compareInterval(AIntervalPointable ipBuild, AIntervalPointable ipProbe) throws HyracksDataException;

    /**
     * Check to see if the left tuple should stop checking for matches.
     * The check is true if there can be no more matches
     *
     * @param buildAccessor
     * @param probeAccessor
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkForEarlyExit(IFrameTupleAccessor buildAccessor, int buildTupleIndex, IFrameTupleAccessor probeAccessor,
            int probeTupleIndex);

    /**
     * Check if next tuple should be loaded into memory.
     * The check is true if there are more tuples
     *
     * @param buildAccessor
     * @param probeAccessor
     * @return boolean
     * @throws HyracksDataException
     */
    boolean checkToLoadNextProbeTuple(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex);

}
