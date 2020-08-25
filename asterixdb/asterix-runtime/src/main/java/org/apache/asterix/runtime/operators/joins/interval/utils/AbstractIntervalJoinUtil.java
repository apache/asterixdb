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
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalLogic;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.IntervalJoinUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractIntervalJoinUtil implements IIntervalJoinUtil {

    protected final int idBuild;
    protected final int idProbe;

    protected final IntervalLogic il = new IntervalLogic();
    protected final AIntervalPointable ipBuild = (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();
    protected final AIntervalPointable ipProbe = (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();

    public AbstractIntervalJoinUtil(int idBuild, int idProbe) {
        this.idBuild = idBuild;
        this.idProbe = idProbe;
    }

    /**
     * Right (second argument) interval starts before left (first argument) interval ends.
     */
    @Override
    public boolean checkToSaveInMemory(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) {
        long buildStart = IntervalJoinUtil.getIntervalStart(buildAccessor, buildTupleIndex, idBuild);
        long probeStart = IntervalJoinUtil.getIntervalStart(probeAccessor, probeTupleIndex, idProbe);
        return buildStart <= probeStart;
    }

    /**
     * Left (first argument) interval starts after the Right (second argument) interval ends.
     */
    @Override
    public boolean checkToRemoveInMemory(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) {
        long buildStart = IntervalJoinUtil.getIntervalStart(buildAccessor, buildTupleIndex, idBuild);
        long probeStart = IntervalJoinUtil.getIntervalStart(probeAccessor, probeTupleIndex, idProbe);
        return buildStart > probeStart;
    }

    @Override
    public boolean checkToSaveInResult(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) throws HyracksDataException {
        IntervalJoinUtil.getIntervalPointable(buildAccessor, buildTupleIndex, idBuild, ipBuild);
        IntervalJoinUtil.getIntervalPointable(probeAccessor, probeTupleIndex, idProbe, ipProbe);
        return compareInterval(ipBuild, ipProbe);
    }

    /**
     * Right (second argument) interval starts before left (first argument) interval ends.
     */
    @Override
    public boolean checkForEarlyExit(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) {
        long buildEnd = IntervalJoinUtil.getIntervalEnd(buildAccessor, buildTupleIndex, idBuild);
        long probeStart = IntervalJoinUtil.getIntervalStart(probeAccessor, probeTupleIndex, idProbe);
        return buildEnd < probeStart;
    }

    @Override
    public abstract boolean compareInterval(AIntervalPointable ipBuild, AIntervalPointable ipProbe)
            throws HyracksDataException;

    @Override
    public boolean checkToLoadNextProbeTuple(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) {
        long buildEnd = IntervalJoinUtil.getIntervalEnd(buildAccessor, buildTupleIndex, idBuild);
        long probeStart = IntervalJoinUtil.getIntervalStart(probeAccessor, probeTupleIndex, idProbe);
        return buildEnd > probeStart;
    }
}
