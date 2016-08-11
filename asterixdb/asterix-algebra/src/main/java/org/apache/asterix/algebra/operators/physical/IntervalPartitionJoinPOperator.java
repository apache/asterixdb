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
package org.apache.asterix.algebra.operators.physical;

import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.intervalpartition.IntervalPartitionJoinOperatorDescriptor;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRangeMap;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.RangeId;

public class IntervalPartitionJoinPOperator extends AbstractIntervalJoinPOperator {

    private final int memSizeInFrames;
    private final long probeTupleCount;
    private final long probeMaxDuration;
    private final long buildTupleCount;
    private final long buildMaxDuration;
    private final int avgTuplesInFrame;

    private static final Logger LOGGER = Logger.getLogger(IntervalPartitionJoinPOperator.class.getName());

    public IntervalPartitionJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities,
            int memSizeInFrames, long buildTupleCount, long probeTupleCount, long buildMaxDuration,
            long probeMaxDuration, int avgTuplesInFrame, IIntervalMergeJoinCheckerFactory mjcf, RangeId leftRangeId,
            RangeId rightRangeId, IRangeMap rangeMapHint) {
        super(kind, partitioningType, sideLeftOfEqualities, sideRightOfEqualities, mjcf, leftRangeId, rightRangeId,
                rangeMapHint);
        this.memSizeInFrames = memSizeInFrames;
        this.buildTupleCount = buildTupleCount;
        this.probeTupleCount = probeTupleCount;
        this.buildMaxDuration = buildMaxDuration;
        this.probeMaxDuration = probeMaxDuration;
        this.avgTuplesInFrame = avgTuplesInFrame;

        LOGGER.fine("IntervalPartitionJoinPOperator constructed with: JoinKind=" + kind + ", JoinPartitioningType="
                + partitioningType + ", List<LogicalVariable>=" + sideLeftOfEqualities + ", List<LogicalVariable>="
                + sideRightOfEqualities + ", int memSizeInFrames=" + memSizeInFrames + ", int buildTupleCount="
                + buildTupleCount + ", int probeTupleCount=" + probeTupleCount + ", int buildMaxDuration="
                + buildMaxDuration + ", int probeMaxDuration=" + probeMaxDuration + ", int avgTuplesInFrame="
                + avgTuplesInFrame + ", IMergeJoinCheckerFactory mjcf=" + mjcf + ", RangeId leftRangeId=" + leftRangeId
                + ", RangeId rightRangeId=" + rightRangeId + ".");
    }

    public long getProbeTupleCount() {
        return probeTupleCount;
    }

    public long getProbeMaxDuration() {
        return probeMaxDuration;
    }

    public long getBuildTupleCount() {
        return buildTupleCount;
    }

    public long getBuildMaxDuration() {
        return buildMaxDuration;
    }

    public int getAvgTuplesInFrame() {
        return avgTuplesInFrame;
    }

    @Override
    public String getIntervalJoin() {
        return "INTERVAL_PARTITION_JOIN";
    }

    @Override
    IOperatorDescriptor getIntervalOperatorDescriptor(int[] keysLeft, int[] keysRight, IOperatorDescriptorRegistry spec,
            RecordDescriptor recordDescriptor, IIntervalMergeJoinCheckerFactory mjcf, RangeId rangeId) {
        return new IntervalPartitionJoinOperatorDescriptor(spec, memSizeInFrames, buildTupleCount, probeTupleCount,
                buildMaxDuration, probeMaxDuration, avgTuplesInFrame, keysLeft, keysRight, recordDescriptor, mjcf,
                rangeId);
    }

}