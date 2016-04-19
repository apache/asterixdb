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
import org.apache.asterix.runtime.operators.joins.intervalindex.IntervalIndexJoinOperatorDescriptor;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;

public class IntervalIndexJoinPOperator extends AbstractIntervalJoinPOperator {

    private final int memSizeInFrames;
    private final int leftCountInFrames;
    private final int rightCountInFrames;

    private static final Logger LOGGER = Logger.getLogger(IntervalIndexJoinPOperator.class.getName());

    public IntervalIndexJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities,
            int memSizeInFrames, int leftCountInFrames, int rightCountInFrames, IIntervalMergeJoinCheckerFactory mjcf,
            IRangeMap rangeMap) {
        super(kind, partitioningType, sideLeftOfEqualities, sideRightOfEqualities, mjcf, rangeMap);
        this.memSizeInFrames = memSizeInFrames;
        this.leftCountInFrames = leftCountInFrames;
        this.rightCountInFrames = rightCountInFrames;

        LOGGER.fine("IntervalIndexJoinPOperator constructed with: JoinKind=" + kind + ", JoinPartitioningType="
                + partitioningType + ", List<LogicalVariable>=" + sideLeftOfEqualities + ", List<LogicalVariable>="
                + sideRightOfEqualities + ", int memSizeInFrames=" + memSizeInFrames + ", int leftCountInFrames="
                + leftCountInFrames + ", int rightCountInFrames=" + rightCountInFrames
                + ", IMergeJoinCheckerFactory mjcf=" + mjcf + ", IRangeMap rangeMap=" + rangeMap + ".");
    }

    @Override
    public String getIntervalJoin() {
        return "INTERVAL_INDEX_JOIN";
    }

    @Override
    IOperatorDescriptor getIntervalOperatorDescriptor(int[] keysLeft, int[] keysRight, IOperatorDescriptorRegistry spec,
            RecordDescriptor recordDescriptor, IIntervalMergeJoinCheckerFactory mjcf, IRangeMap rangeMap) {
        return new IntervalIndexJoinOperatorDescriptor(spec, memSizeInFrames, leftCountInFrames, rightCountInFrames,
                keysLeft, keysRight, recordDescriptor, mjcf);
    }

}