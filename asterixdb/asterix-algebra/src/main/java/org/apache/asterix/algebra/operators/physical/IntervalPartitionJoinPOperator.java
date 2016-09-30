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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.intervalpartition2.IntervalPartitionJoinOperatorDescriptor;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IRangeMap;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.RangeId;

public class IntervalPartitionJoinPOperator extends AbstractIntervalJoinPOperator {
    private static final int START = 0;
    private static final int END = 1;

    private final int memSizeInFrames;
    private final int k;
    private final List<LogicalVariable> leftPartitionVar;
    private final List<LogicalVariable> rightPartitionVar;

    private static final Logger LOGGER = Logger.getLogger(IntervalPartitionJoinPOperator.class.getName());

    public IntervalPartitionJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities,
            int memSizeInFrames, int k, IIntervalMergeJoinCheckerFactory mjcf, List<LogicalVariable> leftPartitionVar,
            List<LogicalVariable> rightPartitionVar, RangeId leftRangeId, RangeId rightRangeId,
            IRangeMap rangeMapHint) {
        super(kind, partitioningType, sideLeftOfEqualities, sideRightOfEqualities, mjcf, leftRangeId, rightRangeId,
                rangeMapHint);
        this.memSizeInFrames = memSizeInFrames;
        this.k = k;
        this.leftPartitionVar = leftPartitionVar;
        this.rightPartitionVar = rightPartitionVar;

        LOGGER.fine("IntervalPartitionJoinPOperator constructed with: JoinKind=" + kind + ", JoinPartitioningType="
                + partitioningType + ", List<LogicalVariable>=" + sideLeftOfEqualities + ", List<LogicalVariable>="
                + sideRightOfEqualities + ", int memSizeInFrames=" + memSizeInFrames + ", int k=" + k
                + ", IMergeJoinCheckerFactory mjcf=" + mjcf + ", RangeId leftRangeId=" + leftRangeId
                + ", RangeId rightRangeId=" + rightRangeId + ".");
    }

    public int getK() {
        return k;
    }

    public List<LogicalVariable> getLeftPartitionVar() {
        return leftPartitionVar;
    }

    public List<LogicalVariable> getRightPartitionVar() {
        return rightPartitionVar;
    }

    @Override
    public String getIntervalJoin() {
        return "INTERVAL_PARTITION_JOIN";
    }

    @Override
    IOperatorDescriptor getIntervalOperatorDescriptor(int[] keysLeft, int[] keysRight, IOperatorDescriptorRegistry spec,
            RecordDescriptor recordDescriptor, IIntervalMergeJoinCheckerFactory mjcf, RangeId rangeId) {
        return new IntervalPartitionJoinOperatorDescriptor(spec, memSizeInFrames, k, keysLeft, keysRight,
                recordDescriptor, mjcf, rangeId);
    }

    @Override
    protected ArrayList<OrderColumn> getLeftLocalSortOrderColumn() {
        ArrayList<OrderColumn> order = new ArrayList<>();
        if (mjcf.isOrderAsc()) {
            order.add(new OrderColumn(leftPartitionVar.get(END), OrderKind.ASC));
            order.add(new OrderColumn(leftPartitionVar.get(START), OrderKind.DESC));
        } else {
            // TODO What does Desc'ing mean?
            order.add(new OrderColumn(leftPartitionVar.get(START), OrderKind.ASC));
            order.add(new OrderColumn(leftPartitionVar.get(END), OrderKind.DESC));
        }
        return order;
    }

    @Override
    protected ArrayList<OrderColumn> getRightLocalSortOrderColumn() {
        ArrayList<OrderColumn> order = new ArrayList<>();
        if (mjcf.isOrderAsc()) {
            order.add(new OrderColumn(rightPartitionVar.get(END), OrderKind.ASC));
            order.add(new OrderColumn(rightPartitionVar.get(START), OrderKind.DESC));
        } else {
            // TODO What does Desc'ing mean?
            order.add(new OrderColumn(rightPartitionVar.get(START), OrderKind.ASC));
            order.add(new OrderColumn(rightPartitionVar.get(END), OrderKind.DESC));
        }
        return order;
    }

}