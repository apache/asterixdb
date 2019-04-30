/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.app.resource;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.WindowPOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.api.job.resource.IClusterCapacity;

// The current implementation aggregates the memory requirement for each operator.
// TODO(buyingyi): consider stages for calculating the memory requirement.
public class RequiredCapacityVisitor implements ILogicalOperatorVisitor<Void, Void> {

    private static final long MAX_BUFFER_PER_CONNECTION = 1L;

    private final long numComputationPartitions;
    private final long groupByMemorySize;
    private final long joinMemorySize;
    private final long sortMemorySize;
    private final long frameSize;
    private final IClusterCapacity clusterCapacity;
    private final Set<ILogicalOperator> visitedOperators = new HashSet<>();
    private long stageMemorySoFar = 0L;

    public RequiredCapacityVisitor(int numComputationPartitions, int sortFrameLimit, int groupFrameLimit,
            int joinFrameLimit, int frameSize, IClusterCapacity clusterCapacity) {
        this.numComputationPartitions = numComputationPartitions;
        this.frameSize = frameSize;
        this.groupByMemorySize = groupFrameLimit * (long) frameSize;
        this.joinMemorySize = joinFrameLimit * (long) frameSize;
        this.sortMemorySize = sortFrameLimit * (long) frameSize;
        this.clusterCapacity = clusterCapacity;
        this.clusterCapacity.setAggregatedCores(1); // At least one core is needed.
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        calculateMemoryUsageForBlockingOperators(op, groupByMemorySize);
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        calculateMemoryUsageForBlockingOperators(op, joinMemorySize);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        calculateMemoryUsageForBlockingOperators(op, joinMemorySize);
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        calculateMemoryUsageForBlockingOperators(op, sortMemorySize);
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        // Makes sure that the downstream of a replicate operator is only visited once.
        if (!visitedOperators.contains(op)) {
            visitedOperators.add(op);
            visitInternal(op, true);
        }
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        // Makes sure that the downstream of a split operator is only visited once.
        if (!visitedOperators.contains(op)) {
            visitedOperators.add(op);
            visitInternal(op, true);
        }
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        calculateMemoryUsageForExchange(op);
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void arg)
            throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        visitInternal(op, true);
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        WindowPOperator physOp = (WindowPOperator) op.getPhysicalOperator();
        visitInternal(op, true);
        addOutputBuffer(op); // + previous frame
        if (physOp.getOperatorTag() == PhysicalOperatorTag.WINDOW) {
            addOutputBuffer(op); // + run frame
        }
        return null;
    }

    // Calculates the memory usage for exchange operators.
    private void calculateMemoryUsageForExchange(ExchangeOperator op) throws AlgebricksException {
        visitInternal(op, false);
        IPhysicalOperator physicalOperator = op.getPhysicalOperator();
        PhysicalOperatorTag physicalOperatorTag = physicalOperator.getOperatorTag();
        if (physicalOperatorTag == PhysicalOperatorTag.ONE_TO_ONE_EXCHANGE
                || physicalOperatorTag == PhysicalOperatorTag.SORT_MERGE_EXCHANGE) {
            addOutputBuffer(op);
            return;
        }
        stageMemorySoFar +=
                2L * MAX_BUFFER_PER_CONNECTION * numComputationPartitions * numComputationPartitions * frameSize;
        clusterCapacity.setAggregatedMemoryByteSize(stageMemorySoFar);
    }

    // Calculates the cluster-wide memory usage for blocking activities like group-by, sort, and join.
    private void calculateMemoryUsageForBlockingOperators(ILogicalOperator op, long memSize)
            throws AlgebricksException {
        visitInternal(op, false);
        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED
                || op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL) {
            stageMemorySoFar += memSize * numComputationPartitions;
        } else {
            stageMemorySoFar += memSize;
        }
        clusterCapacity.setAggregatedMemoryByteSize(stageMemorySoFar);
    }

    // Recursively visits input operators of an operator and sets the CPU core usage.
    private void visitInternal(ILogicalOperator op, boolean toAddOuputBuffer) throws AlgebricksException {
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            inputOpRef.getValue().accept(this, null);
        }
        if (toAddOuputBuffer) {
            addOutputBuffer(op);
        }
        setAvailableCores(op);
    }

    // Adds output buffer for an operator.
    private void addOutputBuffer(ILogicalOperator op) {
        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED
                || op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL) {
            stageMemorySoFar += frameSize * numComputationPartitions; // every operator needs one output buffer.
        } else {
            stageMemorySoFar += frameSize; // every operator needs one output buffer.
        }
        clusterCapacity.setAggregatedMemoryByteSize(stageMemorySoFar);
    }

    // Sets the number of available cores
    private void setAvailableCores(ILogicalOperator op) {
        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED
                || op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL) {
            clusterCapacity.setAggregatedCores((int) numComputationPartitions);
        }
    }
}
