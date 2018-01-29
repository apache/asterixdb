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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.util.annotations.NotThreadSafe;

@NotThreadSafe
public class PlanStagesGenerator implements ILogicalOperatorVisitor<Void, Void> {

    private static final int JOIN_FIRST_INPUT = 1;
    private static final int JOIN_SECOND_INPUT = 2;
    private final Set<ILogicalOperator> visitedOperators = new HashSet<>();
    private final LinkedList<ILogicalOperator> pendingBlockingOperators = new LinkedList<>();
    private final List<PlanStage> stages = new ArrayList<>();
    private PlanStage currentStage;
    private int stageCounter;

    public PlanStagesGenerator() {
        currentStage = new PlanStage(++stageCounter);
        stages.add(currentStage);
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        // Makes sure that the downstream of a replicate operator is only visited once.
        if (!visitedOperators.contains(op)) {
            visitedOperators.add(op);
            visit(op);
        } else {
            merge(op);
        }
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        // Makes sure that the downstream of a split operator is only visited once.
        if (!visitedOperators.contains(op)) {
            visitedOperators.add(op);
            visit(op);
        } else {
            merge(op);
        }
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void arg)
            throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    public List<PlanStage> getStages() {
        return stages;
    }

    private void visit(ILogicalOperator op) throws AlgebricksException {
        addToStage(op);
        if (!pendingBlockingOperators.isEmpty()) {
            final ILogicalOperator firstPending = pendingBlockingOperators.pop();
            visitBlocking(firstPending);
        }
    }

    private void visitBlocking(ILogicalOperator blockingOp) throws AlgebricksException {
        final PlanStage blockingOpStage = new PlanStage(++stageCounter);
        blockingOpStage.getOperators().add(blockingOp);
        stages.add(blockingOpStage);
        currentStage = blockingOpStage;
        switch (blockingOp.getOperatorTag()) {
            case INNERJOIN:
            case LEFTOUTERJOIN:
                // visit only the second input
                ILogicalOperator joinSecondInput = getJoinOperatorInput(blockingOp, JOIN_SECOND_INPUT);
                joinSecondInput.accept(this, null);
                break;
            case GROUP:
            case ORDER:
                visitInputs(blockingOp);
                break;
            default:
                throw new IllegalStateException("Unrecognized blocking operator: " + blockingOp.getOperatorTag());
        }
    }

    private void addToStage(ILogicalOperator op) throws AlgebricksException {
        currentStage.getOperators().add(op);
        switch (op.getOperatorTag()) {
            case INNERJOIN:
            case LEFTOUTERJOIN:
                pendingBlockingOperators.add(op);
                // continue on the same stage
                final ILogicalOperator joinFirstInput = getJoinOperatorInput(op, JOIN_FIRST_INPUT);
                joinFirstInput.accept(this, null);
                break;
            case GROUP:
                if (isBlockingGroupBy((GroupByOperator) op)) {
                    pendingBlockingOperators.add(op);
                    return;
                }
                // continue on the same stage
                visitInputs(op);
                break;
            case ORDER:
                pendingBlockingOperators.add(op);
                break;
            default:
                visitInputs(op);
                break;
        }
    }

    private void visitInputs(ILogicalOperator op) throws AlgebricksException {
        if (isMaterialized(op)) {
            // don't visit the inputs of this operator since it is supposed to be blocking due to materialization.
            // some other non-blocking operator will visit those inputs when reached.
            return;
        }
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            inputOpRef.getValue().accept(this, null);
        }
    }

    private boolean isBlockingGroupBy(GroupByOperator op) {
        return op.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.EXTERNAL_GROUP_BY
                || op.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.SORT_GROUP_BY;
    }

    /**
     * Checks whether the operator {@code op} is supposed to be materialized
     * due to a replicate/split operators.
     *
     * @param op
     * @return true if the operator will be materialized. Otherwise false
     */
    private boolean isMaterialized(ILogicalOperator op) {
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            final ILogicalOperator inputOp = inputOpRef.getValue();
            final LogicalOperatorTag inputOpTag = inputOp.getOperatorTag();
            if (inputOpTag == LogicalOperatorTag.REPLICATE || inputOpTag == LogicalOperatorTag.SPLIT) {
                final AbstractReplicateOperator replicateOperator = (AbstractReplicateOperator) inputOp;
                if (replicateOperator.isMaterialized(op)) {
                    return true;
                }
            }
        }
        return false;
    }

    private ILogicalOperator getJoinOperatorInput(ILogicalOperator op, int inputNum) {
        if (inputNum != JOIN_FIRST_INPUT && inputNum != JOIN_SECOND_INPUT) {
            throw new IllegalArgumentException("invalid input number for join operator");
        }
        final List<Mutable<ILogicalOperator>> inputs = op.getInputs();
        if (inputs.size() != 2) {
            throw new IllegalStateException("Join must have exactly two inputs. Current inputs: " + inputs.size());
        }
        return op.getInputs().get(inputNum - 1).getValue();
    }

    /**
     * Merges all operators on the current stage to the stage on which {@code op} appeared.
     *
     * @param op
     */
    private void merge(ILogicalOperator op) {
        // all operators in this stage belong to the stage of the already visited op
        for (PlanStage stage : stages) {
            if (stage != currentStage && stage.getOperators().contains(op)) {
                stage.getOperators().addAll(currentStage.getOperators());
                stages.remove(currentStage);
                currentStage = stage;
                break;
            }
        }
    }
}
