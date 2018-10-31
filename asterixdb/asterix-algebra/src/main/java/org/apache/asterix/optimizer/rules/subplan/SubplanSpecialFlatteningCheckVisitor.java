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
package org.apache.asterix.optimizer.rules.subplan;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.IQueryOperatorVisitor;

/**
 * This visitor determines whether a nested subplan in a SubplanOperator could be rewritten as
 * a join-related special case.
 * The conditions include:
 * a. there is a join (let's call it J1.) in the nested plan,
 * b. if J1 is an inner join, one input pipeline of J1 has a NestedTupleSource descendant (let's call it N1),
 * c. if J1 is a left outer join, the left branch of J1 has a NestedTupleSource descendant (let's call it N1),
 * d. there is no tuple dropping from N1 to J1.
 */
class SubplanSpecialFlatteningCheckVisitor implements IQueryOperatorVisitor<Boolean, Void> {
    // Accept with an assumption that there doesn't exist an ancestor operator like J1 of the visiting
    // tuple discarding or cardinality reducing operator.
    // That is, a tuple discarding or cardinality operator like SelectOperator could not be
    // on the path from N1 to J1, but could be on top of J1.
    // For instance, during the post order visiting, if we hit a SelectOperator, we set rejectPending
    // bit to be true, and later if we backtrack to J1 which is an ancestor of N1, we think the path
    // from N1 to J1 is not valid because condition d is not met.
    // Then we reset rejectPending to false and traverse another child of J1.
    private boolean rejectPending = false;

    // The qualified NTS to be replaced.
    private ILogicalOperator qualifiedNtsOp;

    public ILogicalOperator getQualifiedNts() {
        return qualifiedNtsOp;
    }

    @Override
    public Boolean visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        return visitCardinalityReduceOperator(op);
    }

    @Override
    public Boolean visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
        return visitCardinalityReduceOperator(op);
    }

    @Override
    public Boolean visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
        return false;
    }

    @Override
    public Boolean visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        return visitCardinalityReduceOperator(op);
    }

    @Override
    public Boolean visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        return visitTupleDiscardingOperator(op);
    }

    @Override
    public Boolean visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            if (childRef.getValue().accept(this, null) && !rejectPending) {
                return true;
            }
            rejectPending = false;
        }
        return false;
    }

    @Override
    public Boolean visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        // Check whether the left branch is a qualified branch.
        boolean isLeftBranchQualified = op.getInputs().get(0).getValue().accept(this, null);
        return !rejectPending && isLeftBranchQualified;
    }

    @Override
    public Boolean visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
        qualifiedNtsOp = op;
        return true;
    }

    @Override
    public Boolean visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        return visitTupleDiscardingOperator(op);
    }

    @Override
    public Boolean visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        return false;
    }

    @Override
    public Boolean visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        return false;
    }

    @Override
    public Boolean visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        // Flattening with an Union Operator in the pipeline will perturb the query semantics,
        // e.g., the result cardinality can change.
        return false;
    }

    @Override
    public Boolean visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        return false;
    }

    @Override
    public Boolean visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        return false;
    }

    @Override
    public Boolean visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg) throws AlgebricksException {
        return false;
    }

    @Override
    public Boolean visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        return false;
    }

    @Override
    public Boolean visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    @Override
    public Boolean visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        throw new CompilationException(ErrorCode.COMPILATION_ERROR, op.getSourceLocation(),
                "Forward operator should have been disqualified for this rewriting!");
    }

    @Override
    public Boolean visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        return visitInputs(op);
    }

    private boolean visitInputs(ILogicalOperator op) throws AlgebricksException {
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            if (childRef.getValue().accept(this, null)) {
                // One input is qualified.
                return true;
            }
        }
        // All inputs are disqualified.
        return false;
    }

    /**
     * If an operator reduces its input cardinality, the operator should not be a descendant
     * of a join operator.
     *
     * @param op
     *            ,
     *            the operator to consider
     * @return FALSE if it is certainly disqualified; TRUE otherwise.
     * @throws AlgebricksException
     */
    private Boolean visitCardinalityReduceOperator(ILogicalOperator op) throws AlgebricksException {
        return visitTupleDiscardingOrCardinalityReduceOperator(op);
    }

    /**
     * If an operator discard tuples variables before the join, the query's
     * semantics cannot be preserved after applying the <code>FlatternSubplanJoinRule</code> rule.
     *
     * @param op
     *            ,
     *            the operator to consider
     * @return FALSE if it is certainly disqualified; TRUE otherwise.
     * @throws AlgebricksException
     */
    private Boolean visitTupleDiscardingOperator(ILogicalOperator op) throws AlgebricksException {
        return visitTupleDiscardingOrCardinalityReduceOperator(op);
    }

    private boolean visitTupleDiscardingOrCardinalityReduceOperator(ILogicalOperator op) throws AlgebricksException {
        boolean result = visitInputs(op);
        rejectPending = true;
        return result;
    }

}
