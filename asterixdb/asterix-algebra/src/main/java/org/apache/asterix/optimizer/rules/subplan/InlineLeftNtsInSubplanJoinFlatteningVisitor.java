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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.visitors.IQueryOperatorVisitor;

/**
 * This visitor inlines the input {@code nts} in the query plan rooted at the operator being visited,
 * with the query plan rooted at the input {@code subplanInputOperator}.
 *
 * The visitor ensures that:
 * 1. live variables at {@code subplanInputOperator} are propagated to the top-most join operator in the query plan
 * rooted at the operator being visited.
 * 2. no available tuple at {@code subplanInputOperator} get lost along the pipeline to the top-most join operator
 * in the query plan rooted at the operator being visited.
 */
class InlineLeftNtsInSubplanJoinFlatteningVisitor implements IQueryOperatorVisitor<ILogicalOperator, Void> {
    // The optimization context.
    private final IOptimizationContext context;

    // The input operator to the subplan.
    private final ILogicalOperator subplanInputOperator;

    // The target Nts operator.
    private final ILogicalOperator targetNts;

    // The live variables in <code>subplanInputOperator</code> to enforce.
    private final Set<LogicalVariable> liveVarsFromSubplanInput = new HashSet<>();

    // The state indicate if the operator tree rooted at the current operator is rewritten.
    private boolean rewritten = false;

    // The state indicate if the operator tree rooted at the current operator contains a rewritten join.
    private boolean hasJoinAncestor = false;

    // A set of variables that are needed for not-null checks in the final group-by operator.
    private Set<LogicalVariable> nullCheckVars = new HashSet<LogicalVariable>();

    // The top join reference.
    private Mutable<ILogicalOperator> topJoinRef;

    /***
     * @param context
     *            the optimization context
     * @param subplanInputOperator
     *            the input operator to the target SubplanOperator
     * @param nts
     *            the NestedTupleSourceOperator to be replaced by <code>subplanInputOperator</code>
     * @throws AlgebricksException
     */
    public InlineLeftNtsInSubplanJoinFlatteningVisitor(IOptimizationContext context,
            ILogicalOperator subplanInputOperator, ILogicalOperator nts) throws AlgebricksException {
        this.context = context;
        this.subplanInputOperator = subplanInputOperator;
        this.targetNts = nts;
        VariableUtilities.getSubplanLocalLiveVariables(subplanInputOperator, liveVarsFromSubplanInput);
    }

    /**
     * @return a set of variables indicating whether a tuple from the right
     *         branch of a left-outer join is a non-match.
     */
    public Set<LogicalVariable> getNullCheckVariables() {
        return nullCheckVars;
    }

    /**
     * @return the top-most join operator after visiting the query plan rooted
     *         at the operator being visited.
     */
    public Mutable<ILogicalOperator> getTopJoinReference() {
        return topJoinRef;
    }

    @Override
    public ILogicalOperator visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitRunningAggregateOperator(RunningAggregateOperator op, Void arg)
            throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg)
            throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        hasJoinAncestor = true;
        boolean needToSwitch = false;
        for (int i = 0; i < op.getInputs().size(); ++i) {
            // Deals with single input operators.
            ILogicalOperator newChild = op.getInputs().get(i).getValue().accept(this, null);
            op.getInputs().get(i).setValue(newChild);
            if (i == 1) {
                needToSwitch = true;
            }
            if (rewritten) {
                break;
            }
        }

        // Checks whether there is a need to switch two join branches.
        if (rewritten && needToSwitch) {
            Mutable<ILogicalOperator> leftBranch = op.getInputs().get(0);
            Mutable<ILogicalOperator> rightBranch = op.getInputs().get(1);
            op.getInputs().set(0, rightBranch);
            op.getInputs().set(1, leftBranch);
        }
        AbstractBinaryJoinOperator returnOp = op;
        // After rewriting, the original inner join should become an left outer join.
        if (rewritten) {
            returnOp = new LeftOuterJoinOperator(op.getCondition());
            returnOp.setSourceLocation(op.getSourceLocation());
            returnOp.getInputs().addAll(op.getInputs());
            injectNullCheckVars(returnOp);
        }
        return returnOp;
    }

    @Override
    public ILogicalOperator visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        hasJoinAncestor = true;
        // Only rewrites the left child.
        ILogicalOperator newChild = op.getInputs().get(0).getValue().accept(this, null);
        op.getInputs().get(0).setValue(newChild);
        return op;
    }

    @Override
    public ILogicalOperator visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg)
            throws AlgebricksException {
        if (op == targetNts) {
            // Inlines the actual <code>subplanInputOperator</code>.
            rewritten = true;
            return subplanInputOperator;
        } else {
            return op;
        }
    }

    @Override
    public ILogicalOperator visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        boolean underJoin = hasJoinAncestor;
        visitSingleInputOperator(op);
        if (!rewritten || !underJoin) {
            return op;
        }

        // Adjust the ordering if its input operator pipeline has been rewritten.
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExprList = new ArrayList<>();
        // Adds keyVars to the prefix of sorting columns.
        for (LogicalVariable liveVar : liveVarsFromSubplanInput) {
            VariableReferenceExpression liveVarRef = new VariableReferenceExpression(liveVar);
            liveVarRef.setSourceLocation(op.getSourceLocation());
            orderExprList.add(new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER,
                    new MutableObject<ILogicalExpression>(liveVarRef)));
        }
        orderExprList.addAll(op.getOrderExpressions());

        // Creates an order operator with the new expression list.
        OrderOperator orderOp = new OrderOperator(orderExprList);
        orderOp.setSourceLocation(op.getSourceLocation());
        orderOp.getInputs().addAll(op.getInputs());
        context.computeAndSetTypeEnvironmentForOperator(orderOp);
        return orderOp;
    }

    @Override
    public ILogicalOperator visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        visitSingleInputOperator(op);
        return op;
    }

    @Override
    public ILogicalOperator visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        boolean underJoin = hasJoinAncestor;
        visitSingleInputOperator(op);
        if (!rewritten || !underJoin) {
            return op;
        }
        // Adds all missing variables that should propagates up.
        for (LogicalVariable keyVar : liveVarsFromSubplanInput) {
            if (!op.getVariables().contains(keyVar)) {
                op.getVariables().add(keyVar);
            }
        }
        return op;
    }

    @Override
    public ILogicalOperator visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        throw new UnsupportedOperationException("Script operators in a subplan are not supported!");
    }

    @Override
    public ILogicalOperator visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        throw new UnsupportedOperationException(
                "Nested subplans with a union operator should have been disqualified for this rewriting!");
    }

    @Override
    public ILogicalOperator visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        throw new UnsupportedOperationException(
                "Nested subplans with a intersect operator should have been disqualified for this rewriting!");
    }

    @Override
    public ILogicalOperator visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg)
            throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        visitSingleInputOperator(op);
        if (!rewritten) {
            return op;
        }
        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(op, liveVars);
        if (!liveVars.containsAll(liveVarsFromSubplanInput)) {
            op.setPropagatesInput(true);
        }
        return op;
    }

    @Override
    public ILogicalOperator visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg)
            throws AlgebricksException {
        throw new AlgebricksException(
                "The subquery de-correlation rule should always be applied before index-access-method related rules.");
    }

    @Override
    public ILogicalOperator visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        boolean underJoin = hasJoinAncestor;
        visitSingleInputOperator(op);
        if (!rewritten || !underJoin) {
            return op;
        }
        List<LogicalVariable> distinctVarList = op.getDistinctByVarList();
        for (LogicalVariable keyVar : liveVarsFromSubplanInput) {
            if (!distinctVarList.contains(keyVar)) {
                distinctVarList.add(keyVar);
            }
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
        return op;
    }

    @Override
    public ILogicalOperator visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        throw new UnsupportedOperationException(
                "Nested subplans with a forward operator should have been disqualified for this rewriting!");
    }

    @Override
    public ILogicalOperator visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    private ILogicalOperator visitSingleInputOperator(ILogicalOperator op) throws AlgebricksException {
        if (op.getInputs().size() == 1) {
            // Deals with single input operators.
            Mutable<ILogicalOperator> childRef = op.getInputs().get(0);
            ILogicalOperator newChild = childRef.getValue().accept(this, null);
            LogicalOperatorTag childOpTag = newChild.getOperatorTag();
            if (childOpTag == LogicalOperatorTag.INNERJOIN || childOpTag == LogicalOperatorTag.LEFTOUTERJOIN) {
                topJoinRef = childRef;
            }
            op.getInputs().get(0).setValue(newChild);
        }
        return op;
    }

    /**
     * Inject variables to indicate non-matches for the right branch of a left-outer join.
     *
     * @param joinOp
     *            the leftouter join operator.
     */
    private void injectNullCheckVars(AbstractBinaryJoinOperator joinOp) {
        LogicalVariable assignVar = context.newVar();
        AssignOperator assignOp =
                new AssignOperator(assignVar, new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        assignOp.setSourceLocation(joinOp.getSourceLocation());
        assignOp.getInputs().add(joinOp.getInputs().get(1));
        joinOp.getInputs().set(1, new MutableObject<ILogicalOperator>(assignOp));
        nullCheckVars.add(assignVar);
    }

}
