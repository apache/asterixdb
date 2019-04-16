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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Pushes aggregate functions into a stand alone aggregate operator (no group by).
 */
public class PushAggFuncIntoStandaloneAggregateRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // Pattern to match: assign <-- aggregate <-- !(group-by)
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assignOp = (AssignOperator) op;

        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        if (op2.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
            AggregateOperator aggOp = (AggregateOperator) op2;
            // Make sure the agg expr is a listify.
            return pushAggregateFunction(aggOp, assignOp, context);
        } else if (op2.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                || op2.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
            AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) op2;
            // Tries to push aggregates through the join.
            if (containsAggregate(assignOp.getExpressions()) && pushableThroughJoin(join)) {
                return pushAggregateFunctionThroughJoin(join, assignOp, context);
            }
        }
        return false;
    }

    /**
     * Recursively check whether the list of expressions contains an aggregate function.
     *
     * @param exprRefs
     * @return true if the list contains an aggregate function and false otherwise.
     */
    private boolean containsAggregate(List<Mutable<ILogicalExpression>> exprRefs) {
        for (Mutable<ILogicalExpression> exprRef : exprRefs) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier funcIdent = BuiltinFunctions.getAggregateFunction(funcExpr.getFunctionIdentifier());
            if (funcIdent == null) {
                // Recursively look in func args.
                if (containsAggregate(funcExpr.getArguments())) {
                    return true;
                }
            } else {
                // This is an aggregation function.
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether the join is aggregate-pushable, that is,
     * 1) the join condition is true;
     * 2) each join branch produces only one tuple.
     *
     * @param join
     * @return true if pushable
     */
    private boolean pushableThroughJoin(AbstractBinaryJoinOperator join) {
        ILogicalExpression condition = join.getCondition().getValue();
        if (condition.equals(ConstantExpression.TRUE)) {
            // Checks if the aggregation functions are pushable through the join
            boolean pushable = true;
            for (Mutable<ILogicalOperator> branchRef : join.getInputs()) {
                AbstractLogicalOperator branch = (AbstractLogicalOperator) branchRef.getValue();
                if (branch.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                    pushable &= true;
                } else if (branch.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                        || branch.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
                    AbstractBinaryJoinOperator childJoin = (AbstractBinaryJoinOperator) branch;
                    pushable &= pushableThroughJoin(childJoin);
                } else {
                    pushable &= false;
                }
            }
            return pushable;
        }
        return false;
    }

    /**
     * Does the actual push of aggregates for qualified joins.
     *
     * @param join
     * @param assignOp
     *            that contains aggregate function calls.
     * @param context
     * @throws AlgebricksException
     */
    private boolean pushAggregateFunctionThroughJoin(AbstractBinaryJoinOperator join, AssignOperator assignOp,
            IOptimizationContext context) throws AlgebricksException {
        boolean applied = false;
        for (Mutable<ILogicalOperator> branchRef : join.getInputs()) {
            AbstractLogicalOperator branch = (AbstractLogicalOperator) branchRef.getValue();
            if (branch.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                AggregateOperator aggOp = (AggregateOperator) branch;
                applied |= pushAggregateFunction(aggOp, assignOp, context);
            } else if (branch.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                    || branch.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
                AbstractBinaryJoinOperator childJoin = (AbstractBinaryJoinOperator) branch;
                applied |= pushAggregateFunctionThroughJoin(childJoin, assignOp, context);
            }
        }
        return applied;
    }

    private boolean pushAggregateFunction(AggregateOperator aggOp, AssignOperator assignOp,
            IOptimizationContext context) throws AlgebricksException {
        Mutable<ILogicalOperator> opRef3 = aggOp.getInputs().get(0);
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) opRef3.getValue();
        // If there's a group by below the agg, then we want to have the agg pushed into the group by
        if (op3.getOperatorTag() == LogicalOperatorTag.GROUP && !((GroupByOperator) op3).getNestedPlans().isEmpty()) {
            return false;
        }
        if (aggOp.getVariables().size() != 1) {
            return false;
        }
        ILogicalExpression aggExpr = aggOp.getExpressions().get(0).getValue();
        if (aggExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression origAggFuncExpr = (AbstractFunctionCallExpression) aggExpr;
        if (origAggFuncExpr.getFunctionIdentifier() != BuiltinFunctions.LISTIFY) {
            return false;
        }

        LogicalVariable aggVar = aggOp.getVariables().get(0);
        List<LogicalVariable> used = new LinkedList<LogicalVariable>();
        VariableUtilities.getUsedVariables(assignOp, used);
        if (!used.contains(aggVar)) {
            return false;
        }

        List<Mutable<ILogicalExpression>> srcAssignExprRefs = new LinkedList<Mutable<ILogicalExpression>>();
        findAggFuncExprRef(assignOp.getExpressions(), aggVar, srcAssignExprRefs);
        if (srcAssignExprRefs.isEmpty()) {
            return false;
        }

        AbstractFunctionCallExpression aggOpExpr =
                (AbstractFunctionCallExpression) aggOp.getExpressions().get(0).getValue();
        aggOp.getExpressions().clear();
        aggOp.getVariables().clear();

        for (Mutable<ILogicalExpression> srcAssignExprRef : srcAssignExprRefs) {
            AbstractFunctionCallExpression assignFuncExpr =
                    (AbstractFunctionCallExpression) srcAssignExprRef.getValue();
            FunctionIdentifier aggFuncIdent =
                    BuiltinFunctions.getAggregateFunction(assignFuncExpr.getFunctionIdentifier());

            // Push the agg func into the agg op.

            List<Mutable<ILogicalExpression>> aggArgs = new ArrayList<Mutable<ILogicalExpression>>();
            aggArgs.add(aggOpExpr.getArguments().get(0));
            int sz = assignFuncExpr.getArguments().size();
            aggArgs.addAll(assignFuncExpr.getArguments().subList(1, sz));
            AggregateFunctionCallExpression aggFuncExpr =
                    BuiltinFunctions.makeAggregateFunctionExpression(aggFuncIdent, aggArgs);

            aggFuncExpr.setSourceLocation(assignFuncExpr.getSourceLocation());
            LogicalVariable newVar = context.newVar();
            aggOp.getVariables().add(newVar);
            aggOp.getExpressions().add(new MutableObject<ILogicalExpression>(aggFuncExpr));

            // The assign now just "renames" the variable to make sure the upstream plan still works.
            VariableReferenceExpression newVarRef = new VariableReferenceExpression(newVar);
            newVarRef.setSourceLocation(assignFuncExpr.getSourceLocation());
            srcAssignExprRef.setValue(newVarRef);
        }

        context.computeAndSetTypeEnvironmentForOperator(aggOp);
        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        return true;
    }

    private void findAggFuncExprRef(List<Mutable<ILogicalExpression>> exprRefs, LogicalVariable aggVar,
            List<Mutable<ILogicalExpression>> srcAssignExprRefs) {
        for (Mutable<ILogicalExpression> exprRef : exprRefs) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
                FunctionIdentifier funcIdent = BuiltinFunctions.getAggregateFunction(funcExpr.getFunctionIdentifier());
                if (funcIdent != null
                        && aggVar.equals(SqlppVariableUtil.getVariable(funcExpr.getArguments().get(0).getValue()))) {
                    srcAssignExprRefs.add(exprRef);
                } else {
                    // Recursively look in func args.
                    findAggFuncExprRef(funcExpr.getArguments(), aggVar, srcAssignExprRefs);
                }
            }
        }
    }
}
