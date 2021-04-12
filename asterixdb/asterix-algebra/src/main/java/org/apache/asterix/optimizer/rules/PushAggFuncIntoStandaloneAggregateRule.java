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
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
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
            return pushAggregateFunction(assignOp, aggOp, context);
        } else if (op2.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                || op2.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
            AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) op2;
            // Tries to push aggregates through the join.
            if (containsAggregate(assignOp.getExpressions()) && pushableThroughJoin(join)) {
                return pushAggregateFunctionThroughJoin(assignOp, join, context);
            }
        }
        return false;
    }

    /**
     * Recursively check whether the list of expressions contains an aggregate function.
     *
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
     * @param assignOp
     *            that contains aggregate function calls.
     * @param context
     * @throws AlgebricksException
     */
    private boolean pushAggregateFunctionThroughJoin(AssignOperator assignOp, AbstractBinaryJoinOperator join,
            IOptimizationContext context) throws AlgebricksException {
        boolean applied = false;
        for (Mutable<ILogicalOperator> branchRef : join.getInputs()) {
            AbstractLogicalOperator branch = (AbstractLogicalOperator) branchRef.getValue();
            if (branch.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                AggregateOperator aggOp = (AggregateOperator) branch;
                applied |= pushAggregateFunction(assignOp, aggOp, context);
            } else if (branch.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                    || branch.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
                AbstractBinaryJoinOperator childJoin = (AbstractBinaryJoinOperator) branch;
                applied |= pushAggregateFunctionThroughJoin(assignOp, childJoin, context);
            }
        }
        return applied;
    }

    private boolean pushAggregateFunction(AssignOperator assignOp, AggregateOperator aggOp,
            IOptimizationContext context) throws AlgebricksException {
        Mutable<ILogicalOperator> aggChilldOpRef = aggOp.getInputs().get(0);
        AbstractLogicalOperator aggChildOp = (AbstractLogicalOperator) aggChilldOpRef.getValue();
        // If there's a group by below the agg, then we want to have the agg pushed into the group by
        if (aggChildOp.getOperatorTag() == LogicalOperatorTag.GROUP
                && !((GroupByOperator) aggChildOp).getNestedPlans().isEmpty()) {
            return false;
        }

        List<LogicalVariable> assignUsedVars = new ArrayList<>();
        VariableUtilities.getUsedVariables(assignOp, assignUsedVars);

        List<Mutable<ILogicalExpression>> assignScalarAggExprRefs = new ArrayList<>();
        List<LogicalVariable> aggAddVars = null;
        List<Mutable<ILogicalExpression>> aggAddExprs = null;

        for (int i = 0, n = aggOp.getVariables().size(); i < n; i++) {
            LogicalVariable aggVar = aggOp.getVariables().get(i);
            Mutable<ILogicalExpression> aggExprRef = aggOp.getExpressions().get(i);
            ILogicalExpression aggExpr = aggExprRef.getValue();
            if (aggExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression listifyCandidateExpr = (AbstractFunctionCallExpression) aggExpr;
            if (listifyCandidateExpr.getFunctionIdentifier() != BuiltinFunctions.LISTIFY) {
                continue;
            }
            if (!assignUsedVars.contains(aggVar)) {
                continue;
            }
            assignScalarAggExprRefs.clear();
            findScalarAggFuncExprRef(assignOp.getExpressions(), aggVar, assignScalarAggExprRefs);
            if (assignScalarAggExprRefs.isEmpty()) {
                continue;
            }
            // perform rewrite
            if (aggAddVars == null) {
                aggAddVars = new ArrayList<>();
                aggAddExprs = new ArrayList<>();
            }
            for (Mutable<ILogicalExpression> assignScalarAggExprRef : assignScalarAggExprRefs) {
                AbstractFunctionCallExpression assignScalarAggExpr =
                        (AbstractFunctionCallExpression) assignScalarAggExprRef.getValue();
                FunctionIdentifier aggFuncIdent =
                        BuiltinFunctions.getAggregateFunction(assignScalarAggExpr.getFunctionIdentifier());

                // Push the scalar aggregate function into the aggregate op.
                int nArgs = assignScalarAggExpr.getArguments().size();
                List<Mutable<ILogicalExpression>> aggArgs = new ArrayList<>(nArgs);
                aggArgs.add(
                        new MutableObject<>(listifyCandidateExpr.getArguments().get(0).getValue().cloneExpression()));
                aggArgs.addAll(OperatorManipulationUtil
                        .cloneExpressions(assignScalarAggExpr.getArguments().subList(1, nArgs)));
                AggregateFunctionCallExpression aggFuncExpr =
                        BuiltinFunctions.makeAggregateFunctionExpression(aggFuncIdent, aggArgs);
                aggFuncExpr.setSourceLocation(assignScalarAggExpr.getSourceLocation());

                LogicalVariable newVar = context.newVar();
                aggAddVars.add(newVar);
                aggAddExprs.add(new MutableObject<>(aggFuncExpr));
                // The assign now just "renames" the variable to make sure the upstream plan still works.
                VariableReferenceExpression newVarRef = new VariableReferenceExpression(newVar);
                newVarRef.setSourceLocation(assignScalarAggExpr.getSourceLocation());
                assignScalarAggExprRef.setValue(newVarRef);
            }
        }

        if (aggAddVars == null) {
            return false;
        }

        // add new variables and expressions to the aggregate operator.
        aggOp.getVariables().addAll(aggAddVars);
        aggOp.getExpressions().addAll(aggAddExprs);

        // Note: we retain the original listify() call in the aggregate operator because
        // the variable it is assigned to might be used upstream by other operators.
        // If the variable is not used upstream then it'll later be removed
        // by {@code RemoveUnusedAssignAndAggregateRule}

        context.computeAndSetTypeEnvironmentForOperator(aggOp);
        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        return true;
    }

    private void findScalarAggFuncExprRef(List<Mutable<ILogicalExpression>> exprRefs, LogicalVariable aggVar,
            List<Mutable<ILogicalExpression>> outScalarAggExprRefs) {
        for (Mutable<ILogicalExpression> exprRef : exprRefs) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
                FunctionIdentifier funcIdent = BuiltinFunctions.getAggregateFunction(funcExpr.getFunctionIdentifier());
                if (funcIdent != null
                        && aggVar.equals(SqlppVariableUtil.getVariable(funcExpr.getArguments().get(0).getValue()))) {
                    outScalarAggExprRefs.add(exprRef);
                } else {
                    // Recursively look in func args.
                    findScalarAggFuncExprRef(funcExpr.getArguments(), aggVar, outScalarAggExprRefs);
                }
            }
        }
    }
}
