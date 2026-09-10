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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Rewrites {@code sql-count-distinct} into the single-pass hash aggregate {@code sql-count-distinct-hash},
 * except where that would cost more than the sort it saves:
 * <ul>
 * <li>when another distinct aggregate in the same operator aggregates the same expression, since the two
 * share one DISTINCT and rewriting only one of them deduplicates that expression twice while the sort
 * needed by the other one remains;</li>
 * <li>when a top-level aggregate operator also holds two-step aggregates, since local aggregation is pushed
 * only if every function in the operator is two-step. Aggregates in a nested plan are exempt: they are
 * combined by the group-by combiner instead, and that is the case the hash aggregate is meant for.</li>
 * <li>when any aggregate in the operator does not take exactly one argument, since the rewrite and the
 * shared-DISTINCT check above only handle single-argument aggregates.</li>
 * </ul>
 */
public class RewriteCountDistinctToHashRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (!context.getPhysicalOptimizationConfig().isCountDistinctToHash()) {
            return false;
        }
        return rewriteOperatorAndNestedPlans(opRef.getValue(), context);
    }

    private boolean rewriteOperatorAndNestedPlans(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        boolean changed = rewriteOneOperator(op);
        if (op instanceof AbstractOperatorWithNestedPlans) {
            for (ILogicalPlan nestedPlan : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                for (Mutable<ILogicalOperator> rootRef : nestedPlan.getRoots()) {
                    changed |= rewriteSubtree(rootRef.getValue(), context);
                }
            }
        }
        return changed;
    }

    /** Walks a nested-plan subtree (bounded by NESTED_TUPLE_SOURCE leaves, which have no inputs). */
    private boolean rewriteSubtree(ILogicalOperator op, IOptimizationContext context) throws AlgebricksException {
        boolean changed = rewriteOperatorAndNestedPlans(op, context);
        for (Mutable<ILogicalOperator> inputRef : op.getInputs()) {
            changed |= rewriteSubtree(inputRef.getValue(), context);
        }
        return changed;
    }

    private boolean rewriteOneOperator(ILogicalOperator op) throws AlgebricksException {
        if (op.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        List<Mutable<ILogicalExpression>> aggExprs = ((AggregateOperator) op).getExpressions();
        boolean canLoseLocalPhase = !isInNestedPlan(op);

        // Collect the arguments already deduplicated by a distinct aggregate this rule leaves alone. Bail out
        // if any aggregate here does not take exactly one argument, or if one would lose its partition-local
        // phase, which local aggregation only keeps when every function in the operator is two-step.
        Set<ILogicalExpression> sharedArgs = new HashSet<>();
        for (Mutable<ILogicalExpression> exprRef : aggExprs) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
            if (callExpr instanceof AggregateFunctionCallExpression && callExpr.getArguments().size() != 1) {
                return false;
            }
            FunctionIdentifier fn = callExpr.getFunctionIdentifier();
            if (fn.equals(BuiltinFunctions.SQL_COUNT_DISTINCT)) {
                continue;
            }
            if (canLoseLocalPhase && callExpr instanceof AggregateFunctionCallExpression
                    && ((AggregateFunctionCallExpression) callExpr).isTwoStep()) {
                return false;
            }
            if (BuiltinFunctions.getAggregateFunctionForDistinct(fn) != null) {
                sharedArgs.add(callExpr.getArguments().get(0).getValue());
            }
        }

        boolean changed = false;
        for (Mutable<ILogicalExpression> exprRef : aggExprs) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
            if (!callExpr.getFunctionIdentifier().equals(BuiltinFunctions.SQL_COUNT_DISTINCT)
                    || sharedArgs.contains(callExpr.getArguments().get(0).getValue())) {
                continue;
            }
            AggregateFunctionCallExpression newExpr = BuiltinFunctions
                    .makeAggregateFunctionExpression(BuiltinFunctions.SQL_COUNT_DISTINCT_HASH, callExpr.getArguments());
            newExpr.setSourceLocation(callExpr.getSourceLocation());
            exprRef.setValue(newExpr);
            changed = true;
        }
        return changed;
    }

    /**
     * Whether this aggregate belongs to a nested plan (GROUP BY, subplan, window), whose subtree is bounded by a
     * NESTED_TUPLE_SOURCE leaf. Derived from the operator itself so that it does not depend on how the rule
     * traversal reached it.
     */
    private static boolean isInNestedPlan(ILogicalOperator op) {
        ILogicalOperator currentOp = op;
        while (currentOp.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            if (currentOp.getInputs().isEmpty()) {
                return false;
            }
            currentOp = currentOp.getInputs().get(0).getValue();
        }
        return true;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
