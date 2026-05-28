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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

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
        AggregateOperator aggOp = (AggregateOperator) op;
        boolean changed = false;
        for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
            if (callExpr.getFunctionIdentifier().equals(BuiltinFunctions.SQL_COUNT_DISTINCT)) {
                AggregateFunctionCallExpression newExpr = BuiltinFunctions.makeAggregateFunctionExpression(
                        BuiltinFunctions.SQL_COUNT_DISTINCT_HASH, callExpr.getArguments());
                newExpr.setSourceLocation(callExpr.getSourceLocation());
                exprRef.setValue(newExpr);
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
