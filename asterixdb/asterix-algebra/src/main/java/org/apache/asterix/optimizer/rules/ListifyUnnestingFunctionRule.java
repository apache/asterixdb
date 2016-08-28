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
import java.util.Collections;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * In principle, an unnest operator produces a sequence of items from a collection.
 * However, the final result of an unnest is still a collection.
 * <p/>
 *
 * Hence, if an unnesting function expression is not called from a unnest operator or left outer unnest operator,
 * it is invalid and we need to extract it out into an unnest operator and then listify the unnested sequence of items
 * so that the listified collection can replace the original call of the unnesting function.
 * <p/>
 *
 * Assuming FacebookUsers is a dataset. Example queries:
 * <p/>
 * COUNT(FacebookUsers);
 * <p/>
 * FacebookUsers;
 * <p/>
 * This rule performs the aforementioned transformations.
 */
public class ListifyUnnestingFunctionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST
                || op.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST || op.getInputs().size() > 1) {
            return false;
        }
        return op.acceptExpressionTransform(exprRef -> rewriteExpressionReference(op, exprRef, context));
    }

    // Recursively rewrites for an expression within an operator.
    private boolean rewriteExpressionReference(ILogicalOperator op, Mutable<ILogicalExpression> exprRef,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        boolean changed = false;
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

        // Rewrites arguments.
        for (Mutable<ILogicalExpression> funcArgRef : funcExpr.getArguments()) {
            changed = changed || rewriteExpressionReference(op, funcArgRef, context);
        }

        // Rewrites the current function expression.
        return changed || listifyUnnestingFunction(op, exprRef, funcExpr, context);
    }

    // Performs the actual logical transformation.
    private boolean listifyUnnestingFunction(ILogicalOperator op, Mutable<ILogicalExpression> exprRef,
            AbstractFunctionCallExpression func, IOptimizationContext context) throws AlgebricksException {
        IFunctionInfo functionInfo = func.getFunctionInfo();

        // Checks if the function is an unnesting function.
        if (!AsterixBuiltinFunctions.isBuiltinUnnestingFunction(functionInfo.getFunctionIdentifier())) {
            return false;
        }

        // Generates the listified collection in a subplan.
        SubplanOperator subplanOperator = new SubplanOperator();
        // Creates a nested tuple source operator.
        NestedTupleSourceOperator ntsOperator = new NestedTupleSourceOperator(new MutableObject<>(subplanOperator));

        // Unnests the dataset.
        LogicalVariable unnestVar = context.newVar();
        ILogicalExpression unnestExpr = new UnnestingFunctionCallExpression(functionInfo, func.getArguments());
        UnnestOperator unnestOperator = new UnnestOperator(unnestVar, new MutableObject<>(unnestExpr));
        unnestOperator.getInputs().add(new MutableObject<>(ntsOperator));

        // Listify the dataset into one collection.
        LogicalVariable aggVar = context.newVar();
        Mutable<ILogicalExpression> aggArgExprRef = new MutableObject<>(new VariableReferenceExpression(unnestVar));
        ILogicalExpression aggExpr = new AggregateFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.LISTIFY), false, new ArrayList<>(
                        Collections.singletonList(aggArgExprRef)));
        AggregateOperator aggregateOperator = new AggregateOperator(new ArrayList<>(Collections.singletonList(aggVar)),
                new ArrayList<>(Collections.singletonList(new MutableObject<>(aggExpr))));
        aggregateOperator.getInputs().add(new MutableObject<>(unnestOperator));


        // Adds the aggregate operator as the root of the subplan.
        subplanOperator.setRootOp(new MutableObject<>(aggregateOperator));

        // Sticks a subplan operator into the query plan.
        // Note: given the way we compile JOINs, the unnesting function expression cannot appear in
        // any binary operators.
        // Example test queries:
        // asterixdb/asterix-app/src/test/resources/runtimets/results/list/query-ASTERIXDB-159-2
        // asterixdb/asterix-app/src/test/resources/runtimets/results/list/query-ASTERIXDB-159-3
        subplanOperator.getInputs().add(op.getInputs().get(0));
        op.getInputs().set(0, new MutableObject<>(subplanOperator));
        exprRef.setValue(new VariableReferenceExpression(aggVar));

        // Computes type environments for new operators.
        context.computeAndSetTypeEnvironmentForOperator(ntsOperator);
        context.computeAndSetTypeEnvironmentForOperator(unnestOperator);
        context.computeAndSetTypeEnvironmentForOperator(aggregateOperator);
        context.computeAndSetTypeEnvironmentForOperator(subplanOperator);
        return true;
    }

}
