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
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.om.util.container.ObjectFactories;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule rewrites agg-distinct() function calls inside AGGREGATE operator into into regular agg() function calls
 * over DISTINCT operator. <br/>
 * In the simplest case:
 * <pre>
 * AGGREGATE [$x] <- [agg-distinct($y)]
 * INPUT_OP
 * --->
 * AGGREGATE [$x] <- [agg($y)]
 * DISTINCT [$y]
 * INPUT_OP
 * </pre>
 *
 * In the general case a single AGGREGATE operator could contain regular aggregate function calls and agg-distinct()
 * function calls that work on different arguments:
 * <pre>
 * AGGREGATE [...] <- [avg($a), sum($b), avg-distinct($c), sum-distinct($c), avg-distinct($d), sum-distinct($d)]
 * </pre>
 * In this case distinct aggregate function calls are divided into groups based on their arguments, with one additional
 * group introduced for all non-distinct aggregates. Each distinct aggregate is the rewritten into regular aggregate
 * over DISTINCT operator as described above. Then all groups are combined together as follows.
 * If the original AGGREGATE operator was in a nested plan of a GROUPBY operator then each aggregate group becomes
 * a separate nested plan, otherwise (if the AGGREGATE operator was not inside GROUPBY) these groups are joined
 * together by a trivial join:
 *
 * <pre>
 * 1. AGGREGATE inside GROUPBY:
 *
 *   GROUPBY (
 *     {
 *       AGGREGATE [...] <- [avg($a), sum($b), avg-distinct($c), sum-distinct($c), avg-distinct($d), sum-distinct($d)]
 *       NESTED_TUPLE_SOURCE
 *     }
 *   )
 *   -->
 *   GROUPBY (
 *     {
 *       AGGREGATE [...] <- [avg($a), sum($b)]
 *       NESTED_TUPLE_SOURCE
 *     },
 *     {
 *       AGGREGATE [...] <- [avg($c), sum($c)]
 *       DISTINCT [$c]
 *       NESTED_TUPLE_SOURCE
 *     },
 *     {
 *       AGGREGATE [...] <- [avg($d), sum($d)]
 *       DISTINCT [$d]
 *       NESTED_TUPLE_SOURCE
 *     }
 *   )
 *
 *  2. AGGREGATE not inside GROUPBY:
 *
 *    AGGREGATE [...] <- [avg($a), sum($b), avg-distinct($c), sum-distinct($c), avg-distinct($d), sum-distinct($d)]
 *    INPUT_OP
 *    -->
 *    JOIN (TRUE) {
 *      JOIN (TRUE) {
 *        AGGREGATE [...] <- [avg($a), sum($b)]
 *          INPUT_OP
 *        AGGREGATE [...] <- [avg($c), sum($c)]
 *          DISTINCT [$c]
 *           INPUT_OP
 *      },
 *      AGGREGATE [...] <- [avg($d), sum($d)]
 *        DISTINCT [$d]
 *          INPUT_OP
 *    }
 *
 *    Note that in this case the input operator (INPUT_OP) is cloned for each aggregate group.
 * </pre>
 *
 * Notes:
 * <ul>
 * <li>
 * If distinct aggregate function argument is not a variable reference expression then an additional ASSIGN
 * operator will be introduced below the DISTINCT</li>
 * <li>
 * This rule must run before {@link AsterixIntroduceGroupByCombinerRule} because distinct aggregates must be converted
 * into regular before the local/global split.
 * </li>
 * <li>
 * Scalar distinct aggregate function calls are not handled by this rule. If they were not rewritten into regular
 * aggregates then they will be evaluated at runtime.
 * </li>
 * </ul>
 */
public final class RewriteDistinctAggregateRule implements IAlgebraicRewriteRule {

    private final IObjectPool<BitSet, Void> bitSetAllocator = new ListObjectPool<>(ObjectFactories.BIT_SET_FACTORY);

    private final Map<ILogicalExpression, BitSet> aggGroups = new LinkedHashMap<>();

    private final List<AggregateOperator> newAggOps = new ArrayList<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        switch (op.getOperatorTag()) {
            case AGGREGATE:
                if (context.checkIfInDontApplySet(this, op)) {
                    return false;
                }
                newAggOps.clear();
                if (rewriteAggregate((AggregateOperator) op, newAggOps, context)) {
                    ILogicalOperator newOp = join(newAggOps, context);
                    opRef.setValue(newOp);
                    return true;
                }
                return false;

            case GROUP:
                if (context.checkIfInDontApplySet(this, op)) {
                    return false;
                }
                GroupByOperator gbyOp = (GroupByOperator) op;
                List<ILogicalPlan> nestedPlans = gbyOp.getNestedPlans();
                boolean applied = false;
                for (int i = nestedPlans.size() - 1; i >= 0; i--) {
                    ILogicalPlan nestedPlan = nestedPlans.get(i);
                    for (Mutable<ILogicalOperator> rootOpRef : nestedPlan.getRoots()) {
                        ILogicalOperator rootOp = rootOpRef.getValue();
                        if (rootOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                            newAggOps.clear();
                            if (rewriteAggregate((AggregateOperator) rootOp, newAggOps, context)) {
                                applied = true;
                                rootOpRef.setValue(newAggOps.get(0));
                                for (int j = 1, ln = newAggOps.size(); j < ln; j++) {
                                    nestedPlans.add(new ALogicalPlanImpl(new MutableObject<>(newAggOps.get(j))));
                                }
                            }
                        }
                    }
                }
                if (applied) {
                    context.computeAndSetTypeEnvironmentForOperator(gbyOp);
                    context.addToDontApplySet(this, gbyOp);
                    return true;
                }
                return false;

            default:
                return false;
        }
    }

    private ILogicalOperator join(List<? extends ILogicalOperator> ops, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator resultOp = ops.get(0);
        for (int i = 1, n = ops.size(); i < n; i++) {
            ILogicalOperator op = ops.get(i);
            InnerJoinOperator joinOp = new InnerJoinOperator(new MutableObject<>(ConstantExpression.TRUE));
            joinOp.setSourceLocation(resultOp.getSourceLocation());
            joinOp.getInputs().add(new MutableObject<>(resultOp));
            joinOp.getInputs().add(new MutableObject<>(op));
            context.computeAndSetTypeEnvironmentForOperator(joinOp);
            resultOp = joinOp;
        }
        return resultOp;
    }

    private boolean rewriteAggregate(AggregateOperator aggOp, List<AggregateOperator> outAggOps,
            IOptimizationContext context) throws AlgebricksException {
        aggGroups.clear();
        bitSetAllocator.reset();

        List<Mutable<ILogicalExpression>> aggExprs = aggOp.getExpressions();
        int aggCount = aggExprs.size();
        int distinctAggCount = 0;

        for (int i = 0; i < aggCount; i++) {
            ILogicalExpression aggExpr = aggExprs.get(i).getValue();
            ILogicalExpression distinctValueExpr = null;
            if (aggExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) aggExpr;
                FunctionIdentifier aggFn = callExpr.getFunctionIdentifier();
                if (BuiltinFunctions.getAggregateFunctionForDistinct(aggFn) != null) {
                    distinctValueExpr = callExpr.getArguments().get(0).getValue();
                    distinctAggCount++;
                }
            }

            BitSet argIndexes = aggGroups.get(distinctValueExpr);
            if (argIndexes == null) {
                argIndexes = bitSetAllocator.allocate(null);
                argIndexes.clear();
                aggGroups.put(distinctValueExpr, argIndexes);
            }
            argIndexes.set(i);
        }

        if (distinctAggCount == 0) {
            return false;
        }

        for (Iterator<Map.Entry<ILogicalExpression, BitSet>> i = aggGroups.entrySet().iterator(); i.hasNext();) {
            Map.Entry<ILogicalExpression, BitSet> me = i.next();
            boolean isDistinct = me.getKey() != null;
            BitSet argIndexes = me.getValue();

            AggregateOperator newAggOp;
            if (i.hasNext()) {
                newAggOp = (AggregateOperator) OperatorManipulationUtil.deepCopyWithNewVars(aggOp, context).first;
                newAggOp.getVariables().clear();
                newAggOp.getVariables().addAll(aggOp.getVariables());
            } else {
                newAggOp = aggOp;
            }

            OperatorManipulationUtil.retainAssignVariablesAndExpressions(newAggOp.getVariables(),
                    newAggOp.getExpressions(), argIndexes);

            if (isDistinct) {
                rewriteDistinctAggregate(newAggOp, context);
            }

            context.computeAndSetTypeEnvironmentForOperator(newAggOp);
            context.addToDontApplySet(this, newAggOp);
            outAggOps.add(newAggOp);
        }

        return true;
    }

    private void rewriteDistinctAggregate(AggregateOperator aggOp, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator inputOp = aggOp.getInputs().get(0).getValue();

        // Introduce ASSIGN operator if distinct expression is not a variable
        ILogicalExpression distinctExpr = ((AbstractFunctionCallExpression) aggOp.getExpressions().get(0).getValue())
                .getArguments().get(0).getValue();
        AssignOperator assignOp = null;
        LogicalVariable distinctVar;
        if (distinctExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            distinctVar = ((VariableReferenceExpression) distinctExpr).getVariableReference();
        } else {
            distinctVar = context.newVar();
            assignOp = new AssignOperator(distinctVar, new MutableObject<>(distinctExpr));
            assignOp.setSourceLocation(aggOp.getSourceLocation());
            assignOp.getInputs().add(new MutableObject<>(inputOp));
            assignOp.setExecutionMode(inputOp.getExecutionMode());
            context.computeAndSetTypeEnvironmentForOperator(assignOp);
            inputOp = assignOp;
        }

        // Introduce DISTINCT operator
        VariableReferenceExpression distinctVarRef = new VariableReferenceExpression(distinctVar);
        distinctVarRef.setSourceLocation(distinctExpr.getSourceLocation());

        List<Mutable<ILogicalExpression>> distinctExprs = new ArrayList<>(1);
        distinctExprs.add(new MutableObject<>(distinctVarRef));
        DistinctOperator distinctOp = new DistinctOperator(distinctExprs);
        distinctOp.setSourceLocation(aggOp.getSourceLocation());
        distinctOp.getInputs().add(new MutableObject<>(inputOp));
        distinctOp.setExecutionMode(inputOp.getExecutionMode());
        context.computeAndSetTypeEnvironmentForOperator(distinctOp);

        // Change function identifiers from agg-distinct(expr) to agg(expr),
        // replace 'expr' with variable reference if ASSIGN was introduced
        for (Mutable<ILogicalExpression> aggExprRef : aggOp.getExpressions()) {
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) aggExprRef.getValue();
            FunctionIdentifier regularAggForDistinct =
                    BuiltinFunctions.getAggregateFunctionForDistinct(callExpr.getFunctionIdentifier());
            if (regularAggForDistinct == null) {
                throw new IllegalStateException(String.valueOf(callExpr.getFunctionIdentifier()));
            }
            callExpr.setFunctionInfo(BuiltinFunctions.getAsterixFunctionInfo(regularAggForDistinct));

            if (assignOp != null) {
                callExpr.getArguments().get(0).setValue(distinctVarRef.cloneExpression());
            }
        }

        aggOp.getInputs().clear();
        aggOp.getInputs().add(new MutableObject<>(distinctOp));
        context.computeAndSetTypeEnvironmentForOperator(aggOp);
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
