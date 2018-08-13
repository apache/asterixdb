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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.UnpartitionedPropertyComputer;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * The rule cancels out redundant pairs of operators unnest-listify aggregate
 * <p>
 * <ul>
 * <li>case 1 (direct):
 * <p>
 * Before plan:
 * <ul>
 * <li>unnest $x [[ at $p ]] <- function-call:scan-collection($y)
 * <li>aggregate $y <- function-call:listify($z)
 * </ul>
 * <p>
 * After plan:
 * <ul>
 * <li>[[ runningaggregate $p <- tid]]
 * <li>assign $x <- $z
 * </ul>
 * <li>case 2 (reverse):
 * <p>
 * Before plan:
 * <ul>
 * <li>aggregate $x <- function-call:listify($y)
 * <li>unnest $y <- function-call:scan-collection($z)
 * </ul>
 * <p>
 * After plan:
 * <ul>
 * <li>assign $x <- $z
 * </ul>
 * </ul>
 */

public class RemoveRedundantListifyRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // apply it only at the top of the plan
        ILogicalOperator op = opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        Set<LogicalVariable> varSet = new HashSet<LogicalVariable>();
        return applyRuleDown(opRef, varSet, context);
    }

    private boolean applyRuleDown(Mutable<ILogicalOperator> opRef, Set<LogicalVariable> varSet,
            IOptimizationContext context) throws AlgebricksException {
        boolean changed = applies(opRef, varSet, context);
        changed |= appliesForReverseCase(opRef, varSet, context);
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        VariableUtilities.getUsedVariables(op, varSet);
        if (op.hasNestedPlans()) {
            // Variables used by the parent operators should be live at op.
            Set<LogicalVariable> localLiveVars = new ListSet<LogicalVariable>();
            VariableUtilities.getLiveVariables(op, localLiveVars);
            varSet.retainAll(localLiveVars);
            AbstractOperatorWithNestedPlans aonp = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : aonp.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    if (applyRuleDown(r, varSet, context)) {
                        changed = true;
                    }
                    context.addToDontApplySet(this, r.getValue());
                }
            }
        }
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            if (applyRuleDown(i, varSet, context)) {
                changed = true;
            }
            context.addToDontApplySet(this, i.getValue());
        }
        return changed;
    }

    private boolean applies(Mutable<ILogicalOperator> opRef, Set<LogicalVariable> varUsedAbove,
            IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest1 = (UnnestOperator) op1;
        ILogicalExpression expr = unnest1.getExpressionRef().getValue();

        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        if (((AbstractFunctionCallExpression) expr).getFunctionIdentifier() != BuiltinFunctions.SCAN_COLLECTION) {
            return false;
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) expr;
        ILogicalExpression functionCallArgExpr = functionCall.getArguments().get(0).getValue();
        if (functionCallArgExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        LogicalVariable unnestedVar = ((VariableReferenceExpression) functionCallArgExpr).getVariableReference();

        if (varUsedAbove.contains(unnestedVar)) {
            return false;
        }
        Mutable<ILogicalOperator> aggregateParentRef = opRef;
        AbstractLogicalOperator r = op1;
        boolean metAggregate = false;
        while (r.getInputs().size() == 1) {
            aggregateParentRef = r.getInputs().get(0);
            r = (AbstractLogicalOperator) aggregateParentRef.getValue();
            if (r.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assign = (AssignOperator) r;
                List<LogicalVariable> variables = assign.getVariables();
                // The assign operator doesn't produce any variable that is used by the unnest.
                if (variables.contains(unnestedVar)) {
                    return false;
                }
            } else {
                if (r.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                    metAggregate = true;
                }
                break;
            }
        }

        if (!metAggregate) {
            return false;
        }
        AggregateOperator agg = (AggregateOperator) r;
        if (agg.getVariables().size() > 1) {
            return false;
        }
        LogicalVariable aggVar = agg.getVariables().get(0);
        ILogicalExpression aggFun = agg.getExpressions().get(0).getValue();
        if (!aggVar.equals(unnestedVar) || aggFun.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) aggFun;
        if (!BuiltinFunctions.LISTIFY.equals(f.getFunctionIdentifier())) {
            return false;
        }
        if (f.getArguments().size() != 1) {
            return false;
        }
        ILogicalExpression arg0 = f.getArguments().get(0).getValue();
        if (arg0.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        LogicalVariable paramVar = ((VariableReferenceExpression) arg0).getVariableReference();

        List<LogicalVariable> assgnVars = new ArrayList<>(1);
        assgnVars.add(unnest1.getVariable());
        List<Mutable<ILogicalExpression>> assgnExprs = new ArrayList<>(1);
        VariableReferenceExpression paramVarRef = new VariableReferenceExpression(paramVar);
        paramVarRef.setSourceLocation(arg0.getSourceLocation());
        assgnExprs.add(new MutableObject<ILogicalExpression>(paramVarRef));
        AssignOperator assign = new AssignOperator(assgnVars, assgnExprs);
        assign.setSourceLocation(agg.getSourceLocation());
        assign.getInputs().add(agg.getInputs().get(0));
        context.computeAndSetTypeEnvironmentForOperator(assign);
        LogicalVariable posVar = unnest1.getPositionalVariable();

        if (posVar == null) {
            // Removes the aggregate operator.
            aggregateParentRef.setValue(assign);
        } else {
            List<LogicalVariable> raggVars = new ArrayList<>(1);
            raggVars.add(posVar);
            List<Mutable<ILogicalExpression>> rAggExprs = new ArrayList<>(1);
            StatefulFunctionCallExpression tidFun = new StatefulFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.TID), UnpartitionedPropertyComputer.INSTANCE);
            tidFun.setSourceLocation(agg.getSourceLocation());
            rAggExprs.add(new MutableObject<ILogicalExpression>(tidFun));
            RunningAggregateOperator rAgg = new RunningAggregateOperator(raggVars, rAggExprs);
            rAgg.setSourceLocation(agg.getSourceLocation());
            rAgg.getInputs().add(new MutableObject<ILogicalOperator>(assign));
            aggregateParentRef.setValue(rAgg);
            context.computeAndSetTypeEnvironmentForOperator(rAgg);
        }
        // Removes the unnest operator.
        opRef.setValue(unnest1.getInputs().get(0).getValue());
        return true;
    }

    private boolean appliesForReverseCase(Mutable<ILogicalOperator> opRef, Set<LogicalVariable> varUsedAbove,
            IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator agg = (AggregateOperator) op1;
        if (agg.getVariables().size() > 1 || agg.getVariables().size() <= 0) {
            return false;
        }
        LogicalVariable aggVar = agg.getVariables().get(0);
        ILogicalExpression aggFun = agg.getExpressions().get(0).getValue();
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) aggFun;
        if (!BuiltinFunctions.LISTIFY.equals(f.getFunctionIdentifier())) {
            return false;
        }
        if (f.getArguments().size() != 1) {
            return false;
        }
        ILogicalExpression arg0 = f.getArguments().get(0).getValue();
        if (arg0.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        LogicalVariable aggInputVar = ((VariableReferenceExpression) arg0).getVariableReference();
        if (varUsedAbove.contains(aggInputVar)) {
            return false;
        }

        if (agg.getInputs().size() == 0) {
            return false;
        }

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) agg.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) op2;
        if (unnest.getPositionalVariable() != null) {
            return false;
        }
        if (!unnest.getVariable().equals(aggInputVar)) {
            return false;
        }
        ILogicalExpression unnestArg = unnest.getExpressionRef().getValue();
        if (unnestArg.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression scanFunc = (AbstractFunctionCallExpression) unnestArg;
        if (scanFunc.getFunctionIdentifier() != BuiltinFunctions.SCAN_COLLECTION) {
            return false;
        }
        if (scanFunc.getArguments().size() != 1) {
            return false;
        }

        List<LogicalVariable> assgnVars = new ArrayList<>(1);
        assgnVars.add(aggVar);
        AssignOperator assign = new AssignOperator(assgnVars, scanFunc.getArguments());
        assign.setSourceLocation(agg.getSourceLocation());
        assign.getInputs().add(unnest.getInputs().get(0));
        context.computeAndSetTypeEnvironmentForOperator(assign);
        opRef.setValue(assign);
        return true;
    }
}
