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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushAggregateIntoNestedSubplanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        Map<LogicalVariable, Integer> nspAggVars = new HashMap<>();
        Map<LogicalVariable, Integer> nspAggVarToPlanIndex = new HashMap<>();
        Map<LogicalVariable, AbstractOperatorWithNestedPlans> nspWithAgg = new HashMap<>();
        Map<ILogicalExpression, ILogicalExpression> aggExprToVarExpr = new HashMap<>();
        // first collect vars. referring to listified sequences
        boolean changed =
                collectVarsBottomUp(opRef, context, nspAggVars, nspWithAgg, nspAggVarToPlanIndex, aggExprToVarExpr);
        if (changed) {
            removeRedundantListifies(nspAggVars, nspWithAgg, nspAggVarToPlanIndex);
        }
        return changed;
    }

    private void removeRedundantListifies(Map<LogicalVariable, Integer> nspAggVars,
            Map<LogicalVariable, AbstractOperatorWithNestedPlans> nspWithAgg,
            Map<LogicalVariable, Integer> nspAggVarToPlanIndex) throws AlgebricksException {
        List<Pair<AbstractOperatorWithNestedPlans, Integer>> removeList = new ArrayList<>();
        for (Map.Entry<LogicalVariable, Integer> aggVarEntry : nspAggVars.entrySet()) {
            LogicalVariable aggVar = aggVarEntry.getKey();
            int occurs = aggVarEntry.getValue();
            if (occurs == 0) {
                AbstractOperatorWithNestedPlans nspOp = nspWithAgg.get(aggVar);
                AggregateOperator aggOp = (AggregateOperator) nspOp.getNestedPlans()
                        .get(nspAggVarToPlanIndex.get(aggVar)).getRoots().get(0).getValue();
                int pos = aggOp.getVariables().indexOf(aggVar);
                if (pos >= 0) {
                    aggOp.getVariables().remove(pos);
                    aggOp.getExpressions().remove(pos);
                    List<LogicalVariable> producedVarsAtAgg = new ArrayList<>();
                    VariableUtilities.getProducedVariablesInDescendantsAndSelf(aggOp, producedVarsAtAgg);
                    if (producedVarsAtAgg.isEmpty()) {
                        removeList.add(new Pair<>(nspOp, nspAggVarToPlanIndex.get(aggVar)));
                    }
                }
            }
        }

        // Collects subplans that is to be removed.
        Map<AbstractOperatorWithNestedPlans, List<ILogicalPlan>> nspToSubplanListMap = new HashMap<>();
        for (Pair<AbstractOperatorWithNestedPlans, Integer> remove : removeList) {
            AbstractOperatorWithNestedPlans groupByOperator = remove.first;
            ILogicalPlan subplan = remove.first.getNestedPlans().get(remove.second);
            if (nspToSubplanListMap.containsKey(groupByOperator)) {
                List<ILogicalPlan> subplans = nspToSubplanListMap.get(groupByOperator);
                subplans.add(subplan);
            } else {
                List<ILogicalPlan> subplans = new ArrayList<>();
                subplans.add(subplan);
                nspToSubplanListMap.put(groupByOperator, subplans);
            }
        }
        // Removes subplans.
        nspToSubplanListMap.forEach((key, value) -> key.getNestedPlans().removeAll(value));
    }

    private boolean collectVarsBottomUp(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            Map<LogicalVariable, Integer> nspListifyVarsCount,
            Map<LogicalVariable, AbstractOperatorWithNestedPlans> nspWithAgg,
            Map<LogicalVariable, Integer> nspAggVarToPlanIndex,
            Map<ILogicalExpression, ILogicalExpression> aggregateExprToVarExpr) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        context.addToDontApplySet(this, op1);
        boolean change = false;
        for (Mutable<ILogicalOperator> child : op1.getInputs()) {
            if (collectVarsBottomUp(child, context, nspListifyVarsCount, nspWithAgg, nspAggVarToPlanIndex,
                    aggregateExprToVarExpr)) {
                change = true;
            }
        }
        Set<LogicalVariable> used = new HashSet<>();
        VariableUtilities.getUsedVariables(op1, used);
        switch (op1.getOperatorTag()) {
            case ASSIGN:
            case SELECT:
                boolean found = false;
                // Do some prefiltering: check if the Assign uses any nsp vars.
                for (LogicalVariable v : used) {
                    if (nspListifyVarsCount.get(v) != null) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    break;
                }
                if (op1.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    AssignOperator assign = (AssignOperator) op1;
                    for (Mutable<ILogicalExpression> exprRef : assign.getExpressions()) {
                        Pair<Boolean, ILogicalExpression> p =
                                extractAggFunctionsFromExpression(exprRef, nspWithAgg, aggregateExprToVarExpr, context);
                        if (p.first) {
                            change = true;
                            exprRef.setValue(p.second);
                        }
                    }
                }
                if (op1.getOperatorTag() == LogicalOperatorTag.SELECT) {
                    SelectOperator select = (SelectOperator) op1;
                    Mutable<ILogicalExpression> exprRef = select.getCondition();
                    Pair<Boolean, ILogicalExpression> p =
                            extractAggFunctionsFromExpression(exprRef, nspWithAgg, aggregateExprToVarExpr, context);
                    if (p.first) {
                        change = true;
                        exprRef.setValue(p.second);
                    }
                }
                used.clear();
                VariableUtilities.getUsedVariables(op1, used);
                // increment the count for the ones which are still used
                for (LogicalVariable v : used) {
                    Integer m = nspListifyVarsCount.get(v);
                    if (m != null) {
                        nspListifyVarsCount.put(v, m + 1);
                    }
                }
                break;
            case SUBPLAN:
                // Try to push the subplan into a group-by operator if possible.
                for (LogicalVariable v : used) {
                    Integer m = nspListifyVarsCount.get(v);
                    if (m != null) {
                        AbstractOperatorWithNestedPlans nspOp = nspWithAgg.get(v);
                        if (pushSubplanAsAggIntoNestedSubplan(opRef, nspOp, v, nspListifyVarsCount, nspWithAgg,
                                nspAggVarToPlanIndex, context)) {
                            change = true;
                        } else {
                            nspListifyVarsCount.put(v, m + 1);
                        }
                    }
                }
                if (!change) {
                    // Collect aggregate variables for pushing aggregates into the subplan (if possible).
                    collectAggregateVars(nspListifyVarsCount, nspWithAgg, nspAggVarToPlanIndex,
                            (AbstractOperatorWithNestedPlans) op1);
                }
                break;
            case GROUP:
                // Collect aggregate variables for pushing aggregates into the nested subplan
                // of the group by operator (if possible).
                collectAggregateVars(nspListifyVarsCount, nspWithAgg, nspAggVarToPlanIndex,
                        (AbstractOperatorWithNestedPlans) op1);
                break;
            default:
                for (LogicalVariable v : used) {
                    Integer m = nspListifyVarsCount.get(v);
                    if (m != null) {
                        nspListifyVarsCount.put(v, m + 1);
                    }
                }
        }
        return change;
    }

    private void collectAggregateVars(Map<LogicalVariable, Integer> nspListifyVarsCount,
            Map<LogicalVariable, AbstractOperatorWithNestedPlans> nspWithAgg,
            Map<LogicalVariable, Integer> nspAggVarToPlanIndex, AbstractOperatorWithNestedPlans op) {
        List<LogicalVariable> vars = collectOneVarPerAggFromOpWithNestedPlans(op);
        for (int i = 0; i < vars.size(); i++) {
            LogicalVariable v = vars.get(i);
            if (v != null) {
                nspListifyVarsCount.put(v, 0);
                nspAggVarToPlanIndex.put(v, i);
                nspWithAgg.put(v, op);
            }
        }
    }

    private List<LogicalVariable> collectOneVarPerAggFromOpWithNestedPlans(AbstractOperatorWithNestedPlans op) {
        List<ILogicalPlan> nPlans = op.getNestedPlans();
        if (nPlans == null || nPlans.isEmpty()) {
            return Collections.emptyList();
        }

        List<LogicalVariable> aggVars = new ArrayList<>();
        // test that the operator computes a "listify" aggregate
        for (int i = 0; i < nPlans.size(); i++) {
            AbstractLogicalOperator topOp = (AbstractLogicalOperator) nPlans.get(i).getRoots().get(0).getValue();
            if (topOp.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
                continue;
            }
            AggregateOperator agg = (AggregateOperator) topOp;
            if (agg.getVariables().size() != 1) {
                continue;
            }
            ILogicalExpression expr = agg.getExpressions().get(0).getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression fceAgg = (AbstractFunctionCallExpression) expr;
            if (fceAgg.getFunctionIdentifier() != BuiltinFunctions.LISTIFY) {
                continue;
            }
            aggVars.add(agg.getVariables().get(0));
        }
        return aggVars;
    }

    /**
     * @param exprRef
     * @param nspWithAgg
     * @param context
     * @return a pair whose first member is a boolean which is true iff
     *         something was changed in the expression tree rooted at expr. The
     *         second member is the result of transforming expr.
     * @throws AlgebricksException
     */
    private Pair<Boolean, ILogicalExpression> extractAggFunctionsFromExpression(Mutable<ILogicalExpression> exprRef,
            Map<LogicalVariable, AbstractOperatorWithNestedPlans> nspWithAgg,
            Map<ILogicalExpression, ILogicalExpression> aggregateExprToVarExpr, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        switch (expr.getExpressionTag()) {
            case FUNCTION_CALL:
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                FunctionIdentifier fi = BuiltinFunctions.getAggregateFunction(fce.getFunctionIdentifier());
                if (fi != null) {
                    ILogicalExpression a1 = fce.getArguments().get(0).getValue();
                    if (a1.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                        LogicalVariable argVar = ((VariableReferenceExpression) a1).getVariableReference();
                        AbstractOperatorWithNestedPlans nspOp = nspWithAgg.get(argVar);

                        if (nspOp != null) {
                            if (!aggregateExprToVarExpr.containsKey(expr)) {
                                LogicalVariable newVar = context.newVar();
                                AggregateFunctionCallExpression aggFun =
                                        BuiltinFunctions.makeAggregateFunctionExpression(fi, fce.getArguments());
                                aggFun.setSourceLocation(expr.getSourceLocation());
                                rewriteAggregateInNestedSubplan(argVar, nspOp, aggFun, newVar, context);
                                VariableReferenceExpression newVarExpr = new VariableReferenceExpression(newVar);
                                newVarExpr.setSourceLocation(expr.getSourceLocation());
                                aggregateExprToVarExpr.put(expr, newVarExpr);
                                return new Pair<>(Boolean.TRUE, newVarExpr);
                            } else {
                                ILogicalExpression varExpr = aggregateExprToVarExpr.get(expr);
                                return new Pair<>(Boolean.TRUE, varExpr);
                            }
                        }
                    }
                }

                boolean change = false;
                for (Mutable<ILogicalExpression> a : fce.getArguments()) {
                    Pair<Boolean, ILogicalExpression> aggArg =
                            extractAggFunctionsFromExpression(a, nspWithAgg, aggregateExprToVarExpr, context);
                    if (aggArg.first.booleanValue()) {
                        a.setValue(aggArg.second);
                        change = true;
                    }
                }
                return new Pair<>(change, fce);
            case VARIABLE:
            case CONSTANT:
                return new Pair<>(Boolean.FALSE, expr);
            default:
                throw new IllegalArgumentException();
        }
    }

    private void rewriteAggregateInNestedSubplan(LogicalVariable oldAggVar, AbstractOperatorWithNestedPlans nspOp,
            AggregateFunctionCallExpression aggFun, LogicalVariable newAggVar, IOptimizationContext context)
            throws AlgebricksException {
        for (int j = 0; j < nspOp.getNestedPlans().size(); j++) {
            AggregateOperator aggOp = (AggregateOperator) nspOp.getNestedPlans().get(j).getRoots().get(0).getValue();
            int n = aggOp.getVariables().size();
            for (int i = 0; i < n; i++) {
                LogicalVariable v = aggOp.getVariables().get(i);
                if (v.equals(oldAggVar)) {
                    AbstractFunctionCallExpression oldAggExpr =
                            (AbstractFunctionCallExpression) aggOp.getExpressions().get(i).getValue();
                    AggregateFunctionCallExpression newAggFun = BuiltinFunctions
                            .makeAggregateFunctionExpression(aggFun.getFunctionIdentifier(), new ArrayList<>());
                    newAggFun.setSourceLocation(oldAggExpr.getSourceLocation());
                    List<Mutable<ILogicalExpression>> oldAggArgs = oldAggExpr.getArguments();
                    for (Mutable<ILogicalExpression> arg : oldAggArgs) {
                        ILogicalExpression cloned = arg.getValue().cloneExpression();
                        newAggFun.getArguments().add(new MutableObject<>(cloned));
                    }
                    List<Mutable<ILogicalExpression>> aggFunArgs = aggFun.getArguments();
                    for (int k = oldAggArgs.size(), ln = aggFunArgs.size(); k < ln; k++) {
                        ILogicalExpression cloned = aggFunArgs.get(k).getValue().cloneExpression();
                        newAggFun.getArguments().add(new MutableObject<>(cloned));
                    }
                    aggOp.getVariables().add(newAggVar);
                    aggOp.getExpressions().add(new MutableObject<>(newAggFun));
                    context.computeAndSetTypeEnvironmentForOperator(aggOp);
                    break;
                }
            }
        }
    }

    private boolean pushSubplanAsAggIntoNestedSubplan(Mutable<ILogicalOperator> subplanOpRef,
            AbstractOperatorWithNestedPlans nspOp, LogicalVariable varFromNestedAgg,
            Map<LogicalVariable, Integer> nspAggVars, Map<LogicalVariable, AbstractOperatorWithNestedPlans> nspWithAgg,
            Map<LogicalVariable, Integer> nspAggVarToPlanIndex, IOptimizationContext context)
            throws AlgebricksException {
        SubplanOperator subplan = (SubplanOperator) subplanOpRef.getValue();
        // only free var can be varFromNestedAgg
        HashSet<LogicalVariable> freeVars = new HashSet<>();
        OperatorPropertiesUtil.getFreeVariablesInSubplans(subplan, freeVars);
        for (LogicalVariable vFree : freeVars) {
            if (!vFree.equals(varFromNestedAgg)) {
                return false;
            }
        }

        List<ILogicalPlan> plans = subplan.getNestedPlans();
        if (plans.size() > 1) {
            return false;
        }
        ILogicalPlan p = plans.get(0);
        if (p.getRoots().size() > 1) {
            return false;
        }
        Mutable<ILogicalOperator> opRef = p.getRoots().get(0);
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggInSubplanOp = (AggregateOperator) op;
        LogicalVariable unnestVar = null;
        boolean pushableNestedSubplan = false;
        while (op.getInputs().size() == 1) {
            opRef = op.getInputs().get(0);
            op = (AbstractLogicalOperator) opRef.getValue();
            switch (op.getOperatorTag()) {
                case ASSIGN:
                    break;
                case UNNEST:
                    UnnestOperator unnest = (UnnestOperator) op;
                    if (unnest.getPositionalVariable() != null) {
                        // TODO currently subplan with both accumulating and running aggregate is not supported.
                        return false;
                    }
                    ILogicalExpression expr = unnest.getExpressionRef().getValue();
                    if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                        return false;
                    }
                    AbstractFunctionCallExpression fun = (AbstractFunctionCallExpression) expr;
                    if (fun.getFunctionIdentifier() != BuiltinFunctions.SCAN_COLLECTION) {
                        return false;
                    }
                    ILogicalExpression arg0 = fun.getArguments().get(0).getValue();
                    if (arg0.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        return false;
                    }
                    VariableReferenceExpression varExpr = (VariableReferenceExpression) arg0;
                    if (!varExpr.getVariableReference().equals(varFromNestedAgg)) {
                        return false;
                    }
                    opRef = op.getInputs().get(0);
                    op = (AbstractLogicalOperator) opRef.getValue();
                    if (op.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
                        return false;
                    }
                    pushableNestedSubplan = true;
                    unnestVar = unnest.getVariable();
                    break;
                default:
                    return false;
            }
        }
        if (!pushableNestedSubplan) {
            return false;
        }

        for (int i = 0; i < nspOp.getNestedPlans().size(); i++) {
            Mutable<ILogicalOperator> nspAggRef = nspOp.getNestedPlans().get(i).getRoots().get(0);
            AggregateOperator nspAgg = (AggregateOperator) nspAggRef.getValue();
            Mutable<ILogicalOperator> nspAggChildRef = nspAgg.getInputs().get(0);
            LogicalVariable listifyVar = findListifiedVariable(nspAgg, varFromNestedAgg);
            if (listifyVar != null) {
                OperatorManipulationUtil.substituteVarRec(aggInSubplanOp, unnestVar, listifyVar, true, context);
                nspAgg.getVariables().addAll(aggInSubplanOp.getVariables());
                nspAgg.getExpressions().addAll(aggInSubplanOp.getExpressions());
                for (LogicalVariable v : aggInSubplanOp.getVariables()) {
                    nspWithAgg.put(v, nspOp);
                    nspAggVars.put(v, 0);
                    nspAggVarToPlanIndex.put(v, i);
                }

                Mutable<ILogicalOperator> opRef1InSubplan = aggInSubplanOp.getInputs().get(0);
                if (!opRef1InSubplan.getValue().getInputs().isEmpty()) {
                    Mutable<ILogicalOperator> opRef2InSubplan = opRef1InSubplan.getValue().getInputs().get(0);
                    AbstractLogicalOperator op2InSubplan = (AbstractLogicalOperator) opRef2InSubplan.getValue();
                    if (op2InSubplan.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
                        List<Mutable<ILogicalOperator>> nspInpList = nspAgg.getInputs();
                        nspInpList.clear();
                        nspInpList.add(opRef1InSubplan);
                        while (true) {
                            opRef2InSubplan = opRef1InSubplan.getValue().getInputs().get(0);
                            op2InSubplan = (AbstractLogicalOperator) opRef2InSubplan.getValue();
                            if (op2InSubplan.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                                List<Mutable<ILogicalOperator>> opInpList = opRef1InSubplan.getValue().getInputs();
                                opInpList.clear();
                                opInpList.add(nspAggChildRef);
                                break;
                            }
                            opRef1InSubplan = opRef2InSubplan;
                            if (opRef1InSubplan.getValue().getInputs().isEmpty()) {
                                throw new IllegalStateException(
                                        "PushAggregateIntoNestedSubplanRule: could not find UNNEST.");
                            }
                        }
                    }
                }
                subplanOpRef.setValue(subplan.getInputs().get(0).getValue());
                OperatorPropertiesUtil.typeOpRec(nspAggRef, context);
                return true;
            }
        }
        return false;
    }

    private LogicalVariable findListifiedVariable(AggregateOperator nspAgg, LogicalVariable varFromNestedAgg) {
        int n = nspAgg.getVariables().size();
        for (int i = 0; i < n; i++) {
            if (nspAgg.getVariables().get(i).equals(varFromNestedAgg)) {
                AbstractFunctionCallExpression fce =
                        (AbstractFunctionCallExpression) nspAgg.getExpressions().get(i).getValue();
                if (fce.getFunctionIdentifier().equals(BuiltinFunctions.LISTIFY)) {
                    ILogicalExpression argExpr = fce.getArguments().get(0).getValue();
                    if (argExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                        return ((VariableReferenceExpression) argExpr).getVariableReference();
                    }
                }
            }
        }
        return null;
    }

}
