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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushAggregateIntoGroupbyRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        Map<LogicalVariable, Integer> gbyAggVars = new HashMap<LogicalVariable, Integer>();
        Map<LogicalVariable, Integer> gbyAggVarToPlanIndex = new HashMap<LogicalVariable, Integer>();
        Map<LogicalVariable, GroupByOperator> gbyWithAgg = new HashMap<LogicalVariable, GroupByOperator>();
        Map<ILogicalExpression, ILogicalExpression> aggExprToVarExpr = new HashMap<ILogicalExpression, ILogicalExpression>();
        // first collect vars. referring to listified sequences
        boolean changed = collectVarsBottomUp(opRef, context, gbyAggVars, gbyWithAgg, gbyAggVarToPlanIndex,
                aggExprToVarExpr);
        if (changed) {
            removeRedundantListifies(opRef, context, gbyAggVars, gbyWithAgg, gbyAggVarToPlanIndex);
        }
        return changed;
    }

    private void removeRedundantListifies(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            Map<LogicalVariable, Integer> gbyAggVars, Map<LogicalVariable, GroupByOperator> gbyWithAgg,
            Map<LogicalVariable, Integer> gbyAggVarToPlanIndex) throws AlgebricksException {
        for (LogicalVariable aggVar : gbyAggVars.keySet()) {
            int occurs = gbyAggVars.get(aggVar);
            if (occurs == 0) {
                GroupByOperator gbyOp = gbyWithAgg.get(aggVar);
                AggregateOperator aggOp = (AggregateOperator) gbyOp.getNestedPlans()
                        .get(gbyAggVarToPlanIndex.get(aggVar)).getRoots().get(0).getValue();
                int pos = aggOp.getVariables().indexOf(aggVar);
                if (pos >= 0) {
                    aggOp.getVariables().remove(pos);
                    aggOp.getExpressions().remove(pos);
                    List<LogicalVariable> producedVarsAtAgg = new ArrayList<LogicalVariable>();
                    VariableUtilities.getProducedVariablesInDescendantsAndSelf(aggOp, producedVarsAtAgg);
                    if (producedVarsAtAgg.isEmpty()) {
                        gbyOp.getNestedPlans().remove(gbyAggVarToPlanIndex.get(aggVar));
                    }
                }
            }
        }
    }

    private boolean collectVarsBottomUp(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            Map<LogicalVariable, Integer> gbyListifyVarsCount, Map<LogicalVariable, GroupByOperator> gbyWithAgg,
            Map<LogicalVariable, Integer> gbyAggVarToPlanIndex,
            Map<ILogicalExpression, ILogicalExpression> aggregateExprToVarExpr) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        context.addToDontApplySet(this, op1);
        boolean change = false;
        for (Mutable<ILogicalOperator> child : op1.getInputs()) {
            if (collectVarsBottomUp(child, context, gbyListifyVarsCount, gbyWithAgg, gbyAggVarToPlanIndex,
                    aggregateExprToVarExpr)) {
                change = true;
            }
        }
        // Need to use a list instead of a hash-set, because a var. may appear
        // several times in the same op.
        List<LogicalVariable> used = new LinkedList<LogicalVariable>();
        VariableUtilities.getUsedVariables(op1, used);
        switch (op1.getOperatorTag()) {
            case ASSIGN:
            case SELECT: {
                boolean found = false;
                // Do some prefiltering: check if the Assign uses any gby vars.
                for (LogicalVariable v : used) {
                    if (gbyListifyVarsCount.get(v) != null) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    if (op1.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                        AssignOperator assign = (AssignOperator) op1;
                        for (Mutable<ILogicalExpression> exprRef : assign.getExpressions()) {
                            Pair<Boolean, ILogicalExpression> p = extractAggFunctionsFromExpression(exprRef,
                                    gbyWithAgg, aggregateExprToVarExpr, context);
                            if (p.first) {
                                change = true;
                                exprRef.setValue(p.second);
                            }
                        }
                    }
                    if (op1.getOperatorTag() == LogicalOperatorTag.SELECT) {
                        SelectOperator select = (SelectOperator) op1;
                        Mutable<ILogicalExpression> exprRef = select.getCondition();
                        Pair<Boolean, ILogicalExpression> p = extractAggFunctionsFromExpression(exprRef, gbyWithAgg,
                                aggregateExprToVarExpr, context);
                        if (p.first) {
                            change = true;
                            exprRef.setValue(p.second);
                        }
                    }
                    used.clear();
                    VariableUtilities.getUsedVariables(op1, used);
                    // increment the count for the ones which are still used
                    for (LogicalVariable v : used) {
                        Integer m = gbyListifyVarsCount.get(v);
                        if (m != null) {
                            gbyListifyVarsCount.put(v, m + 1);
                        }
                    }
                }
                break;
            }
            case SUBPLAN: {
                for (LogicalVariable v : used) {
                    Integer m = gbyListifyVarsCount.get(v);
                    if (m != null) {
                        GroupByOperator gbyOp = gbyWithAgg.get(v);
                        if (pushSubplanAsAggIntoGby(opRef, gbyOp, v, gbyListifyVarsCount, gbyWithAgg,
                                gbyAggVarToPlanIndex, context)) {
                            change = true;
                        } else {
                            gbyListifyVarsCount.put(v, m + 1);
                        }
                        break;
                    }
                }
                break;
            }
            case GROUP: {
                List<LogicalVariable> vars = collectOneVarPerAggFromGroupOp((GroupByOperator) op1);
                if (vars != null) {
                    for (int i = 0; i < vars.size(); i++) {
                        LogicalVariable v = vars.get(i);
                        if (v != null) {
                            gbyListifyVarsCount.put(v, 0);
                            gbyAggVarToPlanIndex.put(v, i);
                            gbyWithAgg.put(v, (GroupByOperator) op1);
                        }
                    }
                }
                break;
            }
            default: {
                for (LogicalVariable v : used) {
                    Integer m = gbyListifyVarsCount.get(v);
                    if (m != null) {
                        gbyListifyVarsCount.put(v, m + 1);
                    }
                }
            }
        }
        return change;
    }

    private List<LogicalVariable> collectOneVarPerAggFromGroupOp(GroupByOperator group) {
        List<ILogicalPlan> nPlans = group.getNestedPlans();
        if (nPlans == null || nPlans.size() < 1) {
            return null;
        }

        List<LogicalVariable> aggVars = new ArrayList<LogicalVariable>();
        // test that the group-by computes a "listify" aggregate
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
            if (((AbstractLogicalExpression) expr).getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression fceAgg = (AbstractFunctionCallExpression) expr;
            if (fceAgg.getFunctionIdentifier() != AsterixBuiltinFunctions.LISTIFY) {
                continue;
            }
            aggVars.add(agg.getVariables().get(0));
        }
        return aggVars;
    }

    /**
     * @param expr
     * @param aggVars
     * @param gbyWithAgg
     * @param context
     * @return a pair whose first member is a boolean which is true iff
     *         something was changed in the expression tree rooted at expr. The
     *         second member is the result of transforming expr.
     * @throws AlgebricksException
     */
    private Pair<Boolean, ILogicalExpression> extractAggFunctionsFromExpression(Mutable<ILogicalExpression> exprRef,
            Map<LogicalVariable, GroupByOperator> gbyWithAgg,
            Map<ILogicalExpression, ILogicalExpression> aggregateExprToVarExpr, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        switch (expr.getExpressionTag()) {
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                FunctionIdentifier fi = AsterixBuiltinFunctions.getAggregateFunction(fce.getFunctionIdentifier());
                if (fi != null) {
                    ILogicalExpression a1 = fce.getArguments().get(0).getValue();
                    if (a1.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                        LogicalVariable argVar = ((VariableReferenceExpression) a1).getVariableReference();
                        GroupByOperator gbyOp = gbyWithAgg.get(argVar);

                        if (gbyOp != null) {
                            if (!aggregateExprToVarExpr.containsKey(expr)) {
                                LogicalVariable newVar = context.newVar();
                                AggregateFunctionCallExpression aggFun = AsterixBuiltinFunctions
                                        .makeAggregateFunctionExpression(fi, fce.getArguments());
                                rewriteGroupByAggregate(argVar, gbyOp, aggFun, newVar, context);
                                ILogicalExpression newVarExpr = new VariableReferenceExpression(newVar);
                                aggregateExprToVarExpr.put(expr, newVarExpr);
                                return new Pair<Boolean, ILogicalExpression>(Boolean.TRUE, newVarExpr);
                            } else {
                                ILogicalExpression varExpr = aggregateExprToVarExpr.get(expr);
                                return new Pair<Boolean, ILogicalExpression>(Boolean.TRUE, varExpr);
                            }
                        }
                    }
                }

                boolean change = false;
                for (Mutable<ILogicalExpression> a : fce.getArguments()) {
                    Pair<Boolean, ILogicalExpression> aggArg = extractAggFunctionsFromExpression(a, gbyWithAgg,
                            aggregateExprToVarExpr, context);
                    if (aggArg.first.booleanValue()) {
                        a.setValue(aggArg.second);
                        change = true;
                    }
                }
                return new Pair<Boolean, ILogicalExpression>(change, fce);
            }
            case VARIABLE:
            case CONSTANT: {
                return new Pair<Boolean, ILogicalExpression>(Boolean.FALSE, expr);
            }
            default: {
                throw new IllegalArgumentException();
            }
        }
    }

    private void rewriteGroupByAggregate(LogicalVariable oldAggVar, GroupByOperator gbyOp,
            AggregateFunctionCallExpression aggFun, LogicalVariable newAggVar, IOptimizationContext context)
            throws AlgebricksException {
        for (int j = 0; j < gbyOp.getNestedPlans().size(); j++) {
            AggregateOperator aggOp = (AggregateOperator) gbyOp.getNestedPlans().get(j).getRoots().get(0).getValue();
            int n = aggOp.getVariables().size();
            for (int i = 0; i < n; i++) {
                LogicalVariable v = aggOp.getVariables().get(i);
                if (v.equals(oldAggVar)) {
                    AbstractFunctionCallExpression oldAggExpr = (AbstractFunctionCallExpression) aggOp.getExpressions()
                            .get(i).getValue();
                    AggregateFunctionCallExpression newAggFun = AsterixBuiltinFunctions
                            .makeAggregateFunctionExpression(aggFun.getFunctionIdentifier(),
                                    new ArrayList<Mutable<ILogicalExpression>>());
                    for (Mutable<ILogicalExpression> arg : oldAggExpr.getArguments()) {
                        ILogicalExpression cloned = ((AbstractLogicalExpression) arg.getValue()).cloneExpression();
                        newAggFun.getArguments().add(new MutableObject<ILogicalExpression>(cloned));
                    }
                    aggOp.getVariables().add(newAggVar);
                    aggOp.getExpressions().add(new MutableObject<ILogicalExpression>(newAggFun));
                    context.computeAndSetTypeEnvironmentForOperator(aggOp);
                    break;
                }
            }
        }
    }

    private boolean pushSubplanAsAggIntoGby(Mutable<ILogicalOperator> subplanOpRef, GroupByOperator gbyOp,
            LogicalVariable varFromGroupAgg, Map<LogicalVariable, Integer> gbyAggVars,
            Map<LogicalVariable, GroupByOperator> gbyWithAgg, Map<LogicalVariable, Integer> gbyAggVarToPlanIndex,
            IOptimizationContext context) throws AlgebricksException {
        SubplanOperator subplan = (SubplanOperator) subplanOpRef.getValue();
        // only free var can be varFromGroupAgg
        HashSet<LogicalVariable> freeVars = new HashSet<LogicalVariable>();
        OperatorPropertiesUtil.getFreeVariablesInSubplans(subplan, freeVars);
        for (LogicalVariable vFree : freeVars) {
            if (!vFree.equals(varFromGroupAgg)) {
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
                case ASSIGN: {
                    break;
                }
                case UNNEST: {
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
                    if (fun.getFunctionIdentifier() != AsterixBuiltinFunctions.SCAN_COLLECTION) {
                        return false;
                    }
                    ILogicalExpression arg0 = fun.getArguments().get(0).getValue();
                    if (arg0.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        return false;
                    }
                    VariableReferenceExpression varExpr = (VariableReferenceExpression) arg0;
                    if (!varExpr.getVariableReference().equals(varFromGroupAgg)) {
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
                }
                default: {
                    return false;
                }
            }
        }
        if (pushableNestedSubplan) {
            for (int i = 0; i < gbyOp.getNestedPlans().size(); i++) {
                Mutable<ILogicalOperator> gbyAggRef = gbyOp.getNestedPlans().get(i).getRoots().get(0);
                AggregateOperator gbyAgg = (AggregateOperator) gbyAggRef.getValue();
                Mutable<ILogicalOperator> gbyAggChildRef = gbyAgg.getInputs().get(0);
                OperatorManipulationUtil.substituteVarRec(aggInSubplanOp, unnestVar,
                        findListifiedVariable(gbyAgg, varFromGroupAgg), true, context);
                gbyAgg.getVariables().addAll(aggInSubplanOp.getVariables());
                gbyAgg.getExpressions().addAll(aggInSubplanOp.getExpressions());
                for (LogicalVariable v : aggInSubplanOp.getVariables()) {
                    gbyWithAgg.put(v, gbyOp);
                    gbyAggVars.put(v, 0);
                    gbyAggVarToPlanIndex.put(v, i);
                }

                Mutable<ILogicalOperator> opRef1InSubplan = aggInSubplanOp.getInputs().get(0);
                if (opRef1InSubplan.getValue().getInputs().size() > 0) {
                    Mutable<ILogicalOperator> opRef2InSubplan = opRef1InSubplan.getValue().getInputs().get(0);
                    AbstractLogicalOperator op2InSubplan = (AbstractLogicalOperator) opRef2InSubplan.getValue();
                    if (op2InSubplan.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
                        List<Mutable<ILogicalOperator>> gbyInpList = gbyAgg.getInputs();
                        gbyInpList.clear();
                        gbyInpList.add(opRef1InSubplan);
                        while (true) {
                            opRef2InSubplan = opRef1InSubplan.getValue().getInputs().get(0);
                            op2InSubplan = (AbstractLogicalOperator) opRef2InSubplan.getValue();
                            if (op2InSubplan.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                                List<Mutable<ILogicalOperator>> opInpList = opRef1InSubplan.getValue().getInputs();
                                opInpList.clear();
                                opInpList.add(gbyAggChildRef);
                                break;
                            }
                            opRef1InSubplan = opRef2InSubplan;
                            if (opRef1InSubplan.getValue().getInputs().size() == 0) {
                                throw new IllegalStateException("PushAggregateIntoGroupbyRule: could not find UNNEST.");
                            }
                        }
                    }
                }
                subplanOpRef.setValue(subplan.getInputs().get(0).getValue());
                OperatorPropertiesUtil.typeOpRec(gbyAggRef, context);
            }
            return true;
        } else {
            return false;
        }
    }

    private LogicalVariable findListifiedVariable(AggregateOperator gbyAgg, LogicalVariable varFromGroupAgg) {
        int n = gbyAgg.getVariables().size();

        for (int i = 0; i < n; i++) {
            if (gbyAgg.getVariables().get(i).equals(varFromGroupAgg)) {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) gbyAgg.getExpressions().get(i)
                        .getValue();
                if (fce.getFunctionIdentifier().equals(AsterixBuiltinFunctions.LISTIFY)) {
                    ILogicalExpression argExpr = fce.getArguments().get(0).getValue();
                    if (((AbstractLogicalExpression) argExpr).getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                        return ((VariableReferenceExpression) argExpr).getVariableReference();
                    }
                }
            }
        }
        return null;
    }

}