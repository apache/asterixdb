/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushAggregateIntoGroupbyRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        HashMap<LogicalVariable, Integer> gbyAggVars = new HashMap<LogicalVariable, Integer>();
        HashMap<LogicalVariable, GroupByOperator> gbyWithAgg = new HashMap<LogicalVariable, GroupByOperator>();
        // first collect vars. referring to listified sequences
        boolean changed = collectVarsBottomUp(opRef, context, gbyAggVars, gbyWithAgg);
        if (changed) {
            removeRedundantListifies(opRef, context, gbyAggVars, gbyWithAgg);
        }
        return changed;
    }

    private void removeRedundantListifies(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            HashMap<LogicalVariable, Integer> gbyAggVars, HashMap<LogicalVariable, GroupByOperator> gbyWithAgg) {
        for (LogicalVariable aggVar : gbyAggVars.keySet()) {
            int occurs = gbyAggVars.get(aggVar);
            if (occurs == 0) {
                GroupByOperator gbyOp = gbyWithAgg.get(aggVar);
                AggregateOperator aggOp = (AggregateOperator) gbyOp.getNestedPlans().get(0).getRoots().get(0)
                        .getValue();
                int pos = aggOp.getVariables().indexOf(aggVar);
                aggOp.getVariables().remove(pos);
                aggOp.getExpressions().remove(pos);
            }
        }
    }

    private boolean collectVarsBottomUp(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            HashMap<LogicalVariable, Integer> gbyListifyVarsCount, HashMap<LogicalVariable, GroupByOperator> gbyWithAgg)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        context.addToDontApplySet(this, op1);
        boolean change = false;
        for (Mutable<ILogicalOperator> child : op1.getInputs()) {
            if (collectVarsBottomUp(child, context, gbyListifyVarsCount, gbyWithAgg)) {
                change = true;
            }
        }
        // Need to use a list instead of a hash-set, because a var. may appear
        // several times in the same op.
        List<LogicalVariable> used = new LinkedList<LogicalVariable>();
        VariableUtilities.getUsedVariables(op1, used);
        switch (op1.getOperatorTag()) {
            case ASSIGN: {
                boolean found = false;
                // Do some prefiltering: check if the Assign uses any gby vars.
                for (LogicalVariable v : used) {
                    if (gbyListifyVarsCount.get(v) != null) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    AssignOperator assign = (AssignOperator) op1;
                    for (Mutable<ILogicalExpression> exprRef : assign.getExpressions()) {
                        Pair<Boolean, ILogicalExpression> p = extractAggFunctionsFromExpression(exprRef, gbyWithAgg,
                                context);
                        if (p.first) {
                            change = true;
                            exprRef.setValue(p.second);
                        }
                    }
                    used.clear();
                    VariableUtilities.getUsedVariables(assign, used);
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
                        if (pushSubplanAsAggIntoGby(opRef, gbyOp, v, gbyListifyVarsCount, gbyWithAgg, context)) {
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
                LogicalVariable v1 = collectOneVarFromGroupOp((GroupByOperator) op1);
                if (v1 != null) {
                    gbyListifyVarsCount.put(v1, 0);
                    gbyWithAgg.put(v1, (GroupByOperator) op1);
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

    private LogicalVariable collectOneVarFromGroupOp(GroupByOperator group) {
        List<ILogicalPlan> nPlans = group.getNestedPlans();
        if (nPlans == null || nPlans.size() < 1) {
            return null;
        }

        // test that the group-by computes a "listify" aggregate
        AbstractLogicalOperator topOp = (AbstractLogicalOperator) nPlans.get(0).getRoots().get(0).getValue();
        if (topOp.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return null;
        }
        AggregateOperator agg = (AggregateOperator) topOp;
        if (agg.getVariables().size() != 1) {
            return null;
        }
        ILogicalExpression expr = agg.getExpressions().get(0).getValue();
        if (((AbstractLogicalExpression) expr).getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression fceAgg = (AbstractFunctionCallExpression) expr;
        if (fceAgg.getFunctionIdentifier() != AsterixBuiltinFunctions.LISTIFY) {
            return null;
        }

        return agg.getVariables().get(0);
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
            HashMap<LogicalVariable, GroupByOperator> gbyWithAgg, IOptimizationContext context)
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
                            LogicalVariable newVar = context.newVar();
                            AggregateFunctionCallExpression aggFun = AsterixBuiltinFunctions
                                    .makeAggregateFunctionExpression(fi, fce.getArguments());
                            rewriteGroupByAggregate(argVar, gbyOp, aggFun, newVar, context);
                            return new Pair<Boolean, ILogicalExpression>(Boolean.TRUE, new VariableReferenceExpression(
                                    newVar));
                        }
                    }
                }

                boolean change = false;
                for (Mutable<ILogicalExpression> a : fce.getArguments()) {
                    Pair<Boolean, ILogicalExpression> aggArg = extractAggFunctionsFromExpression(a, gbyWithAgg, context);
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
        AggregateOperator aggOp = (AggregateOperator) gbyOp.getNestedPlans().get(0).getRoots().get(0).getValue();
        int n = aggOp.getVariables().size();
        for (int i = 0; i < n; i++) {
            LogicalVariable v = aggOp.getVariables().get(i);
            if (v.equals(oldAggVar)) {
                AbstractFunctionCallExpression oldAggExpr = (AbstractFunctionCallExpression) aggOp.getExpressions()
                        .get(i).getValue();
                AggregateFunctionCallExpression newAggFun = AsterixBuiltinFunctions.makeAggregateFunctionExpression(
                        aggFun.getFunctionIdentifier(), new ArrayList<Mutable<ILogicalExpression>>());
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

    private boolean pushSubplanAsAggIntoGby(Mutable<ILogicalOperator> subplanOpRef, GroupByOperator gbyOp,
            LogicalVariable varFromGroupAgg, HashMap<LogicalVariable, Integer> gbyAggVars,
            HashMap<LogicalVariable, GroupByOperator> gbyWithAgg, IOptimizationContext context)
            throws AlgebricksException {
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
            Mutable<ILogicalOperator> gbyAggRef = gbyOp.getNestedPlans().get(0).getRoots().get(0);
            AggregateOperator gbyAgg = (AggregateOperator) gbyAggRef.getValue();
            Mutable<ILogicalOperator> gbyAggChildRef = gbyAgg.getInputs().get(0);
            OperatorManipulationUtil.substituteVarRec(aggInSubplanOp, unnestVar,
                    findListifiedVariable(gbyAgg, varFromGroupAgg), true, context);
            gbyAgg.getVariables().addAll(aggInSubplanOp.getVariables());
            gbyAgg.getExpressions().addAll(aggInSubplanOp.getExpressions());
            for (LogicalVariable v : aggInSubplanOp.getVariables()) {
                gbyWithAgg.put(v, gbyOp);
                gbyAggVars.put(v, 0);
            }

            Mutable<ILogicalOperator> opRef1InSubplan = aggInSubplanOp.getInputs().get(0);
            if (opRef1InSubplan.getValue().getInputs().size() > 0) {
                List<Mutable<ILogicalOperator>> gbyInpList = gbyAgg.getInputs();
                gbyInpList.clear();
                gbyInpList.add(opRef1InSubplan);
                while (true) {
                    Mutable<ILogicalOperator> opRef2InSubplan = opRef1InSubplan.getValue().getInputs().get(0);
                    AbstractLogicalOperator op2InSubplan = (AbstractLogicalOperator) opRef2InSubplan.getValue();
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
            subplanOpRef.setValue(subplan.getInputs().get(0).getValue());
            OperatorPropertiesUtil.typeOpRec(gbyAggRef, context);
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
