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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RunningAggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.UnpartitionedPropertyComputer;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

import com.google.common.collect.ImmutableSet;

/**
 * This rule cancels the unnest with the nested listify. Formally, the following plan<br/>
 *
 * <pre>
 * unnset $x <- $y
 *   group-by($k){
 *     aggregate $y <- listify($z)
 *     ...
 *   }
 * </pre>
 *
 * will be converted into<br/>
 *
 * <pre>
 * assign $x <- $z
 *   sort($k)
 * </pre>
 *
 * When the positional variable exists, for example the original plan is like<br/>
 *
 * <pre>
 * unnset $x at $p <- $y
 *   group-by($k){
 *     aggregate $y <- listify($z)
 *     ...
 *   }
 * </pre>
 *
 * will be converted into<br/>
 *
 * <pre>
 * group-by($k){
 *   running-aggregate $p <- tid()
 * }
 * </pre>
 */
public class CancelUnnestWithNestedListifyRule implements IAlgebraicRewriteRule {

    /* (non-Javadoc)
     * @see org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule#rewritePost(org.apache.commons.lang3.mutable.Mutable, org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext)
     */
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
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
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        VariableUtilities.getUsedVariables(op, varSet);
        if (op.hasNestedPlans()) {
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
        LogicalVariable unnestedVar;
        switch (expr.getExpressionTag()) {
            case VARIABLE:
                unnestedVar = ((VariableReferenceExpression) expr).getVariableReference();
                break;
            case FUNCTION_CALL:
                if (((AbstractFunctionCallExpression) expr)
                        .getFunctionIdentifier() != BuiltinFunctions.SCAN_COLLECTION) {
                    return false;
                }
                AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) expr;
                ILogicalExpression functionCallArgExpr = functionCall.getArguments().get(0).getValue();
                if (functionCallArgExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                    return false;
                }
                unnestedVar = ((VariableReferenceExpression) functionCallArgExpr).getVariableReference();
                break;
            default:
                return false;
        }
        if (varUsedAbove.contains(unnestedVar)) {
            return false;
        }

        Mutable<ILogicalOperator> opRef2 = op1.getInputs().get(0);
        AbstractLogicalOperator r = (AbstractLogicalOperator) opRef2.getValue();

        if (r.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }

        // go inside of a group-by plan
        GroupByOperator gby = (GroupByOperator) r;
        if (gby.getNestedPlans().size() != 1) {
            return false;
        }
        if (gby.getNestedPlans().get(0).getRoots().size() != 1) {
            return false;
        }

        AbstractLogicalOperator nestedPlanRoot =
                (AbstractLogicalOperator) gby.getNestedPlans().get(0).getRoots().get(0).getValue();
        if (nestedPlanRoot.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator agg = (AggregateOperator) nestedPlanRoot;
        Mutable<ILogicalOperator> aggInputOpRef = agg.getInputs().get(0);

        if (agg.getVariables().size() > 1) {
            return false;
        }

        if (OperatorManipulationUtil.ancestorOfOperators(agg, ImmutableSet.of(LogicalOperatorTag.LIMIT,
                LogicalOperatorTag.ORDER, LogicalOperatorTag.GROUP, LogicalOperatorTag.DISTINCT))) {
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

        ArrayList<LogicalVariable> assgnVars = new ArrayList<LogicalVariable>(1);
        assgnVars.add(unnest1.getVariable());
        ArrayList<Mutable<ILogicalExpression>> assgnExprs = new ArrayList<Mutable<ILogicalExpression>>(1);
        VariableReferenceExpression paramRef = new VariableReferenceExpression(paramVar);
        paramRef.setSourceLocation(arg0.getSourceLocation());
        assgnExprs.add(new MutableObject<ILogicalExpression>(paramRef));
        AssignOperator assign = new AssignOperator(assgnVars, assgnExprs);
        assign.setSourceLocation(arg0.getSourceLocation());

        LogicalVariable posVar = unnest1.getPositionalVariable();
        if (posVar == null) {
            // Creates assignment for group-by keys.
            ArrayList<LogicalVariable> gbyKeyAssgnVars = new ArrayList<LogicalVariable>();
            ArrayList<Mutable<ILogicalExpression>> gbyKeyAssgnExprs = new ArrayList<Mutable<ILogicalExpression>>();
            for (int i = 0; i < gby.getGroupByList().size(); i++) {
                if (gby.getGroupByList().get(i).first != null) {
                    gbyKeyAssgnVars.add(gby.getGroupByList().get(i).first);
                    gbyKeyAssgnExprs.add(gby.getGroupByList().get(i).second);
                }
            }

            // Moves the nested pipeline before aggregation out of the group-by op.
            Mutable<ILogicalOperator> bottomOpRef = aggInputOpRef;
            AbstractLogicalOperator bottomOp = (AbstractLogicalOperator) bottomOpRef.getValue();
            while (bottomOp.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
                bottomOpRef = bottomOp.getInputs().get(0);
                bottomOp = (AbstractLogicalOperator) bottomOpRef.getValue();
            }

            // Removes the group-by operator.
            opRef.setValue(assign);
            assign.getInputs().add(aggInputOpRef);
            AssignOperator gbyKeyAssign = new AssignOperator(gbyKeyAssgnVars, gbyKeyAssgnExprs);
            gbyKeyAssign.setSourceLocation(gby.getSourceLocation());
            gbyKeyAssign.getInputs().add(gby.getInputs().get(0));
            bottomOpRef.setValue(gbyKeyAssign);

            context.computeAndSetTypeEnvironmentForOperator(gbyKeyAssign);
            context.computeAndSetTypeEnvironmentForOperator(assign);
        } else {
            // if positional variable is used in unnest, the unnest will be pushed into the group-by as a running-aggregate

            // First create assign for the unnest variable
            List<LogicalVariable> nestedAssignVars = new ArrayList<LogicalVariable>();
            List<Mutable<ILogicalExpression>> nestedAssignExprs = new ArrayList<Mutable<ILogicalExpression>>();
            nestedAssignVars.add(unnest1.getVariable());
            nestedAssignExprs.add(new MutableObject<ILogicalExpression>(arg0));
            AssignOperator nestedAssign = new AssignOperator(nestedAssignVars, nestedAssignExprs);
            SourceLocation sourceLoc = unnest1.getSourceLocation();
            nestedAssign.setSourceLocation(sourceLoc);
            nestedAssign.getInputs().add(opRef2);

            // Then create running aggregation for the positional variable
            List<LogicalVariable> raggVars = new ArrayList<LogicalVariable>();
            List<Mutable<ILogicalExpression>> raggExprs = new ArrayList<Mutable<ILogicalExpression>>();
            raggVars.add(posVar);
            StatefulFunctionCallExpression fce = new StatefulFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.TID), UnpartitionedPropertyComputer.INSTANCE);
            fce.setSourceLocation(sourceLoc);
            raggExprs.add(new MutableObject<ILogicalExpression>(fce));
            RunningAggregateOperator raggOp = new RunningAggregateOperator(raggVars, raggExprs);
            raggOp.setSourceLocation(sourceLoc);
            raggOp.setExecutionMode(unnest1.getExecutionMode());
            RunningAggregatePOperator raggPOp = new RunningAggregatePOperator();
            raggOp.setPhysicalOperator(raggPOp);
            raggOp.getInputs().add(nestedPlanRoot.getInputs().get(0));
            gby.getNestedPlans().get(0).getRoots().set(0, new MutableObject<ILogicalOperator>(raggOp));

            opRef.setValue(nestedAssign);

            context.computeAndSetTypeEnvironmentForOperator(nestedAssign);
            context.computeAndSetTypeEnvironmentForOperator(raggOp);
            context.computeAndSetTypeEnvironmentForOperator(gby);

        }

        return true;
    }
}
