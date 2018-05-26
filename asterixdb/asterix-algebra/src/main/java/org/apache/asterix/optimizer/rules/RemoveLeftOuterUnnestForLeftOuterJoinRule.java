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

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule cancels left outer unnest on top of a group-by and left-outer join:<br/>
 *
 * <pre>
 * left-outer-unnest $x <- scan-collection($y)
 *   group-by($k){
 *     aggregate $y <- listify($z1)
 *       select not(is-missing($z2))
 *         NTS
 *   }
 *     left outer join ($a=$b)
 *       left input branch
 *       right input branch ($z1 and $z2 comes from this branch)
 * </pre>
 *
 * will be converted into<br/>
 *
 * <pre>
 * left outer join ($a=$b)
 *       left input branch
 *       right input branch ($z1 and $z2 comes from this branch)
 * </pre>
 */
public class RemoveLeftOuterUnnestForLeftOuterJoinRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op1 = opRef.getValue();

        // Checks the plan pattern.
        if (!checkOperatorPattern(op1)) {
            return false;
        }

        LeftOuterUnnestOperator outerUnnest = (LeftOuterUnnestOperator) op1;
        GroupByOperator gbyOperator = (GroupByOperator) outerUnnest.getInputs().get(0).getValue();
        LeftOuterJoinOperator lojOperator = (LeftOuterJoinOperator) gbyOperator.getInputs().get(0).getValue();

        // Checks whether the left outer unnest and the group-by operator are qualified for rewriting.
        Triple<Boolean, ILogicalExpression, ILogicalExpression> checkGbyResult =
                checkUnnestAndGby(outerUnnest, gbyOperator);
        // The argument for listify and not(is-missing(...)) check should be variables.
        if (!isVariableReference(checkGbyResult.second) || !isVariableReference(checkGbyResult.third)) {
            return false;
        }

        // Checks whether both the listify variable and the condition test variable are from the right input
        // branch of the left outer join.
        LogicalVariable listifyVar = ((VariableReferenceExpression) checkGbyResult.second).getVariableReference();
        LogicalVariable conditionTestVar = ((VariableReferenceExpression) checkGbyResult.third).getVariableReference();
        if (!checkListifyAndConditionVar(lojOperator, listifyVar, conditionTestVar)) {
            return false;
        }

        // Does the rewrite.
        removeGroupByAndOuterUnnest(opRef, context, outerUnnest, gbyOperator, lojOperator, listifyVar);
        return true;
    }

    // Checks the plan pattern.
    private boolean checkOperatorPattern(ILogicalOperator op1) {
        if (op1.getOperatorTag() != LogicalOperatorTag.LEFT_OUTER_UNNEST) {
            return false;
        }
        ILogicalOperator op2 = op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        ILogicalOperator op3 = op2.getInputs().get(0).getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        return true;
    }

    // Checks left outer unnest and gby.
    private Triple<Boolean, ILogicalExpression, ILogicalExpression> checkUnnestAndGby(
            LeftOuterUnnestOperator outerUnnest, GroupByOperator gbyOperator) {
        // Checks left outer unnest.
        Pair<Boolean, LogicalVariable> checkUnnestResult = checkUnnest(outerUnnest);
        if (!checkUnnestResult.first) {
            return new Triple<>(false, null, null);
        }

        // Checks group-by.
        LogicalVariable varToUnnest = checkUnnestResult.second;
        Triple<Boolean, ILogicalExpression, ILogicalExpression> checkGbyResult = checkGroupBy(gbyOperator, varToUnnest);
        if (!checkGbyResult.first) {
            return new Triple<>(false, null, null);
        }
        return checkGbyResult;
    }

    // Checks whether the left outer unnest operator does not have positional variable and uses
    // only one variable for unnesting.
    private Pair<Boolean, LogicalVariable> checkUnnest(LeftOuterUnnestOperator outerUnnest) {
        if (outerUnnest.getPositionalVariable() != null) {
            return new Pair<>(false, null);
        }
        Set<LogicalVariable> varsToUnnest = new HashSet<>();
        outerUnnest.getExpressionRef().getValue().getUsedVariables(varsToUnnest);
        if (varsToUnnest.size() > 1) {
            return new Pair<>(false, null);
        }
        return new Pair<>(true, varsToUnnest.iterator().next());
    }

    // Checks the group-by operator on top of the left outer join operator.
    private Triple<Boolean, ILogicalExpression, ILogicalExpression> checkGroupBy(GroupByOperator gbyOperator,
            LogicalVariable varToUnnest) {
        Pair<Boolean, ILogicalOperator> checkNestedPlanResult = checkNestedPlan(gbyOperator);
        if (!checkNestedPlanResult.first) {
            return new Triple<>(false, null, null);
        }
        ILogicalOperator root = checkNestedPlanResult.second;
        if (root.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return new Triple<>(false, null, null);
        }

        // Checks aggregate.
        AggregateOperator agg = (AggregateOperator) root;
        Pair<Boolean, ILogicalExpression> listifyArgPair = checksAggregate(agg, varToUnnest);
        if (!listifyArgPair.first) {
            return new Triple<>(false, null, null);
        }

        // Checks select.
        ILogicalOperator rootInputOp = root.getInputs().get(0).getValue();
        if (rootInputOp.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return new Triple<>(false, null, null);
        }
        SelectOperator select = (SelectOperator) rootInputOp;
        Pair<Boolean, ILogicalExpression> conditionArgPair = checkSelect(select);
        return new Triple<>(true, listifyArgPair.second, conditionArgPair.second);
    }

    // Checks the nested plan for the group-by operator.
    private Pair<Boolean, ILogicalOperator> checkNestedPlan(GroupByOperator gbyOperator) {
        List<ILogicalPlan> nestedPlans = gbyOperator.getNestedPlans();
        if (nestedPlans.size() > 1) {
            return new Pair<>(false, null);
        }
        ILogicalPlan plan = nestedPlans.get(0);
        List<Mutable<ILogicalOperator>> roots = plan.getRoots();
        if (roots.size() > 1) {
            return new Pair<>(false, null);
        }
        ILogicalOperator root = roots.get(0).getValue();
        return new Pair<>(true, root);
    }

    // Checks the aggregate expression.
    private Pair<Boolean, ILogicalExpression> checksAggregate(AggregateOperator agg, LogicalVariable varToUnnest) {
        if (!agg.getVariables().contains(varToUnnest)) {
            return new Pair<>(false, null);
        }
        List<Mutable<ILogicalExpression>> exprRefs = agg.getExpressions();
        if (exprRefs.size() > 1) {
            return new Pair<>(false, null);
        }
        ILogicalExpression expr = exprRefs.get(0).getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return new Pair<>(false, null);
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (!funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.LISTIFY)) {
            return new Pair<>(false, null);
        }
        return new Pair<>(true, funcExpr.getArguments().get(0).getValue());
    }

    // Checks the expression for the nested select operator inside the group-by operator.
    private Pair<Boolean, ILogicalExpression> checkSelect(SelectOperator select) {
        ILogicalExpression condition = select.getCondition().getValue();
        if (condition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return new Pair<>(false, null);
        }
        AbstractFunctionCallExpression conditionFunc = (AbstractFunctionCallExpression) condition;
        if (!conditionFunc.getFunctionIdentifier().equals(BuiltinFunctions.NOT)) {
            return new Pair<>(false, null);
        }
        condition = conditionFunc.getArguments().get(0).getValue();
        if (condition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return new Pair<>(false, null);
        }
        conditionFunc = (AbstractFunctionCallExpression) condition;
        if (!conditionFunc.getFunctionIdentifier().equals(BuiltinFunctions.IS_MISSING)) {
            return new Pair<>(false, null);
        }
        ILogicalExpression conditionArg = conditionFunc.getArguments().get(0).getValue();
        return new Pair<>(true, conditionArg);
    }

    // Checks whether the listify variable and the condition test variable come from the right input
    // branch of the left outer join.
    private boolean checkListifyAndConditionVar(LeftOuterJoinOperator lojOperator, LogicalVariable listifyVar,
            LogicalVariable conditionTestVar) throws AlgebricksException {
        ILogicalOperator rightJoinInputOp = lojOperator.getInputs().get(1).getValue();
        Set<LogicalVariable> rightProducedVars = new HashSet<>();
        VariableUtilities.getLiveVariables(rightJoinInputOp, rightProducedVars);
        if (!rightProducedVars.contains(conditionTestVar) || !rightProducedVars.contains(listifyVar)) {
            return false;
        }
        return true;
    }

    // Performs the actual rewrite.
    private void removeGroupByAndOuterUnnest(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            LeftOuterUnnestOperator outerUnnest, GroupByOperator gbyOperator, LeftOuterJoinOperator lojOperator,
            LogicalVariable listifyVar) throws AlgebricksException {
        List<LogicalVariable> lhs = new ArrayList<>();
        List<Mutable<ILogicalExpression>> rhs = new ArrayList<>();
        lhs.add(outerUnnest.getVariable());
        VariableReferenceExpression listifyVarRef = new VariableReferenceExpression(listifyVar);
        listifyVarRef.setSourceLocation(gbyOperator.getSourceLocation());
        rhs.add(new MutableObject<ILogicalExpression>(listifyVarRef));
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList = gbyOperator.getGroupByList();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> gbyPair : gbyList) {
            lhs.add(gbyPair.first);
            rhs.add(gbyPair.second);
        }
        AssignOperator assignOp = new AssignOperator(lhs, rhs);
        assignOp.setSourceLocation(outerUnnest.getSourceLocation());
        assignOp.getInputs().add(new MutableObject<ILogicalOperator>(lojOperator));
        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        opRef.setValue(assignOp);
    }

    private boolean isVariableReference(ILogicalExpression expr) {
        return expr.getExpressionTag() == LogicalExpressionTag.VARIABLE;
    }

}
