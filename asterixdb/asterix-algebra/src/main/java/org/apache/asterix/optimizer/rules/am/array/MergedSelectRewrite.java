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

package org.apache.asterix.optimizer.rules.am.array;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

/**
 * For all expressions that pertain to a single dataset but are spread across various SELECTs due to requiring an
 * intermediate UNNEST, "push up" the lower SELECT expressions into a single unifying SELECT. This is used to
 * recognize composite atomic-array index applicability.
 */
public class MergedSelectRewrite implements IIntroduceAccessMethodRuleLocalRewrite<SelectOperator> {
    private final Set<Mutable<ILogicalExpression>> selectConjuncts = new LinkedHashSet<>();
    private final Deque<SelectOperator> selectRootStack = new ArrayDeque<>();
    private Mutable<ILogicalOperator> originalDataSourceRef;

    @Override
    public SelectOperator createOperator(SelectOperator originalOperator, IOptimizationContext context)
            throws AlgebricksException {
        // Initialize the conjuncts for our SELECT.
        selectConjuncts.clear();
        List<Mutable<ILogicalExpression>> thisSelectConjuncts = new ArrayList<>();
        if (!originalOperator.getCondition().getValue().splitIntoConjuncts(thisSelectConjuncts)) {
            thisSelectConjuncts.add(originalOperator.getCondition());
        }
        selectConjuncts.addAll(thisSelectConjuncts);

        // Explore all operators below this SELECT until the first data source operator.
        selectRootStack.push(originalOperator);
        if (!collectSelectConjuncts(originalOperator.getInputs().get(0))) {
            // We encountered a JOIN operator. Exit early.
            return null;
        }

        if (thisSelectConjuncts.size() == selectConjuncts.size()) {
            // No other SELECTs were found. Return null to indicate there were no SELECTs to merge.
            return null;

        } else {
            // We have found additional SELECTs. Form a conjunction.
            AbstractFunctionCallExpression andCond = new ScalarFunctionCallExpression(
                    context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND));
            andCond.setSourceLocation(originalOperator.getSourceLocation());
            for (Mutable<ILogicalExpression> conjunct : selectConjuncts) {
                andCond.getArguments().add(conjunct);
            }

            // Return a new plan that removes all SELECTs that were pushed up.
            SelectOperator newSelectOperator = new SelectOperator(new MutableObject<>(andCond),
                    originalOperator.getRetainMissingAsValue(), originalOperator.getMissingPlaceholderVariable());
            newSelectOperator.setSourceLocation(originalOperator.getSourceLocation());
            ILogicalPlan newSelectInputPlan = OperatorManipulationUtil
                    .deepCopy(new ALogicalPlanImpl(originalOperator.getInputs().get(0)), context);
            newSelectOperator.getInputs().add(newSelectInputPlan.getRoots().get(0));
            removeSelectsFromPlan(newSelectOperator, newSelectInputPlan.getRoots().get(0));
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(newSelectOperator, context);
            return newSelectOperator;
        }
    }

    @Override
    public SelectOperator restoreBeforeRewrite(List<Mutable<ILogicalOperator>> afterOperatorRefs,
            IOptimizationContext context) throws AlgebricksException {
        return selectRootStack.pop();
    }

    private boolean collectSelectConjuncts(Mutable<ILogicalOperator> workingOp) {
        switch (workingOp.getValue().getOperatorTag()) {
            case DATASOURCESCAN:
            case EMPTYTUPLESOURCE:
            case UNNEST_MAP:
                // If we have reached a datasource operator, stop our search.
                originalDataSourceRef = workingOp;
                break;

            case INNERJOIN:
            case LEFTOUTERJOIN:
                // We are not interested in exploring joins in this class.
                return false;

            case SELECT:
                SelectOperator selectOperator = (SelectOperator) workingOp.getValue();
                List<Mutable<ILogicalExpression>> thisSelectConjuncts = new ArrayList<>();
                if (!selectOperator.getCondition().getValue().splitIntoConjuncts(thisSelectConjuncts)) {
                    thisSelectConjuncts.add(selectOperator.getCondition());
                }
                selectConjuncts.addAll(thisSelectConjuncts);

            default:
                // Explore the rest of our plan in our DFS fashion.
                for (Mutable<ILogicalOperator> input : workingOp.getValue().getInputs()) {
                    if (!collectSelectConjuncts(input)) {
                        return false;
                    }
                }
        }
        return true;
    }

    private void removeSelectsFromPlan(ILogicalOperator parentOp, Mutable<ILogicalOperator> workingOp) {
        Mutable<ILogicalOperator> workingOpInParent =
                parentOp.getInputs().stream().filter(i -> i.equals(workingOp)).collect(Collectors.toList()).get(0);
        int indexOfWorkingOpInParent = parentOp.getInputs().indexOf(workingOpInParent);

        switch (workingOp.getValue().getOperatorTag()) {
            case DATASOURCESCAN:
            case EMPTYTUPLESOURCE:
            case UNNEST_MAP:
                // If we have reached a datasource operator, stop and replace this with our original datasource.
                // (IntroduceSelectAccessMethodRule replaces this specific operator, so this must be the original.)
                parentOp.getInputs().set(indexOfWorkingOpInParent, originalDataSourceRef);
                break;

            case SELECT:
                parentOp.getInputs().set(indexOfWorkingOpInParent, workingOp.getValue().getInputs().get(0));

            default:
                // Explore the rest of our plan in our DFS fashion.
                for (Mutable<ILogicalOperator> input : workingOp.getValue().getInputs()) {
                    removeSelectsFromPlan(workingOp.getValue(), input);
                }
        }
    }
}
