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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

/**
 * For use in writing a "throwaway" branch which removes NTS and subplan operators. The result of this invocation is to
 * be given to the {@code IntroduceSelectAccessMethodRule} to check if an array index can be used.
 * <br>
 * If we are given the pattern (an existential quantification query):
 * <pre>
 * SELECT_1(some variable)
 * SUBPLAN_1 -------------------------------|
 * (parent branch input)        AGGREGATE(NON-EMPTY-STREAM)
 *                              SELECT_2(some predicate)
 *                              (UNNEST/ASSIGN)*
 *                              UNNEST(on variable)
 *                              NESTED-TUPLE-SOURCE
 * </pre>
 * We return the following branch:
 * <pre>
 * SELECT_2(some predicate)
 * (UNNEST/ASSIGN)*
 * UNNEST(on variable)
 * (parent branch input)
 * </pre>
 * <p>
 * If we are given the pattern (a universal quantification query):
 * <pre>
 * SELECT_1(some variable AND array is not empty)
 * SUBPLAN_1 -------------------------------|
 * (parent branch input)        AGGREGATE(EMPTY-STREAM)
 *                              SELECT_2(NOT(IF-MISSING-OR-NULL(some optimizable predicate)))
 *                              (UNNEST/ASSIGN)*
 *                              UNNEST(on variable)
 *                              NESTED-TUPLE-SOURCE
 * </pre>
 * We return the following branch:
 * <pre>
 * SELECT_2(some optimizable predicate)  <--- removed the NOT(IF-MISSING-OR-NULL(...))!
 * (UNNEST/ASSIGN)*
 * UNNEST(on variable)
 * (parent branch input)
 * </pre>
 * <p>
 * In the case of nested-subplans, we return a copy of the innermost SELECT followed by all relevant UNNEST/ASSIGNs.
 */
public class SelectFromSubplanRewrite extends AbstractOperatorFromSubplanRewrite<SelectOperator> {
    private final static Set<FunctionIdentifier> optimizableFunctions = new HashSet<>();
    private final Deque<SelectOperator> selectRootStack = new ArrayDeque<>();

    /**
     * Add an optimizable function from an access method that can take advantage of this throwaway branch.
     */
    public static void addOptimizableFunction(FunctionIdentifier functionIdentifier) {
        optimizableFunctions.add(functionIdentifier);
    }

    /**
     * Create a new branch to match that of the form:
     *
     * <pre>
     * SELECT (...)
     * (UNNEST/ASSIGN)*
     * UNNEST
     * ...
     * </pre>
     * <p>
     * Operators are *created* here, rather than just reconnected from the original branch.
     */
    @Override
    public SelectOperator createOperator(SelectOperator originalOperator, IOptimizationContext context)
            throws AlgebricksException {
        // Reset our context.
        selectRootStack.push(originalOperator);
        reset(originalOperator.getSourceLocation(), context, optimizableFunctions);

        // Gather all boolean variables and SUBPLANs.
        List<VariableReferenceExpression> booleanVariables = new ArrayList<>();
        List<ILogicalExpression> miscExpressions = new ArrayList<>();
        List<SubplanOperator> subplanOperators = new ArrayList<>();
        gatherBooleanVariables(originalOperator.getCondition().getValue(), booleanVariables, miscExpressions);
        gatherSubplanOperators(originalOperator, subplanOperators);
        Iterator<SubplanOperator> subplanIterator = subplanOperators.listIterator();
        if (booleanVariables.isEmpty() || subplanOperators.isEmpty()) {
            return null;
        }

        // TODO (GLENN): We currently assume that SUBPLAN-SELECTs are back-to-back.
        SubplanOperator bottommostSubplanOperator = subplanOperators.get(subplanOperators.size() - 1);

        // We now need to match these variables to SUBPLANs downstream.
        while (subplanIterator.hasNext()) {
            SubplanOperator workingSubplanOperator = subplanIterator.next();
            AggregateOperator aggregateFromSubplan = getAggregateFromSubplan(workingSubplanOperator);
            if (aggregateFromSubplan == null) {
                continue;
            }

            boolean isMatchingAggregateVariableFound = false;
            for (LogicalVariable aggregateVariable : aggregateFromSubplan.getVariables()) {
                Optional<VariableReferenceExpression> matchingBooleanVariable = booleanVariables.stream()
                        .filter(v -> v.getVariableReference().equals(aggregateVariable)).findFirst();
                if (matchingBooleanVariable.isPresent()) {
                    isMatchingAggregateVariableFound = true;

                    // Note: we (currently) don't expect variables to shared in multiple subplan outputs.
                    booleanVariables.remove(matchingBooleanVariable.get());
                }
            }
            if (!isMatchingAggregateVariableFound) {
                subplanIterator.remove();
            }
        }
        if (subplanOperators.isEmpty()) {
            // No <boolean variable, SUBPLAN> pairs could be found.
            return null;
        }

        // For all unused boolean variables, we'll add them back to our misc. expression set.
        miscExpressions.addAll(booleanVariables);

        // For each subplan, traverse and generate a SELECT branch if applicable.
        List<Pair<SelectOperator, UnnestOperator>> traversalOutputs = new ArrayList<>();
        for (SubplanOperator subplanBranch : subplanOperators) {
            Pair<SelectOperator, UnnestOperator> traversalOutput = traverseSubplanBranch(subplanBranch, null, false);
            if (traversalOutput != null) {
                traversalOutputs.add(traversalOutput);
            }
        }
        if (traversalOutputs.size() == 0) {
            return null;

        } else if (traversalOutputs.size() == 1) {
            Pair<SelectOperator, UnnestOperator> traversalOutput = traversalOutputs.get(0);
            ILogicalOperator bottommostOperator = traversalOutput.second;
            SelectOperator selectRewriteOperator = traversalOutput.first;
            bottommostOperator.getInputs().addAll(bottommostSubplanOperator.getInputs());
            return finalizeSelectOperator(selectRewriteOperator, miscExpressions, context);

        } else {
            ScalarFunctionCallExpression workingSelectCondition =
                    new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND));
            SelectOperator mergedSelectOperator = new SelectOperator(new MutableObject<>(workingSelectCondition));
            ILogicalOperator workingLeafOperator = mergedSelectOperator;
            for (Pair<SelectOperator, UnnestOperator> traversalOutput : traversalOutputs) {
                SelectOperator selectRewriteOperator = traversalOutput.first;
                ILogicalExpression selectRewriteExpr = selectRewriteOperator.getCondition().getValue();

                // First, we coalesce our SELECT conditions.
                List<Mutable<ILogicalExpression>> selectRewriteExprConjuncts = new ArrayList<>();
                if (selectRewriteExpr.splitIntoConjuncts(selectRewriteExprConjuncts)) {
                    for (Mutable<ILogicalExpression> conjunct : selectRewriteExprConjuncts) {
                        workingSelectCondition.getArguments().add(new MutableObject<>(conjunct.getValue()));
                    }
                } else {
                    workingSelectCondition.getArguments().add(new MutableObject<>(selectRewriteExpr));
                }

                // Next, we connect the bottommost operator back to the current leaf.
                workingLeafOperator.getInputs().add(new MutableObject<>(traversalOutput.second));
                workingLeafOperator = traversalOutput.second;
            }

            // Finally, we connect the leaf to the bottommost subplan input.
            workingLeafOperator.getInputs().addAll(bottommostSubplanOperator.getInputs());
            return finalizeSelectOperator(mergedSelectOperator, miscExpressions, context);
        }
    }

    /**
     * To undo this process is to return what was passed to us at {@code createOperator} time. We do not touch the
     * operators after the SELECT.
     */
    @Override
    public SelectOperator restoreBeforeRewrite(List<Mutable<ILogicalOperator>> afterOperatorRefs,
            IOptimizationContext context) {
        return selectRootStack.pop();
    }

    private SelectOperator finalizeSelectOperator(SelectOperator selectOp, List<ILogicalExpression> auxiliaryExprs,
            IOptimizationContext context) throws AlgebricksException {
        if (auxiliaryExprs.isEmpty()) {
            // There are no auxiliary expressions to add.
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(selectOp, context);
            return selectOp;
        }

        // Otherwise... we need to build a new SELECT.
        ScalarFunctionCallExpression workingSelectCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND));
        if (!selectOp.getCondition().getValue().splitIntoConjuncts(workingSelectCondition.getArguments())) {
            workingSelectCondition.getArguments().add(selectOp.getCondition());
        }
        for (ILogicalExpression auxiliaryExpr : auxiliaryExprs) {
            workingSelectCondition.getArguments().add(new MutableObject<>(auxiliaryExpr));
            //workingSelectCondition.getArguments().add(auxiliaryExpr); // MMK
        }
        SelectOperator mergedSelectOperator = new SelectOperator(new MutableObject<>(workingSelectCondition));
        mergedSelectOperator.getInputs().addAll(selectOp.getInputs());
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(mergedSelectOperator, context);
        return mergedSelectOperator;
    }
}
