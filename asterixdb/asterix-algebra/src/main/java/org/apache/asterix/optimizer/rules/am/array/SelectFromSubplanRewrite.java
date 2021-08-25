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
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

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
 *
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
 *
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
     *
     * Operators are *created* here, rather than just reconnected from the original branch.
     */
    @Override
    public SelectOperator createOperator(SelectOperator originalOperator, IOptimizationContext context)
            throws AlgebricksException {
        // Reset our context.
        selectRootStack.push(originalOperator);
        reset(originalOperator.getSourceLocation(), context, optimizableFunctions);

        // We expect a) a SUBPLAN as input to this SELECT, and b) our SELECT to be conditioning on a variable.
        LogicalVariable originalSelectVar = getConditioningVariable(originalOperator.getCondition().getValue());
        if (!originalOperator.getInputs().get(0).getValue().getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)
                || originalSelectVar == null) {
            return null;
        }

        // Traverse our subplan and generate a SELECT branch if applicable.
        SubplanOperator subplanOperator = (SubplanOperator) originalOperator.getInputs().get(0).getValue();
        Pair<SelectOperator, UnnestOperator> traversalOutput = traverseSubplanBranch(subplanOperator, null, true);
        return (traversalOutput == null) ? null : traversalOutput.first;
    }

    /**
     * To undo this process is to return what was passed to us at {@code createOperator} time. We do not touch the
     * operators after the SELECT.
     */
    @Override
    public SelectOperator restoreBeforeRewrite(List<Mutable<ILogicalOperator>> afterOperatorRefs,
            IOptimizationContext context) throws AlgebricksException {
        return selectRootStack.pop();
    }
}
