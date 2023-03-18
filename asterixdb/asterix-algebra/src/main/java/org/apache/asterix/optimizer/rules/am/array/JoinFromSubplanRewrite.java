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
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

/**
 * For use in writing a "throwaway" branch which removes NTS and subplan operators. The result of this invocation is to
 * be given to the {@code IntroduceJoinAccessMethodRule} to check if an array index can be used.
 * <br>
 * If we are given the pattern (an existential quantification over a cross product):
 * <pre>
 * SELECT_1(some variable)
 * SUBPLAN_1 -----------------------|
 * |                      AGGREGATE(NON-EMPTY-STREAM)
 * |                      SELECT_2(some predicate)
 * |                      (UNNEST/ASSIGN)*
 * |                      UNNEST(on variable)
 * |                      NESTED-TUPLE-SOURCE
 * JOIN(true)
 * |     |----------------- (potential) index branch ...
 * |----------------- probe branch ...
 * </pre>
 * We return the following branch:
 * <pre>
 * JOIN(some predicate from SELECT_2)
 * |     |----------------- (UNNEST/ASSIGN)*
 * |                        UNNEST(on variable)
 * |                        (potential) index branch ...
 * |----------------- probe branch ...
 * </pre>
 * <p>
 * If we are given the pattern (a universal quantification over a cross product):
 * <pre>
 * SELECT_1(some variable AND array is not empty)
 * SUBPLAN_1 -----------------------|
 * |                      AGGREGATE(EMPTY-STREAM)
 * |                      SELECT_2(NOT(IF-MISSING-OR-NULL(some optimizable predicate)))
 * |                      (UNNEST/ASSIGN)*
 * |                      UNNEST(on variable)
 * |                      NESTED-TUPLE-SOURCE
 * JOIN(true)
 * |     |----------------- (potential) index branch ...
 * |----------------- probe branch ...
 * </pre>
 * We return the following branch:
 * <pre>
 * JOIN(some optimizable predicate)  <--- removed the NOT(IF-MISSING-OR-NULL(...))!
 * |     |----------------- (UNNEST/ASSIGN)*
 * |                        UNNEST(on variable)
 * |                        (potential) index branch ...
 * |----------------- probe branch ...
 * </pre>
 * <p>
 * In the case of nested-subplans, we return a copy of the innermost SELECT followed by all relevant UNNEST/ASSIGNs.
 */
public class JoinFromSubplanRewrite extends AbstractOperatorFromSubplanRewrite<AbstractBinaryJoinOperator> {
    private final static Set<FunctionIdentifier> optimizableFunctions = new HashSet<>();
    private final Deque<JoinFromSubplanContext> contextStack = new ArrayDeque<>();

    /**
     * Add an optimizable function from an access method that can take advantage of this throwaway branch.
     */
    public static void addOptimizableFunction(FunctionIdentifier functionIdentifier) {
        optimizableFunctions.add(functionIdentifier);
    }

    /**
     * The subplan we want to push to the JOIN operator is located *above/after* the JOIN itself.
     */
    public void findAfterSubplanSelectOperator(List<Mutable<ILogicalOperator>> afterJoinRefs)
            throws AlgebricksException {
        JoinFromSubplanContext joinContext = new JoinFromSubplanContext();
        contextStack.push(joinContext);

        // Minimally, we need to have a DISTRIBUTE <- SELECT <- SUBPLAN.
        if (afterJoinRefs.size() < 3) {
            return;
        }

        // TODO (GLENN): These assumptions should be relaxed in the future, but for now we'll roll with it.
        // We expect a) the operator immediately above to be a SUBPLAN, and b) the next operator above to be a SELECT.
        Mutable<ILogicalOperator> afterJoinOpRef1 = afterJoinRefs.get(afterJoinRefs.size() - 1);
        Mutable<ILogicalOperator> afterJoinOpRef2 = afterJoinRefs.get(afterJoinRefs.size() - 2);
        Mutable<ILogicalOperator> afterJoinOpRef3 = afterJoinRefs.get(afterJoinRefs.size() - 3);
        ILogicalOperator afterJoinOp1 = afterJoinOpRef1.getValue();
        ILogicalOperator afterJoinOp2 = afterJoinOpRef2.getValue();
        ILogicalOperator afterJoinOp3 = afterJoinOpRef3.getValue();
        if (!afterJoinOp1.getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)
                || !afterJoinOp2.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
            return;
        }

        // Additionally, verify that our SELECT is conditioning on a variable.
        List<VariableReferenceExpression> booleanVariables = new ArrayList<>();
        joinContext.selectAfterSubplan = (SelectOperator) afterJoinOp2;
        gatherBooleanVariables(joinContext.selectAfterSubplan.getCondition().getValue(), booleanVariables,
                new ArrayList<>());
        if (booleanVariables.isEmpty()) {
            return;
        }

        // Modify the given after-join operators. We will reconnect these after the join-rule transformation.
        joinContext.removedAfterJoinOperators = new ArrayList<>();
        joinContext.removedAfterJoinOperators.add(afterJoinOpRef2);
        joinContext.removedAfterJoinOperators.add(afterJoinOpRef1);
        afterJoinRefs.remove(afterJoinOpRef2);
        afterJoinRefs.remove(afterJoinOpRef1);

        // Connect our inputs here. We will compute the type environment for this copy in {@code createOperator}.
        joinContext.afterJoinOpForRewrite = OperatorManipulationUtil.deepCopy(afterJoinOp3);
        joinContext.afterJoinOpForRewrite.getInputs().clear();
        joinContext.afterJoinOpForRewrite.getInputs().addAll(afterJoinOp3.getInputs());
    }

    /**
     * Create a new branch to match that of the form:
     *
     * <pre>
     * JOIN(...)
     * |     |----------------- (UNNEST/ASSIGN)*
     * |                        UNNEST
     * |                        (potential) index branch ...
     * |----------------- probe branch ...
     * </pre>
     * <p>
     * Operators are *created* here, rather than just reconnected from the original branch.
     */
    @Override
    public AbstractBinaryJoinOperator createOperator(AbstractBinaryJoinOperator originalOperator,
            IOptimizationContext context) throws AlgebricksException {
        // Reset our context.
        this.reset(originalOperator.getSourceLocation(), context, optimizableFunctions);
        JoinFromSubplanContext joinContext = contextStack.getFirst();
        joinContext.originalJoinRoot = originalOperator;
        if (joinContext.removedAfterJoinOperators == null) {
            return null;
        }

        // Traverse our subplan and generate a SELECT branch if applicable.
        SubplanOperator subplanOperator =
                (SubplanOperator) joinContext.selectAfterSubplan.getInputs().get(0).getValue();
        List<Mutable<ILogicalOperator>> originalOpInputs = originalOperator.getInputs();
        Pair<SelectOperator, UnnestOperator> traversalOutput =
                traverseSubplanBranch(subplanOperator, originalOpInputs.get(1).getValue(), true);
        if (traversalOutput == null) {
            return null;
        }

        // We have successfully generated a SELECT branch. Create the new JOIN operator.
        ScalarFunctionCallExpression newCond = coalesceConditions(traversalOutput.first, joinContext.originalJoinRoot);
        joinContext.newJoinRoot = new InnerJoinOperator(new MutableObject<>(newCond));
        joinContext.newJoinRoot.getInputs().add(0, new MutableObject<>(originalOpInputs.get(0).getValue()));

        // Connect the join branches together.
        traversalOutput.second.getInputs().clear();
        traversalOutput.second.getInputs().add(new MutableObject<>(originalOpInputs.get(1).getValue()));
        joinContext.newJoinRoot.getInputs().add(1, traversalOutput.first.getInputs().get(0));
        context.computeAndSetTypeEnvironmentForOperator(joinContext.newJoinRoot);

        // To support type casting that is performed on the index subtree and still make this expression recognizable,
        // push all function calls to a lower ASSIGN.
        List<Mutable<ILogicalExpression>> conjuncts = new ArrayList<>();
        if (newCond.splitIntoConjuncts(conjuncts)) {
            for (Mutable<ILogicalExpression> conjunct : conjuncts) {
                extractFunctionCallToAssign(joinContext.newJoinRoot, context, conjunct.getValue());
            }

        } else {
            extractFunctionCallToAssign(joinContext.newJoinRoot, context, newCond);
        }

        // Reconnect our after-join operator to our new join.
        OperatorManipulationUtil.substituteOpInInput(joinContext.afterJoinOpForRewrite,
                joinContext.removedAfterJoinOperators.get(0).getValue(), new MutableObject<>(joinContext.newJoinRoot));
        context.computeAndSetTypeEnvironmentForOperator(joinContext.afterJoinOpForRewrite);
        return joinContext.newJoinRoot;
    }

    /**
     * To undo this process is to return what was passed to us at {@code createOperator} time. If we removed any
     * after-join references, add them back in the order they were originally given.
     */
    @Override
    public AbstractBinaryJoinOperator restoreBeforeRewrite(List<Mutable<ILogicalOperator>> afterOperatorRefs,
            IOptimizationContext context) {
        JoinFromSubplanContext joinContext = contextStack.pop();
        if (joinContext.removedAfterJoinOperators != null) {
            afterOperatorRefs.addAll(joinContext.removedAfterJoinOperators);
        }

        return joinContext.originalJoinRoot;
    }

    private void extractFunctionCallToAssign(AbstractBinaryJoinOperator joinOp, IOptimizationContext context,
            ILogicalExpression condition) throws AlgebricksException {
        if (!condition.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            return;
        }
        AbstractFunctionCallExpression conditionAsFuncCall = (AbstractFunctionCallExpression) condition;
        if (!AlgebricksBuiltinFunctions.isComparisonFunction(conditionAsFuncCall.getFunctionIdentifier())) {
            return;
        }

        for (Mutable<ILogicalExpression> arg : conditionAsFuncCall.getArguments()) {
            if (!arg.getValue().getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                continue;
            }

            LogicalVariable newVar = context.newVar();
            VariableReferenceExpression newVarRef = new VariableReferenceExpression(newVar);
            newVarRef.setSourceLocation(joinOp.getSourceLocation());
            Mutable<ILogicalExpression> clonedArgRef = new MutableObject<>(arg.getValue().cloneExpression());
            AssignOperator newAssign = new AssignOperator(newVar, clonedArgRef);
            newAssign.setSourceLocation(clonedArgRef.getValue().getSourceLocation());
            newAssign.setExecutionMode(joinOp.getExecutionMode());

            // Place the new ASSIGN in the appropriate join branch.
            ILogicalOperator leftBranchRoot = joinOp.getInputs().get(0).getValue();
            ILogicalOperator rightBranchRoot = joinOp.getInputs().get(1).getValue();
            List<LogicalVariable> usedVarsFromFunc = new ArrayList<>();
            List<LogicalVariable> varsFromLeftBranch = new ArrayList<>();
            List<LogicalVariable> varsFromRightBranch = new ArrayList<>();
            VariableUtilities.getUsedVariables(newAssign, usedVarsFromFunc);
            VariableUtilities.getProducedVariablesInDescendantsAndSelf(leftBranchRoot, varsFromLeftBranch);
            VariableUtilities.getProducedVariablesInDescendantsAndSelf(rightBranchRoot, varsFromRightBranch);
            if (new HashSet<>(varsFromLeftBranch).containsAll(usedVarsFromFunc)) {
                newAssign.getInputs().add(new MutableObject<>(leftBranchRoot));
                context.computeAndSetTypeEnvironmentForOperator(newAssign);
                joinOp.getInputs().get(0).setValue(newAssign);
                context.computeAndSetTypeEnvironmentForOperator(joinOp);
                arg.setValue(newVarRef);

            } else if (new HashSet<>(varsFromRightBranch).containsAll(usedVarsFromFunc)) {
                newAssign.getInputs().add(new MutableObject<>(rightBranchRoot));
                context.computeAndSetTypeEnvironmentForOperator(newAssign);
                joinOp.getInputs().get(1).setValue(newAssign);
                context.computeAndSetTypeEnvironmentForOperator(joinOp);
                arg.setValue(newVarRef);

            }
        }
    }

    /**
     * All state associated with a single call of {@code createOperator}.
     */
    private static class JoinFromSubplanContext {
        private List<Mutable<ILogicalOperator>> removedAfterJoinOperators;
        private AbstractBinaryJoinOperator originalJoinRoot;
        private AbstractBinaryJoinOperator newJoinRoot;
        private SelectOperator selectAfterSubplan;
        private ILogicalOperator afterJoinOpForRewrite;
    }
}
