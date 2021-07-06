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
package org.apache.asterix.optimizer.rules.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * For use in writing a "throwaway" branch which removes NTS and subplan operators. The result of this invocation is to
 * be given to the {@code IntroduceSelectAccessMethodRule} to check if an array index can be used.
 * <br>
 * If we are given the pattern (an existential query):
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
 * If we are given the pattern (a universal query):
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
public class SelectInSubplanBranchCreator {
    private final static List<IAlgebricksConstantValue> zerosAsAsterixConstants =
            Arrays.asList(new IAlgebricksConstantValue[] { new AsterixConstantValue(new AInt64(0)),
                    new AsterixConstantValue(new AInt32(0)), new AsterixConstantValue(new AInt16((short) 0)),
                    new AsterixConstantValue(new AInt8((byte) 0)) });
    private final static List<FunctionIdentifier> optimizableFunctions = new ArrayList<>();

    private IOptimizationContext context;
    private SourceLocation sourceLocation;
    private SelectOperator originalSelectRoot;

    /**
     * Add an optimizable function from an access method that can take advantage of this throwaway branch rewrite.
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
    public SelectOperator createSelect(SelectOperator originalSelect, IOptimizationContext context)
            throws AlgebricksException {
        // Reset our context.
        this.sourceLocation = originalSelect.getSourceLocation();
        this.originalSelectRoot = originalSelect;
        this.context = context;

        // We expect a) a SUBPLAN as input to this SELECT, and b) our SELECT to be conditioning on a variable.
        if (!originalSelect.getInputs().get(0).getValue().getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)
                || !originalSelect.getCondition().getValue().getExpressionTag().equals(LogicalExpressionTag.VARIABLE)) {
            return null;
        }
        LogicalVariable originalSelectVar =
                ((VariableReferenceExpression) originalSelect.getCondition().getValue()).getVariableReference();

        // Additionally, verify that the subplan does not produce any other variable other than the SELECT var above.
        SubplanOperator subplanOperator = (SubplanOperator) originalSelect.getInputs().get(0).getValue();
        List<LogicalVariable> subplanProducedVars = new ArrayList<>();
        VariableUtilities.getProducedVariables(subplanOperator, subplanProducedVars);
        if (subplanProducedVars.size() != 1 || !subplanProducedVars.get(0).equals(originalSelectVar)) {
            return null;
        }

        return traverseSubplanBranch(subplanOperator, null);
    }

    /**
     * To undo this process is to return what was passed to us at {@code createSelect} time.
     */
    public SelectOperator getOriginalSelect() {
        return originalSelectRoot;
    }

    private SelectOperator traverseSubplanBranch(SubplanOperator subplanOperator, ILogicalOperator parentInput)
            throws AlgebricksException {
        // We only expect one plan, and one root.
        if (subplanOperator.getNestedPlans().size() > 1
                || subplanOperator.getNestedPlans().get(0).getRoots().size() > 1) {
            return null;
        }

        // This root of our "subplan" should always be an aggregate.
        ILogicalOperator workingSubplanRoot = subplanOperator.getNestedPlans().get(0).getRoots().get(0).getValue();
        AggregateOperator workingSubplanRootAsAggregate;
        if (!workingSubplanRoot.getOperatorTag().equals(LogicalOperatorTag.AGGREGATE)) {
            return null;
        }
        workingSubplanRootAsAggregate = (AggregateOperator) workingSubplanRoot;

        // Try to find a SELECT that we can optimize (i.e. has a function call).
        SelectOperator optimizableSelect = null;
        for (Mutable<ILogicalOperator> opInput : workingSubplanRoot.getInputs()) {
            ILogicalOperator subplanOrSelect = findSubplanOrOptimizableSelect(opInput.getValue());
            if (subplanOrSelect == null) {
                return null;

            } else if (subplanOrSelect.getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)) {
                optimizableSelect = traverseSubplanBranch((SubplanOperator) subplanOrSelect, opInput.getValue());

            } else {
                optimizableSelect = (SelectOperator) subplanOrSelect;
                break;
            }
        }
        if (optimizableSelect == null) {
            return null;
        }

        // We have found a SELECT with a variable. Create a copy, and set this to our rewrite root.
        SelectOperator newSelectOperator = new SelectOperator(optimizableSelect.getCondition(),
                optimizableSelect.getRetainMissing(), optimizableSelect.getMissingPlaceholderVariable());

        // Ensure that this SELECT represents a predicate for an existential query, and is a query we can optimize.
        newSelectOperator = normalizeSelectCondition(workingSubplanRootAsAggregate, newSelectOperator);
        if (newSelectOperator == null) {
            return null;
        }
        newSelectOperator.setSourceLocation(sourceLocation);
        newSelectOperator.setExecutionMode(optimizableSelect.getExecutionMode());

        // Follow this SELECT to the root of our nested-plan branch (i.e. the NESTED-TUPLE-SOURCE).
        ILogicalOperator workingOriginalOperator = optimizableSelect, workingNewOperator = newSelectOperator;
        UnnestOperator bottommostNewUnnest = null;
        while (!workingOriginalOperator.getOperatorTag().equals(LogicalOperatorTag.NESTEDTUPLESOURCE)) {
            if (workingOriginalOperator.getInputs().isEmpty()) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        workingSubplanRoot.getSourceLocation(),
                        "NESTED-TUPLE-SOURCE expected in nested plan branch, but not found.");
            }

            switch (workingOriginalOperator.getOperatorTag()) {
                case UNNEST:
                    UnnestOperator originalUnnest = (UnnestOperator) workingOriginalOperator;
                    UnnestOperator newUnnest =
                            new UnnestOperator(originalUnnest.getVariable(), originalUnnest.getExpressionRef());
                    newUnnest.setSourceLocation(sourceLocation);
                    workingNewOperator.getInputs().add(new MutableObject<>(newUnnest));
                    workingNewOperator = newUnnest;
                    bottommostNewUnnest = (UnnestOperator) workingNewOperator;
                    break;

                case ASSIGN:
                    AssignOperator originalAssign = (AssignOperator) workingOriginalOperator;
                    AssignOperator newAssign =
                            new AssignOperator(originalAssign.getVariables(), originalAssign.getExpressions());
                    newAssign.setSourceLocation(sourceLocation);
                    workingNewOperator.getInputs().add(new MutableObject<>(newAssign));
                    workingNewOperator = newAssign;
                    break;

                case AGGREGATE:
                case SELECT:
                    break;

                default:
                    return null;
            }

            workingOriginalOperator = workingOriginalOperator.getInputs().get(0).getValue();
        }

        // Sanity check: we should always be working with an UNNEST at this stage.
        if (bottommostNewUnnest == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, workingSubplanRoot.getSourceLocation(),
                    "UNNEST expected in nested plan branch, but not found.");
        }

        // If we are working with universal quantification, then we must also check whether or not we have a conjunct 
        // that asserts that the array should also be non-empty.
        if (isUniversalQuantification(workingSubplanRootAsAggregate)
                && isArrayNonEmptyConjunctMissing(bottommostNewUnnest, subplanOperator.getInputs().get(0).getValue())
                && (parentInput == null || isArrayNonEmptyConjunctMissing(bottommostNewUnnest, parentInput))) {
            return null;
        }

        // We have added everything we need in our nested-plan branch. Now, connect the input of our SUBPLAN to our
        // current working branch.
        bottommostNewUnnest.getInputs().addAll(subplanOperator.getInputs());
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(newSelectOperator, context);

        return newSelectOperator;
    }

    private boolean isUniversalQuantification(AggregateOperator workingSubplanRoot) throws CompilationException {
        AggregateFunctionCallExpression aggregateFunctionCallExpression =
                (AggregateFunctionCallExpression) workingSubplanRoot.getExpressions().get(0).getValue();
        if (aggregateFunctionCallExpression.getFunctionIdentifier().equals(BuiltinFunctions.EMPTY_STREAM)) {
            return true;

        } else if (aggregateFunctionCallExpression.getFunctionIdentifier().equals(BuiltinFunctions.NON_EMPTY_STREAM)) {
            return false;

        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, workingSubplanRoot.getSourceLocation(),
                    "Unexpected aggregate function: " + aggregateFunctionCallExpression.getFunctionIdentifier());
        }
    }

    private boolean isArrayNonEmptyConjunctMissing(UnnestOperator firstUnnestInNTS, ILogicalOperator subplanInput)
            throws AlgebricksException {
        UnnestingFunctionCallExpression unnestFunction =
                (UnnestingFunctionCallExpression) firstUnnestInNTS.getExpressionRef().getValue();
        VariableReferenceExpression unnestVarExpr =
                (VariableReferenceExpression) unnestFunction.getArguments().get(0).getValue();
        LogicalVariable arrayVariable = unnestVarExpr.getVariableReference();

        if (!subplanInput.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
            return true;
        }
        SelectOperator subplanInputAsSelect = normalizeSelectCondition(null, (SelectOperator) subplanInput);
        ILogicalExpression selectCondExpr = subplanInputAsSelect.getCondition().getValue();
        List<Mutable<ILogicalExpression>> conjunctsFromSelect = new ArrayList<>();
        if (selectCondExpr.splitIntoConjuncts(conjunctsFromSelect)) {
            // We have a collection of conjuncts. Analyze each conjunct w/ a function.
            for (Mutable<ILogicalExpression> mutableConjunct : conjunctsFromSelect) {
                ILogicalExpression workingConjunct = mutableConjunct.getValue();
                if (workingConjunct.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)
                        && analyzeConjunctForArrayNonEmptiness(arrayVariable,
                                (ScalarFunctionCallExpression) workingConjunct)) {
                    return false;
                }
            }

            // No such conjunct found.
            return true;
        }

        if (!selectCondExpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            return true;
        }
        return !analyzeConjunctForArrayNonEmptiness(arrayVariable, (ScalarFunctionCallExpression) selectCondExpr);
    }

    private boolean analyzeConjunctForArrayNonEmptiness(LogicalVariable arrayVariable,
            ScalarFunctionCallExpression workingSelectCondExpr) {
        // Handle the conjunct: LEN(arrayVar) > 0
        if (workingSelectCondExpr.getFunctionIdentifier().equals(BuiltinFunctions.GT)) {
            ILogicalExpression firstArg = workingSelectCondExpr.getArguments().get(0).getValue();
            ILogicalExpression secondArg = workingSelectCondExpr.getArguments().get(1).getValue();

            if (firstArg.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)
                    && ((ScalarFunctionCallExpression) firstArg).getFunctionIdentifier().equals(BuiltinFunctions.LEN)) {
                ScalarFunctionCallExpression lenFunction = (ScalarFunctionCallExpression) firstArg;
                List<LogicalVariable> usedVariables = new ArrayList<>();
                lenFunction.getUsedVariables(usedVariables);

                return usedVariables.contains(arrayVariable)
                        && secondArg.getExpressionTag().equals(LogicalExpressionTag.CONSTANT)
                        && zerosAsAsterixConstants.contains(((ConstantExpression) secondArg).getValue());
            }
        }

        // Handle the conjunct: 0 < LEN(arrayVar)
        else if (workingSelectCondExpr.getFunctionIdentifier().equals(BuiltinFunctions.LT)) {
            ILogicalExpression firstArg = workingSelectCondExpr.getArguments().get(0).getValue();
            ILogicalExpression secondArg = workingSelectCondExpr.getArguments().get(1).getValue();

            if (secondArg.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)
                    && ((ScalarFunctionCallExpression) secondArg).getFunctionIdentifier()
                            .equals(BuiltinFunctions.LEN)) {
                ScalarFunctionCallExpression lenFunction = (ScalarFunctionCallExpression) secondArg;
                List<LogicalVariable> usedVariables = new ArrayList<>();
                lenFunction.getUsedVariables(usedVariables);

                return usedVariables.contains(arrayVariable)
                        && firstArg.getExpressionTag().equals(LogicalExpressionTag.CONSTANT)
                        && zerosAsAsterixConstants.contains(((ConstantExpression) firstArg).getValue());
            }
        }

        // TODO (GLENN): Handle the cases 1) where the arrayVar is explicitly indexed, 2) the NOT function.
        return false;
    }

    private SelectOperator normalizeSelectCondition(AggregateOperator aggregateOperator, SelectOperator selectOperator)
            throws AlgebricksException {
        // The purpose of this function is to remove the NOT(IF-MISSING-OR-NULL(...)) functions for a universal
        // quantification query. The {@code ArrayBTreeAccessMethod} does not recognize the former as optimizable
        // functions, so we remove them here. This SELECT will never make it to the final query plan (after the
        // {@code IntroduceSelectAccessMethodRule}), which allows us to get away with this logically incorrect branch.
        if (aggregateOperator != null && !isUniversalQuantification(aggregateOperator)) {
            // We are working with an existential quantification query. Do not modify the SELECT.
            return selectOperator;

        } else {
            // We are working with a universal quantification query.
            ScalarFunctionCallExpression notFunction =
                    (ScalarFunctionCallExpression) selectOperator.getCondition().getValue();
            if (!notFunction.getFunctionIdentifier().equals(BuiltinFunctions.NOT)) {
                return selectOperator;
            }

            ScalarFunctionCallExpression ifMissingOrNullFunction =
                    (ScalarFunctionCallExpression) notFunction.getArguments().get(0).getValue();
            if (!ifMissingOrNullFunction.getFunctionIdentifier().equals(BuiltinFunctions.IF_MISSING_OR_NULL)) {
                return selectOperator;
            }

            Mutable<ILogicalExpression> newSelectCondition =
                    new MutableObject<>(ifMissingOrNullFunction.getArguments().get(0).getValue().cloneExpression());
            return new SelectOperator(newSelectCondition, selectOperator.getRetainMissing(),
                    selectOperator.getMissingPlaceholderVariable());

        }
    }

    private ILogicalOperator findSubplanOrOptimizableSelect(ILogicalOperator operator) {
        // We are trying to find a SELECT operator with an optimizable function call.
        if (operator.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
            SelectOperator selectOperator = (SelectOperator) operator;
            ILogicalExpression selectCondExpr = selectOperator.getCondition().getValue();
            if (selectCondExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {

                // We have a NOT function call. Determine if this follows the NOT(IF-MISSING-OR-NULL(...)) pattern.
                ScalarFunctionCallExpression notExpr = (ScalarFunctionCallExpression) selectCondExpr;
                if (notExpr.getFunctionIdentifier().equals(BuiltinFunctions.NOT)) {

                    // This does not follow the NOT(IF-MISSING-OR-NULL(...)) pattern, but NOT is an optimizable
                    // function call. Return this.
                    ILogicalExpression notCondExpr = notExpr.getArguments().get(0).getValue();
                    if (!notCondExpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)
                            && optimizableFunctions.contains(BuiltinFunctions.NOT)) {
                        return selectOperator;
                    }

                    // Inside the NOT(IF-MISSING-OR-NULL(...)) is an optimizable function. Return this.
                    ScalarFunctionCallExpression ifMissingOrNullExpr = (ScalarFunctionCallExpression) notCondExpr;
                    ILogicalExpression finalExpr = ifMissingOrNullExpr.getArguments().get(0).getValue();
                    if (doesExpressionContainOptimizableFunction(finalExpr)) {
                        return selectOperator;
                    }

                } else if (doesExpressionContainOptimizableFunction(selectCondExpr)) {
                    // We have an optimizable function. Return this.
                    return selectOperator;

                }
            }
        } else if (operator.getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)) {
            // We have found an additional SUBPLAN branch to explore. Recurse w/ caller function.
            return operator;
        }

        // No matching operator found. Recurse on current operator input.
        if (operator.getInputs().isEmpty()) {
            return null;

        } else {
            return findSubplanOrOptimizableSelect(operator.getInputs().get(0).getValue());
        }
    }

    private boolean doesExpressionContainOptimizableFunction(ILogicalExpression inputExpr) {
        if (!inputExpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            return false;
        }

        // Check if the input expression itself is an optimizable function.
        ScalarFunctionCallExpression inputExprAsFunc = (ScalarFunctionCallExpression) inputExpr;
        if (isFunctionOptimizable(inputExprAsFunc)) {
            return true;
        }

        // We have a collection of conjuncts. Return true if any of these conjuncts are optimizable.
        List<Mutable<ILogicalExpression>> conjuncts = new ArrayList<>();
        if (inputExprAsFunc.splitIntoConjuncts(conjuncts)) {
            for (Mutable<ILogicalExpression> mutableConjunct : conjuncts) {
                ILogicalExpression workingConjunct = mutableConjunct.getValue();
                if (workingConjunct.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)
                        && (isFunctionOptimizable((ScalarFunctionCallExpression) workingConjunct))) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean isFunctionOptimizable(ScalarFunctionCallExpression inputExpr) {
        if (inputExpr.getFunctionIdentifier().equals(BuiltinFunctions.GT)) {
            // Avoid the GT(LEN(array-field), 0) function.
            ILogicalExpression gtExpr = inputExpr.getArguments().get(0).getValue();
            return ((!gtExpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL))
                    || !((ScalarFunctionCallExpression) gtExpr).getFunctionIdentifier().equals(BuiltinFunctions.LEN))
                    && optimizableFunctions.contains(BuiltinFunctions.GT);

        } else if (inputExpr.getFunctionIdentifier().equals(BuiltinFunctions.LT)) {
            // Avoid the LT(0, LEN(array-field)) function.
            ILogicalExpression ltExpr = inputExpr.getArguments().get(1).getValue();
            return ((!ltExpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL))
                    || !((ScalarFunctionCallExpression) ltExpr).getFunctionIdentifier().equals(BuiltinFunctions.LEN))
                    && optimizableFunctions.contains(BuiltinFunctions.LT);

        }

        // Otherwise, check if the function itself is optimizable.
        return (optimizableFunctions.contains(inputExpr.getFunctionIdentifier()));
    }
}
