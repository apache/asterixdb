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
package org.apache.asterix.optimizer.rules.subplan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
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

abstract public class AbstractOperatorFromSubplanCreator<T> {
    private final static List<IAlgebricksConstantValue> ZEROS_AS_ASTERIX_CONSTANTS =
            Arrays.asList(new IAlgebricksConstantValue[] { new AsterixConstantValue(new AInt64(0)),
                    new AsterixConstantValue(new AInt32(0)), new AsterixConstantValue(new AInt16((short) 0)),
                    new AsterixConstantValue(new AInt8((byte) 0)) });

    private Set<FunctionIdentifier> optimizableFunctions;
    private IOptimizationContext context;
    private SourceLocation sourceLocation;

    abstract public T createOperator(T originalOperatorRef, IOptimizationContext context) throws AlgebricksException;

    abstract public T restoreBeforeRewrite(List<Mutable<ILogicalOperator>> afterOperatorRefs,
            IOptimizationContext context) throws AlgebricksException;

    protected void reset(SourceLocation sourceLocation, IOptimizationContext context,
            Set<FunctionIdentifier> optimizableFunctions) {
        this.optimizableFunctions = optimizableFunctions;
        this.sourceLocation = sourceLocation;
        this.context = context;
    }

    protected LogicalVariable getConditioningVariable(ILogicalExpression condition) {
        List<Mutable<ILogicalExpression>> selectConjuncts = new ArrayList<>();
        if (condition.splitIntoConjuncts(selectConjuncts)) {
            for (Mutable<ILogicalExpression> conjunct : selectConjuncts) {
                if (conjunct.getValue().getExpressionTag().equals(LogicalExpressionTag.VARIABLE)) {
                    return ((VariableReferenceExpression) conjunct.getValue()).getVariableReference();
                }
            }

        } else if (condition.getExpressionTag().equals(LogicalExpressionTag.VARIABLE)) {
            return ((VariableReferenceExpression) condition).getVariableReference();

        }
        return null;
    }

    protected Pair<SelectOperator, UnnestOperator> traverseSubplanBranch(SubplanOperator subplanOperator,
            ILogicalOperator parentInput) throws AlgebricksException {
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

        // Find (or create, in the SOME AND EVERY case) a SELECT that we can potentially optimize.
        SelectOperator optimizableSelect = getSelectFromPlan(workingSubplanRootAsAggregate);
        if (optimizableSelect == null) {
            return null;
        }

        // We have found a SELECT with a variable. Create a copy, and set this to our rewrite root.
        SelectOperator rewriteRootSelect = new SelectOperator(optimizableSelect.getCondition(),
                optimizableSelect.getRetainMissing(), optimizableSelect.getMissingPlaceholderVariable());

        // Ensure that this SELECT represents a predicate for an existential query, and is a query we can optimize.
        rewriteRootSelect = normalizeSelectCondition(workingSubplanRootAsAggregate, rewriteRootSelect);
        if (rewriteRootSelect == null) {
            return null;
        }
        rewriteRootSelect.setSourceLocation(sourceLocation);
        rewriteRootSelect.setExecutionMode(optimizableSelect.getExecutionMode());

        // Follow this SELECT to the root of our nested-plan branch (i.e. the NESTED-TUPLE-SOURCE).
        ILogicalOperator workingNewOperator = rewriteRootSelect;
        UnnestOperator bottommostNewUnnest = null;
        ILogicalOperator workingOriginalOperator = optimizableSelect.getInputs().get(0).getValue();
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

                case SELECT:
                    // If we encounter another SELECT, then we have multiple quantifiers. Transform our new SELECT to
                    // include this condition.
                    List<Mutable<ILogicalExpression>> selectArguments = new ArrayList<>();
                    if (!rewriteRootSelect.getCondition().getValue().splitIntoConjuncts(selectArguments)) {
                        selectArguments.add(rewriteRootSelect.getCondition());
                    }
                    if (!((SelectOperator) workingOriginalOperator).getCondition().getValue()
                            .splitIntoConjuncts(selectArguments)) {
                        selectArguments.add(((SelectOperator) workingOriginalOperator).getCondition());
                    }
                    ScalarFunctionCallExpression andCond = new ScalarFunctionCallExpression(
                            BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND), selectArguments);
                    SelectOperator updatedSelectOperator = new SelectOperator(new MutableObject<>(andCond),
                            rewriteRootSelect.getRetainMissing(), rewriteRootSelect.getMissingPlaceholderVariable());
                    updatedSelectOperator.setSourceLocation(sourceLocation);
                    updatedSelectOperator.getInputs().addAll(rewriteRootSelect.getInputs());
                    rewriteRootSelect = updatedSelectOperator;
                    break;

                case AGGREGATE:
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

        // If we are working with strict universal quantification, then we must also check whether or not we have a
        // conjunct that asserts that the array should also be non-empty.
        if (isStrictUniversalQuantification(workingSubplanRootAsAggregate)
                && isArrayNonEmptyConjunctMissing(bottommostNewUnnest, subplanOperator.getInputs().get(0).getValue())
                && (parentInput == null || isArrayNonEmptyConjunctMissing(bottommostNewUnnest, parentInput))) {
            return null;
        }

        // We have added everything we need in our nested-plan branch. Now, connect the input of our SUBPLAN to our
        // current working branch.
        bottommostNewUnnest.getInputs().addAll(subplanOperator.getInputs());
        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(rewriteRootSelect, context);

        return new Pair<>(rewriteRootSelect, bottommostNewUnnest);
    }

    protected SelectOperator getSelectFromPlan(AggregateOperator subplanRoot) throws AlgebricksException {
        ILogicalOperator subplanRootInput = subplanRoot.getInputs().get(0).getValue();
        ILogicalOperator subplanOrSelect = findSubplanOrOptimizableSelect(subplanRootInput);

        if (subplanOrSelect != null && subplanOrSelect.getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)) {
            // We have found a SUBPLAN. Recurse by calling our caller.
            Pair<SelectOperator, UnnestOperator> traversalOutput =
                    traverseSubplanBranch((SubplanOperator) subplanOrSelect, subplanRootInput);
            return (traversalOutput != null) ? traversalOutput.first : null;

        } else if (subplanOrSelect != null && subplanOrSelect.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
            // We have found a SELECT. Return this to our caller.
            return (SelectOperator) subplanOrSelect;

        } else {
            // We were not able to find a SELECT or a SUBPLAN. Try to find an expression in our aggregate that we
            // can optimize (i.e. handle SOME AND EVERY case).
            AbstractFunctionCallExpression optimizableCondition = null;
            boolean isNonEmptyStream = false;
            for (Mutable<ILogicalExpression> expression : subplanRoot.getExpressions()) {
                AggregateFunctionCallExpression aggExpression = (AggregateFunctionCallExpression) expression.getValue();
                if (aggExpression.getFunctionIdentifier().equals(BuiltinFunctions.NON_EMPTY_STREAM)) {
                    isNonEmptyStream = true;

                } else if (aggExpression.isTwoStep()
                        && aggExpression.getStepOneAggregate().getFunctionIdentifier()
                                .equals(BuiltinFunctions.SQL_COUNT)
                        && aggExpression.getStepTwoAggregate().getFunctionIdentifier().equals(BuiltinFunctions.SQL_SUM)
                        && aggExpression.getArguments().get(0).getValue().getExpressionTag()
                                .equals(LogicalExpressionTag.FUNCTION_CALL)) {
                    AbstractFunctionCallExpression switchExpression =
                            (AbstractFunctionCallExpression) aggExpression.getArguments().get(0).getValue();
                    if (!switchExpression.getArguments().get(0).getValue().getExpressionTag()
                            .equals(LogicalExpressionTag.FUNCTION_CALL)) {
                        continue;
                    }
                    AbstractFunctionCallExpression switchCondition =
                            (AbstractFunctionCallExpression) switchExpression.getArguments().get(0).getValue();

                    ILogicalExpression arg2 = switchExpression.getArguments().get(1).getValue();
                    ILogicalExpression arg3 = switchExpression.getArguments().get(2).getValue();
                    ILogicalExpression arg4 = switchExpression.getArguments().get(3).getValue();
                    if (arg2.getExpressionTag().equals(LogicalExpressionTag.CONSTANT)
                            && arg3.getExpressionTag().equals(LogicalExpressionTag.CONSTANT)
                            && arg4.getExpressionTag().equals(LogicalExpressionTag.CONSTANT)
                            && ((ConstantExpression) arg2).getValue().isTrue()
                            && ((ConstantExpression) arg3).getValue().isNull()
                            && ((ConstantExpression) arg4).getValue().isTrue()) {
                        optimizableCondition = switchCondition;
                    }
                }
            }
            if (isNonEmptyStream && optimizableCondition != null) {
                SelectOperator newSelectFromAggregate =
                        new SelectOperator(new MutableObject<>(optimizableCondition), false, null);
                newSelectFromAggregate.getInputs().addAll(subplanRoot.getInputs());
                newSelectFromAggregate.setSourceLocation(sourceLocation);
                return newSelectFromAggregate;
            }
            return null;
        }
    }

    private boolean isStrictUniversalQuantification(AggregateOperator workingSubplanRoot) {
        for (Mutable<ILogicalExpression> expression : workingSubplanRoot.getExpressions()) {
            AggregateFunctionCallExpression funcExpr = (AggregateFunctionCallExpression) expression.getValue();
            if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.EMPTY_STREAM)) {
                return true;
            }
        }
        return false;
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
                        && ZEROS_AS_ASTERIX_CONSTANTS.contains(((ConstantExpression) secondArg).getValue());
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
                        && ZEROS_AS_ASTERIX_CONSTANTS.contains(((ConstantExpression) firstArg).getValue());
            }
        }

        return false;
    }

    private SelectOperator normalizeSelectCondition(AggregateOperator aggregateOperator, SelectOperator selectOperator)
            throws AlgebricksException {
        // The purpose of this function is to remove the NOT(IF-MISSING-OR-NULL(...)) functions for a strict universal
        // quantification query. The {@code ArrayBTreeAccessMethod} does not recognize the former as optimizable
        // functions, so we remove them here. This SELECT will never make it to the final query plan (after the
        // {@code IntroduceSelectAccessMethodRule}), which allows us to get away with this logically incorrect branch.
        if (aggregateOperator != null && !isStrictUniversalQuantification(aggregateOperator)) {
            // We are working with an existential quantification OR an EACH AND EVERY query. Do not modify the SELECT.
            return selectOperator;

        } else {
            // We are working with a strict universal quantification query.
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

    private ILogicalOperator findSubplanOrOptimizableSelect(ILogicalOperator operator) throws AlgebricksException {
        // We are trying to find a SELECT operator with an optimizable function call.
        if (operator.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
            SelectOperator selectOperator = (SelectOperator) operator;
            ILogicalExpression selectCondExpr = selectOperator.getCondition().getValue();
            boolean containsValidVar = isAnyVarFromUnnestOrAssign(operator);
            if (containsValidVar && selectCondExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {

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
        return (operator.getInputs().isEmpty()) ? null
                : findSubplanOrOptimizableSelect(operator.getInputs().get(0).getValue());
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
                        && isFunctionOptimizable((ScalarFunctionCallExpression) workingConjunct)) {
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

    private boolean isAnyVarFromUnnestOrAssign(ILogicalOperator op) throws AlgebricksException {
        List<LogicalVariable> opUsedVars = new ArrayList<>(), relevantVars = new ArrayList<>();
        VariableUtilities.getUsedVariables(op, opUsedVars);
        ILogicalOperator workingOp = op;
        boolean isMatchFound = false;
        while (workingOp != null) {
            if (workingOp.getOperatorTag().equals(LogicalOperatorTag.UNNEST)
                    || workingOp.getOperatorTag().equals(LogicalOperatorTag.ASSIGN)) {
                VariableUtilities.getProducedVariables(workingOp, relevantVars);
                if (opUsedVars.stream().anyMatch(relevantVars::contains)) {
                    isMatchFound = true;
                    break;
                }
            }
            workingOp = (workingOp.getInputs().isEmpty()) ? null : workingOp.getInputs().get(0).getValue();
        }
        return isMatchFound;
    }
}
