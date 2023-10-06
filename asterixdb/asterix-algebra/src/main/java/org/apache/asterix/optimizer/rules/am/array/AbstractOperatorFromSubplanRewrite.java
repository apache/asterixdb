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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;

abstract public class AbstractOperatorFromSubplanRewrite<T> implements IIntroduceAccessMethodRuleLocalRewrite<T> {
    private final static List<IAlgebricksConstantValue> ZEROS_AS_ASTERIX_CONSTANTS =
            Arrays.asList(new IAlgebricksConstantValue[] { new AsterixConstantValue(new AInt64(0)),
                    new AsterixConstantValue(new AInt32(0)), new AsterixConstantValue(new AInt16((short) 0)),
                    new AsterixConstantValue(new AInt8((byte) 0)) });

    private Set<FunctionIdentifier> optimizableFunctions;
    private IOptimizationContext context;
    private SourceLocation sourceLocation;

    public static boolean isApplicableForRewriteCursory(MetadataProvider metadataProvider, ILogicalOperator workingOp)
            throws AlgebricksException {
        boolean isApplicableForRewrite = false;
        if (workingOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator dataSourceScanOperator = (DataSourceScanOperator) workingOp;
            DataSourceId srcId = (DataSourceId) dataSourceScanOperator.getDataSource().getId();
            DataverseName dataverseName = srcId.getDataverseName();
            String database = srcId.getDatabaseName();
            String datasetName = srcId.getDatasourceName();
            if (metadataProvider.getDatasetIndexes(database, dataverseName, datasetName).stream()
                    .anyMatch(i -> i.getIndexType() == DatasetConfig.IndexType.ARRAY)) {
                return true;
            }
        }

        for (Mutable<ILogicalOperator> inputOp : workingOp.getInputs()) {
            isApplicableForRewrite |= isApplicableForRewriteCursory(metadataProvider, inputOp.getValue());
        }
        return isApplicableForRewrite;
    }

    protected void reset(SourceLocation sourceLocation, IOptimizationContext context,
            Set<FunctionIdentifier> optimizableFunctions) {
        this.optimizableFunctions = optimizableFunctions;
        this.sourceLocation = sourceLocation;
        this.context = context;
    }

    protected void gatherBooleanVariables(ILogicalExpression condition, List<VariableReferenceExpression> outputList,
            List<ILogicalExpression> miscExpressions) {
        List<Mutable<ILogicalExpression>> selectConjuncts = new ArrayList<>();
        if (splitIntoConjuncts(condition, selectConjuncts)) {
            for (Mutable<ILogicalExpression> conjunct : selectConjuncts) {
                if (conjunct.getValue().getExpressionTag().equals(LogicalExpressionTag.VARIABLE)) {
                    outputList.add(((VariableReferenceExpression) conjunct.getValue()));
                } else {
                    miscExpressions.add(conjunct.getValue());
                }
            }
        } else if (condition.getExpressionTag().equals(LogicalExpressionTag.VARIABLE)) {
            outputList.add(((VariableReferenceExpression) condition));
        } else {
            miscExpressions.add(condition);
        }
    }

    protected void gatherSubplanOperators(ILogicalOperator rootOperator, List<SubplanOperator> outputList) {
        for (Mutable<ILogicalOperator> inputOpRef : rootOperator.getInputs()) {
            LogicalOperatorTag operatorTag = inputOpRef.getValue().getOperatorTag();
            switch (operatorTag) {
                case SUBPLAN:
                    outputList.add((SubplanOperator) inputOpRef.getValue());
                    gatherSubplanOperators(inputOpRef.getValue(), outputList);
                    break;

                case ASSIGN:
                case UNNEST:
                case SELECT:
                    gatherSubplanOperators(inputOpRef.getValue(), outputList);
                    break;

                default:
                    // We will break early if we encounter any other operator.
                    return;
            }
        }
    }

    protected Pair<SelectOperator, UnnestOperator> traverseSubplanBranch(SubplanOperator subplanOperator,
            ILogicalOperator parentInput, boolean isConnectInput) throws AlgebricksException {
        AggregateOperator workingSubplanRootAsAggregate = getAggregateFromSubplan(subplanOperator);
        if (workingSubplanRootAsAggregate == null) {
            return null;
        }

        // Find (or create, in the SOME AND EVERY case) a SELECT that we can potentially optimize.
        SelectOperator optimizableSelect = getSelectFromPlan(workingSubplanRootAsAggregate);
        if (optimizableSelect == null) {
            return null;
        }

        // Ensure that this SELECT represents a predicate for an existential query, and is a query we can optimize.
        ILogicalExpression normalizedSelectCondition =
                normalizeCondition(workingSubplanRootAsAggregate, optimizableSelect.getCondition().getValue());
        normalizedSelectCondition = keepOptimizableFunctions(normalizedSelectCondition);

        // Create a copy of this SELECT, and set this to our rewrite root.
        SelectOperator rewriteRootSelect = new SelectOperator(new MutableObject<>(normalizedSelectCondition),
                optimizableSelect.getRetainMissingAsValue(), optimizableSelect.getMissingPlaceholderVariable());
        rewriteRootSelect.setSourceLocation(sourceLocation);
        rewriteRootSelect.setExecutionMode(optimizableSelect.getExecutionMode());

        // Follow this SELECT to the root of our nested-plan branch (i.e. the NESTED-TUPLE-SOURCE).
        ILogicalOperator workingNewOperator = rewriteRootSelect;
        UnnestOperator bottommostNewUnnest = null;
        ILogicalOperator workingOriginalOperator = optimizableSelect.getInputs().get(0).getValue();
        while (!workingOriginalOperator.getOperatorTag().equals(LogicalOperatorTag.NESTEDTUPLESOURCE)) {
            if (workingOriginalOperator.getInputs().isEmpty()) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        workingSubplanRootAsAggregate.getSourceLocation(),
                        "NESTED-TUPLE-SOURCE expected in nested plan branch, but not found.");
            }

            ScalarFunctionCallExpression updatedSelectCond;
            SelectOperator updatedSelectOperator;
            switch (workingOriginalOperator.getOperatorTag()) {
                case UNNEST:
                    UnnestOperator originalUnnest = (UnnestOperator) workingOriginalOperator;
                    UnnestOperator newUnnest = (UnnestOperator) OperatorManipulationUtil.deepCopy(originalUnnest);
                    workingNewOperator.getInputs().add(new MutableObject<>(newUnnest));
                    workingNewOperator = newUnnest;
                    bottommostNewUnnest = (UnnestOperator) workingNewOperator;
                    break;

                case ASSIGN:
                    AssignOperator originalAssign = (AssignOperator) workingOriginalOperator;
                    AssignOperator newAssign = (AssignOperator) OperatorManipulationUtil.deepCopy(originalAssign);
                    newAssign.setSourceLocation(sourceLocation);
                    workingNewOperator.getInputs().add(new MutableObject<>(newAssign));
                    workingNewOperator = newAssign;
                    break;

                case SELECT:
                    // If we encounter another SELECT, then we have multiple quantifiers. Transform our new SELECT to
                    // include this condition.
                    updatedSelectCond = coalesceConditions(rewriteRootSelect, workingOriginalOperator);
                    updatedSelectOperator = new SelectOperator(new MutableObject<>(updatedSelectCond),
                            rewriteRootSelect.getRetainMissingAsValue(),
                            rewriteRootSelect.getMissingPlaceholderVariable());
                    updatedSelectOperator.setSourceLocation(sourceLocation);
                    updatedSelectOperator.getInputs().addAll(rewriteRootSelect.getInputs());
                    rewriteRootSelect = updatedSelectOperator;
                    if (workingNewOperator.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                        workingNewOperator = rewriteRootSelect;
                    }
                    break;

                case SUBPLAN:
                    // If we encounter another subplan, then we must look to include any SELECTs from here as well.
                    Pair<SelectOperator, UnnestOperator> traversalOutput =
                            traverseSubplanBranch((SubplanOperator) workingOriginalOperator, optimizableSelect, false);
                    if (traversalOutput != null) {
                        updatedSelectCond = coalesceConditions(rewriteRootSelect, traversalOutput.first);
                        updatedSelectOperator = new SelectOperator(new MutableObject<>(updatedSelectCond),
                                rewriteRootSelect.getRetainMissingAsValue(),
                                rewriteRootSelect.getMissingPlaceholderVariable());
                        updatedSelectOperator.setSourceLocation(sourceLocation);
                        updatedSelectOperator.getInputs().addAll(rewriteRootSelect.getInputs());
                        rewriteRootSelect = updatedSelectOperator;
                        if (workingNewOperator.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                            workingNewOperator = rewriteRootSelect;
                        }

                        // Add the inputs from our subplan.
                        Mutable<ILogicalOperator> traversalOperator = traversalOutput.first.getInputs().get(0);
                        while (traversalOperator != null) {
                            ILogicalOperator traversalOperatorDeepCopy =
                                    OperatorManipulationUtil.deepCopy(traversalOperator.getValue());
                            if (traversalOperator.getValue().equals(traversalOutput.second)) {
                                traversalOutput.second = (UnnestOperator) traversalOperatorDeepCopy;
                            }
                            workingNewOperator.getInputs().add(new MutableObject<>(traversalOperatorDeepCopy));
                            workingNewOperator = workingNewOperator.getInputs().get(0).getValue();
                            traversalOperator = (traversalOperator.getValue().getInputs().isEmpty()) ? null
                                    : traversalOperator.getValue().getInputs().get(0);
                        }
                        workingNewOperator.getInputs().clear();
                        bottommostNewUnnest = traversalOutput.second;
                        break;
                    }

                default:
                    return null;
            }

            workingOriginalOperator = workingOriginalOperator.getInputs().get(0).getValue();
        }

        // Sanity check: we should always be working with an UNNEST at this stage.
        if (bottommostNewUnnest == null) {
            return null;
        }

        // If we are working with strict universal quantification, then we must also check whether we have a
        // conjunct that asserts that the array should also be non-empty.
        if (isStrictUniversalQuantification(workingSubplanRootAsAggregate)
                && isArrayNonEmptyConjunctMissing(bottommostNewUnnest, subplanOperator.getInputs().get(0).getValue())
                && (parentInput == null || isArrayNonEmptyConjunctMissing(bottommostNewUnnest, parentInput))) {
            return null;
        }

        // We have added everything we need in our nested-plan branch. Now, connect the input of our SUBPLAN to our
        // current working branch.
        if (isConnectInput) {
            bottommostNewUnnest.getInputs().addAll(subplanOperator.getInputs());
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(rewriteRootSelect, context);
        }

        return new Pair<>(rewriteRootSelect, bottommostNewUnnest);
    }

    protected ScalarFunctionCallExpression coalesceConditions(SelectOperator selectOp, ILogicalOperator auxOp) {
        ScalarFunctionCallExpression combinedCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND));
        combinedCondition.setSourceLocation(selectOp.getSourceLocation());

        List<Mutable<ILogicalExpression>> conjuncts = new ArrayList<>();
        if (splitIntoConjuncts(selectOp.getCondition().getValue(), conjuncts)) {
            combinedCondition.getArguments().addAll(conjuncts);
            conjuncts.clear();
        } else {
            combinedCondition.getArguments().add(selectOp.getCondition());
        }

        switch (auxOp.getOperatorTag()) {
            case LEFTOUTERJOIN:
            case INNERJOIN:
                AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) auxOp;
                if (splitIntoConjuncts(joinOp.getCondition().getValue(), conjuncts)) {
                    combinedCondition.getArguments().addAll(conjuncts);
                } else {
                    combinedCondition.getArguments().add(joinOp.getCondition());
                }
                break;

            case SELECT:
                SelectOperator selectOp2 = (SelectOperator) auxOp;
                if (splitIntoConjuncts(selectOp2.getCondition().getValue(), conjuncts)) {
                    combinedCondition.getArguments().addAll(conjuncts);
                } else {
                    combinedCondition.getArguments().add(selectOp2.getCondition());
                }
                break;
        }

        return combinedCondition.cloneExpression();
    }

    public static SelectOperator getSelectFromPlan(AggregateOperator subplanRoot) {
        ILogicalExpression aggregateCondition = null;
        boolean isNonEmptyStream = false;
        for (Mutable<ILogicalExpression> expression : subplanRoot.getExpressions()) {
            AggregateFunctionCallExpression aggExpression = (AggregateFunctionCallExpression) expression.getValue();
            if (aggExpression.getFunctionIdentifier().equals(BuiltinFunctions.NON_EMPTY_STREAM)) {
                isNonEmptyStream = true;

            } else if (aggExpression.isTwoStep()
                    && aggExpression.getStepOneAggregate().getFunctionIdentifier().equals(BuiltinFunctions.SQL_COUNT)
                    && aggExpression.getStepTwoAggregate().getFunctionIdentifier().equals(BuiltinFunctions.SQL_SUM)
                    && aggExpression.getArguments().get(0).getValue().getExpressionTag()
                            .equals(LogicalExpressionTag.FUNCTION_CALL)) {
                AbstractFunctionCallExpression switchExpression =
                        (AbstractFunctionCallExpression) aggExpression.getArguments().get(0).getValue();

                ILogicalExpression arg1 = switchExpression.getArguments().get(0).getValue();
                ILogicalExpression arg2 = switchExpression.getArguments().get(1).getValue();
                ILogicalExpression arg3 = switchExpression.getArguments().get(2).getValue();
                ILogicalExpression arg4 = switchExpression.getArguments().get(3).getValue();
                if (arg2.getExpressionTag().equals(LogicalExpressionTag.CONSTANT)
                        && arg3.getExpressionTag().equals(LogicalExpressionTag.CONSTANT)
                        && arg4.getExpressionTag().equals(LogicalExpressionTag.CONSTANT)
                        && ((ConstantExpression) arg2).getValue().isTrue()
                        && ((ConstantExpression) arg3).getValue().isNull()
                        && ((ConstantExpression) arg4).getValue().isTrue()) {
                    aggregateCondition = arg1;
                }
            }
        }

        // First, try to create a SELECT from the aggregate itself (i.e. handle the SOME AND EVERY case).
        if (isNonEmptyStream && aggregateCondition != null) {
            SelectOperator selectFromAgg = new SelectOperator(new MutableObject<>(aggregateCondition));
            selectFromAgg.getInputs().addAll(subplanRoot.getInputs());
            selectFromAgg.setSourceLocation(subplanRoot.getSourceLocation());

            return selectFromAgg;
        }

        // If we could not create a SELECT from the aggregate, try to find a SELECT inside the subplan itself.
        ILogicalOperator workingOperator = subplanRoot.getInputs().get(0).getValue();
        while (workingOperator != null) {
            if (workingOperator.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                return (SelectOperator) workingOperator;
            }
            workingOperator =
                    (workingOperator.getInputs().isEmpty()) ? null : workingOperator.getInputs().get(0).getValue();
        }

        // We could not find a SELECT.
        return null;
    }

    private ILogicalExpression keepOptimizableFunctions(ILogicalExpression cond) {
        if (cond.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            AbstractFunctionCallExpression func = (AbstractFunctionCallExpression) cond;
            List<Mutable<ILogicalExpression>> conjuncts = new ArrayList<>();
            if (splitIntoConjuncts(func, conjuncts)) {
                List<Mutable<ILogicalExpression>> optimizableConjuncts = new ArrayList<>();
                for (Mutable<ILogicalExpression> conjunct : conjuncts) {
                    if (conjunct.getValue().getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)
                            && optimizableFunctions.contains(
                                    ((AbstractFunctionCallExpression) conjunct.getValue()).getFunctionIdentifier())) {
                        optimizableConjuncts.add(conjunct);
                    }
                }

                if (optimizableConjuncts.size() == 1) {
                    return optimizableConjuncts.get(0).getValue();

                } else if (optimizableConjuncts.size() > 1) {
                    ScalarFunctionCallExpression andCond = new ScalarFunctionCallExpression(
                            BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND));
                    andCond.setSourceLocation(cond.getSourceLocation());
                    andCond.getArguments().addAll(optimizableConjuncts);
                    return andCond;
                }

            } else if (func.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)
                    && optimizableFunctions.contains(func.getFunctionIdentifier())) {
                return cond;

            }
        }

        return ConstantExpression.TRUE;
    }

    protected AggregateOperator getAggregateFromSubplan(SubplanOperator subplanOperator) {
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
        return workingSubplanRootAsAggregate;
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

    private boolean isArrayNonEmptyConjunctMissing(UnnestOperator firstUnnestInNTS, ILogicalOperator subplanInput) {
        UnnestingFunctionCallExpression unnestFunction =
                (UnnestingFunctionCallExpression) firstUnnestInNTS.getExpressionRef().getValue();
        VariableReferenceExpression unnestVarExpr =
                (VariableReferenceExpression) unnestFunction.getArguments().get(0).getValue();
        LogicalVariable arrayVariable = unnestVarExpr.getVariableReference();

        if (!subplanInput.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
            return true;
        }

        ILogicalExpression selectCondExpr =
                normalizeCondition(null, ((SelectOperator) subplanInput).getCondition().getValue());
        List<Mutable<ILogicalExpression>> conjunctsFromSelect = new ArrayList<>();
        if (splitIntoConjuncts(selectCondExpr, conjunctsFromSelect)) {
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

    private ILogicalExpression normalizeCondition(AggregateOperator aggregateOperator, ILogicalExpression expr) {
        // The purpose of this function is to remove the NOT(IF-MISSING-OR-NULL(...)) functions for a strict universal
        // quantification query. The {@code ArrayBTreeAccessMethod} does not recognize the former as optimizable
        // functions, so we remove them here. This SELECT will never make it to the final query plan (after the
        // {@code IntroduceSelectAccessMethodRule}), which allows us to get away with this logically incorrect branch.
        if (aggregateOperator != null && !isStrictUniversalQuantification(aggregateOperator)) {
            // We are working with an existential quantification OR an EACH AND EVERY query. Do not modify the SELECT.
            return expr;

        } else {
            // We are working with a strict universal quantification query.
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return expr;
            }
            ScalarFunctionCallExpression notFunction = (ScalarFunctionCallExpression) expr;
            if (!notFunction.getFunctionIdentifier().equals(BuiltinFunctions.NOT)) {
                return expr;
            }

            ScalarFunctionCallExpression ifMissingOrNullFunction =
                    (ScalarFunctionCallExpression) notFunction.getArguments().get(0).getValue();
            if (!ifMissingOrNullFunction.getFunctionIdentifier().equals(BuiltinFunctions.IF_MISSING_OR_NULL)) {
                return expr;
            }
            return ifMissingOrNullFunction.getArguments().get(0).getValue();

        }
    }

    private boolean splitIntoConjuncts(ILogicalExpression expression, List<Mutable<ILogicalExpression>> conjuncts) {
        List<Mutable<ILogicalExpression>> exprConjuncts = new ArrayList<>();
        if (expression.splitIntoConjuncts(exprConjuncts)) {
            for (Mutable<ILogicalExpression> conjunct : exprConjuncts) {
                List<Mutable<ILogicalExpression>> innerExprConjuncts = new ArrayList<>();
                if (splitIntoConjuncts(conjunct.getValue(), innerExprConjuncts)) {
                    conjuncts.addAll(innerExprConjuncts);
                } else {
                    conjuncts.add(new MutableObject<>(conjunct.getValue()));
                }
            }
            return true;
        }
        return false;
    }
}
