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

package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.IndexedNLJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.util.LogRedactionUtil;

/**
 * Class for helping rewrite rules to choose and apply BTree indexes.
 */
public class BTreeAccessMethod implements IAccessMethod {

    // Describes whether a search predicate is an open/closed interval.
    private enum LimitType {
        LOW_INCLUSIVE,
        LOW_EXCLUSIVE,
        HIGH_INCLUSIVE,
        HIGH_EXCLUSIVE,
        EQUAL
    }

    // The second boolean value tells whether the given function generates false positive results.
    // That is, this function can produce false positive results if it is set to true.
    // In this case, an index-search alone cannot replace the given SELECT condition and
    // that SELECT condition needs to be applied after the index-search to get the correct results.
    // For B+Tree indexes, there are no false positive results unless the given index is a composite index.
    private static final List<Pair<FunctionIdentifier, Boolean>> FUNC_IDENTIFIERS = Collections
            .unmodifiableList(Arrays.asList(new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.EQ, false),
                    new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.LE, false),
                    new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.GE, false),
                    new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.LT, false),
                    new Pair<FunctionIdentifier, Boolean>(AlgebricksBuiltinFunctions.GT, false)));

    public static final BTreeAccessMethod INSTANCE = new BTreeAccessMethod();

    @Override
    public List<Pair<FunctionIdentifier, Boolean>> getOptimizableFunctions() {
        return FUNC_IDENTIFIERS;
    }

    @Override
    public boolean analyzeFuncExprArgsAndUpdateAnalysisCtx(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        boolean matches = AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVarAndUpdateAnalysisCtx(funcExpr,
                analysisCtx, context, typeEnvironment);
        if (!matches) {
            matches = AccessMethodUtils.analyzeFuncExprArgsForTwoVarsAndUpdateAnalysisCtx(funcExpr, analysisCtx);
        }
        return matches;
    }

    @Override
    public boolean matchAllIndexExprs() {
        return false;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        return true;
    }

    @Override
    public boolean applySelectPlanTransformation(List<Mutable<ILogicalOperator>> afterSelectRefs,
            Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) throws AlgebricksException {
        SelectOperator selectOp = (SelectOperator) selectRef.getValue();
        Mutable<ILogicalExpression> conditionRef = selectOp.getCondition();
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) conditionRef.getValue();

        // Check whether assign (unnest) operator exists before the select operator
        Mutable<ILogicalOperator> assignBeforeSelectOpRef =
                subTree.getAssignsAndUnnestsRefs().isEmpty() ? null : subTree.getAssignsAndUnnestsRefs().get(0);
        ILogicalOperator assignBeforeSelectOp = null;
        if (assignBeforeSelectOpRef != null) {
            assignBeforeSelectOp = assignBeforeSelectOpRef.getValue();
        }

        Dataset dataset = subTree.getDataset();

        // To check whether the given plan is an index-only plan.
        // index-only plan possible?
        boolean isIndexOnlyPlan = false;

        // Whether a verification is required after this secondary index search.
        // In other words, can the chosen method generate any false positive results?
        // Currently, for the B+ Tree index, there cannot be any false positive results unless it's a composite index.
        boolean requireVerificationAfterSIdxSearch = false;

        Pair<Boolean, Boolean> functionFalsePositiveCheck =
                AccessMethodUtils.canFunctionGenerateFalsePositiveResultsUsingIndex(funcExpr, FUNC_IDENTIFIERS);
        if (functionFalsePositiveCheck.first) {
            // An index-utilizable function found? then, get the info about false positive results generation.
            requireVerificationAfterSIdxSearch = functionFalsePositiveCheck.second;
        } else {
            return false;
        }

        Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo =
                new Quadruple<>(isIndexOnlyPlan, false, requireVerificationAfterSIdxSearch, false);

        if (dataset.getDatasetType() == DatasetType.INTERNAL && !chosenIndex.isPrimaryIndex()) {
            AccessMethodUtils.indexOnlyPlanCheck(afterSelectRefs, selectRef, subTree, null, chosenIndex, analysisCtx,
                    context, indexOnlyPlanInfo);
            isIndexOnlyPlan = indexOnlyPlanInfo.getFirst();
        }

        // Sets the result of index-only plan check into AccessMethodAnalysisContext.
        analysisCtx.setIndexOnlyPlanInfo(indexOnlyPlanInfo);

        // Transform the current path to the path that is utilizing the corresponding indexes
        ILogicalOperator primaryIndexUnnestOp = createIndexSearchPlan(afterSelectRefs, selectRef, conditionRef,
                subTree.getAssignsAndUnnestsRefs(), subTree, null, chosenIndex, analysisCtx,
                AccessMethodUtils.retainInputs(subTree.getDataSourceVariables(), subTree.getDataSourceRef().getValue(),
                        afterSelectRefs),
                false, subTree.getDataSourceRef().getValue().getInputs().get(0).getValue()
                        .getExecutionMode() == ExecutionMode.UNPARTITIONED,
                context, null);

        if (primaryIndexUnnestOp == null) {
            return false;
        }

        // Generate new path using the new condition.
        if (conditionRef.getValue() != null) {
            if (assignBeforeSelectOp != null) {
                if (isIndexOnlyPlan && dataset.getDatasetType() == DatasetType.INTERNAL) {
                    // Case 1: index-only plan
                    // The whole plan is now changed. Replace the current path with the given new plan.
                    // Gets the revised dataSourceRef operator
                    //  - a secondary index-search that generates trustworthy results.
                    ILogicalOperator dataSourceRefOp =
                            AccessMethodUtils.findDataSourceFromIndexUtilizationPlan(primaryIndexUnnestOp);

                    if (dataSourceRefOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                        subTree.getDataSourceRef().setValue(dataSourceRefOp);
                    }

                    // Replace the current operator with the newly created operator.
                    selectRef.setValue(primaryIndexUnnestOp);
                } else {
                    // Case 2: Non-index only plan case
                    // Right now, the order of operators is: select <- assign <- unnest-map (primary index look-up).
                    selectOp.getInputs().clear();
                    subTree.getDataSourceRef().setValue(primaryIndexUnnestOp);
                    selectOp.getInputs().add(new MutableObject<ILogicalOperator>(assignBeforeSelectOp));
                }
            } else {
                // A secondary-index-only plan without any assign cannot exist. This is a non-index only plan.
                selectOp.getInputs().clear();
                selectOp.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
            }
        } else {
            // All condition is now gone. UNNEST-MAP can replace the SELECT operator.
            ((AbstractLogicalOperator) primaryIndexUnnestOp).setExecutionMode(ExecutionMode.PARTITIONED);
            if (assignBeforeSelectOp != null) {
                subTree.getDataSourceRef().setValue(primaryIndexUnnestOp);
                selectRef.setValue(assignBeforeSelectOp);
            } else {
                selectRef.setValue(primaryIndexUnnestOp);
            }
        }

        return true;
    }

    @Override
    public boolean applyJoinPlanTransformation(List<Mutable<ILogicalOperator>> afterJoinRefs,
            Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree leftSubTree,
            OptimizableOperatorSubTree rightSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, boolean isLeftOuterJoin, boolean hasGroupBy) throws AlgebricksException {
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinRef.getValue();
        Mutable<ILogicalExpression> conditionRef = joinOp.getCondition();

        AbstractFunctionCallExpression funcExpr = null;
        if (conditionRef.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            funcExpr = (AbstractFunctionCallExpression) conditionRef.getValue();
        }

        Dataset dataset = analysisCtx.getIndexDatasetMap().get(chosenIndex);

        // Determine if the index is applicable on the right (inner) side.
        OptimizableOperatorSubTree indexSubTree = null;
        OptimizableOperatorSubTree probeSubTree = null;
        // We assume that the left subtree is the outer branch and the right subtree is the inner branch.
        // This assumption holds true since we only use an index from the right subtree.
        // The following is just a sanity check.
        if (rightSubTree.hasDataSourceScan()
                && dataset.getDatasetName().equals(rightSubTree.getDataset().getDatasetName())) {
            indexSubTree = rightSubTree;
            probeSubTree = leftSubTree;
        } else {
            return false;
        }

        LogicalVariable newNullPlaceHolderVar = null;
        if (isLeftOuterJoin) {
            // Gets a new null place holder variable that is the first field variable of the primary key
            // from the indexSubTree's datasourceScanOp.
            newNullPlaceHolderVar = indexSubTree.getDataSourceVariables().get(0);
        }

        boolean canContinue = AccessMethodUtils.setIndexOnlyPlanInfo(afterJoinRefs, joinRef, probeSubTree, indexSubTree,
                chosenIndex, analysisCtx, context, funcExpr, FUNC_IDENTIFIERS);
        if (!canContinue) {
            return false;
        }

        ILogicalOperator indexSearchOp = createIndexSearchPlan(afterJoinRefs, joinRef, conditionRef,
                indexSubTree.getAssignsAndUnnestsRefs(), indexSubTree, probeSubTree, chosenIndex, analysisCtx, true,
                isLeftOuterJoin, true, context, newNullPlaceHolderVar);

        if (indexSearchOp == null) {
            return false;
        }

        return AccessMethodUtils.finalizeJoinPlanTransformation(afterJoinRefs, joinRef, indexSubTree, analysisCtx,
                context, isLeftOuterJoin, hasGroupBy, indexSearchOp, newNullPlaceHolderVar, conditionRef, dataset);
    }

    /**
     * Creates an index utilization plan optimization - in case of an index-only select plan:
     * union < project < select < assign? < unnest-map(pidx) < split < unnest-map(sidx) < assign? < datasource-scan
     * ..... < project < ................................... <
     */
    @Override
    public ILogicalOperator createIndexSearchPlan(List<Mutable<ILogicalOperator>> afterTopOpRefs,
            Mutable<ILogicalOperator> topOpRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignBeforeTheOpRefs, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            boolean retainInput, boolean retainMissing, boolean requiresBroadcast, IOptimizationContext context,
            LogicalVariable newMissingPlaceHolderForLOJ) throws AlgebricksException {
        Dataset dataset = indexSubTree.getDataset();
        ARecordType recordType = indexSubTree.getRecordType();
        ARecordType metaRecordType = indexSubTree.getMetaRecordType();
        // we made sure indexSubTree has datasource scan
        AbstractDataSourceOperator dataSourceOp =
                (AbstractDataSourceOperator) indexSubTree.getDataSourceRef().getValue();
        List<Pair<Integer, Integer>> exprAndVarList = analysisCtx.getIndexExprsFromIndexExprsAndVars(chosenIndex);
        int numSecondaryKeys = analysisCtx.getNumberOfMatchedKeys(chosenIndex);

        // Whether the given plan is an index-only plan or not.
        Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo = analysisCtx.getIndexOnlyPlanInfo();
        boolean isIndexOnlyPlan = indexOnlyPlanInfo.getFirst();

        // We only apply index-only plan for an internal dataset.
        boolean generateInstantTrylockResultFromIndexSearch = false;
        if (dataset.getDatasetType() == DatasetType.INTERNAL && isIndexOnlyPlan) {
            generateInstantTrylockResultFromIndexSearch = true;
        }

        // List of function expressions that will be replaced by the secondary-index search.
        // These func exprs will be removed from the select condition at the very end of this method.
        Set<ILogicalExpression> replacedFuncExprs = new HashSet<>();
        // Info on high and low keys for the BTree search predicate.
        ILogicalExpression[] lowKeyExprs = new ILogicalExpression[numSecondaryKeys];
        ILogicalExpression[] highKeyExprs = new ILogicalExpression[numSecondaryKeys];
        LimitType[] lowKeyLimits = new LimitType[numSecondaryKeys];
        LimitType[] highKeyLimits = new LimitType[numSecondaryKeys];
        boolean[] lowKeyInclusive = new boolean[numSecondaryKeys];
        boolean[] highKeyInclusive = new boolean[numSecondaryKeys];
        ILogicalExpression[] lowKeyConstAtRuntimeExpressions = new ILogicalExpression[numSecondaryKeys];
        ILogicalExpression[] highKeyConstantAtRuntimeExpressions = new ILogicalExpression[numSecondaryKeys];
        LogicalVariable[] lowKeyConstAtRuntimeExprVars = new LogicalVariable[numSecondaryKeys];
        LogicalVariable[] highKeyConstAtRuntimeExprVars = new LogicalVariable[numSecondaryKeys];

        /* TODO: For now we don't do any sophisticated analysis of the func exprs to come up with "the best" range
         * predicate. If we can't figure out how to integrate a certain funcExpr into the current predicate,
         * we just bail by setting this flag.*/
        boolean couldntFigureOut = false;
        boolean doneWithExprs = false;
        boolean isEqCondition = false;
        BitSet setLowKeys = new BitSet(numSecondaryKeys);
        BitSet setHighKeys = new BitSet(numSecondaryKeys);
        // Go through the func exprs listed as optimizable by the chosen index,
        // and formulate a range predicate on the secondary-index keys.

        // Checks whether a type casting happened from a real (FLOAT, DOUBLE) value to an INT value
        // since we have a round issue when dealing with LT(<) OR GT(>) operator.
        for (Pair<Integer, Integer> exprIndex : exprAndVarList) {
            // Position of the field of matchedFuncExprs.get(exprIndex) in the chosen index's indexed exprs.
            IOptimizableFuncExpr optFuncExpr = analysisCtx.getMatchedFuncExpr(exprIndex.first);
            int keyPos = indexOf(optFuncExpr.getFieldName(0), optFuncExpr.getFieldSource(0),
                    chosenIndex.getKeyFieldNames(), chosenIndex.getKeyFieldSourceIndicators());
            if (keyPos < 0 && optFuncExpr.getNumLogicalVars() > 1) {
                // If we are optimizing a join, the matching field may be the second field name.
                keyPos = indexOf(optFuncExpr.getFieldName(1), optFuncExpr.getFieldSource(1),
                        chosenIndex.getKeyFieldNames(), chosenIndex.getKeyFieldSourceIndicators());
            }
            if (keyPos < 0) {
                throw CompilationException.create(ErrorCode.NO_INDEX_FIELD_NAME_FOR_GIVEN_FUNC_EXPR,
                        optFuncExpr.getFuncExpr().getSourceLocation());
            }
            // returnedSearchKeyExpr contains a pair of search expression.
            // The second expression will not be null only if we are creating an EQ search predicate
            // with a FLOAT or a DOUBLE constant that will be fed into an INTEGER index.
            // This is required because of type-casting. Refer to AccessMethodUtils.createSearchKeyExpr for details.
            IAType indexedFieldType = chosenIndex.getKeyFieldTypes().get(keyPos);
            Triple<ILogicalExpression, ILogicalExpression, Boolean> returnedSearchKeyExpr =
                    AccessMethodUtils.createSearchKeyExpr(chosenIndex, optFuncExpr, indexedFieldType, probeSubTree);
            ILogicalExpression searchKeyExpr = returnedSearchKeyExpr.first;
            ILogicalExpression searchKeyEQExpr = null;
            boolean realTypeConvertedToIntegerType = returnedSearchKeyExpr.third;

            LimitType limit = getLimitType(optFuncExpr, probeSubTree);
            if (limit == null) {
                return null;
            }

            if (limit == LimitType.EQUAL && returnedSearchKeyExpr.second != null) {
                // The given search predicate is EQ and
                // we have two type-casted values from FLOAT or DOUBLE to an INT constant.
                searchKeyEQExpr = returnedSearchKeyExpr.second;
            }

            // Deals with the non-enforced index case here.
            if (relaxLimitTypeToInclusive(chosenIndex, keyPos, realTypeConvertedToIntegerType)) {
                if (limit == LimitType.HIGH_EXCLUSIVE) {
                    limit = LimitType.HIGH_INCLUSIVE;
                } else if (limit == LimitType.LOW_EXCLUSIVE) {
                    limit = LimitType.LOW_INCLUSIVE;
                }
            }

            if (searchKeyExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                LogicalVariable constAtRuntimeExprVar = context.newVar();
                VariableReferenceExpression constAtRuntimeExprVarRef =
                        new VariableReferenceExpression(constAtRuntimeExprVar);
                constAtRuntimeExprVarRef.setSourceLocation(searchKeyExpr.getSourceLocation());

                if (limit == LimitType.LOW_INCLUSIVE || limit == LimitType.LOW_EXCLUSIVE || limit == LimitType.EQUAL) {
                    lowKeyConstAtRuntimeExpressions[keyPos] = searchKeyExpr;
                    lowKeyConstAtRuntimeExprVars[keyPos] = constAtRuntimeExprVar;
                }
                if (limit == LimitType.HIGH_INCLUSIVE || limit == LimitType.HIGH_EXCLUSIVE
                        || limit == LimitType.EQUAL) {
                    highKeyConstantAtRuntimeExpressions[keyPos] = searchKeyExpr;
                    highKeyConstAtRuntimeExprVars[keyPos] = constAtRuntimeExprVar;
                }
                searchKeyExpr = constAtRuntimeExprVarRef;
            }

            switch (limit) {
                case EQUAL: {
                    if (lowKeyLimits[keyPos] == null && highKeyLimits[keyPos] == null) {
                        lowKeyLimits[keyPos] = highKeyLimits[keyPos] = limit;
                        lowKeyInclusive[keyPos] = highKeyInclusive[keyPos] = true;
                        if (searchKeyEQExpr == null) {
                            // No type-casting was happened.
                            lowKeyExprs[keyPos] = highKeyExprs[keyPos] = searchKeyExpr;
                        } else {
                            // We have two type-casted FLOAT or DOUBLE values to be fed into an INT index.
                            // They contain the same value if their fraction value is 0.
                            // Refer to AccessMethodUtils.createSearchKeyExpr() for more details.
                            lowKeyExprs[keyPos] = searchKeyExpr;
                            highKeyExprs[keyPos] = searchKeyEQExpr;
                        }
                        setLowKeys.set(keyPos);
                        setHighKeys.set(keyPos);
                        isEqCondition = true;
                    } else {
                        // Has already been set to the identical values.
                        // When optimizing join we may encounter the same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (lowKeyLimits[keyPos] == limit && lowKeyInclusive[keyPos] == true
                                && lowKeyExprs[keyPos].equals(searchKeyExpr) && highKeyLimits[keyPos] == limit
                                && highKeyInclusive[keyPos] == true && highKeyExprs[keyPos].equals(searchKeyExpr)) {
                            isEqCondition = true;
                            break;
                        }
                        couldntFigureOut = true;
                    }
                    // TODO: For now don't consider prefix searches.
                    // If high and low keys are set, we exit for now.
                    if (setLowKeys.cardinality() == numSecondaryKeys && setHighKeys.cardinality() == numSecondaryKeys) {
                        doneWithExprs = true;
                    }
                    break;
                }
                case HIGH_EXCLUSIVE: {
                    if (highKeyLimits[keyPos] == null || (highKeyLimits[keyPos] != null && highKeyInclusive[keyPos])) {
                        highKeyLimits[keyPos] = limit;
                        highKeyExprs[keyPos] = searchKeyExpr;
                        highKeyInclusive[keyPos] = false;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the
                        // same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (highKeyLimits[keyPos] == limit && highKeyInclusive[keyPos] == false
                                && highKeyExprs[keyPos].equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case HIGH_INCLUSIVE: {
                    if (highKeyLimits[keyPos] == null) {
                        highKeyLimits[keyPos] = limit;
                        highKeyExprs[keyPos] = searchKeyExpr;
                        highKeyInclusive[keyPos] = true;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the
                        // same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (highKeyLimits[keyPos] == limit && highKeyInclusive[keyPos] == true
                                && highKeyExprs[keyPos].equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case LOW_EXCLUSIVE: {
                    if (lowKeyLimits[keyPos] == null || (lowKeyLimits[keyPos] != null && lowKeyInclusive[keyPos])) {
                        lowKeyLimits[keyPos] = limit;
                        lowKeyExprs[keyPos] = searchKeyExpr;
                        lowKeyInclusive[keyPos] = false;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the
                        // same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (lowKeyLimits[keyPos] == limit && lowKeyInclusive[keyPos] == false
                                && lowKeyExprs[keyPos].equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case LOW_INCLUSIVE: {
                    if (lowKeyLimits[keyPos] == null) {
                        lowKeyLimits[keyPos] = limit;
                        lowKeyExprs[keyPos] = searchKeyExpr;
                        lowKeyInclusive[keyPos] = true;
                    } else {
                        // Has already been set to the identical values. When optimizing join we may encounter the
                        // same optimizable expression twice
                        // (once from analyzing each side of the join)
                        if (lowKeyLimits[keyPos] == limit && lowKeyInclusive[keyPos] == true
                                && lowKeyExprs[keyPos].equals(searchKeyExpr)) {
                            break;
                        }
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
            if (!couldntFigureOut) {
                // Remember to remove this funcExpr later.
                replacedFuncExprs.add(analysisCtx.getMatchedFuncExpr(exprIndex.first).getFuncExpr());
            }
            if (doneWithExprs) {
                break;
            }
        }
        if (couldntFigureOut) {
            return null;
        }

        // if we have composite search keys, we should always need a post-processing to ensure the correctness
        // of search results because of the way a BTree is searched, unless only the last key is a range search.
        // During a BTree search, we iterate from the start index
        // (based on the low keys) to the end index (based on the high keys). During the iteration,
        // we can encounter a lot of false positives
        boolean primaryIndexPostProccessingIsNeeded = false;
        for (int i = 0; i < numSecondaryKeys - 1; i++) {
            if (!LimitType.EQUAL.equals(lowKeyLimits[i]) || !LimitType.EQUAL.equals(highKeyLimits[i])) {
                primaryIndexPostProccessingIsNeeded = true;
            }
        }

        // determine cases when prefix search could be applied
        for (int i = 1; i < lowKeyExprs.length; i++) {
            if (lowKeyLimits[0] == null && lowKeyLimits[i] != null || lowKeyLimits[0] != null && lowKeyLimits[i] == null
                    || highKeyLimits[0] == null && highKeyLimits[i] != null
                    || highKeyLimits[0] != null && highKeyLimits[i] == null) {
                numSecondaryKeys--;
                primaryIndexPostProccessingIsNeeded = true;
            }
        }

        if (primaryIndexPostProccessingIsNeeded) {
            Arrays.fill(lowKeyInclusive, true);
            Arrays.fill(highKeyInclusive, true);
        }

        if (lowKeyLimits[0] == null) {
            lowKeyInclusive[0] = true;
        }
        if (highKeyLimits[0] == null) {
            highKeyInclusive[0] = true;
        }

        // Here we generate vars and funcs for assigning the secondary-index keys to be fed into
        // the secondary-index search.
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<>();
        // List of variables and expressions for the assign.
        ArrayList<LogicalVariable> assignKeyVarList = new ArrayList<>();
        ArrayList<Mutable<ILogicalExpression>> assignKeyExprList = new ArrayList<>();
        int numLowKeys = createKeyVarsAndExprs(numSecondaryKeys, lowKeyLimits, lowKeyExprs, assignKeyVarList,
                assignKeyExprList, keyVarList, context, lowKeyConstAtRuntimeExpressions, lowKeyConstAtRuntimeExprVars);
        int numHighKeys = createKeyVarsAndExprs(numSecondaryKeys, highKeyLimits, highKeyExprs, assignKeyVarList,
                assignKeyExprList, keyVarList, context, highKeyConstantAtRuntimeExpressions,
                highKeyConstAtRuntimeExprVars);

        BTreeJobGenParams jobGenParams = new BTreeJobGenParams(chosenIndex.getIndexName(), IndexType.BTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, requiresBroadcast);
        jobGenParams
                .setLowKeyInclusive(lowKeyInclusive[primaryIndexPostProccessingIsNeeded ? 0 : numSecondaryKeys - 1]);
        jobGenParams
                .setHighKeyInclusive(highKeyInclusive[primaryIndexPostProccessingIsNeeded ? 0 : numSecondaryKeys - 1]);
        jobGenParams.setIsEqCondition(isEqCondition);
        jobGenParams.setLowKeyVarList(keyVarList, 0, numLowKeys);
        jobGenParams.setHighKeyVarList(keyVarList, numLowKeys, numHighKeys);

        ILogicalOperator inputOp;
        if (!assignKeyVarList.isEmpty()) {
            // Assign operator that sets the constant secondary-index search-key fields if necessary.
            AssignOperator assignSearchKeys = new AssignOperator(assignKeyVarList, assignKeyExprList);
            assignSearchKeys.setSourceLocation(dataSourceOp.getSourceLocation());
            if (probeSubTree == null) {
                // We are optimizing a selection query.
                // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
                assignSearchKeys.getInputs().add(new MutableObject<>(
                        OperatorManipulationUtil.deepCopy(dataSourceOp.getInputs().get(0).getValue())));
                assignSearchKeys.setExecutionMode(dataSourceOp.getExecutionMode());
            } else {
                // We are optimizing a join, place the assign op top of the probe subtree.
                assignSearchKeys.getInputs().add(probeSubTree.getRootRef());
                assignSearchKeys.setExecutionMode(probeSubTree.getRootRef().getValue().getExecutionMode());
            }
            context.computeAndSetTypeEnvironmentForOperator(assignSearchKeys);
            inputOp = assignSearchKeys;
        } else if (probeSubTree == null) {
            //nonpure case
            //Make sure that the nonpure function is unpartitioned
            ILogicalOperator checkOp = dataSourceOp.getInputs().get(0).getValue();
            while (checkOp.getExecutionMode() != ExecutionMode.UNPARTITIONED) {
                if (checkOp.getInputs().size() == 1) {
                    checkOp = checkOp.getInputs().get(0).getValue();
                } else {
                    return null;
                }
            }
            inputOp = dataSourceOp.getInputs().get(0).getValue();
        } else {
            // All index search keys are variables.
            inputOp = probeSubTree.getRoot();
        }

        // Creates an unnest-map for the secondary index search.
        // The result: SK, PK, [Optional - the result of an instantTrylock on PK]
        ILogicalOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                metaRecordType, chosenIndex, inputOp, jobGenParams, context, retainInput, retainMissing,
                generateInstantTrylockResultFromIndexSearch);

        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        ILogicalOperator indexSearchOp = null;

        boolean isPrimaryIndex = chosenIndex.isPrimaryIndex();
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            // External dataset
            UnnestMapOperator externalDataAccessOp =
                    AccessMethodUtils.createExternalDataLookupUnnestMap(dataSourceOp, dataset, recordType,
                            metaRecordType, secondaryIndexUnnestOp, context, chosenIndex, retainInput, retainMissing);
            indexSubTree.getDataSourceRef().setValue(externalDataAccessOp);
            return externalDataAccessOp;
        } else if (!isPrimaryIndex) {
            indexSearchOp = AccessMethodUtils.createRestOfIndexSearchPlan(afterTopOpRefs, topOpRef, conditionRef,
                    assignBeforeTheOpRefs, dataSourceOp, dataset, recordType, metaRecordType, secondaryIndexUnnestOp,
                    context, true, retainInput, retainMissing, false, chosenIndex, analysisCtx, indexSubTree,
                    newMissingPlaceHolderForLOJ);

            // Replaces the datasource scan with the new plan rooted at
            // Get dataSourceRef operator -
            // 1) unnest-map (PK, record) for a non-index only plan
            // 2) unnest-map (SK, PK) for an index-only plan
            if (isIndexOnlyPlan) {
                // Index-only plan
                ILogicalOperator dataSourceRefOp =
                        AccessMethodUtils.findDataSourceFromIndexUtilizationPlan(indexSearchOp);

                if (dataSourceRefOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP
                        || dataSourceRefOp.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
                    // Adds equivalence classes --- one equivalent class between a primary key
                    // variable and a record field-access expression.
                    EquivalenceClassUtils.addEquivalenceClassesForPrimaryIndexAccess(indexSearchOp,
                            dataSourceOp.getVariables(), recordType, metaRecordType, dataset, context);
                }
            } else {
                // Non-indexonly plan cases
                // Adds equivalence classes --- one equivalent class between a primary key
                // variable and a record field-access expression.
                EquivalenceClassUtils.addEquivalenceClassesForPrimaryIndexAccess(indexSearchOp,
                        dataSourceOp.getVariables(), recordType, metaRecordType, dataset, context);
            }
        } else {
            // Primary index search case
            List<Object> primaryIndexOutputTypes = new ArrayList<>();
            AccessMethodUtils.appendPrimaryIndexTypes(dataset, recordType, metaRecordType, primaryIndexOutputTypes);
            List<LogicalVariable> scanVariables = dataSourceOp.getVariables();

            // Checks whether the primary index search can replace the given SELECT condition.
            // If so, the condition will be set to null and eventually the SELECT operator will be removed.
            // If not, we create a new condition based on remaining ones.
            if (!primaryIndexPostProccessingIsNeeded) {
                List<Mutable<ILogicalExpression>> remainingFuncExprs = new ArrayList<>();
                try {
                    getNewConditionExprs(conditionRef, replacedFuncExprs, remainingFuncExprs);
                } catch (CompilationException e) {
                    return null;
                }
                // Generates the new condition.
                if (!remainingFuncExprs.isEmpty()) {
                    ILogicalExpression pulledCond = createSelectCondition(remainingFuncExprs);
                    conditionRef.setValue(pulledCond);
                } else {
                    conditionRef.setValue(null);
                }
            }

            // Checks whether LEFT_OUTER_UNNESTMAP operator is required.
            boolean leftOuterUnnestMapRequired = false;
            if (retainMissing && retainInput) {
                leftOuterUnnestMapRequired = true;
            } else {
                leftOuterUnnestMapRequired = false;
            }

            if (conditionRef.getValue() != null) {
                // The job gen parameters are transferred to the actual job gen
                // via the UnnestMapOperator's function arguments.
                List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<>();
                jobGenParams.writeToFuncArgs(primaryIndexFuncArgs);
                // An index search is expressed as an unnest-map over an index-search function.
                IFunctionInfo primaryIndexSearch = FunctionUtil.getFunctionInfo(BuiltinFunctions.INDEX_SEARCH);
                UnnestingFunctionCallExpression primaryIndexSearchFunc =
                        new UnnestingFunctionCallExpression(primaryIndexSearch, primaryIndexFuncArgs);
                primaryIndexSearchFunc.setSourceLocation(dataSourceOp.getSourceLocation());
                primaryIndexSearchFunc.setReturnsUniqueValues(true);
                AbstractUnnestMapOperator unnestMapOp;
                if (!leftOuterUnnestMapRequired) {
                    unnestMapOp = new UnnestMapOperator(scanVariables,
                            new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes,
                            retainInput);
                } else {
                    unnestMapOp = new LeftOuterUnnestMapOperator(scanVariables,
                            new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes,
                            true);
                }
                unnestMapOp.setSourceLocation(dataSourceOp.getSourceLocation());
                indexSearchOp = unnestMapOp;
            } else {
                AbstractUnnestMapOperator unnestMapOp;
                if (!leftOuterUnnestMapRequired) {
                    unnestMapOp = new UnnestMapOperator(scanVariables,
                            ((UnnestMapOperator) secondaryIndexUnnestOp).getExpressionRef(), primaryIndexOutputTypes,
                            retainInput);
                } else {
                    unnestMapOp = new LeftOuterUnnestMapOperator(scanVariables,
                            ((LeftOuterUnnestMapOperator) secondaryIndexUnnestOp).getExpressionRef(),
                            primaryIndexOutputTypes, true);
                }
                unnestMapOp.setSourceLocation(dataSourceOp.getSourceLocation());
                indexSearchOp = unnestMapOp;
            }
            // TODO: shouldn't indexSearchOp execution mode be set to that of the input? the default is UNPARTITIONED
            indexSearchOp.getInputs().add(new MutableObject<>(inputOp));

            // Adds equivalence classes --- one equivalent class between a primary key
            // variable and a record field-access expression.
            EquivalenceClassUtils.addEquivalenceClassesForPrimaryIndexAccess(indexSearchOp, scanVariables, recordType,
                    metaRecordType, dataset, context);
        }

        return indexSearchOp;
    }

    private int createKeyVarsAndExprs(int numKeys, LimitType[] keyLimits, ILogicalExpression[] searchKeyExprs,
            ArrayList<LogicalVariable> assignKeyVarList, ArrayList<Mutable<ILogicalExpression>> assignKeyExprList,
            ArrayList<LogicalVariable> keyVarList, IOptimizationContext context, ILogicalExpression[] constExpressions,
            LogicalVariable[] constExprVars) {
        if (keyLimits[0] == null) {
            return 0;
        }
        for (int i = 0; i < numKeys; i++) {
            ILogicalExpression searchKeyExpr = searchKeyExprs[i];
            ILogicalExpression constExpression = constExpressions[i];
            LogicalVariable keyVar = null;
            if (searchKeyExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                keyVar = context.newVar();
                assignKeyExprList.add(new MutableObject<>(searchKeyExpr));
                assignKeyVarList.add(keyVar);
            } else {
                keyVar = ((VariableReferenceExpression) searchKeyExpr).getVariableReference();
                if (constExpression != null) {
                    assignKeyExprList.add(new MutableObject<>(constExpression));
                    assignKeyVarList.add(constExprVars[i]);
                }
            }
            keyVarList.add(keyVar);
        }
        return numKeys;
    }

    private void getNewConditionExprs(Mutable<ILogicalExpression> conditionRef,
            Set<ILogicalExpression> replacedFuncExprs, List<Mutable<ILogicalExpression>> remainingFuncExprs)
            throws CompilationException {
        remainingFuncExprs.clear();
        if (replacedFuncExprs.isEmpty()) {
            return;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) conditionRef.getValue();
        if (replacedFuncExprs.size() == 1) {
            Iterator<ILogicalExpression> it = replacedFuncExprs.iterator();
            if (!it.hasNext()) {
                return;
            }
            if (funcExpr == it.next()) {
                // There are no remaining function exprs.
                return;
            }
        }
        // The original select cond must be an AND. Check it just to be sure.
        if (funcExpr.getFunctionIdentifier() != AlgebricksBuiltinFunctions.AND) {
            throw new CompilationException(ErrorCode.COMPILATION_FUNC_EXPRESSION_CANNOT_UTILIZE_INDEX,
                    funcExpr.getSourceLocation(), LogRedactionUtil.userData(funcExpr.toString()));
        }
        // Clean the conjuncts.
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            ILogicalExpression argExpr = arg.getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            // If the function expression was not replaced by the new index
            // plan, then add it to the list of remaining function expressions.
            if (!replacedFuncExprs.contains(argExpr)) {
                remainingFuncExprs.add(arg);
            }
        }
    }

    private static int indexOf(List<String> fieldName, int fieldSource, List<List<String>> keyNames,
            List<Integer> keySources) {
        int i = 0;
        for (List<String> keyName : keyNames) {
            if (keyName.equals(fieldName) && keyMatches(keySources, i, fieldSource)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    private static boolean keyMatches(List<Integer> keySources, int keyIndex, int fieldSource) {
        return keySources == null ? fieldSource == 0 : keySources.get(keyIndex) == fieldSource;
    }

    private LimitType getLimitType(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree probeSubTree) {
        ComparisonKind ck =
                AlgebricksBuiltinFunctions.getComparisonType(optFuncExpr.getFuncExpr().getFunctionIdentifier());
        LimitType limit = null;
        switch (ck) {
            case EQ: {
                limit = LimitType.EQUAL;
                break;
            }
            case GE: {
                limit = probeIsOnLhs(optFuncExpr, probeSubTree) ? LimitType.HIGH_INCLUSIVE : LimitType.LOW_INCLUSIVE;
                break;
            }
            case GT: {
                limit = probeIsOnLhs(optFuncExpr, probeSubTree) ? LimitType.HIGH_EXCLUSIVE : LimitType.LOW_EXCLUSIVE;
                break;
            }
            case LE: {
                limit = probeIsOnLhs(optFuncExpr, probeSubTree) ? LimitType.LOW_INCLUSIVE : LimitType.HIGH_INCLUSIVE;
                break;
            }
            case LT: {
                limit = probeIsOnLhs(optFuncExpr, probeSubTree) ? LimitType.LOW_EXCLUSIVE : LimitType.HIGH_EXCLUSIVE;
                break;
            }
            case NEQ: {
                limit = null;
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
        return limit;
    }

    private boolean relaxLimitTypeToInclusive(Index chosenIndex, int keyPos, boolean realTypeConvertedToIntegerType) {
        // For a non-enforced index or an enforced index that stores a casted value on the given index,
        // we need to apply the following transformation.
        // For an index on a closed field, this transformation is not necessary since the value between
        // the index and the actual record match.
        //
        // Check AccessMethodUtils.createSearchKeyExpr for more details.
        //
        // If a DOUBLE or FLOAT constant is converted to an INT type value,
        // we need to check a corner case where two real values are located between an INT value.
        // For example, for the following query,
        //
        // for $emp in dataset empDataset
        // where $emp.age > double("2.3") and $emp.age < double("3.3")
        // return $emp.id
        //
        // It should generate a result if there is a tuple that satisfies the condition, which is 3,
        // however, it does not generate the desired result since finding candidates
        // fail after truncating the fraction part (there is no INT whose value is greater than 2 and less than 3.)
        //
        // Therefore, we convert LT(<) to LE(<=) and GT(>) to GE(>=) to find candidates.
        // This does not change the result of an actual comparison since this conversion is only applied
        // for finding candidates from an index.
        if (chosenIndex.isEnforced() && realTypeConvertedToIntegerType) {
            return true;
        }

        if (chosenIndex.isOverridingKeyFieldTypes() && !chosenIndex.isEnforced()) {
            IAType indexedKeyType = chosenIndex.getKeyFieldTypes().get(keyPos);
            if (NonTaggedFormatUtil.isOptional(indexedKeyType)) {
                indexedKeyType = ((AUnionType) indexedKeyType).getActualType();
            }
            switch (indexedKeyType.getTypeTag()) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                    return true;
                default:
                    break;
            }
        }

        return false;
    }

    private boolean probeIsOnLhs(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree probeSubTree) {
        if (probeSubTree == null) {
            if (optFuncExpr.getConstantExpressions().length == 0) {
                return optFuncExpr.getLogicalExpr(0) == null;
            }
            // We are optimizing a selection query. Search key is a constant. Return true if constant is on lhs.
            return optFuncExpr.getFuncExpr().getArguments().get(0) == optFuncExpr.getConstantExpr(0);
        } else {
            // We are optimizing a join query. Determine whether the feeding variable is on the lhs.
            return (optFuncExpr.getOperatorSubTree(0) == null || optFuncExpr.getOperatorSubTree(0) == probeSubTree);
        }
    }

    private ILogicalExpression createSelectCondition(List<Mutable<ILogicalExpression>> predList) {
        if (predList.size() > 1) {
            IFunctionInfo finfo = FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.AND);
            ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(finfo, predList);
            andExpr.setSourceLocation(predList.get(0).getValue().getSourceLocation());
            return andExpr;
        }
        return predList.get(0).getValue();
    }

    @Override
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) throws AlgebricksException {
        // If we are optimizing a join, check for the indexed nested-loop join hint.
        if (optFuncExpr.getNumLogicalVars() == 2) {
            if (optFuncExpr.getOperatorSubTree(0) == optFuncExpr.getOperatorSubTree(1)) {
                if ((optFuncExpr.getSourceVar(0) == null && optFuncExpr.getFieldType(0) != null)
                        || (optFuncExpr.getSourceVar(1) == null && optFuncExpr.getFieldType(1) != null)) {
                    //We are in the select case (trees are the same, and one field comes from non-scan)
                    //We can do the index search
                } else {
                    //One of the vars was from an assign rather than a scan
                    //And we were unable to determine its type
                    return false;
                }
            } else if (!optFuncExpr.getFuncExpr().getAnnotations()
                    .containsKey(IndexedNLJoinExpressionAnnotation.INSTANCE)) {
                return false;
            }
        }
        if (!index.isPrimaryIndex() && optFuncExpr.getFuncExpr().getAnnotations()
                .containsKey(SkipSecondaryIndexSearchExpressionAnnotation.INSTANCE)) {
            return false;
        }
        // No additional analysis required for BTrees.
        return true;
    }

    @Override
    public String getName() {
        return "BTREE_ACCESS_METHOD";
    }

    @Override
    public int compareTo(IAccessMethod o) {
        return this.getName().compareTo(o.getName());
    }

}
