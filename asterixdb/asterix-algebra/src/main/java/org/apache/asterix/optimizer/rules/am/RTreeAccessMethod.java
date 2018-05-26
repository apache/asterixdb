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
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * Class for helping rewrite rules to choose and apply RTree indexes.
 */
public class RTreeAccessMethod implements IAccessMethod {

    // The second boolean value tells whether the given function generates false positive results.
    // That is, this function can produce false positive results if it is set to true.
    // In this case, an index-search alone cannot replace the given SELECT condition and
    // that SELECT condition needs to be applied after the index-search to get the final results.
    // In R-Tree case, depending on the parameters of the SPATIAL_INTERSECT function, it may/may not produce
    // false positive results. Thus, we need to have one more step to check whether the SPATIAL_INTERSECT generates
    // false positive results or not.
    private static final List<Pair<FunctionIdentifier, Boolean>> FUNC_IDENTIFIERS = Collections.unmodifiableList(
            Arrays.asList(new Pair<FunctionIdentifier, Boolean>(BuiltinFunctions.SPATIAL_INTERSECT, true)));

    public static final RTreeAccessMethod INSTANCE = new RTreeAccessMethod();

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
        return true;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        return false;
    }

    @Override
    public boolean applySelectPlanTransformation(List<Mutable<ILogicalOperator>> afterSelectRefs,
            Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) throws AlgebricksException {
        SelectOperator selectOp = (SelectOperator) selectRef.getValue();
        Mutable<ILogicalExpression> conditionRef = selectOp.getCondition();
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) conditionRef.getValue();
        ARecordType recordType = subTree.getRecordType();

        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(chosenIndex, analysisCtx);

        int optFieldIdx = AccessMethodUtils.chooseFirstOptFuncVar(chosenIndex, analysisCtx);
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(optFuncExpr.getFieldType(optFieldIdx),
                optFuncExpr.getFieldName(optFieldIdx), recordType);
        if (keyPairType == null) {
            return false;
        }

        // To check whether the given plan is an index-only plan:
        // index-only plan possible?
        boolean isIndexOnlyPlan = false;

        // secondary key field usage after the select operator
        boolean secondaryKeyFieldUsedAfterSelectOp = false;

        // Whether a verification is required after the secondary index search
        // In other words, can the chosen method generate any false positive results?
        boolean requireVerificationAfterSIdxSearch = false;
        Pair<Boolean, Boolean> functionFalsePositiveCheck =
                AccessMethodUtils.canFunctionGenerateFalsePositiveResultsUsingIndex(funcExpr, FUNC_IDENTIFIERS);

        if (!functionFalsePositiveCheck.first) {
            return false;
        }

        // Does the given index can cover all search predicates?
        boolean doesSIdxSearchCoverAllPredicates = false;

        // Preliminary check for the index-only plan for R-Tree:
        // If the given index is not built on a POINT or a RECTANGLE field,
        // the query result can include false positives. And the result from secondary index search is an MBR,
        // thus we can't construct original secondary field value to remove any false positive results.
        if (keyPairType.first.getTypeTag() == BuiltinType.APOINT.getTypeTag()
                || keyPairType.first.getTypeTag() == BuiltinType.ARECTANGLE.getTypeTag()) {
            isIndexOnlyPlan = true;
            // The following variable can be changed if a query shape is not a POINT or rectangle.
            requireVerificationAfterSIdxSearch = false;
        } else {
            isIndexOnlyPlan = false;
            requireVerificationAfterSIdxSearch = true;
        }

        Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo =
                new Quadruple<>(isIndexOnlyPlan, secondaryKeyFieldUsedAfterSelectOp, requireVerificationAfterSIdxSearch,
                        doesSIdxSearchCoverAllPredicates);

        Dataset dataset = subTree.getDataset();

        // Is this plan an index-only plan?
        if (isIndexOnlyPlan) {
            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                AccessMethodUtils.indexOnlyPlanCheck(afterSelectRefs, selectRef, subTree, null, chosenIndex,
                        analysisCtx, context, indexOnlyPlanInfo);
                isIndexOnlyPlan = indexOnlyPlanInfo.getFirst();
            } else {
                // An index on an external dataset can't be optimized for the index-only plan.
                isIndexOnlyPlan = false;
                indexOnlyPlanInfo.setFirst(isIndexOnlyPlan);
            }
        }

        analysisCtx.setIndexOnlyPlanInfo(indexOnlyPlanInfo);

        ILogicalOperator primaryIndexUnnestOp = createIndexSearchPlan(afterSelectRefs, selectRef,
                selectOp.getCondition(), subTree.getAssignsAndUnnestsRefs(), subTree, null, chosenIndex, analysisCtx,
                AccessMethodUtils.retainInputs(subTree.getDataSourceVariables(), subTree.getDataSourceRef().getValue(),
                        afterSelectRefs),
                false, false, context, null);

        if (primaryIndexUnnestOp == null) {
            return false;
        }

        // Replace the datasource scan with the new plan rooted at primaryIndexUnnestMap.
        if (!isIndexOnlyPlan || dataset.getDatasetType() == DatasetType.EXTERNAL) {
            subTree.getDataSourceRef().setValue(primaryIndexUnnestOp);
        } else {
            // If this is an index-only plan, the topmost operator returned is UNIONALL operator.
            if (primaryIndexUnnestOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                selectRef.setValue(primaryIndexUnnestOp);
            } else {
                subTree.getDataSourceRef().setValue(primaryIndexUnnestOp);
            }
        }
        return true;
    }

    @Override
    public ILogicalOperator createIndexSearchPlan(List<Mutable<ILogicalOperator>> afterTopRefs,
            Mutable<ILogicalOperator> topRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignBeforeTopRefs, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            boolean retainInput, boolean retainNull, boolean requiresBroadcast, IOptimizationContext context,
            LogicalVariable newNullPlaceHolderForLOJ) throws AlgebricksException {
        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(chosenIndex, analysisCtx);

        Dataset dataset = indexSubTree.getDataset();
        ARecordType recordType = indexSubTree.getRecordType();
        ARecordType metaRecordType = indexSubTree.getMetaRecordType();

        int optFieldIdx = AccessMethodUtils.chooseFirstOptFuncVar(chosenIndex, analysisCtx);
        IAType optFieldType = optFuncExpr.getFieldType(optFieldIdx);
        List<String> optFieldName = optFuncExpr.getFieldName(optFieldIdx);
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(optFieldType, optFieldName, recordType);
        if (keyPairType == null) {
            return null;
        }

        // Get the number of dimensions corresponding to the field indexed by chosenIndex.
        IAType spatialType = keyPairType.first;
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numSecondaryKeys = numDimensions * 2;

        Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo = analysisCtx.getIndexOnlyPlanInfo();
        boolean isIndexOnlyPlan = indexOnlyPlanInfo.getFirst();
        // We apply index-only plan for an internal dataset.
        boolean generateInstantTrylockResultFromIndexSearch =
                dataset.getDatasetType() == DatasetType.INTERNAL && isIndexOnlyPlan ? true : false;

        // We made sure that the indexSubTree has a datasource scan.
        AbstractDataSourceOperator dataSourceOp =
                (AbstractDataSourceOperator) indexSubTree.getDataSourceRef().getValue();
        RTreeJobGenParams jobGenParams = new RTreeJobGenParams(chosenIndex.getIndexName(), IndexType.RTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, requiresBroadcast);
        // A spatial object is serialized in the constant of the func expr we are optimizing.
        // The R-Tree expects as input an MBR represented with 1 field per dimension.
        // Here we generate vars and funcs for extracting MBR fields from the constant into fields of a tuple
        // (as the R-Tree expects them). List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<>();
        // List of expressions for the assign.
        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<>();
        ILogicalExpression returnedSearchKeyExpr =
                AccessMethodUtils.createSearchKeyExpr(chosenIndex, optFuncExpr, optFieldType, probeSubTree).first;

        for (int i = 0; i < numSecondaryKeys; i++) {
            // The create MBR function "extracts" one field of an MBR around the given spatial object.
            AbstractFunctionCallExpression createMBR =
                    new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CREATE_MBR));
            createMBR.setSourceLocation(optFuncExpr.getFuncExpr().getSourceLocation());
            // Spatial object is the constant from the func expr we are optimizing.
            createMBR.getArguments().add(new MutableObject<>(returnedSearchKeyExpr));
            // The number of dimensions
            createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                    new ConstantExpression(new AsterixConstantValue(new AInt32(numDimensions)))));
            // Which part of the MBR to extract?
            createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                    new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
            // Adds a variable and its expr to the lists which will be passed into an assign op.
            LogicalVariable keyVar = context.newVar();
            keyVarList.add(keyVar);
            keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
        }
        jobGenParams.setKeyVarList(keyVarList);

        // Assigns an operator that "extracts" the MBR fields from the func-expr constant into a tuple.
        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        if (probeSubTree == null) {
            // We are optimizing a selection query.
            // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
            assignSearchKeys.setSourceLocation(dataSourceOp.getSourceLocation());
            assignSearchKeys.getInputs().add(
                    new MutableObject<>(OperatorManipulationUtil.deepCopy(dataSourceOp.getInputs().get(0).getValue())));
            assignSearchKeys.setExecutionMode(dataSourceOp.getExecutionMode());
        } else {
            // We are optimizing a join, place the assign op top of the probe subtree.
            assignSearchKeys.setSourceLocation(probeSubTree.getRoot().getSourceLocation());
            assignSearchKeys.getInputs().add(probeSubTree.getRootRef());
            assignSearchKeys.setExecutionMode(dataSourceOp.getExecutionMode());
            OperatorPropertiesUtil.typeOpRec(probeSubTree.getRootRef(), context);
        }
        context.computeAndSetTypeEnvironmentForOperator(assignSearchKeys);

        ILogicalOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                metaRecordType, chosenIndex, assignSearchKeys, jobGenParams, context, retainInput, retainNull,
                generateInstantTrylockResultFromIndexSearch);

        // Generates the rest of the upstream plan which feeds the search results into the primary index.
        return dataset.getDatasetType() == DatasetType.EXTERNAL
                ? AccessMethodUtils.createExternalDataLookupUnnestMap(dataSourceOp, dataset, recordType, metaRecordType,
                        secondaryIndexUnnestOp, context, chosenIndex, retainInput, retainNull)
                : AccessMethodUtils.createRestOfIndexSearchPlan(afterTopRefs, topRef, conditionRef, assignBeforeTopRefs,
                        dataSourceOp, dataset, recordType, metaRecordType, secondaryIndexUnnestOp, context, true,
                        retainInput, retainNull, false, chosenIndex, analysisCtx, indexSubTree,
                        newNullPlaceHolderForLOJ);
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

    @Override
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        if (optFuncExpr.getFuncExpr().getAnnotations()
                .containsKey(SkipSecondaryIndexSearchExpressionAnnotation.INSTANCE)) {
            return false;
        }
        // No additional analysis required.
        return true;
    }

    @Override
    public String getName() {
        return "RTREE_ACCESS_METHOD";
    }

    @Override
    public int compareTo(IAccessMethod o) {
        return this.getName().compareTo(o.getName());
    }

}
