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
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

/**
 * Class for helping rewrite rules to choose and apply RTree indexes.
 */
public class RTreeAccessMethod implements IAccessMethod {

    private static List<FunctionIdentifier> funcIdents = new ArrayList<>();

    static {
        funcIdents.add(BuiltinFunctions.SPATIAL_INTERSECT);
    }

    public static final RTreeAccessMethod INSTANCE = new RTreeAccessMethod();

    @Override
    public List<FunctionIdentifier> getOptimizableFunctions() {
        return funcIdents;
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
        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(subTree, null, chosenIndex, analysisCtx,
                AccessMethodUtils.retainInputs(subTree.getDataSourceVariables(), subTree.getDataSourceRef().getValue(),
                        afterSelectRefs),
                false, false, context);
        if (primaryIndexUnnestOp == null) {
            return false;
        }
        // Replace the datasource scan with the new plan rooted at primaryIndexUnnestMap.
        subTree.getDataSourceRef().setValue(primaryIndexUnnestOp);
        return true;
    }

    @Override
    public ILogicalOperator createSecondaryToPrimaryPlan(Mutable<ILogicalExpression> conditionRef,
            OptimizableOperatorSubTree indexSubTree, OptimizableOperatorSubTree probeSubTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, boolean retainInput, boolean retainNull, boolean requiresBroadcast,
            IOptimizationContext context) throws AlgebricksException {
        return createSecondaryToPrimaryPlan(indexSubTree, probeSubTree, chosenIndex, analysisCtx, retainInput,
                retainNull, requiresBroadcast, context);
    }

    @Override
    public boolean applyJoinPlanTransformation(Mutable<ILogicalOperator> joinRef,
            OptimizableOperatorSubTree leftSubTree, OptimizableOperatorSubTree rightSubTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context, boolean isLeftOuterJoin,
            boolean hasGroupBy) throws AlgebricksException {
        // Determine if the index is applicable on the left or right side (if both, we arbitrarily prefer the left
        // side).
        Dataset dataset = analysisCtx.getDatasetFromIndexDatasetMap(chosenIndex);
        OptimizableOperatorSubTree indexSubTree;
        OptimizableOperatorSubTree probeSubTree;

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
            // get a new null place holder variable that is the first field variable of the primary key
            // from the indexSubTree's datasourceScanOp
            newNullPlaceHolderVar = indexSubTree.getDataSourceVariables().get(0);
        }

        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        ILogicalOperator primaryIndexUnnestOp = createSecondaryToPrimaryPlan(indexSubTree, probeSubTree, chosenIndex,
                analysisCtx, true, isLeftOuterJoin, true, context);
        if (primaryIndexUnnestOp == null) {
            return false;
        }

        if (isLeftOuterJoin && hasGroupBy) {
            // reset the null place holder variable
            AccessMethodUtils.resetLOJNullPlaceholderVariableInGroupByOp(analysisCtx, newNullPlaceHolderVar, context);
        }

        indexSubTree.getDataSourceRef().setValue(primaryIndexUnnestOp);
        // Change join into a select with the same condition.
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinRef.getValue();
        SelectOperator topSelect = new SelectOperator(joinOp.getCondition(), isLeftOuterJoin, newNullPlaceHolderVar);
        topSelect.getInputs().add(indexSubTree.getRootRef());
        topSelect.setExecutionMode(ExecutionMode.LOCAL);
        context.computeAndSetTypeEnvironmentForOperator(topSelect);
        // Replace the original join with the new subtree rooted at the select op.
        joinRef.setValue(topSelect);
        return true;
    }

    private ILogicalOperator createSecondaryToPrimaryPlan(OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            boolean retainInput, boolean retainNull, boolean requiresBroadcast, IOptimizationContext context)
            throws AlgebricksException {

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
        // we made sure indexSubTree has datasource scan
        AbstractDataSourceOperator dataSourceOp = (AbstractDataSourceOperator) indexSubTree.getDataSourceRef()
                .getValue();
        RTreeJobGenParams jobGenParams = new RTreeJobGenParams(chosenIndex.getIndexName(), IndexType.RTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, requiresBroadcast);
        // A spatial object is serialized in the constant of the func expr we are optimizing.
        // The R-Tree expects as input an MBR represented with 1 field per dimension.
        // Here we generate vars and funcs for extracting MBR fields from the constant into fields of a tuple (as the
        // R-Tree expects them).
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<>();
        // List of expressions for the assign.
        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<>();
        Pair<ILogicalExpression, Boolean> returnedSearchKeyExpr = AccessMethodUtils.createSearchKeyExpr(chosenIndex,
                optFuncExpr, optFieldType, indexSubTree, probeSubTree);
        ILogicalExpression searchKeyExpr = returnedSearchKeyExpr.first;

        for (int i = 0; i < numSecondaryKeys; i++) {
            // The create MBR function "extracts" one field of an MBR around the given spatial object.
            AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.CREATE_MBR));
            // Spatial object is the constant from the func expr we are optimizing.
            createMBR.getArguments().add(new MutableObject<>(searchKeyExpr));
            // The number of dimensions.
            createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                    new ConstantExpression(new AsterixConstantValue(new AInt32(numDimensions)))));
            // Which part of the MBR to extract.
            createMBR.getArguments().add(new MutableObject<ILogicalExpression>(
                    new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
            // Add a variable and its expr to the lists which will be passed into an assign op.
            LogicalVariable keyVar = context.newVar();
            keyVarList.add(keyVar);
            keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
        }
        jobGenParams.setKeyVarList(keyVarList);

        // Assign operator that "extracts" the MBR fields from the func-expr constant into a tuple.
        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        if (probeSubTree == null) {
            // We are optimizing a selection query.
            // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
            assignSearchKeys.getInputs().add(new MutableObject<>(
                    OperatorManipulationUtil.deepCopy(dataSourceOp.getInputs().get(0).getValue())));
            assignSearchKeys.setExecutionMode(dataSourceOp.getExecutionMode());
        } else {
            // We are optimizing a join, place the assign op top of the probe subtree.
            assignSearchKeys.getInputs().add(probeSubTree.getRootRef());
        }

        ILogicalOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                metaRecordType, chosenIndex, assignSearchKeys, jobGenParams, context, false, retainInput, retainNull);

        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        return dataset.getDatasetType() == DatasetType.EXTERNAL
                ? AccessMethodUtils.createExternalDataLookupUnnestMap(dataSourceOp, dataset, recordType,
                        secondaryIndexUnnestOp, context, retainInput, retainNull)
                : AccessMethodUtils.createPrimaryIndexUnnestMap(dataSourceOp, dataset, recordType, metaRecordType,
                        secondaryIndexUnnestOp, context, true, retainInput, false, false);
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
