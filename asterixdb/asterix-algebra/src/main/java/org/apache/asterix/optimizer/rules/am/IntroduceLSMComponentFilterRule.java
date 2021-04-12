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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.ArrayIndexUtil;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class IntroduceLSMComponentFilterRule implements IAlgebraicRewriteRule {

    protected IVariableTypeEnvironment typeEnvironment = null;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (!checkIfRuleIsApplicable(opRef, context)) {
            return false;
        }

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        Dataset dataset = getDataset(op, context);
        Integer filterSourceIndicator = null;
        List<String> filterFieldName = null;
        ARecordType itemType = null;
        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
        if (dataset != null && dataset.getDatasetType() == DatasetType.INTERNAL) {
            filterSourceIndicator = DatasetUtil.getFilterSourceIndicator(dataset);
            filterFieldName = DatasetUtil.getFilterField(dataset);
            IAType filterSourceType = filterSourceIndicator == null || filterSourceIndicator == 0
                    ? mp.findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName())
                    : mp.findType(dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());

            if (filterSourceType.getTypeTag() == ATypeTag.OBJECT) {
                itemType = (ARecordType) filterSourceType;
            }
        }
        if (filterFieldName == null || itemType == null) {
            return false;
        }

        IAType filterType = itemType.getSubFieldType(filterFieldName);

        typeEnvironment = context.getOutputTypeEnvironment(op);
        ILogicalExpression condExpr = ((SelectOperator) op).getCondition().getValue();
        AccessMethodAnalysisContext analysisCtx = analyzeCondition(condExpr, context, typeEnvironment);

        List<IOptimizableFuncExpr> optFuncExprs = new ArrayList<>();

        if (!analysisCtx.getMatchedFuncExprs().isEmpty()) {
            List<Index> datasetIndexes = mp.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName());

            for (int i = 0; i < analysisCtx.getMatchedFuncExprs().size(); i++) {
                IOptimizableFuncExpr optFuncExpr = analysisCtx.getMatchedFuncExpr(i);
                boolean found = findMatchedExprFieldName(optFuncExpr, op, dataset, itemType, datasetIndexes, context,
                        filterSourceIndicator);
                // the field name source should be consistent with the filter source indicator
                if (found && optFuncExpr.getFieldName(0).equals(filterFieldName)
                        && optFuncExpr.getFieldSource(0) == filterSourceIndicator) {
                    optFuncExprs.add(optFuncExpr);
                }
            }
        }

        if (optFuncExprs.isEmpty()) {
            assignFilterFromSecondaryUnnestMap(op, dataset, context, filterType);
        } else {
            assignFilterFromQuery(optFuncExprs, op, dataset, context, filterType);
        }

        OperatorPropertiesUtil.typeOpRec(opRef, context);
        context.addToDontApplySet(this, op);
        return true;
    }

    private AssignOperator createAssignOperator(List<IOptimizableFuncExpr> optFuncExprs,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars, IOptimizationContext context,
            SourceLocation sourceLoc) {
        List<LogicalVariable> assignKeyVarList = new ArrayList<>();
        List<Mutable<ILogicalExpression>> assignKeyExprList = new ArrayList<>();

        for (IOptimizableFuncExpr optFuncExpr : optFuncExprs) {
            ComparisonKind ck =
                    AlgebricksBuiltinFunctions.getComparisonType(optFuncExpr.getFuncExpr().getFunctionIdentifier());
            ILogicalExpression searchKeyExpr = optFuncExpr.getConstantExpr(0);
            LogicalVariable var = context.newVar();
            assignKeyExprList.add(new MutableObject<>(searchKeyExpr));
            assignKeyVarList.add(var);
            if (ck == ComparisonKind.GE || ck == ComparisonKind.GT) {
                minFilterVars.add(var);
            } else if (ck == ComparisonKind.LE || ck == ComparisonKind.LT) {
                maxFilterVars.add(var);
            } else if (ck == ComparisonKind.EQ) {
                minFilterVars.add(var);
                maxFilterVars.add(var);
            }
        }
        AssignOperator assignOp = new AssignOperator(assignKeyVarList, assignKeyExprList);
        assignOp.setSourceLocation(sourceLoc);
        return assignOp;
    }

    private void assignFilterFromQuery(List<IOptimizableFuncExpr> optFuncExprs, AbstractLogicalOperator op,
            Dataset dataset, IOptimizationContext context, IAType filterType) throws AlgebricksException {

        List<UnnestMapOperator> primaryUnnestMapOps = new ArrayList<>();
        boolean hasSecondaryIndexMap = false;
        Queue<Mutable<ILogicalOperator>> queue = new LinkedList<>(op.getInputs());
        while (!queue.isEmpty()) {
            AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) queue.poll().getValue();
            if (descendantOp == null) {
                continue;
            }
            if (descendantOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                DataSourceScanOperator dataSourceScanOp = (DataSourceScanOperator) descendantOp;
                DataSource ds = (DataSource) dataSourceScanOp.getDataSource();
                if (dataset.getDatasetName().compareTo(((DatasetDataSource) ds).getDataset().getDatasetName()) == 0) {
                    List<LogicalVariable> minFilterVars = new ArrayList<>();
                    List<LogicalVariable> maxFilterVars = new ArrayList<>();

                    AssignOperator assignOp = createAssignOperator(optFuncExprs, minFilterVars, maxFilterVars, context,
                            dataSourceScanOp.getSourceLocation());

                    dataSourceScanOp.setMinFilterVars(minFilterVars);
                    dataSourceScanOp.setMaxFilterVars(maxFilterVars);

                    List<Mutable<ILogicalExpression>> additionalFilteringExpressions = new ArrayList<>();
                    for (LogicalVariable var : assignOp.getVariables()) {
                        VariableReferenceExpression varRef = new VariableReferenceExpression(var);
                        varRef.setSourceLocation(assignOp.getSourceLocation());
                        additionalFilteringExpressions.add(new MutableObject<ILogicalExpression>(varRef));
                    }

                    dataSourceScanOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);

                    assignOp.getInputs().add(new MutableObject<>(dataSourceScanOp.getInputs().get(0).getValue()));
                    dataSourceScanOp.getInputs().get(0).setValue(assignOp);
                }
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                UnnestMapOperator unnestMapOp = (UnnestMapOperator) descendantOp;
                ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
                if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                    FunctionIdentifier fid = f.getFunctionIdentifier();
                    if (!fid.equals(BuiltinFunctions.INDEX_SEARCH)) {
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                unnestMapOp.getSourceLocation(),
                                "Illegal function found, expected an " + "index-search.");
                    }
                    AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                    jobGenParams.readFromFuncArgs(f.getArguments());
                    if (dataset.getDatasetName().compareTo(jobGenParams.datasetName) == 0) {
                        List<LogicalVariable> minFilterVars = new ArrayList<>();
                        List<LogicalVariable> maxFilterVars = new ArrayList<>();

                        AssignOperator assignOp = createAssignOperator(optFuncExprs, minFilterVars, maxFilterVars,
                                context, unnestMapOp.getSourceLocation());

                        unnestMapOp.setMinFilterVars(minFilterVars);
                        unnestMapOp.setMaxFilterVars(maxFilterVars);

                        List<Mutable<ILogicalExpression>> additionalFilteringExpressions = new ArrayList<>();
                        for (LogicalVariable var : assignOp.getVariables()) {
                            VariableReferenceExpression varRef = new VariableReferenceExpression(var);
                            varRef.setSourceLocation(assignOp.getSourceLocation());
                            additionalFilteringExpressions.add(new MutableObject<ILogicalExpression>(varRef));
                        }
                        unnestMapOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
                        assignOp.getInputs().add(new MutableObject<>(unnestMapOp.getInputs().get(0).getValue()));
                        unnestMapOp.getInputs().get(0).setValue(assignOp);

                        if (jobGenParams.isPrimaryIndex) {
                            primaryUnnestMapOps.add(unnestMapOp);
                        } else {
                            hasSecondaryIndexMap = true;
                        }
                    }
                }
            }
            queue.addAll(descendantOp.getInputs());
        }
        if (hasSecondaryIndexMap && !primaryUnnestMapOps.isEmpty()) {
            propagateFilterToPrimaryIndex(primaryUnnestMapOps, filterType, context, false);
        }
    }

    private void propagateFilterToPrimaryIndex(List<UnnestMapOperator> primaryUnnestMapOps, IAType filterType,
            IOptimizationContext context, boolean isIndexOnlyPlan) throws AlgebricksException {
        for (UnnestMapOperator primaryOp : primaryUnnestMapOps) {
            Mutable<ILogicalOperator> assignOrOrderOrIntersect = primaryOp.getInputs().get(0);
            Mutable<ILogicalOperator> intersectOrSortOrSplit = assignOrOrderOrIntersect;

            if (assignOrOrderOrIntersect.getValue().getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                intersectOrSortOrSplit = assignOrOrderOrIntersect.getValue().getInputs().get(0);
            }

            switch (intersectOrSortOrSplit.getValue().getOperatorTag()) {
                case INTERSECT:
                    IntersectOperator intersect = (IntersectOperator) (intersectOrSortOrSplit.getValue());
                    List<List<LogicalVariable>> filterVars = new ArrayList<>(intersect.getInputs().size());
                    for (Mutable<ILogicalOperator> mutableOp : intersect.getInputs()) {
                        ILogicalOperator child = mutableOp.getValue();
                        while (!child.getOperatorTag().equals(LogicalOperatorTag.UNNEST_MAP)) {
                            child = child.getInputs().get(0).getValue();
                        }
                        UnnestMapOperator unnestMap = (UnnestMapOperator) child;
                        propagateFilterInSecondaryUnnsetMap(unnestMap, filterType, context);

                        List<LogicalVariable> extraVars = Arrays.asList(unnestMap.getPropagateIndexMinFilterVar(),
                                unnestMap.getPropagateIndexMaxFilterVar());
                        filterVars.add(extraVars);
                    }
                    if (!filterVars.isEmpty()) {
                        int outputFilterVarsCount = filterVars.get(0).size();
                        List<LogicalVariable> outputFilterVars = new ArrayList<>(outputFilterVarsCount);
                        for (int i = 0; i < outputFilterVarsCount; i++) {
                            outputFilterVars.add(context.newVar());
                        }
                        IntersectOperator intersectWithFilter =
                                createIntersectWithFilter(outputFilterVars, filterVars, intersect);
                        intersectOrSortOrSplit.setValue(intersectWithFilter);
                        context.computeAndSetTypeEnvironmentForOperator(intersectWithFilter);
                        setPrimaryFilterVar(primaryOp, outputFilterVars.get(0), outputFilterVars.get(1), context);
                    }
                    break;
                case SPLIT:
                    if (!isIndexOnlyPlan) {
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                intersectOrSortOrSplit.getValue().getOperatorTag().toString());
                    }
                    SplitOperator split = (SplitOperator) (intersectOrSortOrSplit.getValue());
                    for (Mutable<ILogicalOperator> childOp : split.getInputs()) {
                        ILogicalOperator child = childOp.getValue();
                        while (!child.getOperatorTag().equals(LogicalOperatorTag.UNNEST_MAP)) {
                            child = child.getInputs().get(0).getValue();
                        }
                        UnnestMapOperator unnestMap = (UnnestMapOperator) child;
                        propagateFilterInSecondaryUnnsetMap(unnestMap, filterType, context);
                        setPrimaryFilterVar(primaryOp, unnestMap.getPropagateIndexMinFilterVar(),
                                unnestMap.getPropagateIndexMaxFilterVar(), context);
                    }
                    break;
                case ORDER:
                case DISTINCT:
                    ILogicalOperator child = intersectOrSortOrSplit.getValue().getInputs().get(0).getValue();
                    if (child.getOperatorTag().equals(LogicalOperatorTag.UNNEST_MAP)) {
                        UnnestMapOperator secondaryMap = (UnnestMapOperator) child;

                        // If we are already propagating our index filter, do not repeat this action.
                        if (!secondaryMap.propagateIndexFilter()) {
                            propagateFilterInSecondaryUnnsetMap(secondaryMap, filterType, context);
                            setPrimaryFilterVar(primaryOp, secondaryMap.getPropagateIndexMinFilterVar(),
                                    secondaryMap.getPropagateIndexMaxFilterVar(), context);
                        }
                    }
                    break;

                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                            intersectOrSortOrSplit.getValue().getOperatorTag().toString());
            }
        }
    }

    private IntersectOperator createIntersectWithFilter(List<LogicalVariable> outputFilterVars,
            List<List<LogicalVariable>> filterVars, IntersectOperator intersect) throws AlgebricksException {
        int nInput = intersect.getNumInput();
        List<List<LogicalVariable>> inputCompareVars = new ArrayList<>(nInput);
        for (int i = 0; i < nInput; i++) {
            inputCompareVars.add(new ArrayList<>(intersect.getInputCompareVariables(i)));
        }
        IntersectOperator intersectWithFilter = new IntersectOperator(intersect.getOutputCompareVariables(),
                outputFilterVars, inputCompareVars, filterVars);
        intersectWithFilter.setSourceLocation(intersect.getSourceLocation());
        intersectWithFilter.getInputs().addAll(intersect.getInputs());
        return intersectWithFilter;
    }

    private void propagateFilterInSecondaryUnnsetMap(UnnestMapOperator secondaryUnnest, IAType filterType,
            IOptimizationContext context) throws AlgebricksException {

        LogicalVariable minIndexFilterVar = context.newVar();
        LogicalVariable maxIndexFilterVar = context.newVar();
        secondaryUnnest.markPropagageIndexFilter();
        secondaryUnnest.getVariables().add(minIndexFilterVar);
        secondaryUnnest.getVariableTypes().add(filterType);
        secondaryUnnest.getVariables().add(maxIndexFilterVar);
        secondaryUnnest.getVariableTypes().add(filterType);

        context.computeAndSetTypeEnvironmentForOperator(secondaryUnnest);
    }

    private void setPrimaryFilterVar(UnnestMapOperator primaryOp, LogicalVariable minFilterVar,
            LogicalVariable maxFilterVar, IOptimizationContext context) throws AlgebricksException {
        primaryOp.setMinFilterVars(Collections.singletonList(minFilterVar));
        primaryOp.setMaxFilterVars(Collections.singletonList(maxFilterVar));

        VariableReferenceExpression minFilterVarRef = new VariableReferenceExpression(minFilterVar);
        minFilterVarRef.setSourceLocation(primaryOp.getSourceLocation());
        VariableReferenceExpression maxFilterVarRef = new VariableReferenceExpression(maxFilterVar);
        maxFilterVarRef.setSourceLocation(primaryOp.getSourceLocation());
        List<Mutable<ILogicalExpression>> indexFilterExpression =
                Arrays.asList(new MutableObject<>(minFilterVarRef), new MutableObject<>(maxFilterVarRef));

        primaryOp.setAdditionalFilteringExpressions(indexFilterExpression);
        context.computeAndSetTypeEnvironmentForOperator(primaryOp);
    }

    private void assignFilterFromSecondaryUnnestMap(AbstractLogicalOperator op, Dataset dataset,
            IOptimizationContext context, IAType filterType) throws AlgebricksException {
        List<UnnestMapOperator> primaryUnnestMapOps = new ArrayList<>();
        boolean hasSecondaryIndexMap = false;
        boolean isIndexOnlyPlan = false;
        Queue<Mutable<ILogicalOperator>> queue = new LinkedList<>(op.getInputs());
        while (!queue.isEmpty()) {
            ILogicalOperator descendantOp = queue.poll().getValue();
            if (descendantOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                UnnestMapOperator unnestMapOp = (UnnestMapOperator) descendantOp;
                ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
                if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                    FunctionIdentifier fid = f.getFunctionIdentifier();
                    if (!fid.equals(BuiltinFunctions.INDEX_SEARCH)) {
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, f.getSourceLocation(),
                                fid.getName());
                    }
                    AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                    jobGenParams.readFromFuncArgs(f.getArguments());
                    if (dataset.getDatasetName().compareTo(jobGenParams.datasetName) == 0) {
                        if (jobGenParams.isPrimaryIndex) {
                            primaryUnnestMapOps.add(unnestMapOp);
                        } else {
                            hasSecondaryIndexMap = true;
                            isIndexOnlyPlan = unnestMapOp.getGenerateCallBackProceedResultVar();
                        }
                    }
                }
            }
            queue.addAll(descendantOp.getInputs());
        }
        if (hasSecondaryIndexMap && !primaryUnnestMapOps.isEmpty()) {
            propagateFilterToPrimaryIndex(primaryUnnestMapOps, filterType, context, isIndexOnlyPlan);
        }
    }

    private Dataset getDataset(AbstractLogicalOperator op, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        while (descendantOp != null) {
            if (descendantOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                DataSourceScanOperator dataSourceScanOp = (DataSourceScanOperator) descendantOp;
                DataSource ds = (DataSource) dataSourceScanOp.getDataSource();
                if (ds.getDatasourceType() != DataSource.Type.INTERNAL_DATASET) {
                    return null;
                }
                return ((DatasetDataSource) ds).getDataset();
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                UnnestMapOperator unnestMapOp = (UnnestMapOperator) descendantOp;
                ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
                if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                    FunctionIdentifier fid = f.getFunctionIdentifier();
                    DataverseName dataverseName;
                    String datasetName;
                    if (BuiltinFunctions.EXTERNAL_LOOKUP.equals(fid)) {
                        dataverseName = DataverseName
                                .createFromCanonicalForm(AccessMethodUtils.getStringConstant(f.getArguments().get(0)));
                        datasetName = AccessMethodUtils.getStringConstant(f.getArguments().get(1));
                    } else if (fid.equals(BuiltinFunctions.INDEX_SEARCH)) {
                        AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                        jobGenParams.readFromFuncArgs(f.getArguments());
                        dataverseName = jobGenParams.dataverseName;
                        datasetName = jobGenParams.datasetName;
                    } else {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, f.getSourceLocation(),
                                "Unexpected function for Unnest Map: " + fid);
                    }
                    return ((MetadataProvider) context.getMetadataProvider()).findDataset(dataverseName, datasetName);
                }
            }
            if (descendantOp.getInputs().isEmpty()) {
                break;
            }
            descendantOp = (AbstractLogicalOperator) descendantOp.getInputs().get(0).getValue();
        }
        return null;
    }

    private boolean checkIfRuleIsApplicable(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        // First check that the operator is a select and its condition is a function call.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        ILogicalExpression condExpr = ((SelectOperator) op).getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        return true;
    }

    private AccessMethodAnalysisContext analyzeCondition(ILogicalExpression cond, IOptimizationContext context,
            IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        AccessMethodAnalysisContext analysisCtx = new AccessMethodAnalysisContext();
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) cond;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (funcIdent != AlgebricksBuiltinFunctions.OR) {
            analyzeFunctionExpr(funcExpr, analysisCtx, context, typeEnvironment);
            for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                ILogicalExpression argExpr = arg.getValue();
                if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    continue;
                }
                analyzeFunctionExpr((AbstractFunctionCallExpression) argExpr, analysisCtx, context, typeEnvironment);
            }
        }
        return analysisCtx;
    }

    private void analyzeFunctionExpr(AbstractFunctionCallExpression funcExpr, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (funcIdent == AlgebricksBuiltinFunctions.LE || funcIdent == AlgebricksBuiltinFunctions.GE
                || funcIdent == AlgebricksBuiltinFunctions.LT || funcIdent == AlgebricksBuiltinFunctions.GT
                || funcIdent == AlgebricksBuiltinFunctions.EQ) {
            AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVarAndUpdateAnalysisCtx(funcExpr, analysisCtx, context,
                    typeEnvironment);
        }
    }

    private boolean findMatchedExprFieldName(IOptimizableFuncExpr optFuncExpr, AbstractLogicalOperator op,
            Dataset dataset, ARecordType filterSourceType, List<Index> datasetIndexes, IOptimizationContext context,
            Integer filterSourceIndicator) throws AlgebricksException {
        AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        while (descendantOp != null) {
            if (descendantOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) descendantOp;
                List<LogicalVariable> varList = assignOp.getVariables();
                for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                    LogicalVariable var = varList.get(varIndex);
                    int funcVarIndex = optFuncExpr.findLogicalVar(var);
                    if (funcVarIndex == -1) {
                        continue;
                    }
                    Pair<ARecordType, List<String>> fieldNamePairs =
                            getFieldNameFromSubAssignTree(optFuncExpr, descendantOp, varIndex, filterSourceType,
                                    filterSourceIndicator, dataset.getPrimaryKeys().size());
                    if (fieldNamePairs == null) {
                        return false;
                    }
                    List<String> fieldName = fieldNamePairs.second;
                    // Since we validated the filter source in getFieldNameFromSubAssignTree, we can safely set the
                    // fieldSource to be filterSourceIndicator
                    optFuncExpr.setFieldName(funcVarIndex, fieldName, filterSourceIndicator);
                    return true;
                }
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                DataSourceScanOperator scanOp = (DataSourceScanOperator) descendantOp;
                List<LogicalVariable> varList = scanOp.getVariables();
                for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                    LogicalVariable var = varList.get(varIndex);
                    int funcVarIndex = optFuncExpr.findLogicalVar(var);
                    if (funcVarIndex == -1) {
                        continue;
                    }
                    // The variable value is one of the partitioning fields.
                    List<String> fieldName = dataset.getPrimaryKeys().get(varIndex);
                    if (fieldName == null) {
                        return false;
                    }
                    List<Integer> keySourceIndicators = DatasetUtil.getKeySourceIndicators(dataset);
                    int keySource = getKeySource(keySourceIndicators, varIndex);
                    optFuncExpr.setFieldName(funcVarIndex, fieldName, keySource);
                    return true;
                }
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                UnnestMapOperator unnestMapOp = (UnnestMapOperator) descendantOp;
                List<LogicalVariable> varList = unnestMapOp.getVariables();
                for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                    LogicalVariable var = varList.get(varIndex);
                    int funcVarIndex = optFuncExpr.findLogicalVar(var);
                    if (funcVarIndex == -1) {
                        continue;
                    }

                    String indexName;
                    Index index = null;
                    ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
                    if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                        FunctionIdentifier fid = f.getFunctionIdentifier();
                        if (!fid.equals(BuiltinFunctions.INDEX_SEARCH)) {
                            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                    unnestMapOp.getSourceLocation(),
                                    "Illegal function found, expected an " + "index-search.");
                        }
                        AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                        jobGenParams.readFromFuncArgs(f.getArguments());
                        indexName = jobGenParams.indexName;
                        for (Index idx : datasetIndexes) {
                            if (idx.getIndexName().compareTo(indexName) == 0) {
                                index = idx;
                                break;
                            }
                        }
                    }
                    if (index == null) {
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                unnestMapOp.getSourceLocation(),
                                "Could not find the corresponding index for an" + " index search.");
                    }

                    IAType metaItemType = ((MetadataProvider) context.getMetadataProvider())
                            .findType(dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());
                    IAType recordItemType = ((MetadataProvider) context.getMetadataProvider())
                            .findType(dataset.getMetaItemTypeDataverseName(), dataset.getItemTypeName());
                    ARecordType recordType = (ARecordType) recordItemType;
                    ARecordType metaRecType = (ARecordType) metaItemType;
                    int numSecondaryKeys = KeyFieldTypeUtil.getNumSecondaryKeys(index, recordType, metaRecType);
                    List<String> fieldName;
                    int keySource;
                    if (varIndex >= numSecondaryKeys) {
                        int idx = varIndex - numSecondaryKeys;
                        fieldName = dataset.getPrimaryKeys().get(idx);
                        keySource = getKeySource(DatasetUtil.getKeySourceIndicators(dataset), idx);
                    } else {
                        List<List<String>> keyFieldNames;
                        List<Integer> keySources;
                        switch (Index.IndexCategory.of(index.getIndexType())) {
                            case ARRAY:
                                Index.ArrayIndexDetails arrayIndexDetails =
                                        (Index.ArrayIndexDetails) index.getIndexDetails();
                                keyFieldNames = new ArrayList<>();
                                keySources = new ArrayList<>();
                                for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
                                    for (List<String> project : e.getProjectList()) {
                                        keyFieldNames.add(
                                                ArrayIndexUtil.getFlattenedKeyFieldNames(e.getUnnestList(), project));
                                        keySources.add(e.getSourceIndicator());
                                    }
                                }
                                break;
                            case VALUE:
                                Index.ValueIndexDetails valueIndexDetails =
                                        (Index.ValueIndexDetails) index.getIndexDetails();
                                keyFieldNames = valueIndexDetails.getKeyFieldNames();
                                keySources = valueIndexDetails.getKeyFieldSourceIndicators();
                                break;
                            case TEXT:
                                Index.TextIndexDetails textIndexDetails =
                                        (Index.TextIndexDetails) index.getIndexDetails();
                                keyFieldNames = textIndexDetails.getKeyFieldNames();
                                keySources = textIndexDetails.getKeyFieldSourceIndicators();
                                break;
                            default:
                                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE,
                                        String.valueOf(index.getIndexType()));
                        }
                        fieldName = keyFieldNames.get(varIndex);
                        keySource = getKeySource(keySources, varIndex);
                    }
                    if (fieldName == null) {
                        return false;
                    }
                    optFuncExpr.setFieldName(funcVarIndex, fieldName, keySource);
                    return true;
                }
            }

            if (descendantOp.getInputs().isEmpty()) {
                break;
            }
            descendantOp = (AbstractLogicalOperator) descendantOp.getInputs().get(0).getValue();
        }
        return false;
    }

    private static int getKeySource(List<Integer> keySourceIndicators, int keyIdx) {
        return keySourceIndicators == null ? 0 : keySourceIndicators.get(keyIdx);
    }

    private Pair<ARecordType, List<String>> getFieldNameFromSubAssignTree(IOptimizableFuncExpr optFuncExpr,
            AbstractLogicalOperator op, int varIndex, ARecordType filterSourceType, Integer filterSourceIndicator,
            int numOfPKeys) {
        AbstractLogicalExpression expr = null;
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) op;
            expr = (AbstractLogicalExpression) assignOp.getExpressions().get(varIndex).getValue();
        }
        if (expr == null || expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (funcIdent == BuiltinFunctions.FIELD_ACCESS_BY_NAME || funcIdent == BuiltinFunctions.FIELD_ACCESS_BY_INDEX) {

            //get the variable from here. Figure out which input it came from. Go to that input!!!
            ArrayList<LogicalVariable> usedVars = new ArrayList<>();
            expr.getUsedVariables(usedVars);
            LogicalVariable usedVar = usedVars.get(0);
            List<String> returnList = new ArrayList<>();

            //Find the input that it came from
            for (int varCheck = 0; varCheck < op.getInputs().size(); varCheck++) {
                AbstractLogicalOperator nestedOp = (AbstractLogicalOperator) op.getInputs().get(varCheck).getValue();
                if (nestedOp.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
                    if (nestedOp.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
                        return null;
                    }
                    List<LogicalVariable> scannedVars = ((DataSourceScanOperator) nestedOp).getScanVariables();
                    if (scannedVars.indexOf(usedVar) != filterSourceIndicator + numOfPKeys) {
                        return null;
                    }
                } else {
                    int nestedAssignVar = ((AssignOperator) nestedOp).getVariables().indexOf(usedVar);
                    if (nestedAssignVar == -1) {
                        continue;
                    }
                    //get the nested info from the lower input
                    Pair<ARecordType, List<String>> lowerInfo = getFieldNameFromSubAssignTree(optFuncExpr,
                            (AbstractLogicalOperator) op.getInputs().get(varCheck).getValue(), nestedAssignVar,
                            filterSourceType, filterSourceIndicator, numOfPKeys);
                    if (lowerInfo != null) {
                        // propagate filterSourceType in case the filter value comes from a nested attribute.
                        filterSourceType = lowerInfo.first;
                        returnList = lowerInfo.second;
                    }
                }
            }

            if (funcIdent == BuiltinFunctions.FIELD_ACCESS_BY_NAME) {
                String fieldName = ConstantExpressionUtil.getStringArgument(funcExpr, 1);
                if (fieldName == null) {
                    return null;
                }
                returnList.add(fieldName);
                return new Pair<>(filterSourceType, returnList);
            } else if (funcIdent == BuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
                Integer fieldIndex = ConstantExpressionUtil.getIntArgument(funcExpr, 1);
                if (fieldIndex == null) {
                    return null;
                }
                returnList.add(filterSourceType.getFieldNames()[fieldIndex]);
                IAType subType = filterSourceType.getFieldTypes()[fieldIndex];
                if (subType.getTypeTag() == ATypeTag.OBJECT) {
                    filterSourceType = (ARecordType) subType;
                }
                return new Pair<>(filterSourceType, returnList);
            }

        }

        ILogicalExpression argExpr = funcExpr.getArguments().get(0).getValue();
        if (argExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return null;
        }

        return null;
    }
}
