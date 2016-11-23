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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtils;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.ConstantExpressionUtil;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

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
        typeEnvironment = context.getOutputTypeEnvironment(op);
        ILogicalExpression condExpr = ((SelectOperator) op).getCondition().getValue();
        AccessMethodAnalysisContext analysisCtx = analyzeCondition(condExpr, context, typeEnvironment);
        if (analysisCtx.matchedFuncExprs.isEmpty()) {
            return false;
        }

        Dataset dataset = getDataset(op, context);
        List<String> filterFieldName = null;
        ARecordType recType = null;
        if (dataset != null && dataset.getDatasetType() == DatasetType.INTERNAL) {
            filterFieldName = DatasetUtils.getFilterField(dataset);
            IAType itemType = ((MetadataProvider) context.getMetadataProvider())
                    .findType(dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
            if (itemType.getTypeTag() == ATypeTag.RECORD) {
                recType = (ARecordType) itemType;
            }
        }
        if (filterFieldName == null || recType == null) {
            return false;
        }
        List<Index> datasetIndexes = ((MetadataProvider) context.getMetadataProvider())
                .getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName());

        List<IOptimizableFuncExpr> optFuncExprs = new ArrayList<>();

        for (int i = 0; i < analysisCtx.matchedFuncExprs.size(); i++) {
            IOptimizableFuncExpr optFuncExpr = analysisCtx.matchedFuncExprs.get(i);
            boolean found = findMacthedExprFieldName(optFuncExpr, op, dataset, recType, datasetIndexes, context);
            if (found && optFuncExpr.getFieldName(0).equals(filterFieldName)) {
                optFuncExprs.add(optFuncExpr);
            }
        }
        if (optFuncExprs.isEmpty()) {
            return false;
        }
        changePlan(optFuncExprs, op, dataset, context);

        OperatorPropertiesUtil.typeOpRec(opRef, context);
        context.addToDontApplySet(this, op);
        return true;
    }

    private AssignOperator createAssignOperator(List<IOptimizableFuncExpr> optFuncExprs,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars, IOptimizationContext context) {
        List<LogicalVariable> assignKeyVarList = new ArrayList<>();
        List<Mutable<ILogicalExpression>> assignKeyExprList = new ArrayList<>();

        for (IOptimizableFuncExpr optFuncExpr : optFuncExprs) {
            ComparisonKind ck = AlgebricksBuiltinFunctions
                    .getComparisonType(optFuncExpr.getFuncExpr().getFunctionIdentifier());
            ILogicalExpression searchKeyExpr = optFuncExpr.getConstantAtRuntimeExpr(0);
            LogicalVariable var = context.newVar();
            assignKeyExprList.add(new MutableObject<ILogicalExpression>(searchKeyExpr));
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
        return new AssignOperator(assignKeyVarList, assignKeyExprList);
    }

    private void changePlan(List<IOptimizableFuncExpr> optFuncExprs, AbstractLogicalOperator op, Dataset dataset,
            IOptimizationContext context) throws AlgebricksException {

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

                    AssignOperator assignOp = createAssignOperator(optFuncExprs, minFilterVars, maxFilterVars, context);

                    dataSourceScanOp.setMinFilterVars(minFilterVars);
                    dataSourceScanOp.setMaxFilterVars(maxFilterVars);

                    List<Mutable<ILogicalExpression>> additionalFilteringExpressions = new ArrayList<>();
                    for (LogicalVariable var : assignOp.getVariables()) {
                        additionalFilteringExpressions
                                .add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var)));
                    }

                    dataSourceScanOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);

                    assignOp.getInputs()
                            .add(new MutableObject<ILogicalOperator>(dataSourceScanOp.getInputs().get(0).getValue()));
                    dataSourceScanOp.getInputs().get(0).setValue(assignOp);
                }
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                UnnestMapOperator unnestMapOp = (UnnestMapOperator) descendantOp;
                ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
                if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                    FunctionIdentifier fid = f.getFunctionIdentifier();
                    if (!fid.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
                        throw new IllegalStateException();
                    }
                    AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                    jobGenParams.readFromFuncArgs(f.getArguments());
                    if (dataset.getDatasetName().compareTo(jobGenParams.datasetName) == 0) {
                        List<LogicalVariable> minFilterVars = new ArrayList<>();
                        List<LogicalVariable> maxFilterVars = new ArrayList<>();

                        AssignOperator assignOp = createAssignOperator(optFuncExprs, minFilterVars, maxFilterVars,
                                context);

                        unnestMapOp.setMinFilterVars(minFilterVars);
                        unnestMapOp.setMaxFilterVars(maxFilterVars);

                        List<Mutable<ILogicalExpression>> additionalFilteringExpressions = new ArrayList<>();
                        for (LogicalVariable var : assignOp.getVariables()) {
                            additionalFilteringExpressions
                                    .add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var)));
                        }
                        unnestMapOp.setAdditionalFilteringExpressions(additionalFilteringExpressions);
                        assignOp.getInputs()
                                .add(new MutableObject<ILogicalOperator>(unnestMapOp.getInputs().get(0).getValue()));
                        unnestMapOp.getInputs().get(0).setValue(assignOp);
                    }
                }
            }
            queue.addAll(descendantOp.getInputs());
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
                    String dataverseName;
                    String datasetName;
                    if (AsterixBuiltinFunctions.EXTERNAL_LOOKUP.equals(fid)) {
                        dataverseName = AccessMethodUtils.getStringConstant(f.getArguments().get(0));
                        datasetName = AccessMethodUtils.getStringConstant(f.getArguments().get(1));
                    } else if (fid.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
                        AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                        jobGenParams.readFromFuncArgs(f.getArguments());
                        dataverseName = jobGenParams.dataverseName;
                        datasetName = jobGenParams.datasetName;
                    } else {
                        throw new AlgebricksException("Unexpected function for Unnest Map: " + fid);
                    }
                    return ((MetadataProvider) context.getMetadataProvider()).findDataset(dataverseName,
                            datasetName);
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
            AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx, context, typeEnvironment);
        }
    }

    private boolean findMacthedExprFieldName(IOptimizableFuncExpr optFuncExpr, AbstractLogicalOperator op,
            Dataset dataset, ARecordType recType, List<Index> datasetIndexes, IOptimizationContext context)
            throws AlgebricksException {
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
                    List<String> fieldName = getFieldNameFromSubAssignTree(optFuncExpr, descendantOp, varIndex,
                            recType).second;
                    if (fieldName == null) {
                        return false;
                    }
                    optFuncExpr.setFieldName(funcVarIndex, fieldName);
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
                    List<String> fieldName = DatasetUtils.getPartitioningKeys(dataset).get(varIndex);
                    if (fieldName == null) {
                        return false;
                    }
                    optFuncExpr.setFieldName(funcVarIndex, fieldName);
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
                        if (!fid.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
                            throw new IllegalStateException();
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

                    IAType metaItemType = ((MetadataProvider) context.getMetadataProvider())
                            .findType(dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());
                    ARecordType metaRecType = (ARecordType) metaItemType;
                    int numSecondaryKeys = KeyFieldTypeUtils.getNumSecondaryKeys(index, recType, metaRecType);
                    List<String> fieldName;
                    if (varIndex >= numSecondaryKeys) {
                        fieldName = DatasetUtils.getPartitioningKeys(dataset).get(varIndex - numSecondaryKeys);
                    } else {
                        fieldName = index.getKeyFieldNames().get(varIndex);
                    }
                    if (fieldName == null) {
                        return false;
                    }
                    optFuncExpr.setFieldName(funcVarIndex, fieldName);
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

    private Pair<ARecordType, List<String>> getFieldNameFromSubAssignTree(IOptimizableFuncExpr optFuncExpr,
            AbstractLogicalOperator op, int varIndex, ARecordType recType) {
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
        if (funcIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME
                || funcIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX) {

            //get the variable from here. Figure out which input it came from. Go to that input!!!
            ArrayList<LogicalVariable> usedVars = new ArrayList<>();
            expr.getUsedVariables(usedVars);
            LogicalVariable usedVar = usedVars.get(0);
            List<String> returnList = new ArrayList<>();

            //Find the input that it came from
            for (int varCheck = 0; varCheck < op.getInputs().size(); varCheck++) {
                AbstractLogicalOperator nestedOp = (AbstractLogicalOperator) op.getInputs().get(varCheck).getValue();
                if (nestedOp.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
                    if (varCheck == op.getInputs().size() - 1) {

                    }
                } else {
                    int nestedAssignVar = ((AssignOperator) nestedOp).getVariables().indexOf(usedVar);
                    if (nestedAssignVar == -1) {
                        continue;
                    }
                    //get the nested info from the lower input
                    Pair<ARecordType, List<String>> lowerInfo = getFieldNameFromSubAssignTree(optFuncExpr,
                            (AbstractLogicalOperator) op.getInputs().get(varCheck).getValue(), nestedAssignVar,
                            recType);
                    if (lowerInfo != null) {
                        recType = lowerInfo.first;
                        returnList = lowerInfo.second;
                    }
                }
            }

            if (funcIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME) {
                String fieldName = ConstantExpressionUtil.getStringArgument(funcExpr, 1);
                if (fieldName == null) {
                    return null;
                }
                returnList.add(fieldName);
                return new Pair<>(recType, returnList);
            } else if (funcIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
                Integer fieldIndex = ConstantExpressionUtil.getIntArgument(funcExpr, 1);
                if (fieldIndex == null) {
                    return null;
                }
                returnList.add(recType.getFieldNames()[fieldIndex]);
                IAType subType = recType.getFieldTypes()[fieldIndex];
                if (subType.getTypeTag() == ATypeTag.RECORD) {
                    recType = (ARecordType) subType;
                }
                return new Pair<>(recType, returnList);
            }

        }

        ILogicalExpression argExpr = funcExpr.getArguments().get(0).getValue();
        if (argExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return null;
        }

        return null;
    }
}
