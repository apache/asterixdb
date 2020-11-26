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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.ExternalDataProjectionInfo;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Pushes field-access expression to the external dataset scan to minimize the size of the record.
 * This rule currently does not remove the field access expression in ASSIGN and SCAN operators. Instead,
 * it adds the requested field names to external dataset details to produce records that only contain the requested
 * fields. Thus, no changes would occur in the plan's structure after firing this rule.
 * Example:
 * Before plan:
 * ...
 * select (and(gt($$00, 20), gt($$r.getField("salary"), 70000)))
 * ...
 * assign [$$00] <- [$$r.getField("personalInfo").getField("age")]
 * ...
 * data-scan []<-[$$r] <- ParquetDataverse.ParquetDataset
 * <p>
 * After plan:
 * ...
 * select (and(gt($$00, 20), gt($$r.getField("salary"), 70000)))
 * ...
 * assign [$$00] <- [$$r.getField("personalInfo").getField("age")]
 * ...
 * data-scan []<-[$$r] <- ParquetDataverse.ParquetDataset project (personalInfo.age, salary)
 * <p>
 * The resulting record $$r will be {"personalInfo":{"age": *AGE*}, "salary": *SALARY*}
 * and other fields will not be included in $$r.
 */
public class PushFieldAccessToExternalDataScanRule implements IAlgebraicRewriteRule {
    //Datasets payload variables
    private final List<LogicalVariable> recordVariables = new ArrayList<>();
    //Dataset scan operators' projection info
    private final List<ExternalDataProjectionInfo> projectionInfos = new ArrayList<>();
    //Final result live variables
    private final Set<LogicalVariable> projectedVariables = new HashSet<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        final ILogicalOperator currentOp = opRef.getValue();
        final LogicalOperatorTag currentOpTag = currentOp.getOperatorTag();
        if (!context.getPhysicalOptimizationConfig().isExternalFieldPushdown()) {
            return false;
        }
        if (currentOpTag == LogicalOperatorTag.PROJECT) {
            ProjectOperator projectOp = (ProjectOperator) currentOp;
            projectedVariables.addAll(projectOp.getVariables());
            return false;
        }

        if (currentOpTag != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }

        return setDatasetProperties(currentOp, (MetadataProvider) context.getMetadataProvider());
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        final ILogicalOperator op = opRef.getValue();
        if (!context.getPhysicalOptimizationConfig().isExternalFieldPushdown()
                || context.checkIfInDontApplySet(this, op) || projectionInfos.isEmpty()) {
            return false;
        }

        if (op.getOperatorTag() != LogicalOperatorTag.SELECT && op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }

        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            final SelectOperator selectOp = (SelectOperator) op;
            pushFieldAccessExpression(selectOp.getCondition(), context);
        } else {
            final AssignOperator assignOp = (AssignOperator) op;
            pushFieldAccessExpression(assignOp.getExpressions(), context);
        }

        //Add to do not apply to avoid pushing the same expression twice when the plan contains REPLICATE
        context.addToDontApplySet(this, op);

        return false;
    }

    private void pushFieldAccessExpression(List<Mutable<ILogicalExpression>> exprList, IOptimizationContext context)
            throws AlgebricksException {

        for (Mutable<ILogicalExpression> exprRef : exprList) {
            pushFieldAccessExpression(exprRef, context);
        }
    }

    private void pushFieldAccessExpression(Mutable<ILogicalExpression> exprRef, IOptimizationContext context)
            throws AlgebricksException {
        final ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }

        final AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

        //Only field access expressions are allowed
        if (!isFieldAccessByName(funcExpr)) {
            pushFieldAccessExpression(funcExpr.getArguments(), context);
            return;
        }

        //Get root expression input variable in case it is nested field access
        final LogicalVariable funcRootInputVar = getRootExpressionInputVariable(funcExpr);
        if (funcRootInputVar != null) {
            final int recordVarIndex = recordVariables.indexOf(funcRootInputVar);
            //Is funcRootInputVar originated from a data-scan operator?
            if (recordVarIndex >= 0) {
                final List<List<String>> projectedFieldNames = projectionInfos.get(recordVarIndex).getProjectionInfo();
                final List<String> fieldNames = new ArrayList<>();
                //Add fieldAccessExpr to field names list
                buildFieldNames(funcExpr, fieldNames);
                if (!fieldNames.isEmpty()) {
                    projectedFieldNames.add(fieldNames);
                }
            }
        } else {
            //Descend to the arguments expressions to see if any can be pushed
            pushFieldAccessExpression(funcExpr.getArguments(), context);
        }
    }

    private boolean setDatasetProperties(ILogicalOperator op, MetadataProvider mp) throws AlgebricksException {
        final DataSourceScanOperator scan = (DataSourceScanOperator) op;
        final DataSource dataSource = (DataSource) scan.getDataSource();

        if (dataSource == null) {
            return false;
        }
        final DataverseName dataverse = dataSource.getId().getDataverseName();
        final String datasetName = dataSource.getId().getDatasourceName();
        final Dataset dataset = mp.findDataset(dataverse, datasetName);

        //Only external dataset can have pushed down expressions
        if (dataset == null || dataset.getDatasetType() == DatasetType.INTERNAL
                || dataset.getDatasetType() == DatasetType.EXTERNAL && !ExternalDataUtils
                        .supportsPushdown(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties())) {
            return false;
        }

        boolean changed = false;
        final DatasetDataSource datasetDataSource = (DatasetDataSource) dataSource;
        final LogicalVariable recordVar = datasetDataSource.getDataRecordVariable(scan.getVariables());
        if (!projectedVariables.contains(recordVar) && scan.getProjectionInfo() == null) {
            //Do not push expressions to data scan if the whole record is needed
            recordVariables.add(recordVar);
            ExternalDataProjectionInfo projectionInfo = new ExternalDataProjectionInfo();
            scan.setProjectionInfo(projectionInfo);
            projectionInfos.add(projectionInfo);
            changed = true;
        }
        return changed;
    }

    private static LogicalVariable getRootExpressionInputVariable(AbstractFunctionCallExpression funcExpr) {
        ILogicalExpression currentExpr = funcExpr.getArguments().get(0).getValue();
        while (isFieldAccessByName(currentExpr)) {
            currentExpr = ((AbstractFunctionCallExpression) currentExpr).getArguments().get(0).getValue();
        }

        if (currentExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return ((VariableReferenceExpression) currentExpr).getVariableReference();
        }
        return null;
    }

    private static boolean isFieldAccessByName(ILogicalExpression expression) {
        return expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                && BuiltinFunctions.FIELD_ACCESS_BY_NAME
                        .equals(((AbstractFunctionCallExpression) expression).getFunctionIdentifier());
    }

    private static void buildFieldNames(ILogicalExpression expr, List<String> fieldNames) throws CompilationException {
        if (!isFieldAccessByName(expr)) {
            /*
             * We only push nested field-access expressions.
             * This is a sanity check if the previous checks have missed.
             */
            return;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        ILogicalExpression objectExpr = funcExpr.getArguments().get(0).getValue();
        if (!isPayload(objectExpr)) {
            buildFieldNames(objectExpr, fieldNames);
        }
        fieldNames.add(ConstantExpressionUtil.getStringArgument(funcExpr, 1));
    }

    private static boolean isPayload(ILogicalExpression expr) {
        return expr.getExpressionTag() == LogicalExpressionTag.VARIABLE;
    }
}
