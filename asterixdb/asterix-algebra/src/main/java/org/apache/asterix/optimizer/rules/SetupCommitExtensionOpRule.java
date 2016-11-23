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
import java.util.List;

import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.algebra.operators.physical.CommitPOperator;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class SetupCommitExtensionOpRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.DELEGATE_OPERATOR) {
            return false;
        }
        DelegateOperator eOp = (DelegateOperator) op;
        if (!(eOp.getDelegate() instanceof CommitOperator)) {
            return false;
        }
        boolean isSink = ((CommitOperator) eOp.getDelegate()).isSink();

        List<Mutable<ILogicalExpression>> primaryKeyExprs = null;
        int datasetId = 0;
        String dataverse = null;
        String datasetName = null;
        AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) eOp.getInputs().get(0).getValue();
        LogicalVariable upsertVar = null;
        while (descendantOp != null) {
            if (descendantOp.getOperatorTag() == LogicalOperatorTag.INDEX_INSERT_DELETE_UPSERT) {
                IndexInsertDeleteUpsertOperator operator = (IndexInsertDeleteUpsertOperator) descendantOp;
                if (!operator.isBulkload() && operator.getPrevSecondaryKeyExprs() == null) {
                    primaryKeyExprs = operator.getPrimaryKeyExpressions();
                    datasetId = ((DatasetDataSource) operator.getDataSourceIndex().getDataSource()).getDataset()
                            .getDatasetId();
                    dataverse = ((DatasetDataSource) operator.getDataSourceIndex().getDataSource()).getDataset()
                            .getDataverseName();
                    datasetName = ((DatasetDataSource) operator.getDataSourceIndex().getDataSource()).getDataset()
                            .getDatasetName();
                    break;
                }
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.INSERT_DELETE_UPSERT) {
                InsertDeleteUpsertOperator insertDeleteUpsertOperator = (InsertDeleteUpsertOperator) descendantOp;
                if (!insertDeleteUpsertOperator.isBulkload()) {
                    primaryKeyExprs = insertDeleteUpsertOperator.getPrimaryKeyExpressions();
                    datasetId = ((DatasetDataSource) insertDeleteUpsertOperator.getDataSource()).getDataset()
                            .getDatasetId();
                    dataverse = ((DatasetDataSource) insertDeleteUpsertOperator.getDataSource()).getDataset()
                            .getDataverseName();
                    datasetName = ((DatasetDataSource) insertDeleteUpsertOperator.getDataSource()).getDataset()
                            .getDatasetName();
                    if (insertDeleteUpsertOperator.getOperation() == Kind.UPSERT) {
                        //we need to add a function that checks if previous record was found
                        upsertVar = context.newVar();
                        AbstractFunctionCallExpression orFunc = new ScalarFunctionCallExpression(
                                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.OR));
                        // is new value missing? -> this means that the expected operation is delete
                        AbstractFunctionCallExpression isNewMissingFunc = new ScalarFunctionCallExpression(
                                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.IS_MISSING));
                        isNewMissingFunc.getArguments().add(insertDeleteUpsertOperator.getPayloadExpression());
                        AbstractFunctionCallExpression isPrevMissingFunc = new ScalarFunctionCallExpression(
                                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.IS_MISSING));
                        // argument is the previous record
                        isPrevMissingFunc.getArguments().add(new MutableObject<ILogicalExpression>(
                                new VariableReferenceExpression(insertDeleteUpsertOperator.getBeforeOpRecordVar())));
                        orFunc.getArguments().add(new MutableObject<ILogicalExpression>(isPrevMissingFunc));
                        orFunc.getArguments().add(new MutableObject<ILogicalExpression>(isNewMissingFunc));

                        // AssignOperator puts in the cast var the casted record
                        AssignOperator upsertFlagAssign = new AssignOperator(upsertVar,
                                new MutableObject<ILogicalExpression>(orFunc));
                        // Connect the current top of the plan to the cast operator
                        upsertFlagAssign.getInputs()
                                .add(new MutableObject<ILogicalOperator>(eOp.getInputs().get(0).getValue()));
                        eOp.getInputs().clear();
                        eOp.getInputs().add(new MutableObject<ILogicalOperator>(upsertFlagAssign));
                        context.computeAndSetTypeEnvironmentForOperator(upsertFlagAssign);
                    }
                    break;
                }
            }
            if (descendantOp.getInputs().isEmpty()) {
                break;
            }
            descendantOp = (AbstractLogicalOperator) descendantOp.getInputs().get(0).getValue();
        }

        if (primaryKeyExprs == null) {
            return false;
        }

        //copy primaryKeyExprs
        List<LogicalVariable> primaryKeyLogicalVars = new ArrayList<>();
        for (Mutable<ILogicalExpression> expr : primaryKeyExprs) {
            VariableReferenceExpression varRefExpr = (VariableReferenceExpression) expr.getValue();
            primaryKeyLogicalVars.add(new LogicalVariable(varRefExpr.getVariableReference().getId()));
        }

        //get JobId(TransactorId)
        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
        JobId jobId = mp.getJobId();

        //create the logical and physical operator
        CommitOperator commitOperator = new CommitOperator(primaryKeyLogicalVars, upsertVar, isSink);
        CommitPOperator commitPOperator = new CommitPOperator(jobId, dataverse, datasetName, datasetId,
                primaryKeyLogicalVars, upsertVar, isSink);
        commitOperator.setPhysicalOperator(commitPOperator);

        //create ExtensionOperator and put the commitOperator in it.
        DelegateOperator extensionOperator = new DelegateOperator(commitOperator);
        extensionOperator.setPhysicalOperator(commitPOperator);

        //update plan link
        extensionOperator.getInputs().add(eOp.getInputs().get(0));
        context.computeAndSetTypeEnvironmentForOperator(extensionOperator);
        opRef.setValue(extensionOperator);
        return true;
    }
}
