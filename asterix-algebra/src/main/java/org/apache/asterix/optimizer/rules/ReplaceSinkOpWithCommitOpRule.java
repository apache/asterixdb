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
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.DatasetDataSource;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ReplaceSinkOpWithCommitOpRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SINK) {
            return false;
        }
        SinkOperator sinkOperator = (SinkOperator) op;

        List<Mutable<ILogicalExpression>> primaryKeyExprs = null;
        int datasetId = 0;
        AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) sinkOperator.getInputs().get(0).getValue();
        LogicalVariable upsertVar = null;
        AssignOperator upsertFlagAssign = null;
        while (descendantOp != null) {
            if (descendantOp.getOperatorTag() == LogicalOperatorTag.INDEX_INSERT_DELETE_UPSERT) {
                IndexInsertDeleteUpsertOperator indexInsertDeleteUpsertOperator = (IndexInsertDeleteUpsertOperator) descendantOp;
                if (!indexInsertDeleteUpsertOperator.isBulkload()
                        && indexInsertDeleteUpsertOperator.getPrevSecondaryKeyExprs() == null) {
                    primaryKeyExprs = indexInsertDeleteUpsertOperator.getPrimaryKeyExpressions();
                    datasetId = ((DatasetDataSource) indexInsertDeleteUpsertOperator.getDataSourceIndex()
                            .getDataSource()).getDataset().getDatasetId();
                    break;
                }
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.INSERT_DELETE_UPSERT) {
                InsertDeleteUpsertOperator insertDeleteUpsertOperator = (InsertDeleteUpsertOperator) descendantOp;
                if (!insertDeleteUpsertOperator.isBulkload()) {
                    primaryKeyExprs = insertDeleteUpsertOperator.getPrimaryKeyExpressions();
                    datasetId = ((DatasetDataSource) insertDeleteUpsertOperator.getDataSource()).getDataset()
                            .getDatasetId();
                    if (insertDeleteUpsertOperator.getOperation() == Kind.UPSERT) {
                        //we need to add a function that checks if previous record was found
                        upsertVar = context.newVar();
                        //introduce casting to enforced type
                        AbstractFunctionCallExpression isNullFunc = new ScalarFunctionCallExpression(
                                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.IS_NULL));
                        // The first argument is the record
                        isNullFunc.getArguments().add(new MutableObject<ILogicalExpression>(
                                new VariableReferenceExpression(insertDeleteUpsertOperator.getPrevRecordVar())));
                        // AssignOperator puts in the cast var the casted record
                        upsertFlagAssign = new AssignOperator(upsertVar,
                                new MutableObject<ILogicalExpression>(isNullFunc));
                        // Connect the current top of the plan to the cast operator
                        upsertFlagAssign.getInputs()
                                .add(new MutableObject<ILogicalOperator>(sinkOperator.getInputs().get(0).getValue()));
                        sinkOperator.getInputs().clear();
                        sinkOperator.getInputs().add(new MutableObject<ILogicalOperator>(upsertFlagAssign));
                        context.computeAndSetTypeEnvironmentForOperator(upsertFlagAssign);
                    }
                    break;
                }
            }
            if (descendantOp.getInputs().size() < 1) {
                break;
            }
            descendantOp = (AbstractLogicalOperator) descendantOp.getInputs().get(0).getValue();
        }

        if (primaryKeyExprs == null) {
            return false;
        }

        //copy primaryKeyExprs
        List<LogicalVariable> primaryKeyLogicalVars = new ArrayList<LogicalVariable>();
        for (Mutable<ILogicalExpression> expr : primaryKeyExprs) {
            VariableReferenceExpression varRefExpr = (VariableReferenceExpression) expr.getValue();
            primaryKeyLogicalVars.add(new LogicalVariable(varRefExpr.getVariableReference().getId()));
        }

        //get JobId(TransactorId)
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        JobId jobId = mp.getJobId();

        //create the logical and physical operator
        CommitOperator commitOperator = new CommitOperator(primaryKeyLogicalVars, upsertVar);
        CommitPOperator commitPOperator = new CommitPOperator(jobId, datasetId, primaryKeyLogicalVars, upsertVar);
        commitOperator.setPhysicalOperator(commitPOperator);

        //create ExtensionOperator and put the commitOperator in it.
        ExtensionOperator extensionOperator = new ExtensionOperator(commitOperator);
        extensionOperator.setPhysicalOperator(commitPOperator);

        //update plan link
        extensionOperator.getInputs().add(sinkOperator.getInputs().get(0));
        context.computeAndSetTypeEnvironmentForOperator(extensionOperator);
        opRef.setValue(extensionOperator);
        return true;
    }

}
