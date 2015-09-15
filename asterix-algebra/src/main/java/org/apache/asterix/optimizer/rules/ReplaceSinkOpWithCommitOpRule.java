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

import org.apache.commons.lang3.mutable.Mutable;

import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.algebra.operators.physical.CommitPOperator;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ReplaceSinkOpWithCommitOpRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        // TODO Auto-generated method stub
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
        while (descendantOp != null) {
            if (descendantOp.getOperatorTag() == LogicalOperatorTag.INDEX_INSERT_DELETE) {
                IndexInsertDeleteOperator indexInsertDeleteOperator = (IndexInsertDeleteOperator) descendantOp;
                if (!indexInsertDeleteOperator.isBulkload()) {
                    primaryKeyExprs = indexInsertDeleteOperator.getPrimaryKeyExpressions();
                    datasetId = ((DatasetDataSource) indexInsertDeleteOperator.getDataSourceIndex().getDataSource())
                            .getDataset().getDatasetId();
                    break;
                }
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.INSERT_DELETE) {
                InsertDeleteOperator insertDeleteOperator = (InsertDeleteOperator) descendantOp;
                if (!insertDeleteOperator.isBulkload()) {
                    primaryKeyExprs = insertDeleteOperator.getPrimaryKeyExpressions();
                    datasetId = ((DatasetDataSource) insertDeleteOperator.getDataSource()).getDataset().getDatasetId();
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
        CommitOperator commitOperator = new CommitOperator(primaryKeyLogicalVars);
        CommitPOperator commitPOperator = new CommitPOperator(jobId, datasetId, primaryKeyLogicalVars);
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
