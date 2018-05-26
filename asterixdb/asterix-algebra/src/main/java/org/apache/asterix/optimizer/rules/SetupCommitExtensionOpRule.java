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
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
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
        Dataset dataset = null;
        AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) eOp.getInputs().get(0).getValue();
        while (descendantOp != null) {
            if (descendantOp.getOperatorTag() == LogicalOperatorTag.INDEX_INSERT_DELETE_UPSERT) {
                IndexInsertDeleteUpsertOperator operator = (IndexInsertDeleteUpsertOperator) descendantOp;
                if (!operator.isBulkload() && operator.getPrevSecondaryKeyExprs() == null) {
                    primaryKeyExprs = operator.getPrimaryKeyExpressions();
                    dataset = ((DatasetDataSource) operator.getDataSourceIndex().getDataSource()).getDataset();
                    break;
                }
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.INSERT_DELETE_UPSERT) {
                InsertDeleteUpsertOperator insertDeleteUpsertOperator = (InsertDeleteUpsertOperator) descendantOp;
                if (!insertDeleteUpsertOperator.isBulkload()) {
                    primaryKeyExprs = insertDeleteUpsertOperator.getPrimaryKeyExpressions();
                    dataset = ((DatasetDataSource) insertDeleteUpsertOperator.getDataSource()).getDataset();
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

        //create the logical and physical operator
        CommitOperator commitOperator = new CommitOperator(primaryKeyLogicalVars, isSink);
        CommitPOperator commitPOperator = new CommitPOperator(dataset, primaryKeyLogicalVars, isSink);
        commitOperator.setPhysicalOperator(commitPOperator);

        //create ExtensionOperator and put the commitOperator in it.
        DelegateOperator extensionOperator = new DelegateOperator(commitOperator);
        extensionOperator.setSourceLocation(eOp.getSourceLocation());
        extensionOperator.setPhysicalOperator(commitPOperator);

        //update plan link
        extensionOperator.getInputs().add(eOp.getInputs().get(0));
        context.computeAndSetTypeEnvironmentForOperator(extensionOperator);
        opRef.setValue(extensionOperator);
        return true;
    }
}
