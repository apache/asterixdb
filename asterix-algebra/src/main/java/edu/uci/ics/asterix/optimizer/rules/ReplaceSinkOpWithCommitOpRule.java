/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.algebra.operators.CommitOperator;
import edu.uci.ics.asterix.algebra.operators.physical.CommitPOperator;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

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
                primaryKeyExprs = indexInsertDeleteOperator.getPrimaryKeyExpressions();
                datasetId = ((AqlDataSource) indexInsertDeleteOperator.getDataSourceIndex().getDataSource()).getDataset().getDatasetId();
                break;
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.INSERT_DELETE) {
                InsertDeleteOperator insertDeleteOperator = (InsertDeleteOperator) descendantOp;
                primaryKeyExprs = insertDeleteOperator.getPrimaryKeyExpressions();
                datasetId = ((AqlDataSource) insertDeleteOperator.getDataSource()).getDataset().getDatasetId();
                break;
            }
            descendantOp = (AbstractLogicalOperator) descendantOp.getInputs().get(0).getValue();
        }

        if (primaryKeyExprs != null) {

            //copy primaryKeyExprs
            List<LogicalVariable> primaryKeyLogicalVars = new ArrayList<LogicalVariable>();
            for (Mutable<ILogicalExpression> expr : primaryKeyExprs) {
                VariableReferenceExpression varRefExpr = (VariableReferenceExpression)expr.getValue();
                primaryKeyLogicalVars.add(new LogicalVariable(varRefExpr.getVariableReference().getId()));
            }

            //get JobId(TransactorId)
            AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
            JobId jobId = mp.getJobId();

            //create the logical and physical operator
            CommitOperator commitOperator = new CommitOperator(primaryKeyLogicalVars);
            CommitPOperator commitPOperator = new CommitPOperator(jobId, datasetId, primaryKeyLogicalVars, mp.isWriteTransaction());
            commitOperator.setPhysicalOperator(commitPOperator);

            //create ExtensionOperator and put the commitOperator in it.
            ExtensionOperator extensionOperator = new ExtensionOperator(commitOperator);
            extensionOperator.setPhysicalOperator(commitPOperator);
            
            //update plan link
            extensionOperator.getInputs().add(sinkOperator.getInputs().get(0));
            context.computeAndSetTypeEnvironmentForOperator(extensionOperator);
            opRef.setValue(extensionOperator);
        }

        return true;
    }

}
