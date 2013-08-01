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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.algebra.operators.MaterializeOperator;
import edu.uci.ics.asterix.algebra.operators.physical.MaterializePOperator;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource.AqlDataSourceType;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceMaterializationForInsertWithSelfScanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE) {
            return false;
        }

        InsertDeleteOperator insertOp = (InsertDeleteOperator) op;
        boolean sameDataset = checkIfInsertAndScanDatasetsSame(op, ((AqlDataSource) insertOp.getDataSource())
                .getDataset().getDatasetName());

        if (sameDataset) {
            MaterializeOperator materializeOperator = new MaterializeOperator();
            MaterializePOperator materializePOperator = new MaterializePOperator(true);
            materializeOperator.setPhysicalOperator(materializePOperator);

            ExtensionOperator extensionOperator = new ExtensionOperator(materializeOperator);
            extensionOperator.setPhysicalOperator(materializePOperator);

            extensionOperator.getInputs().add(
                    new MutableObject<ILogicalOperator>(insertOp.getInputs().get(0).getValue()));
            context.computeAndSetTypeEnvironmentForOperator(extensionOperator);

            insertOp.getInputs().clear();
            insertOp.getInputs().add(new MutableObject<ILogicalOperator>(extensionOperator));
            context.computeAndSetTypeEnvironmentForOperator(insertOp);
            return true;
        } else {
            return false;
        }

    }

    private boolean checkIfInsertAndScanDatasetsSame(AbstractLogicalOperator op, String insertDatasetName) {
        boolean sameDataset = false;
        for (int i = 0; i < op.getInputs().size(); ++i) {
            AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) op.getInputs().get(i).getValue();

            if (descendantOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
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
                    boolean isPrimaryIndex = jobGenParams.isPrimaryIndex();
                    String indexName = jobGenParams.getIndexName();
                    if (isPrimaryIndex && indexName.compareTo(insertDatasetName) == 0) {
                        return true;
                    }
                }
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                DataSourceScanOperator dataSourceScanOp = (DataSourceScanOperator) descendantOp;
                AqlDataSource ds = (AqlDataSource) dataSourceScanOp.getDataSource();
                if (ds.getDatasourceType() != AqlDataSourceType.FEED
                        && ds.getDatasourceType() != AqlDataSourceType.EXTERNAL_FEED) {
                    if (ds.getDataset().getDatasetName().compareTo(insertDatasetName) == 0) {
                        return true;
                    }
                }
            }
            sameDataset = checkIfInsertAndScanDatasetsSame(descendantOp, insertDatasetName);
            if (sameDataset) {
                break;
            }
        }
        return sameDataset;
    }
}
