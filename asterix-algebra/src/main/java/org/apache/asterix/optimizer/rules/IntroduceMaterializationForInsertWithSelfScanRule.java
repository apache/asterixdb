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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import org.apache.asterix.metadata.declared.AqlDataSource;
import org.apache.asterix.metadata.declared.AqlDataSource.AqlDataSourceType;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.MaterializePOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

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
        boolean sameDataset = checkIfInsertAndScanDatasetsSame(op, ((DatasetDataSource) insertOp.getDataSource())
                .getDataset().getDatasetName());

        if (sameDataset) {
            MaterializeOperator materializeOperator = new MaterializeOperator();
            MaterializePOperator materializePOperator = new MaterializePOperator(true);
            materializeOperator.setPhysicalOperator(materializePOperator);

            materializeOperator.getInputs().add(
                    new MutableObject<ILogicalOperator>(insertOp.getInputs().get(0).getValue()));
            context.computeAndSetTypeEnvironmentForOperator(materializeOperator);

            insertOp.getInputs().clear();
            insertOp.getInputs().add(new MutableObject<ILogicalOperator>(materializeOperator));
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
                        && ds.getDatasourceType() != AqlDataSourceType.LOADABLE) {
                    if (((DatasetDataSource) ds).getDataset().getDatasetName().compareTo(insertDatasetName) == 0) {
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
