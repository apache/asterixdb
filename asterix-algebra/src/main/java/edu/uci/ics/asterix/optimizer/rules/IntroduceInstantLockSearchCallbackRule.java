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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.algebra.operators.CommitOperator;
import edu.uci.ics.asterix.algebra.operators.physical.BTreeSearchPOperator;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataImplConfig;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.DataSourceScanPOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceInstantLockSearchCallbackRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    private void extractDataSourcesInfo(AbstractLogicalOperator op,
            Map<String, Triple<Integer, LogicalOperatorTag, IPhysicalOperator>> dataSourcesMap) {

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
                    if (isPrimaryIndex) {
                        if (dataSourcesMap.containsKey(indexName)) {
                            ++(dataSourcesMap.get(indexName).first);
                        } else {
                            dataSourcesMap.put(indexName, new Triple<Integer, LogicalOperatorTag, IPhysicalOperator>(1,
                                    LogicalOperatorTag.UNNEST_MAP, unnestMapOp.getPhysicalOperator()));
                        }
                    }
                }
            } else if (descendantOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                DataSourceScanOperator dataSourceScanOp = (DataSourceScanOperator) descendantOp;
                String datasetName = ((AqlDataSource) dataSourceScanOp.getDataSource()).getDataset().getDatasetName();
                if (dataSourcesMap.containsKey(datasetName)) {
                    ++(dataSourcesMap.get(datasetName).first);
                } else {
                    dataSourcesMap.put(datasetName, new Triple<Integer, LogicalOperatorTag, IPhysicalOperator>(1,
                            LogicalOperatorTag.DATASOURCESCAN, dataSourceScanOp.getPhysicalOperator()));
                }
            }
            extractDataSourcesInfo(descendantOp, dataSourcesMap);
        }

    }

    private boolean checkIfRuleIsApplicable(AbstractLogicalOperator op) {
        if (op.getPhysicalOperator() == null) {
            return false;
        }
        if (op.getOperatorTag() == LogicalOperatorTag.EXTENSION_OPERATOR) {
            ExtensionOperator extensionOp = (ExtensionOperator) op;
            if (extensionOp.getDelegate() instanceof CommitOperator) {
                return true;
            }
        }
        if (op.getOperatorTag() == LogicalOperatorTag.DISTRIBUTE_RESULT
                || op.getOperatorTag() == LogicalOperatorTag.WRITE_RESULT) {
            return true;
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        if (!checkIfRuleIsApplicable(op)) {
            return false;
        }
        Map<String, Triple<Integer, LogicalOperatorTag, IPhysicalOperator>> dataSourcesMap = new HashMap<String, Triple<Integer, LogicalOperatorTag, IPhysicalOperator>>();
        extractDataSourcesInfo(op, dataSourcesMap);

        boolean introducedInstantLock = false;

        Iterator<Map.Entry<String, Triple<Integer, LogicalOperatorTag, IPhysicalOperator>>> it = dataSourcesMap
                .entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Triple<Integer, LogicalOperatorTag, IPhysicalOperator>> entry = it.next();
            Triple<Integer, LogicalOperatorTag, IPhysicalOperator> triple = entry.getValue();
            if (triple.first == 1) {
                AqlMetadataImplConfig aqlMetadataImplConfig = new AqlMetadataImplConfig(true);
                if (triple.second == LogicalOperatorTag.UNNEST_MAP) {
                    BTreeSearchPOperator pOperator = (BTreeSearchPOperator) triple.third;
                    pOperator.setImplConfig(aqlMetadataImplConfig);
                    introducedInstantLock = true;
                } else {
                    DataSourceScanPOperator pOperator = (DataSourceScanPOperator) triple.third;
                    pOperator.setImplConfig(aqlMetadataImplConfig);
                    introducedInstantLock = true;
                }
            }

        }
        return introducedInstantLock;
    }
}