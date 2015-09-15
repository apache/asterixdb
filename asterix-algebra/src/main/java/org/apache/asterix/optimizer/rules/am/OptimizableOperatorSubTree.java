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
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;

/**
 * Operator subtree that matches the following patterns, and provides convenient access to its nodes:
 * (select)? <-- (assign | unnest)* <-- (datasource scan | unnest-map)
 */
public class OptimizableOperatorSubTree {

    public static enum DataSourceType {
        DATASOURCE_SCAN,
        EXTERNAL_SCAN,
        PRIMARY_INDEX_LOOKUP,
        COLLECTION_SCAN,
        NO_DATASOURCE
    }

    public ILogicalOperator root = null;
    public Mutable<ILogicalOperator> rootRef = null;
    public final List<Mutable<ILogicalOperator>> assignsAndUnnestsRefs = new ArrayList<Mutable<ILogicalOperator>>();
    public final List<AbstractLogicalOperator> assignsAndUnnests = new ArrayList<AbstractLogicalOperator>();
    public Mutable<ILogicalOperator> dataSourceRef = null;
    public DataSourceType dataSourceType = DataSourceType.NO_DATASOURCE;
    // Dataset and type metadata. Set in setDatasetAndTypeMetadata().
    public Dataset dataset = null;
    public ARecordType recordType = null;

    public boolean initFromSubTree(Mutable<ILogicalOperator> subTreeOpRef) {
        reset();
        rootRef = subTreeOpRef;
        root = subTreeOpRef.getValue();
        // Examine the op's children to match the expected patterns.
        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
        do {
            // Skip select operator.
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            }
            // Check primary-index pattern.
            if (subTreeOp.getOperatorTag() != LogicalOperatorTag.ASSIGN
                    && subTreeOp.getOperatorTag() != LogicalOperatorTag.UNNEST) {
                // Pattern may still match if we are looking for primary index matches as well.
                return initializeDataSource(subTreeOpRef);
            }
            // Match (assign | unnest)+.
            while (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN
                    || subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                assignsAndUnnestsRefs.add(subTreeOpRef);
                assignsAndUnnests.add(subTreeOp);

                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            };
        } while (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT);

        // Match data source (datasource scan or primary index search).
        return initializeDataSource(subTreeOpRef);
    }

    private boolean initializeDataSource(Mutable<ILogicalOperator> subTreeOpRef) {
        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
        if (subTreeOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            dataSourceType = DataSourceType.DATASOURCE_SCAN;
            dataSourceRef = subTreeOpRef;
            return true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EXTERNAL_LOOKUP) {
            dataSourceType = DataSourceType.EXTERNAL_SCAN;
            dataSourceRef = subTreeOpRef;
            return true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            dataSourceType = DataSourceType.COLLECTION_SCAN;
            dataSourceRef = subTreeOpRef;
            return true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            UnnestMapOperator unnestMapOp = (UnnestMapOperator) subTreeOp;
            ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
            if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                if (f.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
                    AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                    jobGenParams.readFromFuncArgs(f.getArguments());
                    if (jobGenParams.isPrimaryIndex()) {
                        dataSourceType = DataSourceType.PRIMARY_INDEX_LOOKUP;
                        dataSourceRef = subTreeOpRef;
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Find the dataset corresponding to the datasource scan in the metadata.
     * Also sets recordType to be the type of that dataset.
     */
    public boolean setDatasetAndTypeMetadata(AqlMetadataProvider metadataProvider) throws AlgebricksException {
        String dataverseName = null;
        String datasetName = null;
        switch (dataSourceType) {
            case DATASOURCE_SCAN:
                DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) dataSourceRef.getValue();
                Pair<String, String> datasetInfo = AnalysisUtil.getDatasetInfo(dataSourceScan);
                dataverseName = datasetInfo.first;
                datasetName = datasetInfo.second;
                break;
            case PRIMARY_INDEX_LOOKUP:
                AbstractUnnestOperator unnestMapOp = (AbstractUnnestOperator) dataSourceRef.getValue();
                ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                jobGenParams.readFromFuncArgs(f.getArguments());
                datasetName = jobGenParams.getDatasetName();
                dataverseName = jobGenParams.getDataverseName();
                break;
            case EXTERNAL_SCAN:
                ExternalDataLookupOperator externalScan = (ExternalDataLookupOperator) dataSourceRef.getValue();
                datasetInfo = AnalysisUtil.getDatasetInfo(externalScan);
                dataverseName = datasetInfo.first;
                datasetName = datasetInfo.second;
                break;
            case COLLECTION_SCAN:
                return true;
            case NO_DATASOURCE:
            default:
                return false;
        }
        if (dataverseName == null || datasetName == null) {
            return false;
        }
        // Find the dataset corresponding to the datasource in the metadata.
        dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("No metadata for dataset " + datasetName);
        }
        // Get the record type for that dataset.
        IAType itemType = metadataProvider.findType(dataverseName, dataset.getItemTypeName());
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            return false;
        }
        recordType = (ARecordType) itemType;
        return true;
    }

    public boolean hasDataSource() {
        return dataSourceType != DataSourceType.NO_DATASOURCE;
    }

    public boolean hasDataSourceScan() {
        return dataSourceType == DataSourceType.DATASOURCE_SCAN;
    }

    public void reset() {
        root = null;
        rootRef = null;
        assignsAndUnnestsRefs.clear();
        assignsAndUnnests.clear();
        dataSourceRef = null;
        dataSourceType = DataSourceType.NO_DATASOURCE;
        dataset = null;
        recordType = null;
    }

    public void getPrimaryKeyVars(List<LogicalVariable> target) throws AlgebricksException {
        switch (dataSourceType) {
            case DATASOURCE_SCAN:
                DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) dataSourceRef.getValue();
                int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
                for (int i = 0; i < numPrimaryKeys; i++) {
                    target.add(dataSourceScan.getVariables().get(i));
                }
                break;
            case PRIMARY_INDEX_LOOKUP:
                UnnestMapOperator unnestMapOp = (UnnestMapOperator) dataSourceRef.getValue();
                List<LogicalVariable> primaryKeys = null;
                primaryKeys = AccessMethodUtils.getPrimaryKeyVarsFromPrimaryUnnestMap(dataset, unnestMapOp);
                target.addAll(primaryKeys);
                break;
            case NO_DATASOURCE:
            default:
                throw new AlgebricksException("The subtree does not have any data source.");
        }
    }

    public List<LogicalVariable> getDataSourceVariables() throws AlgebricksException {
        switch (dataSourceType) {
            case DATASOURCE_SCAN:
            case EXTERNAL_SCAN:
            case PRIMARY_INDEX_LOOKUP:
                AbstractScanOperator scanOp = (AbstractScanOperator) dataSourceRef.getValue();
                return scanOp.getVariables();
            case COLLECTION_SCAN:
                return new ArrayList<LogicalVariable>();
            case NO_DATASOURCE:
            default:
                throw new AlgebricksException("The subtree does not have any data source.");
        }
    }
}
