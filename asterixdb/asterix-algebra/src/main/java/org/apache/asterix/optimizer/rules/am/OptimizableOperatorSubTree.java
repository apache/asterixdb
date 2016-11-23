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

import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSource.Type;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * Operator subtree that matches the following patterns, and provides convenient access to its nodes:
 * (select)? <-- (assign | unnest)* <-- (datasource scan | unnest-map)*
 */
public class OptimizableOperatorSubTree {

    public static enum DataSourceType {
        DATASOURCE_SCAN,
        EXTERNAL_SCAN,
        PRIMARY_INDEX_LOOKUP,
        COLLECTION_SCAN,
        NO_DATASOURCE
    }

    private ILogicalOperator root = null;
    private Mutable<ILogicalOperator> rootRef = null;
    private final List<Mutable<ILogicalOperator>> assignsAndUnnestsRefs = new ArrayList<>();
    private final List<AbstractLogicalOperator> assignsAndUnnests = new ArrayList<>();
    private Mutable<ILogicalOperator> dataSourceRef = null;
    private DataSourceType dataSourceType = DataSourceType.NO_DATASOURCE;

    // Dataset and type metadata. Set in setDatasetAndTypeMetadata().
    private Dataset dataset = null;
    private ARecordType recordType = null;
    private ARecordType metaRecordType = null;

    // Additional datasources can exist if IntroduceJoinAccessMethodRule has been applied.
    // (E.g. There are index-nested-loop-joins in the plan.)
    private List<Mutable<ILogicalOperator>> ixJoinOuterAdditionalDataSourceRefs = null;
    private List<DataSourceType> ixJoinOuterAdditionalDataSourceTypes = null;
    private List<Dataset> ixJoinOuterAdditionalDatasets = null;
    private List<ARecordType> ixJoinOuterAdditionalRecordTypes = null;

    public boolean initFromSubTree(Mutable<ILogicalOperator> subTreeOpRef) throws AlgebricksException {
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
            while ((subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN
                    || subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST)) {
                if (!OperatorPropertiesUtil.isMovable(subTreeOp)) {
                    return false;
                } else {
                    getAssignsAndUnnestsRefs().add(subTreeOpRef);
                    getAssignsAndUnnests().add(subTreeOp);
                }
                subTreeOpRef = subTreeOp.getInputs().get(0);
                subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
            }
        } while (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT);

        // Match data source (datasource scan or primary index search).
        return initializeDataSource(subTreeOpRef);
    }

    private boolean initializeDataSource(Mutable<ILogicalOperator> subTreeOpRef) {
        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();

        if (subTreeOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            setDataSourceType(DataSourceType.DATASOURCE_SCAN);
            setDataSourceRef(subTreeOpRef);
            return true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            setDataSourceType(DataSourceType.COLLECTION_SCAN);
            setDataSourceRef(subTreeOpRef);
            return true;
        } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            // There can be multiple unnest-map or datasource-scan operators
            // if index-nested-loop-join has been applied by IntroduceJoinAccessMethodRule.
            // So, we need to traverse the whole path from the subTreeOp.
            boolean dataSourceFound = false;
            while (true) {
                if (subTreeOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                    UnnestMapOperator unnestMapOp = (UnnestMapOperator) subTreeOp;
                    ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();

                    if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                        if (f.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
                            AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                            jobGenParams.readFromFuncArgs(f.getArguments());
                            if (jobGenParams.isPrimaryIndex()) {
                                if (getDataSourceRef() == null) {
                                    setDataSourceRef(subTreeOpRef);
                                    setDataSourceType(DataSourceType.PRIMARY_INDEX_LOOKUP);
                                } else {
                                    // One datasource already exists. This is an additional datasource.
                                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                                    getIxJoinOuterAdditionalDataSourceTypes().add(DataSourceType.PRIMARY_INDEX_LOOKUP);
                                    getIxJoinOuterAdditionalDataSourceRefs().add(subTreeOpRef);
                                }
                                dataSourceFound = true;
                            }
                        } else if (f.getFunctionIdentifier().equals(AsterixBuiltinFunctions.EXTERNAL_LOOKUP)) {
                            // External lookup case
                            if (getDataSourceRef() == null) {
                                setDataSourceRef(subTreeOpRef);
                                setDataSourceType(DataSourceType.EXTERNAL_SCAN);
                            } else {
                                // One datasource already exists. This is an additional datasource.
                                initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                                getIxJoinOuterAdditionalDataSourceTypes().add(DataSourceType.EXTERNAL_SCAN);
                                getIxJoinOuterAdditionalDataSourceRefs().add(subTreeOpRef);
                            }
                            dataSourceFound = true;
                        }
                    }
                } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                    getIxJoinOuterAdditionalDataSourceTypes().add(DataSourceType.DATASOURCE_SCAN);
                    getIxJoinOuterAdditionalDataSourceRefs().add(subTreeOpRef);
                    dataSourceFound = true;
                } else if (subTreeOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                    initializeIxJoinOuterAddtionalDataSourcesIfEmpty();
                    getIxJoinOuterAdditionalDataSourceTypes().add(DataSourceType.COLLECTION_SCAN);
                    getIxJoinOuterAdditionalDataSourceRefs().add(subTreeOpRef);
                }

                // Traverse the subtree while there are operators in the path.
                if (subTreeOp.hasInputs()) {
                    subTreeOpRef = subTreeOp.getInputs().get(0);
                    subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
                } else {
                    break;
                }
            }

            if (dataSourceFound) {
                return true;
            }
        }

        return false;
    }

    /**
     * Find the dataset corresponding to the datasource scan in the metadata.
     * Also sets recordType to be the type of that dataset.
     */
    public boolean setDatasetAndTypeMetadata(MetadataProvider metadataProvider) throws AlgebricksException {
        String dataverseName = null;
        String datasetName = null;

        Dataset ds = null;
        ARecordType rType = null;

        List<Mutable<ILogicalOperator>> sourceOpRefs = new ArrayList<Mutable<ILogicalOperator>>();
        List<DataSourceType> dsTypes = new ArrayList<DataSourceType>();

        sourceOpRefs.add(getDataSourceRef());
        dsTypes.add(getDataSourceType());

        // If there are multiple datasources in the subtree, we need to find the dataset for these.
        if (getIxJoinOuterAdditionalDataSourceRefs() != null) {
            for (int i = 0; i < getIxJoinOuterAdditionalDataSourceRefs().size(); i++) {
                sourceOpRefs.add(getIxJoinOuterAdditionalDataSourceRefs().get(i));
                dsTypes.add(getIxJoinOuterAdditionalDataSourceTypes().get(i));
            }
        }

        for (int i = 0; i < sourceOpRefs.size(); i++) {
            switch (dsTypes.get(i)) {
                case DATASOURCE_SCAN:
                    DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) sourceOpRefs.get(i).getValue();
                    IDataSource<?> datasource = dataSourceScan.getDataSource();
                    if (datasource instanceof DataSource) {
                        byte dsType = ((DataSource) datasource).getDatasourceType();
                        if (dsType != DataSource.Type.INTERNAL_DATASET
                                && dsType != DataSource.Type.EXTERNAL_DATASET) {
                            return false;
                        }
                    }
                    Pair<String, String> datasetInfo = AnalysisUtil.getDatasetInfo(dataSourceScan);
                    dataverseName = datasetInfo.first;
                    datasetName = datasetInfo.second;
                    break;
                case PRIMARY_INDEX_LOOKUP:
                    AbstractUnnestOperator unnestMapOp = (AbstractUnnestOperator) sourceOpRefs.get(i).getValue();
                    ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
                    AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                    AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                    jobGenParams.readFromFuncArgs(f.getArguments());
                    datasetName = jobGenParams.getDatasetName();
                    dataverseName = jobGenParams.getDataverseName();
                    break;
                case EXTERNAL_SCAN:
                    UnnestMapOperator externalScan = (UnnestMapOperator) sourceOpRefs.get(i).getValue();
                    datasetInfo = AnalysisUtil.getExternalDatasetInfo(externalScan);
                    dataverseName = datasetInfo.first;
                    datasetName = datasetInfo.second;
                    break;
                case COLLECTION_SCAN:
                    if (i != 0) {
                        getIxJoinOuterAdditionalDatasets().add(null);
                        getIxJoinOuterAdditionalRecordTypes().add(null);
                    }
                    continue;
                case NO_DATASOURCE:
                default:
                    return false;
            }
            if (dataverseName == null || datasetName == null) {
                return false;
            }
            // Find the dataset corresponding to the datasource in the metadata.
            ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException("No metadata for dataset " + datasetName);
            }
            // Get the record type for that dataset.
            IAType itemType = metadataProvider.findType(ds.getItemTypeDataverseName(), ds.getItemTypeName());
            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                if (i == 0) {
                    return false;
                } else {
                    getIxJoinOuterAdditionalDatasets().add(null);
                    getIxJoinOuterAdditionalRecordTypes().add(null);
                }
            }
            rType = (ARecordType) itemType;

            // Get the meta record type for that dataset.
            IAType metaItemType =
                    metadataProvider.findType(ds.getMetaItemTypeDataverseName(), ds.getMetaItemTypeName());

            // First index is always the primary datasource in this subtree.
            if (i == 0) {
                setDataset(ds);
                setRecordType(rType);
                setMetaRecordType((ARecordType) metaItemType);
            } else {
                getIxJoinOuterAdditionalDatasets().add(ds);
                getIxJoinOuterAdditionalRecordTypes().add(rType);
            }

            dataverseName = null;
            datasetName = null;
            ds = null;
            rType = null;
        }

        return true;
    }

    public boolean hasDataSource() {
        return getDataSourceType() != DataSourceType.NO_DATASOURCE;
    }

    public boolean hasIxJoinOuterAdditionalDataSource() {
        boolean dataSourceFound = false;
        if (getIxJoinOuterAdditionalDataSourceTypes() != null) {
            for (int i = 0; i < getIxJoinOuterAdditionalDataSourceTypes().size(); i++) {
                if (getIxJoinOuterAdditionalDataSourceTypes().get(i) != DataSourceType.NO_DATASOURCE) {
                    dataSourceFound = true;
                    break;
                }
            }
        }
        return dataSourceFound;
    }

    public boolean hasDataSourceScan() {
        return getDataSourceType() == DataSourceType.DATASOURCE_SCAN;
    }

    public boolean hasIxJoinOuterAdditionalDataSourceScan() {
        if (getIxJoinOuterAdditionalDataSourceTypes() != null) {
            for (int i = 0; i < getIxJoinOuterAdditionalDataSourceTypes().size(); i++) {
                if (getIxJoinOuterAdditionalDataSourceTypes().get(i) == DataSourceType.DATASOURCE_SCAN) {
                    return true;
                }
            }
        }
        return false;
    }

    public void reset() {
        setRoot(null);
        setRootRef(null);
        getAssignsAndUnnestsRefs().clear();
        getAssignsAndUnnests().clear();
        setDataSourceRef(null);
        setDataSourceType(DataSourceType.NO_DATASOURCE);
        setIxJoinOuterAdditionalDataSourceRefs(null);
        setIxJoinOuterAdditionalDataSourceTypes(null);
        setDataset(null);
        setIxJoinOuterAdditionalDatasets(null);
        setRecordType(null);
        setIxJoinOuterAdditionalRecordTypes(null);
    }

    public void getPrimaryKeyVars(List<LogicalVariable> target) throws AlgebricksException {
        switch (getDataSourceType()) {
            case DATASOURCE_SCAN:
                DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) getDataSourceRef().getValue();
                int numPrimaryKeys = DatasetUtils.getPartitioningKeys(getDataset()).size();
                for (int i = 0; i < numPrimaryKeys; i++) {
                    target.add(dataSourceScan.getVariables().get(i));
                }
                break;
            case PRIMARY_INDEX_LOOKUP:
                UnnestMapOperator unnestMapOp = (UnnestMapOperator) getDataSourceRef().getValue();
                List<LogicalVariable> primaryKeys = null;
                primaryKeys = AccessMethodUtils.getPrimaryKeyVarsFromPrimaryUnnestMap(getDataset(), unnestMapOp);
                target.addAll(primaryKeys);
                break;
            case NO_DATASOURCE:
            default:
                throw new AlgebricksException("The subtree does not have any data source.");
        }
    }

    public List<LogicalVariable> getDataSourceVariables() throws AlgebricksException {
        switch (getDataSourceType()) {
            case DATASOURCE_SCAN:
            case EXTERNAL_SCAN:
            case PRIMARY_INDEX_LOOKUP:
                AbstractScanOperator scanOp = (AbstractScanOperator) getDataSourceRef().getValue();
                return scanOp.getVariables();
            case COLLECTION_SCAN:
                return new ArrayList<>();
            case NO_DATASOURCE:
            default:
                throw new AlgebricksException("The subtree does not have any data source.");
        }
    }

    public List<LogicalVariable> getIxJoinOuterAdditionalDataSourceVariables(int idx) throws AlgebricksException {
        if (getIxJoinOuterAdditionalDataSourceRefs() != null && getIxJoinOuterAdditionalDataSourceRefs().size() > idx) {
            switch (getIxJoinOuterAdditionalDataSourceTypes().get(idx)) {
                case DATASOURCE_SCAN:
                case EXTERNAL_SCAN:
                case PRIMARY_INDEX_LOOKUP:
                    AbstractScanOperator scanOp =
                            (AbstractScanOperator) getIxJoinOuterAdditionalDataSourceRefs().get(idx).getValue();
                    return scanOp.getVariables();
                case COLLECTION_SCAN:
                    return new ArrayList<>();
                case NO_DATASOURCE:
                default:
                    throw new AlgebricksException("The subtree does not have any additional data sources.");
            }
        } else {
            return null;
        }
    }

    public void initializeIxJoinOuterAddtionalDataSourcesIfEmpty() {
        if (getIxJoinOuterAdditionalDataSourceRefs() == null) {
            setIxJoinOuterAdditionalDataSourceRefs(new ArrayList<Mutable<ILogicalOperator>>());
            setIxJoinOuterAdditionalDataSourceTypes(new ArrayList<DataSourceType>());
            setIxJoinOuterAdditionalDatasets(new ArrayList<Dataset>());
            setIxJoinOuterAdditionalRecordTypes(new ArrayList<ARecordType>());
        }
    }

    public ILogicalOperator getRoot() {
        return root;
    }

    public void setRoot(ILogicalOperator root) {
        this.root = root;
    }

    public Mutable<ILogicalOperator> getRootRef() {
        return rootRef;
    }

    public void setRootRef(Mutable<ILogicalOperator> rootRef) {
        this.rootRef = rootRef;
    }

    public List<Mutable<ILogicalOperator>> getAssignsAndUnnestsRefs() {
        return assignsAndUnnestsRefs;
    }

    public List<AbstractLogicalOperator> getAssignsAndUnnests() {
        return assignsAndUnnests;
    }

    public Mutable<ILogicalOperator> getDataSourceRef() {
        return dataSourceRef;
    }

    public void setDataSourceRef(Mutable<ILogicalOperator> dataSourceRef) {
        this.dataSourceRef = dataSourceRef;
    }

    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    public ARecordType getMetaRecordType() {
        return metaRecordType;
    }

    public void setMetaRecordType(ARecordType metaRecordType) {
        this.metaRecordType = metaRecordType;
    }

    public List<Mutable<ILogicalOperator>> getIxJoinOuterAdditionalDataSourceRefs() {
        return ixJoinOuterAdditionalDataSourceRefs;
    }

    public void setIxJoinOuterAdditionalDataSourceRefs(
            List<Mutable<ILogicalOperator>> ixJoinOuterAdditionalDataSourceRefs) {
        this.ixJoinOuterAdditionalDataSourceRefs = ixJoinOuterAdditionalDataSourceRefs;
    }

    public List<DataSourceType> getIxJoinOuterAdditionalDataSourceTypes() {
        return ixJoinOuterAdditionalDataSourceTypes;
    }

    public void setIxJoinOuterAdditionalDataSourceTypes(List<DataSourceType> ixJoinOuterAdditionalDataSourceTypes) {
        this.ixJoinOuterAdditionalDataSourceTypes = ixJoinOuterAdditionalDataSourceTypes;
    }

    public List<Dataset> getIxJoinOuterAdditionalDatasets() {
        return ixJoinOuterAdditionalDatasets;
    }

    public void setIxJoinOuterAdditionalDatasets(List<Dataset> ixJoinOuterAdditionalDatasets) {
        this.ixJoinOuterAdditionalDatasets = ixJoinOuterAdditionalDatasets;
    }

    public List<ARecordType> getIxJoinOuterAdditionalRecordTypes() {
        return ixJoinOuterAdditionalRecordTypes;
    }

    public void setIxJoinOuterAdditionalRecordTypes(List<ARecordType> ixJoinOuterAdditionalRecordTypes) {
        this.ixJoinOuterAdditionalRecordTypes = ixJoinOuterAdditionalRecordTypes;
    }

}
