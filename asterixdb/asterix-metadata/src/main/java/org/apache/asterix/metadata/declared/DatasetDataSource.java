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
package org.apache.asterix.metadata.declared;

import static org.apache.asterix.external.util.ExternalDataConstants.KEY_EXTERNAL_SCAN_BUFFER_SIZE;
import static org.apache.asterix.external.util.ExternalDataConstants.SUBPATH;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.ITypedAdapterFactory;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.projection.ExternalDatasetProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.DefaultProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

public class DatasetDataSource extends DataSource {

    private final Dataset dataset;

    public DatasetDataSource(DataSourceId id, Dataset dataset, IAType itemType, IAType metaItemType,
            byte datasourceType, IDatasetDetails datasetDetails, INodeDomain datasetDomain) throws AlgebricksException {
        super(id, itemType, metaItemType, datasourceType, datasetDomain);
        this.dataset = dataset;
        switch (dataset.getDatasetType()) {
            case INTERNAL:
                initInternalDataset(itemType, metaItemType, datasetDetails);
                break;
            case EXTERNAL:
                initExternalDataset(itemType);
                break;
        }
    }

    public Dataset getDataset() {
        return dataset;
    }

    private void initInternalDataset(IAType itemType, IAType metaItemType, IDatasetDetails datasetDetails)
            throws AlgebricksException {
        schemaTypes =
                createSchemaTypesForInternalDataset(itemType, metaItemType, (InternalDatasetDetails) datasetDetails);
    }

    static IAType[] createSchemaTypesForInternalDataset(IAType itemType, IAType metaItemType,
            InternalDatasetDetails internalDatasetDetails) throws AlgebricksException {
        ARecordType recordType = (ARecordType) itemType;
        ARecordType metaRecordType = (ARecordType) metaItemType;
        List<IAType> partitioningKeyTypes =
                KeyFieldTypeUtil.getPartitioningKeyTypes(internalDatasetDetails, recordType, metaRecordType);
        int n = partitioningKeyTypes.size();
        IAType[] schemaTypes = metaItemType == null ? new IAType[n + 1] : new IAType[n + 2];
        for (int keyIndex = 0; keyIndex < n; ++keyIndex) {
            schemaTypes[keyIndex] = partitioningKeyTypes.get(keyIndex);
        }
        schemaTypes[n] = itemType;
        if (metaItemType != null) {
            schemaTypes[n + 1] = metaItemType;
        }
        return schemaTypes;
    }

    private void initExternalDataset(IAType itemType) {
        schemaTypes = createSchemaTypesForExternalDataset(itemType);
    }

    static IAType[] createSchemaTypesForExternalDataset(IAType itemType) {
        IAType[] schemaTypes = new IAType[1];
        schemaTypes[0] = itemType;
        return schemaTypes;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDatasourceScanRuntime(
            MetadataProvider metadataProvider, IDataSource<DataSourceId> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig,
            IProjectionFiltrationInfo projectionFiltrationInfo) throws AlgebricksException {
        String itemTypeName = dataset.getItemTypeName();
        String itemTypeDatabase = null;
        IAType itemType = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                itemTypeDatabase, dataset.getItemTypeDataverseName(), itemTypeName).getDatatype();
        switch (dataset.getDatasetType()) {
            case EXTERNAL:
                DatasetDataSource externalDataSource = (DatasetDataSource) dataSource;
                Dataset externalDataset = externalDataSource.getDataset();
                ExternalDatasetDetails edd = (ExternalDatasetDetails) externalDataset.getDatasetDetails();
                PhysicalOptimizationConfig physicalOptimizationConfig = context.getPhysicalOptimizationConfig();
                int externalScanBufferSize = physicalOptimizationConfig.getExternalScanBufferSize();
                Map<String, String> properties =
                        addExternalProjectionInfo(projectionFiltrationInfo, edd.getProperties());
                properties = addSubPath(externalDataSource.getProperties(), properties);
                properties.put(KEY_EXTERNAL_SCAN_BUFFER_SIZE, String.valueOf(externalScanBufferSize));
                IExternalFilterEvaluatorFactory filterEvaluatorFactory = metadataProvider
                        .createExternalFilterEvaluatorFactory(context, typeEnv, projectionFiltrationInfo, properties);
                ITypedAdapterFactory adapterFactory =
                        metadataProvider.getConfiguredAdapterFactory(externalDataset, edd.getAdapter(), properties,
                                (ARecordType) itemType, context.getWarningCollector(), filterEvaluatorFactory);
                return metadataProvider.getExternalDatasetScanRuntime(jobSpec, itemType, adapterFactory,
                        tupleFilterFactory, outputLimit);
            case INTERNAL:
                DataSourceId id = getId();
                DataverseName dataverseName = id.getDataverseName();
                String database = MetadataUtil.resolveDatabase(null, dataverseName);
                String datasetName = id.getDatasourceName();
                Index primaryIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                        database, dataverseName, datasetName, datasetName);

                ARecordType datasetType = (ARecordType) itemType;
                ARecordType metaItemType = null;
                if (dataset.hasMetaPart()) {
                    String metaItemTypeDatabase = null;
                    metaItemType = (ARecordType) MetadataManager.INSTANCE
                            .getDatatype(metadataProvider.getMetadataTxnContext(), metaItemTypeDatabase,
                                    dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName())
                            .getDatatype();
                }
                datasetType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(datasetType, metaItemType,
                        dataset);
                int numberOfPrimaryKeys = dataset.getPrimaryKeys().size();
                ITupleProjectorFactory tupleProjectorFactory =
                        IndexUtil.createTupleProjectorFactory(context, typeEnv, dataset.getDatasetFormatInfo(),
                                projectionFiltrationInfo, datasetType, metaItemType, numberOfPrimaryKeys);

                int[] minFilterFieldIndexes = createFilterIndexes(minFilterVars, opSchema);
                int[] maxFilterFieldIndexes = createFilterIndexes(maxFilterVars, opSchema);
                return metadataProvider.getBtreeSearchRuntime(jobSpec, opSchema, typeEnv, context, true, false, null,
                        ((DatasetDataSource) dataSource).getDataset(), primaryIndex.getIndexName(), null, null, true,
                        true, false, null, minFilterFieldIndexes, maxFilterFieldIndexes, tupleFilterFactory,
                        outputLimit, false, false, tupleProjectorFactory, false);
            default:
                throw new AlgebricksException("Unknown datasource type");
        }
    }

    private Map<String, String> addExternalProjectionInfo(IProjectionFiltrationInfo projectionInfo,
            Map<String, String> properties) {
        Map<String, String> propertiesCopy = properties;
        if (projectionInfo != DefaultProjectionFiltrationInfo.INSTANCE) {
            //properties could be cached and reused, so we make a copy per query
            propertiesCopy = new HashMap<>(properties);
            try {
                ExternalDatasetProjectionFiltrationInfo externalProjectionInfo =
                        (ExternalDatasetProjectionFiltrationInfo) projectionInfo;
                ExternalDataUtils.setExternalDataProjectionInfo(externalProjectionInfo, propertiesCopy);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return propertiesCopy;
    }

    private Map<String, String> addSubPath(Map<String, Serializable> dataSourceProps, Map<String, String> properties) {
        Serializable subPath = dataSourceProps.get(SUBPATH);
        if (!(subPath instanceof String)) {
            return properties;
        }
        Map<String, String> propertiesCopy = new HashMap<>(properties);
        propertiesCopy.put(SUBPATH, (String) subPath);
        return propertiesCopy;
    }

    private int[] createFilterIndexes(List<LogicalVariable> filterVars, IOperatorSchema opSchema) {
        if (filterVars != null && !filterVars.isEmpty()) {
            final int size = filterVars.size();
            int[] result = new int[size];
            for (int i = 0; i < size; ++i) {
                result[i] = opSchema.findVariable(filterVars.get(i));
            }
            return result;
        }
        return null;
    }

    @Override
    public boolean isScanAccessPathALeaf() {
        return dataset.getDatasetType() == DatasetType.EXTERNAL;
    }

    @Override
    public boolean compareProperties() {
        return dataset.getDatasetType() == DatasetType.EXTERNAL;
    }
}
