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

import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;

public class DatasetDataSource extends AqlDataSource {

    private Dataset dataset;

    public DatasetDataSource(AqlSourceId id, Dataset dataset, IAType itemType, IAType metaItemType,
            byte datasourceType, IDatasetDetails datasetDetails, INodeDomain datasetDomain)
            throws AlgebricksException {
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
        InternalDatasetDetails internalDatasetDetails = (InternalDatasetDetails) datasetDetails;
        ARecordType recordType = (ARecordType) itemType;
        ARecordType metaRecordType = (ARecordType) metaItemType;
        List<IAType> partitioningKeyTypes =
                KeyFieldTypeUtils.getPartitioningKeyTypes(internalDatasetDetails, recordType, metaRecordType);
        int n = partitioningKeyTypes.size();
        schemaTypes = metaItemType == null ? new IAType[n + 1] : new IAType[n + 2];
        for (int keyIndex = 0; keyIndex < n; ++keyIndex) {
            schemaTypes[keyIndex] = partitioningKeyTypes.get(keyIndex);
        }
        schemaTypes[n] = itemType;
        if (metaItemType != null) {
            schemaTypes[n + 1] = metaItemType;
        }
    }

    private void initExternalDataset(IAType itemType) {
        schemaTypes = new IAType[1];
        schemaTypes[0] = itemType;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDatasourceScanRuntime(
            AqlMetadataProvider aqlMetadataProvider, IDataSource<AqlSourceId> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig)
            throws AlgebricksException {
        switch (dataset.getDatasetType()) {
            case EXTERNAL:
                Dataset externalDataset = ((DatasetDataSource) dataSource).getDataset();
                String itemTypeName = externalDataset.getItemTypeName();
                IAType itemType = MetadataManager.INSTANCE.getDatatype(aqlMetadataProvider.getMetadataTxnContext(),
                        externalDataset.getItemTypeDataverseName(), itemTypeName).getDatatype();

                ExternalDatasetDetails edd = (ExternalDatasetDetails) externalDataset.getDatasetDetails();
                IAdapterFactory adapterFactory = aqlMetadataProvider.getConfiguredAdapterFactory(externalDataset,
                        edd.getAdapter(), edd.getProperties(), (ARecordType) itemType, false, null, null);
                return aqlMetadataProvider.buildExternalDatasetDataScannerRuntime(jobSpec, itemType, adapterFactory,
                        NonTaggedDataFormat.INSTANCE);
            case INTERNAL:
                AqlSourceId asid = getId();
                String dataverseName = asid.getDataverseName();
                String datasetName = asid.getDatasourceName();
                Index primaryIndex = MetadataManager.INSTANCE.getIndex(aqlMetadataProvider.getMetadataTxnContext(),
                        dataverseName, datasetName, datasetName);

                int[] minFilterFieldIndexes = null;
                if (minFilterVars != null && !minFilterVars.isEmpty()) {
                    minFilterFieldIndexes = new int[minFilterVars.size()];
                    int i = 0;
                    for (LogicalVariable v : minFilterVars) {
                        minFilterFieldIndexes[i] = opSchema.findVariable(v);
                        i++;
                    }
                }
                int[] maxFilterFieldIndexes = null;
                if (maxFilterVars != null && !maxFilterVars.isEmpty()) {
                    maxFilterFieldIndexes = new int[maxFilterVars.size()];
                    int i = 0;
                    for (LogicalVariable v : maxFilterVars) {
                        maxFilterFieldIndexes[i] = opSchema.findVariable(v);
                        i++;
                    }
                }
                return aqlMetadataProvider.buildBtreeRuntime(jobSpec, scanVariables, opSchema, typeEnv, context, true,
                        false, ((DatasetDataSource) dataSource).getDataset(), primaryIndex.getIndexName(), null, null,
                        true, true, implConfig, minFilterFieldIndexes, maxFilterFieldIndexes);
            default:
                throw new AlgebricksException("Unknown datasource type");
        }
    }

    @Override
    public boolean isScanAccessPathALeaf() {
        return dataset.getDatasetType() == DatasetType.EXTERNAL;
    }

}
