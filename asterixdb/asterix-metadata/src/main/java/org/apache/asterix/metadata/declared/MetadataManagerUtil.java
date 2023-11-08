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

import static org.apache.asterix.common.utils.IdentifierUtil.dataverse;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.FullTextConfigMetadataEntity;
import org.apache.asterix.metadata.entities.FullTextFilterMetadataEntity;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

import com.google.common.base.Strings;

public class MetadataManagerUtil {

    private MetadataManagerUtil() {
        throw new AssertionError("This util class should not be initialized.");
    }

    public static IAType findType(MetadataTransactionContext mdTxnCtx, String database, DataverseName dataverseName,
            String typeName) throws AlgebricksException {
        Datatype type = findTypeEntity(mdTxnCtx, database, dataverseName, typeName);
        return type != null ? type.getDatatype() : null;
    }

    /**
     * Checks if a dataset is created without type specification and has no meta part. For such datasets,
     * creates and returns a record type based on the primary key and primary key types information included in the
     * internal details.
     *
     * @param itemType     record type of the dataset
     * @param metaItemType record type of the meta part of the dataset
     * @param dataset      the actual dataset
     * @return type computed from primary keys if dataset without type spec, otherwise the original itemType itself
     * @throws AlgebricksException AlgebricksException
     */
    public static IAType findTypeForDatasetWithoutType(IAType itemType, IAType metaItemType, Dataset dataset)
            throws AlgebricksException {
        ARecordType recordType = (ARecordType) itemType;
        if (recordType.getFieldNames().length == 0 && metaItemType == null
                && dataset.getDatasetType() == DatasetConfig.DatasetType.INTERNAL) {
            InternalDatasetDetails dsDetails = (InternalDatasetDetails) dataset.getDatasetDetails();
            return findType(dsDetails.getPrimaryKey(), dsDetails.getPrimaryKeyType());
        }
        return itemType;
    }

    private static IAType findType(List<List<String>> primaryKeys, List<IAType> primaryKeyTypes)
            throws AlgebricksException {
        return ProjectionFiltrationTypeUtil.getRecordTypeWithFieldTypes(primaryKeys, primaryKeyTypes);
    }

    public static Datatype findTypeEntity(MetadataTransactionContext mdTxnCtx, String database,
            DataverseName dataverseName, String typeName) throws AlgebricksException {
        if (database == null || dataverseName == null || typeName == null) {
            return null;
        }
        Datatype type = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, database, dataverseName, typeName);
        if (type == null) {
            throw new AsterixException(ErrorCode.UNKNOWN_TYPE, dataverseName + "." + typeName);
        }
        return type;
    }

    public static ARecordType findOutputRecordType(MetadataTransactionContext mdTxnCtx, String database,
            DataverseName dataverseName, String outputRecordType) throws AlgebricksException {
        if (outputRecordType == null) {
            return null;
        }
        if (database == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                    "Cannot declare output-record-type with no database");
        }
        if (dataverseName == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                    "Cannot declare output-record-type with no " + dataverse());
        }
        IAType type = findType(mdTxnCtx, database, dataverseName, outputRecordType);
        if (!(type instanceof ARecordType)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                    "Type " + outputRecordType + " is not a record type!");
        }
        return (ARecordType) type;
    }

    public static DatasourceAdapter getAdapter(MetadataTransactionContext mdTxnCtx, String database,
            DataverseName dataverseName, String adapterName) throws AlgebricksException {
        DatasourceAdapter adapter;
        // search in default namespace (built-in adapter)
        adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.SYSTEM_DATABASE,
                MetadataConstants.METADATA_DATAVERSE_NAME, adapterName);

        // search in dataverse (user-defined adapter)
        if (adapter == null) {
            adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, database, dataverseName, adapterName);
        }
        return adapter;
    }

    public static Dataset findDataset(MetadataTransactionContext mdTxnCtx, String database, DataverseName dataverseName,
            String datasetName, boolean includingViews) throws AlgebricksException {
        Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, database, dataverseName, datasetName);
        if (!includingViews && dataset != null && dataset.getDatasetType() == DatasetConfig.DatasetType.VIEW) {
            return null;
        }
        return dataset;
    }

    public static Dataset findDataset(MetadataTransactionContext mdTxnCtx, String database, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        return findDataset(mdTxnCtx, database, dataverseName, datasetName, false);
    }

    public static Dataset findExistingDataset(MetadataTransactionContext mdTxnCtx, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        Dataset dataset = findDataset(mdTxnCtx, database, dataverseName, datasetName);
        if (dataset == null) {
            //TODO(DB): include database
            throw new AsterixException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, datasetName, dataverseName);
        }
        return dataset;
    }

    public static INodeDomain findNodeDomain(IClusterStateManager clusterStateManager,
            MetadataTransactionContext mdTxnCtx, String nodeGroupName) throws AlgebricksException {
        NodeGroup nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodeGroupName);
        List<String> partitions = new ArrayList<>();
        for (String location : nodeGroup.getNodeNames()) {
            int numPartitions = clusterStateManager.getNodePartitionsCount(location);
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(location);
            }
        }
        return new DefaultNodeGroupDomain(partitions);
    }

    public static List<String> findNodes(MetadataTransactionContext mdTxnCtx, String nodeGroupName)
            throws AlgebricksException {
        return MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodeGroupName).getNodeNames();
    }

    public static Feed findFeed(MetadataTransactionContext mdTxnCtx, String database, DataverseName dataverseName,
            String feedName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getFeed(mdTxnCtx, database, dataverseName, feedName);
    }

    public static FeedConnection findFeedConnection(MetadataTransactionContext mdTxnCtx, String database,
            DataverseName dataverseName, String feedName, String datasetName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getFeedConnection(mdTxnCtx, database, dataverseName, feedName, datasetName);
    }

    public static FeedPolicyEntity findFeedPolicy(MetadataTransactionContext mdTxnCtx, String database,
            DataverseName dataverseName, String policyName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getFeedPolicy(mdTxnCtx, database, dataverseName, policyName);
    }

    public static Synonym findSynonym(MetadataTransactionContext mdTxnCtx, String database, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getSynonym(mdTxnCtx, database, dataverseName, synonymName);
    }

    public static FullTextConfigMetadataEntity findFullTextConfigDescriptor(MetadataTransactionContext mdTxnCtx,
            String database, DataverseName dataverseName, String ftConfigName) throws AlgebricksException {
        // If the config name is null, then the default config will be returned
        if (Strings.isNullOrEmpty(ftConfigName)) {
            return FullTextConfigMetadataEntity.getDefaultFullTextConfigMetadataEntity();
        }

        return MetadataManager.INSTANCE.getFullTextConfig(mdTxnCtx, database, dataverseName, ftConfigName);
    }

    public static FullTextFilterMetadataEntity findFullTextFilterDescriptor(MetadataTransactionContext mdTxnCtx,
            String database, DataverseName dataverseName, String ftFilterName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getFullTextFilter(mdTxnCtx, database, dataverseName, ftFilterName);
    }

    public static List<Index> getDatasetIndexes(MetadataTransactionContext mdTxnCtx, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, database, dataverseName, datasetName);
    }

    public static DataSource findDataSource(IClusterStateManager clusterStateManager,
            MetadataTransactionContext mdTxnCtx, DataSourceId id) throws AlgebricksException {
        return lookupSourceInMetadata(clusterStateManager, mdTxnCtx, id);
    }

    public static DataSource lookupSourceInMetadata(IClusterStateManager clusterStateManager,
            MetadataTransactionContext mdTxnCtx, DataSourceId id) throws AlgebricksException {
        Dataset dataset = findDataset(mdTxnCtx, id.getDatabaseName(), id.getDataverseName(), id.getDatasourceName());
        if (dataset == null) {
            //TODO(DB): include database
            throw new AsterixException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, id.getDatasourceName(),
                    id.getDataverseName());
        }
        byte datasourceType;
        switch (dataset.getDatasetType()) {
            case INTERNAL:
                datasourceType = DataSource.Type.INTERNAL_DATASET;
                break;
            case EXTERNAL:
                datasourceType = DataSource.Type.EXTERNAL_DATASET;
                break;
            default:
                //TODO(DB): include database
                throw new AsterixException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, id.getDatasourceName(),
                        id.getDataverseName());
        }

        IAType itemType = findType(mdTxnCtx, dataset.getItemTypeDatabaseName(), dataset.getItemTypeDataverseName(),
                dataset.getItemTypeName());
        IAType metaItemType = findType(mdTxnCtx, dataset.getMetaItemTypeDatabaseName(),
                dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());
        itemType = findTypeForDatasetWithoutType(itemType, metaItemType, dataset);

        INodeDomain domain = findNodeDomain(clusterStateManager, mdTxnCtx, dataset.getNodeGroupName());
        return new DatasetDataSource(id, dataset, itemType, metaItemType, datasourceType, dataset.getDatasetDetails(),
                domain);
    }
}
