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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
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
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

import com.google.common.base.Strings;

public class MetadataManagerUtil {

    private MetadataManagerUtil() {
        throw new AssertionError("This util class should not be initialized.");
    }

    public static IAType findType(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName, String typeName)
            throws AlgebricksException {
        Datatype type = findTypeEntity(mdTxnCtx, dataverseName, typeName);
        return type != null ? type.getDatatype() : null;
    }

    public static Datatype findTypeEntity(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName,
            String typeName) throws AlgebricksException {
        if (dataverseName == null || typeName == null) {
            return null;
        }
        Datatype type = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
        if (type == null) {
            throw new AsterixException(ErrorCode.UNKNOWN_TYPE, dataverseName + "." + typeName);
        }
        return type;
    }

    public static ARecordType findOutputRecordType(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName,
            String outputRecordType) throws AlgebricksException {
        if (outputRecordType == null) {
            return null;
        }
        if (dataverseName == null) {
            throw new AlgebricksException("Cannot declare output-record-type with no dataverse!");
        }
        IAType type = findType(mdTxnCtx, dataverseName, outputRecordType);
        if (!(type instanceof ARecordType)) {
            throw new AlgebricksException("Type " + outputRecordType + " is not a record type!");
        }
        return (ARecordType) type;
    }

    public static DatasourceAdapter getAdapter(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName,
            String adapterName) throws AlgebricksException {
        DatasourceAdapter adapter;
        // search in default namespace (built-in adapter)
        adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME, adapterName);

        // search in dataverse (user-defined adapter)
        if (adapter == null) {
            adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, dataverseName, adapterName);
        }
        return adapter;
    }

    public static Dataset findDataset(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName, String dataset)
            throws AlgebricksException {
        return MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, dataset);
    }

    public static Dataset findExistingDataset(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        Dataset dataset = findDataset(mdTxnCtx, dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
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

    public static Feed findFeed(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName, String feedName)
            throws AlgebricksException {
        return MetadataManager.INSTANCE.getFeed(mdTxnCtx, dataverseName, feedName);
    }

    public static FeedConnection findFeedConnection(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName,
            String feedName, String datasetName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getFeedConnection(mdTxnCtx, dataverseName, feedName, datasetName);
    }

    public static FeedPolicyEntity findFeedPolicy(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName,
            String policyName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getFeedPolicy(mdTxnCtx, dataverseName, policyName);
    }

    public static Synonym findSynonym(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getSynonym(mdTxnCtx, dataverseName, synonymName);
    }

    public static FullTextConfigMetadataEntity findFullTextConfigDescriptor(MetadataTransactionContext mdTxnCtx,
            DataverseName dataverseName, String ftConfigName) throws AlgebricksException {
        // If the config name is null, then the default config will be returned
        if (Strings.isNullOrEmpty(ftConfigName)) {
            return FullTextConfigMetadataEntity.getDefaultFullTextConfigMetadataEntity();
        }

        return MetadataManager.INSTANCE.getFullTextConfig(mdTxnCtx, dataverseName, ftConfigName);
    }

    public static FullTextFilterMetadataEntity findFullTextFilterDescriptor(MetadataTransactionContext mdTxnCtx,
            DataverseName dataverseName, String ftFilterName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getFullTextFilter(mdTxnCtx, dataverseName, ftFilterName);
    }

    public static List<Index> getDatasetIndexes(MetadataTransactionContext mdTxnCtx, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        return MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
    }

    public static DataSource findDataSource(IClusterStateManager clusterStateManager,
            MetadataTransactionContext mdTxnCtx, DataSourceId id) throws AlgebricksException {
        return lookupSourceInMetadata(clusterStateManager, mdTxnCtx, id);
    }

    public static DataSource lookupSourceInMetadata(IClusterStateManager clusterStateManager,
            MetadataTransactionContext mdTxnCtx, DataSourceId id) throws AlgebricksException {
        Dataset dataset = findDataset(mdTxnCtx, id.getDataverseName(), id.getDatasourceName());
        if (dataset == null) {
            throw new AlgebricksException("Datasource with id " + id + " was not found.");
        }
        IAType itemType = findType(mdTxnCtx, dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        IAType metaItemType = findType(mdTxnCtx, dataset.getMetaItemTypeDataverseName(), dataset.getMetaItemTypeName());
        INodeDomain domain = findNodeDomain(clusterStateManager, mdTxnCtx, dataset.getNodeGroupName());
        byte datasourceType = dataset.getDatasetType().equals(DatasetType.EXTERNAL) ? DataSource.Type.EXTERNAL_DATASET
                : DataSource.Type.INTERNAL_DATASET;
        return new DatasetDataSource(id, dataset, itemType, metaItemType, datasourceType, dataset.getDatasetDetails(),
                domain);
    }
}
