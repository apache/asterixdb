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
package org.apache.asterix.utils;

import static org.apache.asterix.common.api.IClusterManagementWork.ClusterState.ACTIVE;
import static org.apache.asterix.common.api.IClusterManagementWork.ClusterState.REBALANCE_REQUIRED;
import static org.apache.asterix.common.exceptions.ErrorCode.REJECT_BAD_CLUSTER_STATE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.message.EstimateColumnCountRequestMessage;
import org.apache.asterix.app.message.StorageSizeRequestMessage;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetPartitions;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.hyracks.api.util.InvokeUtil;

import it.unimi.dsi.fastutil.ints.Int2IntMap;

public class StorageUtil {

    public static long getCollectionSize(ICcApplicationContext appCtx, String database, DataverseName dataverse,
            String collection, String index) throws Exception {
        IClusterManagementWork.ClusterState state = appCtx.getClusterStateManager().getState();
        if (!(state == ACTIVE || state == REBALANCE_REQUIRED)) {
            throw new RuntimeDataException(REJECT_BAD_CLUSTER_STATE, state);
        }

        if (!appCtx.getNamespaceResolver().isUsingDatabase()) {
            database = MetadataConstants.DEFAULT_DATABASE;
        }

        IMetadataLockManager lockManager = appCtx.getMetadataLockManager();
        MetadataProvider metadataProvider = MetadataProvider.createWithDefaultNamespace(appCtx);
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            lockManager.acquireDatabaseReadLock(metadataProvider.getLocks(), database);
            lockManager.acquireDataverseReadLock(metadataProvider.getLocks(), database, dataverse);
            lockManager.acquireDatasetReadLock(metadataProvider.getLocks(), database, dataverse, collection);
            Dataset dataset = metadataProvider.findDataset(database, dataverse, collection);
            if (dataset == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, collection,
                        MetadataUtil.dataverseName(database, dataverse, metadataProvider.isUsingDatabase()));
            }

            if (dataset.getDatasetType() != DatasetConfig.DatasetType.INTERNAL) {
                throw new CompilationException(ErrorCode.STORAGE_SIZE_NOT_APPLICABLE_TO_TYPE, dataset.getDatasetType());
            }

            if (index != null) {
                Index idx = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), database,
                        dataverse, collection, index);
                if (idx == null) {
                    throw new CompilationException(ErrorCode.UNKNOWN_INDEX, index);
                }
            }

            final List<String> ncs = new ArrayList<>(appCtx.getClusterStateManager().getParticipantNodes());
            CCMessageBroker messageBroker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();

            long reqId = messageBroker.newRequestId();
            List<StorageSizeRequestMessage> requests = new ArrayList<>();
            for (int i = 0; i < ncs.size(); i++) {
                requests.add(new StorageSizeRequestMessage(reqId, database, dataverse.getCanonicalForm(), collection,
                        index));
            }
            return (long) messageBroker.sendSyncRequestToNCs(reqId, ncs, requests, TimeUnit.SECONDS.toMillis(60), true);
        } finally {
            InvokeUtil.tryWithCleanups(() -> MetadataManager.INSTANCE.commitTransaction(mdTxnCtx),
                    () -> metadataProvider.getLocks().unlock());
        }
    }

    public static Int2IntMap getEstimatedColumnCount(ICcApplicationContext appCtx, String database,
            DataverseName dataverse, String collection) throws Exception {
        IClusterManagementWork.ClusterState state = appCtx.getClusterStateManager().getState();
        if (!(state == ACTIVE || state == REBALANCE_REQUIRED)) {
            throw new RuntimeDataException(REJECT_BAD_CLUSTER_STATE, state);
        }

        if (!appCtx.getNamespaceResolver().isUsingDatabase()) {
            database = MetadataConstants.DEFAULT_DATABASE;
        }

        IMetadataLockManager lockManager = appCtx.getMetadataLockManager();
        MetadataProvider metadataProvider = MetadataProvider.createWithDefaultNamespace(appCtx);
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            lockManager.acquireDatabaseReadLock(metadataProvider.getLocks(), database);
            lockManager.acquireDataverseReadLock(metadataProvider.getLocks(), database, dataverse);
            lockManager.acquireDatasetReadLock(metadataProvider.getLocks(), database, dataverse, collection);
            Dataset dataset = metadataProvider.findDataset(database, dataverse, collection);
            if (dataset == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, collection,
                        MetadataUtil.dataverseName(database, dataverse, metadataProvider.isUsingDatabase()));
            }

            if (dataset.getDatasetType() != DatasetConfig.DatasetType.INTERNAL) {
                throw new CompilationException(ErrorCode.ESTIMATED_COLUMN_COUNT_NOT_APPLICABLE_TO_TYPE,
                        "collection type: " + dataset.getDatasetType());
            }

            if (dataset.getDatasetFormatInfo().getFormat() != DatasetConfig.DatasetFormat.COLUMN) {
                throw new CompilationException(ErrorCode.ESTIMATED_COLUMN_COUNT_NOT_APPLICABLE_TO_TYPE,
                        "storage format type: " + dataset.getDatasetFormatInfo().getFormat());
            }

            final List<String> ncs = new ArrayList<>(appCtx.getClusterStateManager().getParticipantNodes());
            CCMessageBroker messageBroker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();

            Map<String, List<DatasetPartitions>> nodeResources =
                    DatasetUtil.getNodeResourcesWithoutSecondaries(metadataProvider, dataset);
            long reqId = messageBroker.newRequestId();
            List<EstimateColumnCountRequestMessage> requests = new ArrayList<>();
            for (String nc : ncs) {
                requests.add(new EstimateColumnCountRequestMessage(reqId, nodeResources.get(nc)));
            }

            return (Int2IntMap) messageBroker.sendSyncRequestToNCs(reqId, ncs, requests, TimeUnit.SECONDS.toMillis(60),
                    true);
        } finally {
            InvokeUtil.tryWithCleanups(() -> MetadataManager.INSTANCE.commitTransaction(mdTxnCtx),
                    () -> metadataProvider.getLocks().unlock());
        }
    }
}
