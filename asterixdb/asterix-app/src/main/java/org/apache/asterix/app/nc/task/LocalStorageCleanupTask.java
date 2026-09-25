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
package org.apache.asterix.app.nc.task;

import java.util.Map;

import org.apache.asterix.app.nc.StorageCleanupUtil;
import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.utils.Partitions;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalStorageCleanupTask implements INCLifecycleTask {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final int metadataPartitionId;

    public LocalStorageCleanupTask(int metadataPartitionId) {
        this.metadataPartitionId = metadataPartitionId;
    }

    @Override
    public void perform(CcId ccId, IControllerService cs) throws HyracksDataException {
        INcApplicationContext appContext = (INcApplicationContext) cs.getApplicationContext();
        PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) appContext.getLocalResourceRepository();
        localResourceRepository.deleteCorruptedResources();
        deleteInvalidMetadataIndexes(localResourceRepository);
        final Partitions nodePartitions = appContext.getReplicaManager().getPartitions();
        INcApplicationContext appCtx = (INcApplicationContext) cs.getApplicationContext();
        if (appCtx.isCloudDeployment() && nodePartitions.contains(metadataPartitionId)) {
            appCtx.getTransactionSubsystem().getTransactionManager().rollbackMetadataTransactionsWithoutWAL();
        }
        deleteIncompleteIndexes(appCtx, localResourceRepository, nodePartitions);
        localResourceRepository.cleanup(nodePartitions);
    }

    /**
     * Removes every secondary index of this node's partitions whose creation never finished. Such an index's latest
     * checkpoint still says pending-creation: a finished create clears it when the load lands, even an empty one, and
     * no job of this node's can be in flight while it is registering, so a pending checkpoint seen here is a leftover
     * of a create that died with the node (ASTERIXDB-3839). Local recovery already ignores it; this reclaims the
     * files. Only a secondary whose creator declared {@link org.apache.asterix.common.storage.IndexCompletionMode}
     * ON_LOAD is ever flagged, so primaries and the metadata partition are skipped as a matter of course.
     */
    private void deleteIncompleteIndexes(INcApplicationContext appCtx,
            PersistentLocalResourceRepository localResourceRepository, Partitions nodePartitions)
            throws HyracksDataException {
        IIndexCheckpointManagerProvider checkpointManagerProvider = appCtx.getIndexCheckpointManagerProvider();
        Map<Long, LocalResource> resources = localResourceRepository.getResources(
                resource -> ((DatasetLocalResource) resource.getResource()).getPartition() != metadataPartitionId,
                nodePartitions);
        for (LocalResource resource : resources.values()) {
            DatasetResourceReference ref = DatasetResourceReference.of(resource);
            if (ref.getIndex().equals(ref.getDataset()) || !checkpointManagerProvider.get(ref).isPendingCreation()) {
                continue;
            }
            LOGGER.warn("deleting incomplete index {}: its creation never finished", resource.getPath());
            StorageCleanupUtil.deleteIndex(appCtx, resource.getPath());
        }
    }

    private void deleteInvalidMetadataIndexes(PersistentLocalResourceRepository localResourceRepository)
            throws HyracksDataException {
        localResourceRepository.deleteInvalidIndexes(r -> {
            DatasetLocalResource lr = (DatasetLocalResource) r.getResource();
            return MetadataIndexImmutableProperties.isMetadataDataset(lr.getDatasetId())
                    && lr.getPartition() != metadataPartitionId;
        });
    }

    @Override
    public String toString() {
        return "LocalStorageCleanupTask{" + "metadataPartitionId=" + metadataPartitionId + '}';
    }
}
