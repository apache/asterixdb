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

import java.util.Arrays;
import java.util.Set;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.cloud.IPartitionBootstrapper;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.storage.common.disk.IDiskCacheMonitoringService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CloudToLocalStorageCachingTask implements INCLifecycleTask {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final long serialVersionUID = 1L;
    private final Set<Integer> storagePartitions;
    private final boolean metadataNode;
    private final int metadataPartitionId;
    private final boolean cleanup;

    public CloudToLocalStorageCachingTask(Set<Integer> storagePartitions, boolean metadataNode, int metadataPartitionId,
            boolean cleanup) {
        this.storagePartitions = storagePartitions;
        this.metadataNode = metadataNode;
        this.metadataPartitionId = metadataPartitionId;
        this.cleanup = cleanup;
    }

    @Override
    public void perform(CcId ccId, IControllerService cs) throws HyracksDataException {
        INcApplicationContext applicationContext = (INcApplicationContext) cs.getApplicationContext();
        PersistentLocalResourceRepository lrs =
                (PersistentLocalResourceRepository) applicationContext.getLocalResourceRepository();
        IDiskCacheMonitoringService diskService = applicationContext.getDiskCacheService();

        // Pause all disk caching activities
        diskService.pause();
        try {
            String nodeId = applicationContext.getServiceContext().getNodeId();
            LOGGER.info("Initializing Node {} with storage partitions: {}", nodeId, storagePartitions);

            Checkpoint latestCheckpoint =
                    applicationContext.getTransactionSubsystem().getCheckpointManager().getLatest();
            IPartitionBootstrapper bootstrapper = applicationContext.getPartitionBootstrapper();
            bootstrapper.bootstrap(storagePartitions, lrs.getOnDiskPartitions(), metadataNode, metadataPartitionId,
                    cleanup, latestCheckpoint == null);

            // Report all local resources
            diskService.reportLocalResources(lrs.loadAndGetAllResources());
        } finally {
            // Resume all disk caching activities
            diskService.resume();
        }
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\", \"partitions\" : "
                + Arrays.toString(storagePartitions.toArray()) + " }";
    }
}
