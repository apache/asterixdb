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
package org.apache.asterix.app.message;

import static org.apache.hyracks.util.ExitUtil.EC_NC_FAILED_TO_NOTIFY_TASKS_COMPLETED;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.app.nc.StorageCleanupUtil;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.messaging.CcIdentifiedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.utils.Partitions;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StorageCleanupRequestMessage extends CcIdentifiedMessage implements INcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<Integer, HashSet<String>> validIndexes;
    private final long reqId;

    public StorageCleanupRequestMessage(long reqId, Map<Integer, HashSet<String>> validIndexes) {
        this.validIndexes = validIndexes;
        this.reqId = reqId;
    }

    @Override
    public void handle(INcApplicationContext appContext) throws HyracksDataException, InterruptedException {
        INCMessageBroker broker = (INCMessageBroker) appContext.getServiceContext().getMessageBroker();
        PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) appContext.getLocalResourceRepository();
        Partitions nodePartitions = appContext.getReplicaManager().getPartitions();
        Map<Long, LocalResource> localResources = localResourceRepository.getResources(lr -> true, nodePartitions);
        for (LocalResource resource : localResources.values()) {
            DatasetLocalResource lr = (DatasetLocalResource) resource.getResource();
            if (MetadataIndexImmutableProperties.isMetadataDataset(lr.getDatasetId())) {
                // skip metadata indexes
                continue;
            }
            Set<String> datasetIndexes = validIndexes.get(lr.getDatasetId());
            if (datasetIndexes == null) {
                LOGGER.warn("found invalid index {} with dataset id {}", resource.getPath(), lr.getDatasetId());
                StorageCleanupUtil.deleteIndex(appContext, resource.getPath());
            } else if (isUnknownSecondary(resource, datasetIndexes)) {
                LOGGER.warn("found index {} that metadata has no entry for; its creation never completed",
                        resource.getPath());
                StorageCleanupUtil.deleteIndex(appContext, resource.getPath());
            }
        }
        localResourceRepository.cleanup(nodePartitions);
        try {
            broker.sendMessageToPrimaryCC(new VoidResponse(reqId, null));
        } catch (Exception e) {
            LOGGER.error("failed to notify CC of storage clean up; halting...", e);
            ExitUtil.halt(EC_NC_FAILED_TO_NOTIFY_TASKS_COMPLETED);
        }
    }

    private static boolean isUnknownSecondary(LocalResource resource, Set<String> datasetIndexes) {
        DatasetResourceReference ref = DatasetResourceReference.of(resource);
        return !ref.getIndex().equals(ref.getDataset()) && !datasetIndexes.contains(ref.getIndex());
    }

    @Override
    public String toString() {
        return StorageCleanupRequestMessage.class.getSimpleName();
    }
}
