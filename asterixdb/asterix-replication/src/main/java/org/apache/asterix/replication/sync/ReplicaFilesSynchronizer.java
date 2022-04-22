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
package org.apache.asterix.replication.sync;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IndexCheckpoint;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.replication.api.PartitionReplica;
import org.apache.asterix.replication.messaging.DeletePartitionTask;
import org.apache.asterix.replication.messaging.PartitionResourcesListResponse;
import org.apache.asterix.replication.messaging.PartitionResourcesListTask;
import org.apache.asterix.replication.messaging.ReplicationProtocol;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Ensures that the files between master and a replica are synchronized
 */
public class ReplicaFilesSynchronizer {

    private static final Logger LOGGER = LogManager.getLogger();
    private final PartitionReplica replica;
    private final INcApplicationContext appCtx;
    private final boolean deltaRecovery;

    public ReplicaFilesSynchronizer(INcApplicationContext appCtx, PartitionReplica replica, boolean deltaRecovery) {
        this.appCtx = appCtx;
        this.replica = replica;
        this.deltaRecovery = deltaRecovery;
    }

    public void sync() throws IOException {
        final int partition = replica.getIdentifier().getPartition();
        if (!deltaRecovery) {
            deletePartitionFromReplica(partition);
        }
        LOGGER.trace("getting replica files");
        PartitionResourcesListResponse replicaResourceResponse = getReplicaFiles(partition);
        LOGGER.trace("got replica files");
        Map<ResourceReference, Long> resourceReferenceLongMap = getValidReplicaResources(
                replicaResourceResponse.getPartitionReplicatedResources(), replicaResourceResponse.isOrigin());
        // clean up files for invalid resources (deleted or recreated while the replica was down)
        Set<String> deletedReplicaFiles =
                cleanupReplicaInvalidResources(replicaResourceResponse, resourceReferenceLongMap);
        final PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        final IReplicationStrategy replicationStrategy = appCtx.getReplicationManager().getReplicationStrategy();
        final Set<String> masterFiles =
                localResourceRepository.getPartitionReplicatedFiles(partition, replicationStrategy).stream()
                        .map(StoragePathUtil::getFileRelativePath).collect(Collectors.toSet());
        LOGGER.trace("got master partition files");
        // exclude from the replica files the list of invalid deleted files
        final Set<String> replicaFiles = new HashSet<>(replicaResourceResponse.getFiles());
        replicaFiles.removeAll(deletedReplicaFiles);
        syncMissingFiles(replicaFiles, masterFiles);
        deleteReplicaExtraFiles(replicaFiles, masterFiles);
    }

    private void deletePartitionFromReplica(int partitionId) throws IOException {
        DeletePartitionTask deletePartitionTask = new DeletePartitionTask(partitionId);
        ReplicationProtocol.sendTo(replica, deletePartitionTask);
        ReplicationProtocol.waitForAck(replica);
    }

    private void deleteReplicaExtraFiles(Set<String> replicaFiles, Set<String> masterFiles) {
        final List<String> replicaInvalidFiles =
                replicaFiles.stream().filter(file -> !masterFiles.contains(file)).collect(Collectors.toList());
        if (!replicaInvalidFiles.isEmpty()) {
            LOGGER.debug("deleting files not on current master {} on replica {}", replicaInvalidFiles,
                    replica.getIdentifier());
            deleteInvalidFiles(replicaInvalidFiles);
        }
    }

    private void syncMissingFiles(Set<String> replicaFiles, Set<String> masterFiles) {
        final List<String> replicaMissingFiles =
                masterFiles.stream().filter(file -> !replicaFiles.contains(file)).collect(Collectors.toList());
        if (!replicaMissingFiles.isEmpty()) {
            LOGGER.debug("replicating missing files {} on replica {}", replicaMissingFiles, replica.getIdentifier());
            replicateMissingFiles(replicaMissingFiles);
        }
    }

    private void replicateMissingFiles(List<String> files) {
        final FileSynchronizer sync = new FileSynchronizer(appCtx, replica);
        // sort files to ensure index metadata files starting with "." are replicated first
        files.sort(String::compareTo);
        int missingFilesCount = files.size();
        for (int i = 0; i < missingFilesCount; i++) {
            String file = files.get(i);
            sync.replicate(file);
            replica.setSyncProgress((i + 1d) / missingFilesCount);
        }
    }

    private void deleteInvalidFiles(List<String> files) {
        final FileSynchronizer sync = new FileSynchronizer(appCtx, replica);
        // sort files to ensure index metadata files starting with "." are deleted last
        files.sort(String::compareTo);
        Collections.reverse(files);
        files.forEach(sync::delete);
        LOGGER.debug("completed invalid files deletion");
    }

    private long getResourceMasterValidSeq(ResourceReference rr) throws HyracksDataException {
        IIndexCheckpointManager iIndexCheckpointManager = appCtx.getIndexCheckpointManagerProvider().get(rr);
        int checkpointCount = iIndexCheckpointManager.getCheckpointCount();
        if (checkpointCount > 0) {
            IndexCheckpoint latest = iIndexCheckpointManager.getLatest();
            long masterValidSeq = latest.getMasterValidSeq();
            LOGGER.info("setting resource {} valid component seq to {}", rr, masterValidSeq);
            return masterValidSeq;
        }
        return AbstractLSMIndexFileManager.UNINITIALIZED_COMPONENT_SEQ;
    }

    private Set<String> cleanupReplicaInvalidResources(PartitionResourcesListResponse replicaResourceResponse,
            Map<ResourceReference, Long> validReplicaResources) {
        Set<String> invalidFiles = new HashSet<>();
        for (String replicaResPath : replicaResourceResponse.getFiles()) {
            ResourceReference replicaRes = ResourceReference.of(replicaResPath);
            if (!validReplicaResources.containsKey(replicaRes)) {
                LOGGER.debug("replica invalid file {} to be deleted", replicaRes.getFileRelativePath());
                invalidFiles.add(replicaResPath);
            } else if (replicaResourceResponse.isOrigin() && !replicaRes.isMetadataResource()) {
                // find files where the owner generated and failed before replicating
                Long masterValidSeq = validReplicaResources.get(replicaRes);
                IndexComponentFileReference componentFileReference =
                        IndexComponentFileReference.of(replicaRes.getName());
                if (componentFileReference.getSequenceStart() > masterValidSeq
                        || componentFileReference.getSequenceEnd() > masterValidSeq) {
                    LOGGER.debug("will ask replica {} to delete file {} based on valid master valid seq {}",
                            replica.getIdentifier(), replicaResPath, masterValidSeq);
                    invalidFiles.add(replicaResPath);
                }
            }
        }
        if (!invalidFiles.isEmpty()) {
            LOGGER.debug("will delete the following files from replica {}", invalidFiles);
            deleteInvalidFiles(new ArrayList<>(invalidFiles));
        }
        return invalidFiles;
    }

    private PartitionResourcesListResponse getReplicaFiles(int partition) throws IOException {
        final PartitionResourcesListTask replicaFilesRequest = new PartitionResourcesListTask(partition);
        final ISocketChannel channel = replica.getChannel();
        final ByteBuffer reusableBuffer = replica.getReusableBuffer();
        ReplicationProtocol.sendTo(replica, replicaFilesRequest);
        return (PartitionResourcesListResponse) ReplicationProtocol.read(channel, reusableBuffer);
    }

    private Map<ResourceReference, Long> getValidReplicaResources(Map<String, Long> partitionReplicatedResources,
            boolean origin) throws HyracksDataException {
        Map<ResourceReference, Long> resource2ValidSeqMap = new HashMap<>();
        for (Map.Entry<String, Long> resourceEntry : partitionReplicatedResources.entrySet()) {
            ResourceReference rr = ResourceReference.of(resourceEntry.getKey());
            final PersistentLocalResourceRepository localResourceRepository =
                    (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
            LocalResource localResource = localResourceRepository.get(rr.getRelativePath().toString());
            if (localResource != null) {
                if (localResource.getId() != resourceEntry.getValue()) {
                    LOGGER.info("replica has resource {} but with different resource id; ours {}, theirs {}", rr,
                            localResource.getId(), resourceEntry.getValue());
                } else {
                    long resourceMasterValidSeq = origin ? getResourceMasterValidSeq(rr) : Integer.MAX_VALUE;
                    resource2ValidSeqMap.put(rr, resourceMasterValidSeq);
                }
            }
        }
        return resource2ValidSeqMap;
    }
}
