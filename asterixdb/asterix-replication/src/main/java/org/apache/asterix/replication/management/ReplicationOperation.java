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
package org.apache.asterix.replication.management;

import java.util.Optional;
import java.util.Set;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.replication.IPartitionReplica;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.replication.api.PartitionReplica;
import org.apache.asterix.replication.api.ReplicationDestination;
import org.apache.asterix.replication.sync.IndexSynchronizer;
import org.apache.hyracks.api.replication.IReplicationJob;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractIoOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallbackFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReplicationOperation extends AbstractIoOperation {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final ILSMIOOperationCallback INSTANCE =
            NoOpIOOperationCallbackFactory.INSTANCE.createIoOpCallback(null);
    private final INcApplicationContext appCtx;
    private final DatasetResourceReference indexRef;
    private final IReplicationJob job;
    private final IndexReplicationManager indexReplicationManager;

    public ReplicationOperation(INcApplicationContext appCtx, DatasetResourceReference indexRef, IReplicationJob job,
            IndexReplicationManager indexReplicationManager) {
        super(null, null, INSTANCE, indexRef.getRelativePath().toString());
        this.appCtx = appCtx;
        this.indexRef = indexRef;
        this.job = job;
        this.indexReplicationManager = indexReplicationManager;
    }

    @Override
    public LSMIOOperationType getIOOpertionType() {
        return LSMIOOperationType.REPLICATE;
    }

    @Override
    public LSMIOOperationStatus call() {
        try {
            Set<ReplicationDestination> destinations = indexReplicationManager.getDestinations();
            if (destinations.isEmpty() || indexReplicationManager.skip(indexRef)) {
                return LSMIOOperationStatus.SUCCESS;
            }
            LOGGER.debug("started replicate operation on index {}", indexRef);
            final IndexSynchronizer synchronizer = new IndexSynchronizer(job, appCtx);
            final int indexPartition = indexRef.getPartitionId();
            for (ReplicationDestination dest : destinations) {
                Optional<IPartitionReplica> partitionReplica = dest.getPartitionReplica(indexPartition);
                if (partitionReplica.isEmpty()) {
                    continue;
                }
                PartitionReplica destReplica = null;
                try {
                    destReplica = dest.getPartitionReplicaConnection(partitionReplica.get().getIdentifier(), appCtx);
                    synchronizer.sync(destReplica);
                    dest.recycleConnection(destReplica);
                } catch (Exception e) {
                    if (destReplica != null) {
                        destReplica.close();
                    }
                    indexReplicationManager.handleFailure(dest, e);
                }
            }
            LOGGER.debug("completed replicate operation on index {}", indexRef);
            return LSMIOOperationStatus.SUCCESS;
        } finally {
            indexReplicationManager.afterReplication(job);
        }
    }

    @Override
    protected LSMComponentFileReferences getComponentFiles() {
        return null;
    }

    @Override
    public long getRemainingPages() {
        return 0;
    }
}
