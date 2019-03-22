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

import static org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation.DELETE;
import static org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation.REPLICATE;

import java.io.IOException;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.replication.api.PartitionReplica;
import org.apache.asterix.replication.messaging.ComponentMaskTask;
import org.apache.asterix.replication.messaging.DropIndexTask;
import org.apache.asterix.replication.messaging.MarkComponentValidTask;
import org.apache.asterix.replication.messaging.ReplicationProtocol;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.IReplicationJob;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexReplicationJob;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexSynchronizer {

    private static final Logger LOGGER = LogManager.getLogger();
    public static final long MERGE_LSN = -1;
    public static final long BULKLOAD_LSN = -2;
    private final IReplicationJob job;
    private final INcApplicationContext appCtx;

    public IndexSynchronizer(IReplicationJob job, INcApplicationContext appCtx) {
        this.job = job;
        this.appCtx = appCtx;
    }

    public void sync(PartitionReplica replica) throws IOException {
        switch (job.getJobType()) {
            case LSM_COMPONENT:
                syncComponent(replica);
                break;
            case METADATA:
                syncMetadata(replica);
                break;
            default:
                throw new IllegalStateException("unrecognized job type: " + job.getJobType().name());
        }
    }

    private void syncComponent(PartitionReplica replica) throws IOException {
        if (job.getOperation() == REPLICATE) {
            replicateComponent(replica);
        } else if (job.getOperation() == DELETE) {
            deleteComponent(replica);
        }
    }

    private void syncMetadata(PartitionReplica replica) throws IOException {
        if (job.getOperation() == REPLICATE) {
            replicateIndexMetadata(replica);
        } else if (job.getOperation() == DELETE) {
            deleteIndexMetadata(replica);
        }
    }

    private void replicateComponent(PartitionReplica replica) throws IOException {
        // send component header
        final String anyFile = job.getAnyFile();
        final String indexFile = StoragePathUtil.getFileRelativePath(anyFile);
        final ComponentMaskTask maskTask = new ComponentMaskTask(indexFile);
        ReplicationProtocol.sendTo(replica, maskTask);
        ReplicationProtocol.waitForAck(replica);
        // send component files
        final FileSynchronizer fileSynchronizer = new FileSynchronizer(appCtx, replica);
        job.getJobFiles().stream().map(StoragePathUtil::getFileRelativePath).forEach(fileSynchronizer::replicate);
        // send mark component valid
        MarkComponentValidTask markValidTask =
                new MarkComponentValidTask(indexFile, getReplicatedComponentLsn(), getReplicatedComponentId());
        ReplicationProtocol.sendTo(replica, markValidTask);
        ReplicationProtocol.waitForAck(replica);
        LOGGER.debug("Replicated component ({}) to replica {}", indexFile, replica);
    }

    private void deleteComponent(PartitionReplica replica) {
        FileSynchronizer fileSynchronizer = new FileSynchronizer(appCtx, replica);
        job.getJobFiles().stream().map(StoragePathUtil::getFileRelativePath).forEach(fileSynchronizer::delete);
    }

    private void replicateIndexMetadata(PartitionReplica replica) {
        // send the index metadata file
        final FileSynchronizer fileSynchronizer = new FileSynchronizer(appCtx, replica);
        job.getJobFiles().stream().map(StoragePathUtil::getFileRelativePath)
                .forEach(file -> fileSynchronizer.replicate(file, true));
    }

    private void deleteIndexMetadata(PartitionReplica replica) throws IOException {
        final String file = StoragePathUtil.getFileRelativePath(job.getAnyFile());
        final DropIndexTask task = new DropIndexTask(file);
        ReplicationProtocol.sendTo(replica, task);
        ReplicationProtocol.waitForAck(replica);
    }

    private long getReplicatedComponentLsn() throws HyracksDataException {
        final ILSMIndexReplicationJob indexReplJob = (ILSMIndexReplicationJob) job;
        if (indexReplJob.getLSMOpType() == LSMOperationType.MERGE) {
            return MERGE_LSN;
        } else if (indexReplJob.getLSMOpType() == LSMOperationType.LOAD) {
            return BULKLOAD_LSN;
        }

        if (indexReplJob.getLSMOpType() != LSMOperationType.FLUSH) {
            return LSMIOOperationCallback.INVALID_LSN;
        }
        final ILSMIndex lsmIndex = indexReplJob.getLSMIndex();
        final ILSMIndexOperationContext ctx = indexReplJob.getLSMIndexOperationContext();
        return ((LSMIOOperationCallback) lsmIndex.getIOOperationCallback())
                .getComponentLSN(ctx.getComponentsToBeReplicated());
    }

    private long getReplicatedComponentId() throws HyracksDataException {
        final ILSMIndexReplicationJob indexReplJob = (ILSMIndexReplicationJob) job;
        if (indexReplJob.getLSMOpType() != LSMOperationType.FLUSH) {
            return -1L;
        }
        final ILSMIndexOperationContext ctx = indexReplJob.getLSMIndexOperationContext();
        LSMComponentId id = (LSMComponentId) ctx.getComponentsToBeReplicated().get(0).getId();
        return id.getMinId();
    }
}
