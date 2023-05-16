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
package org.apache.asterix.replication.messaging;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.replication.api.IReplicaTask;
import org.apache.asterix.replication.api.IReplicationWorker;
import org.apache.asterix.replication.sync.IndexSynchronizer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;
import org.apache.hyracks.util.ThreadDumpUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A task to mark a replicated LSM component as valid
 */
public class MarkComponentValidTask implements IReplicaTask {

    private static final Logger LOGGER = LogManager.getLogger();
    private final long masterLsn;
    private final long lastComponentId;
    private final String file;
    private final String masterNodeId;

    public MarkComponentValidTask(String file, long masterLsn, long lastComponentId, String masterNodeId) {
        this.file = file;
        this.lastComponentId = lastComponentId;
        this.masterLsn = masterLsn;
        this.masterNodeId = masterNodeId;
    }

    @Override
    public void perform(INcApplicationContext appCtx, IReplicationWorker worker) {
        try {
            if (masterLsn == IndexSynchronizer.BULKLOAD_LSN) {
                updateBulkLoadedLastComponentSequence(appCtx);
            } else if (masterLsn != IndexSynchronizer.MERGE_LSN) {
                ensureComponentLsnFlushed(appCtx);
            }
            IIOManager ioManager = appCtx.getIoManager();
            // delete mask
            final FileReference maskPath = ComponentMaskTask.getComponentMaskPath(ioManager, file);
            ioManager.delete(maskPath);
            ReplicationProtocol.sendAck(worker.getChannel(), worker.getReusableBuffer());
        } catch (IOException | InterruptedException e) {
            throw new ReplicationException(e);
        }
    }

    private void updateBulkLoadedLastComponentSequence(INcApplicationContext appCtx) throws HyracksDataException {
        final ResourceReference indexRef = ResourceReference.of(file);
        final IIndexCheckpointManagerProvider checkpointManagerProvider = appCtx.getIndexCheckpointManagerProvider();
        final IIndexCheckpointManager indexCheckpointManager = checkpointManagerProvider.get(indexRef);
        final long componentSequence = IndexComponentFileReference.of(indexRef.getName()).getSequenceEnd();
        indexCheckpointManager.advanceValidComponent(componentSequence, lastComponentId);
    }

    private void ensureComponentLsnFlushed(INcApplicationContext appCtx)
            throws HyracksDataException, InterruptedException {
        final ResourceReference indexRef = ResourceReference.of(file);
        final IIndexCheckpointManagerProvider checkpointManagerProvider = appCtx.getIndexCheckpointManagerProvider();
        final IIndexCheckpointManager indexCheckpointManager = checkpointManagerProvider.get(indexRef);
        long replicationTimeOut = TimeUnit.SECONDS.toMillis(appCtx.getReplicationProperties().getReplicationTimeOut());
        synchronized (indexCheckpointManager) {
            // wait until the lsn mapping is flushed to disk
            while (!indexCheckpointManager.isFlushed(masterLsn)) {
                if (replicationTimeOut <= 0) {
                    LOGGER.warn("{} seconds passed without receiving flush lsn {} from master for component {}",
                            appCtx.getReplicationProperties().getReplicationTimeOut(), masterLsn, file);
                    LOGGER.debug("thead dump on receiving flush lsn timeout {}", ThreadDumpUtil::takeDumpString);
                    throw new ReplicationException(new TimeoutException("couldn't receive flush lsn from master"));
                }
                final long startTime = System.nanoTime();
                indexCheckpointManager.wait(replicationTimeOut);
                replicationTimeOut -= TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            }
            final long componentSequence = IndexComponentFileReference.of(indexRef.getName()).getSequenceEnd();
            indexCheckpointManager.replicated(componentSequence, masterLsn, lastComponentId, masterNodeId);
        }
    }

    @Override
    public ReplicationProtocol.ReplicationRequestType getMessageType() {
        return ReplicationProtocol.ReplicationRequestType.MARK_COMPONENT_VALID;
    }

    @Override
    public void serialize(OutputStream out) throws HyracksDataException {
        try {
            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF(file);
            dos.writeLong(masterLsn);
            dos.writeLong(lastComponentId);
            boolean hasMaster = masterNodeId != null;
            dos.writeBoolean(hasMaster);
            if (hasMaster) {
                dos.writeUTF(masterNodeId);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static MarkComponentValidTask create(DataInput input) throws IOException {
        final String indexFile = input.readUTF();
        final long lsn = input.readLong();
        final long lastComponentId = input.readLong();
        final boolean hasMaster = input.readBoolean();
        final String masterNodeId = hasMaster ? input.readUTF() : null;
        return new MarkComponentValidTask(indexFile, lsn, lastComponentId, masterNodeId);
    }
}
