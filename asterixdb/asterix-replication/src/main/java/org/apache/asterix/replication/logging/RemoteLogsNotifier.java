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
package org.apache.asterix.replication.logging;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.replication.messaging.ReplicationProtocol;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class RemoteLogsNotifier implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger();
    private final PersistentLocalResourceRepository localResourceRep;
    private final IIndexCheckpointManagerProvider indexCheckpointManagerProvider;
    private final LinkedBlockingQueue<RemoteLogRecord> remoteLogsQ;
    private final INcApplicationContext appCtx;

    public RemoteLogsNotifier(INcApplicationContext appCtx, LinkedBlockingQueue<RemoteLogRecord> remoteLogsQ) {
        this.appCtx = appCtx;
        this.remoteLogsQ = remoteLogsQ;
        localResourceRep = (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        indexCheckpointManagerProvider = appCtx.getIndexCheckpointManagerProvider();
    }

    @Override
    public void run() {
        final String nodeId = appCtx.getServiceContext().getNodeId();
        Thread.currentThread().setName(nodeId + RemoteLogsNotifier.class.getSimpleName());
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final RemoteLogRecord logRecord = remoteLogsQ.take();
                switch (logRecord.getLogType()) {
                    case LogType.JOB_COMMIT:
                    case LogType.ABORT:
                        // send ACK to requester
                        logRecord.getReplicationWorker().getChannel().getSocketChannel().socket().getOutputStream()
                                .write((nodeId + ReplicationProtocol.LOG_REPLICATION_ACK + logRecord.getTxnId()
                                        + System.lineSeparator()).getBytes());
                        break;
                    case LogType.FLUSH:
                        checkpointReplicaIndexes(logRecord, logRecord.getDatasetId());
                        break;
                    default:
                        throw new IllegalStateException("Unexpected log type: " + logRecord.getLogType());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                LOGGER.error("Failed to process replicated log", e);
            }
        }
    }

    private void checkpointReplicaIndexes(RemoteLogRecord remoteLogMapping, int datasetId) throws HyracksDataException {
        final Set<Integer> masterPartitions = appCtx.getReplicaManager().getPartitions();
        final Predicate<LocalResource> replicaIndexesPredicate = lr -> {
            DatasetLocalResource dls = (DatasetLocalResource) lr.getResource();
            return dls.getDatasetId() == datasetId && !masterPartitions.contains(dls.getPartition());
        };
        final Map<Long, LocalResource> resources = localResourceRep.getResources(replicaIndexesPredicate);
        final List<DatasetResourceReference> replicaIndexesRef =
                resources.values().stream().map(DatasetResourceReference::of).collect(Collectors.toList());
        for (DatasetResourceReference replicaIndexRef : replicaIndexesRef) {
            final IIndexCheckpointManager indexCheckpointManager = indexCheckpointManagerProvider.get(replicaIndexRef);
            synchronized (indexCheckpointManager) {
                indexCheckpointManager.masterFlush(remoteLogMapping.getMasterLsn(), remoteLogMapping.getLSN());
            }
        }
    }
}
