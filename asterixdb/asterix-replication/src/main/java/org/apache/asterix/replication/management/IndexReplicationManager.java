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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.common.replication.IPartitionReplica;
import org.apache.asterix.common.replication.IReplicationDestination;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.replication.api.PartitionReplica;
import org.apache.asterix.replication.api.ReplicationDestination;
import org.apache.asterix.replication.sync.IndexSynchronizer;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.IReplicationJob;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexReplicationJob;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexReplicationManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private final IReplicationManager replicationManager;
    private final Set<ReplicationDestination> destinations = new HashSet<>();
    private final LinkedBlockingQueue<IReplicationJob> replicationJobsQ = new LinkedBlockingQueue<>();
    private final IReplicationStrategy replicationStrategy;
    private final PersistentLocalResourceRepository resourceRepository;
    private final INcApplicationContext appCtx;
    private final Object transferLock = new Object();
    private final Set<ReplicationDestination> failedDest = new HashSet<>();

    public IndexReplicationManager(INcApplicationContext appCtx, IReplicationManager replicationManager) {
        this.appCtx = appCtx;
        this.replicationManager = replicationManager;
        this.resourceRepository = (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        replicationStrategy = replicationManager.getReplicationStrategy();
        appCtx.getThreadExecutor().execute(new ReplicationJobsProcessor());
    }

    public void register(ReplicationDestination dest) {
        synchronized (transferLock) {
            LOGGER.info(() -> "register " + dest);
            destinations.add(dest);
            failedDest.remove(dest);
        }
    }

    public void unregister(IReplicationDestination dest) {
        synchronized (transferLock) {
            LOGGER.info(() -> "unregister " + dest);
            destinations.remove(dest);
            failedDest.remove(dest);
        }
    }

    private void handleFailure(ReplicationDestination dest, Exception e) {
        synchronized (transferLock) {
            if (failedDest.contains(dest)) {
                return;
            }
            LOGGER.error("Replica failed", e);
            if (destinations.contains(dest)) {
                failedDest.add(dest);
            }
            replicationManager.notifyFailure(dest, e);
        }
    }

    public void accept(IReplicationJob job) {
        if (job.getExecutionType() == IReplicationJob.ReplicationExecutionType.ASYNC) {
            replicationJobsQ.add(job);
            return;
        }
        process(job);
    }

    private void process(IReplicationJob job) {
        try {
            if (skip(job)) {
                return;
            }
            synchronized (transferLock) {
                if (destinations.isEmpty()) {
                    return;
                }
                final IndexSynchronizer synchronizer = new IndexSynchronizer(job, appCtx);
                final int indexPartition = getJobPartition(job);
                for (ReplicationDestination dest : destinations) {
                    try {
                        Optional<IPartitionReplica> partitionReplica = dest.getPartitionReplica(indexPartition);
                        if (!partitionReplica.isPresent()) {
                            continue;
                        }
                        PartitionReplica replica = (PartitionReplica) partitionReplica.get();
                        synchronizer.sync(replica);
                    } catch (Exception e) {
                        handleFailure(dest, e);
                    }
                }
                closeChannels();
            }
        } finally {
            afterReplication(job);
        }
    }

    private boolean skip(IReplicationJob job) {
        try {
            final DatasetResourceReference indexFileRef =
                    resourceRepository.getLocalResourceReference(job.getAnyFile());
            return !replicationStrategy.isMatch(indexFileRef.getDatasetId());
        } catch (HyracksDataException e) {
            throw new IllegalStateException("Couldn't find resource for " + job.getAnyFile(), e);
        }
    }

    private int getJobPartition(IReplicationJob job) {
        return ResourceReference.of(job.getAnyFile()).getPartitionNum();
    }

    private void closeChannels() {
        if (!replicationJobsQ.isEmpty()) {
            return;
        }
        LOGGER.log(Level.INFO, "No pending replication jobs. Closing connections to replicas");
        for (ReplicationDestination dest : destinations) {
            dest.getReplicas().stream().map(PartitionReplica.class::cast).forEach(PartitionReplica::close);
        }
    }

    private static void afterReplication(IReplicationJob job) {
        try {
            if (job.getOperation() == IReplicationJob.ReplicationOperation.REPLICATE
                    && job instanceof ILSMIndexReplicationJob) {
                ((ILSMIndexReplicationJob) job).endReplication();
            }
        } catch (HyracksDataException e) {
            throw new ReplicationException(e);
        }
    }

    private class ReplicationJobsProcessor implements Runnable {

        @Override
        public void run() {
            Thread.currentThread().setName(ReplicationJobsProcessor.class.getSimpleName());
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final IReplicationJob job = replicationJobsQ.take();
                    process(job);
                } catch (InterruptedException e) {
                    LOGGER.warn(() -> ReplicationJobsProcessor.class.getSimpleName() + " interrupted.", e);
                    Thread.currentThread().interrupt();
                }
            }
            LOGGER.warn("{} stopped.", ReplicationJobsProcessor.class.getSimpleName());
        }
    }
}
