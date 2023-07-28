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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.common.replication.IReplicationDestination;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.replication.api.ReplicationDestination;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.IReplicationJob;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexReplicationJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexReplicationManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private final IReplicationManager replicationManager;
    private final Set<ReplicationDestination> destinations = ConcurrentHashMap.newKeySet();
    private final LinkedBlockingQueue<IReplicationJob> replicationJobsQ = new LinkedBlockingQueue<>();
    private final IReplicationStrategy replicationStrategy;
    private final PersistentLocalResourceRepository resourceRepository;
    private final INcApplicationContext appCtx;
    private final ILSMIOOperationScheduler ioScheduler;
    private final Object transferLock = new Object();
    private final Set<ReplicationDestination> failedDest = new HashSet<>();
    private final AtomicInteger pendingRepOpsCount = new AtomicInteger();

    public IndexReplicationManager(INcApplicationContext appCtx, IReplicationManager replicationManager) {
        this.appCtx = appCtx;
        this.replicationManager = replicationManager;
        this.resourceRepository = (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        replicationStrategy = replicationManager.getReplicationStrategy();
        appCtx.getThreadExecutor().execute(new ReplicationJobsProcessor());
        ioScheduler = appCtx.getStorageComponentProvider().getIoOperationSchedulerProvider()
                .getIoScheduler(appCtx.getServiceContext());
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
            for (ReplicationDestination existingDest : destinations) {
                if (existingDest.equals(dest)) {
                    existingDest.closeConnections();
                    break;
                }
            }
            destinations.remove(dest);
            failedDest.remove(dest);
        }
    }

    public void handleFailure(ReplicationDestination dest, Exception e) {
        synchronized (transferLock) {
            if (failedDest.contains(dest)) {
                return;
            }
            LOGGER.debug("Replica failed", e);
            if (destinations.contains(dest)) {
                LOGGER.error("replica at {} failed", dest);
                failedDest.add(dest);
            }
            dest.closeConnections();
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

    public Set<ReplicationDestination> getDestinations() {
        synchronized (transferLock) {
            return destinations;
        }
    }

    private void process(IReplicationJob job) {
        pendingRepOpsCount.incrementAndGet();
        Optional<DatasetResourceReference> jobIndexRefOpt = getJobIndexRef(job);
        if (jobIndexRefOpt.isEmpty()) {
            LOGGER.warn("skipping replication of {} due to missing dataset resource reference", job.getAnyFile());
            afterReplication(job);
            return;
        }
        ReplicationOperation rp = new ReplicationOperation(appCtx, jobIndexRefOpt.get(), job, this);
        if (job.getExecutionType() == IReplicationJob.ReplicationExecutionType.SYNC) {
            rp.call();
        } else {
            try {
                ioScheduler.scheduleOperation(rp);
            } catch (HyracksDataException e) {
                throw new ReplicationException(e);
            }
        }
    }

    public boolean skip(DatasetResourceReference indexRef) {
        return !replicationStrategy.isMatch(indexRef.getDatasetId());
    }

    public Optional<DatasetResourceReference> getJobIndexRef(IReplicationJob job) {
        final String fileToReplicate = job.getAnyFile();
        try {
            return resourceRepository.getLocalResourceReference(fileToReplicate);
        } catch (HyracksDataException e) {
            throw new IllegalStateException("Couldn't find resource for " + job.getAnyFile(), e);
        }
    }

    private void closeChannels() {
        LOGGER.trace("no pending replication jobs; closing connections to replicas");
        for (ReplicationDestination dest : destinations) {
            dest.closeConnections();
        }
    }

    public void afterReplication(IReplicationJob job) {
        try {
            int pendingOps = pendingRepOpsCount.decrementAndGet();
            if (job.getOperation() == IReplicationJob.ReplicationOperation.REPLICATE
                    && job instanceof ILSMIndexReplicationJob) {
                ((ILSMIndexReplicationJob) job).endReplication();
            }
            if (pendingOps == 0 && replicationJobsQ.isEmpty()) {
                closeChannels();
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
