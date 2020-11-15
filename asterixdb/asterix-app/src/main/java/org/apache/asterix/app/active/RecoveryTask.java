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
package org.apache.asterix.app.active;

import java.util.concurrent.Callable;

import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IRetryPolicyFactory;
import org.apache.asterix.active.NoRetryPolicyFactory;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.metadata.IMetadataLockUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.IRetryPolicy;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RecoveryTask {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Level level = Level.INFO;
    protected final ActiveEntityEventsListener listener;
    private volatile boolean cancelRecovery = false;
    private final IRetryPolicyFactory retryPolicyFactory;
    protected final MetadataProvider metadataProvider;
    private final IClusterStateManager clusterStateManager;

    public RecoveryTask(ICcApplicationContext appCtx, ActiveEntityEventsListener listener,
            IRetryPolicyFactory retryPolicyFactory) {
        this.listener = listener;
        this.retryPolicyFactory = retryPolicyFactory;
        this.metadataProvider = MetadataProvider.create(appCtx, null);
        this.clusterStateManager = appCtx.getClusterStateManager();
    }

    public Callable<Void> recover() {
        if (retryPolicyFactory == NoRetryPolicyFactory.INSTANCE) {
            return () -> null;
        }
        IRetryPolicy policy = retryPolicyFactory.create(listener);
        return () -> {
            Thread.currentThread().setName("RecoveryTask (" + listener.getEntityId() + ")");
            doRecover(policy);
            return null;
        };
    }

    public void cancel() {
        cancelRecovery = true;
    }

    protected void resumeOrRecover(MetadataProvider metadataProvider) throws HyracksDataException {
        try {
            synchronized (listener) {
                listener.doResume(metadataProvider);
                listener.setState(ActivityState.RUNNING);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Attempt to resume " + listener.getEntityId() + " Failed", e);
            synchronized (listener) {
                if (listener.getState() == ActivityState.RESUMING) {
                    // This will be the case if compilation failure
                    // If the failure is a runtime failure, then the state
                    // would've been set to temporarily failed already
                    listener.setState(ActivityState.TEMPORARILY_FAILED);
                }
            }
            if (retryPolicyFactory == NoRetryPolicyFactory.INSTANCE) {
                synchronized (listener) {
                    if (!cancelRecovery) {
                        listener.setState(ActivityState.STOPPED);
                        listener.setRunning(metadataProvider, false);
                    }
                }
            } else {
                LOGGER.log(Level.WARN, "Submitting recovery task for " + listener.getEntityId());
                metadataProvider.getApplicationContext().getServiceContext().getControllerService().getExecutor()
                        .submit(() -> doRecover(retryPolicyFactory.create(listener)));
            }
            throw e;
        }
    }

    protected Void doRecover(IRetryPolicy policy) throws AlgebricksException, InterruptedException {
        LOGGER.log(level, "Actual Recovery task has started");
        Exception failure;
        do {
            synchronized (listener) {
                while (!cancelRecovery && !canStartRecovery()) {
                    listener.wait();
                }
                if (cancelRecovery) {
                    LOGGER.log(level, "Recovery has been cancelled");
                    return null;
                }
            }
            IMetadataLockManager lockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
            IMetadataLockUtil lockUtil = metadataProvider.getApplicationContext().getMetadataLockUtil();
            try {
                acquireRecoveryLocks(lockManager, lockUtil);
                synchronized (listener) {
                    try {
                        if (!cancelRecovery && listener.getState() == ActivityState.TEMPORARILY_FAILED) {
                            listener.setState(ActivityState.RECOVERING);
                            listener.doStart(metadataProvider);
                        }
                        LOGGER.log(level, "Recovery completed successfully");
                        return null;
                    } finally {
                        listener.notifyAll();
                    }
                }
            } catch (Exception e) {
                LOGGER.log(level, "Attempt to revive " + listener.getEntityId() + " failed", e);
                listener.setState(ActivityState.TEMPORARILY_FAILED);
                failure = e;
            } finally {
                releaseRecoveryLocks(metadataProvider);
            }
        } while (policy.retry(failure));
        // Recovery task is essntially over now either through failure or through cancellation(stop)
        synchronized (listener) {
            listener.notifyAll();
            if (listener.getState() != ActivityState.TEMPORARILY_FAILED
                    // Suspend can happen at the same time, the recovery policy decides to stop... in that case, we
                    // must still do two things:
                    // 1. set the state to permanent failure.
                    // 2. set the entity to not running to avoid auto recovery attempt
                    && listener.getState() != ActivityState.SUSPENDED) {
                LOGGER.log(level, "Recovery is cancelled because the current state {} is neither {} nor {}",
                        listener.getState(), ActivityState.TEMPORARILY_FAILED,
                        listener.getState() != ActivityState.SUSPENDED);
                return null;
            }
        }
        IMetadataLockManager lockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
        IMetadataLockUtil lockUtil = metadataProvider.getApplicationContext().getMetadataLockUtil();
        try {
            acquirePostRecoveryLocks(lockManager, lockUtil);
            synchronized (listener) {
                if (!cancelRecovery && listener.getState() == ActivityState.TEMPORARILY_FAILED) {
                    LOGGER.warn("Recovery for {} permanently failed", listener.getEntityId());
                    listener.setState(ActivityState.STOPPED);
                    listener.setRunning(metadataProvider, false);
                }
                listener.notifyAll();
            }
        } finally {
            releasePostRecoveryLocks();
        }
        return null;
    }

    protected void acquireRecoveryLocks(IMetadataLockManager lockManager, IMetadataLockUtil lockUtil)
            throws AlgebricksException {
        EntityId entityId = listener.getEntityId();
        lockManager.acquireActiveEntityWriteLock(metadataProvider.getLocks(), entityId.getDataverseName(),
                entityId.getEntityName());
        for (Dataset dataset : listener.getDatasets()) {
            lockManager.acquireDataverseReadLock(metadataProvider.getLocks(), dataset.getDataverseName());
            lockManager.acquireDatasetExclusiveModificationLock(metadataProvider.getLocks(), dataset.getDataverseName(),
                    dataset.getDatasetName());
        }
    }

    protected void releaseRecoveryLocks(MetadataProvider metadataProvider) {
        metadataProvider.getLocks().reset();
    }

    protected void acquirePostRecoveryLocks(IMetadataLockManager lockManager, IMetadataLockUtil lockUtil)
            throws AlgebricksException {
        EntityId entityId = listener.getEntityId();
        lockManager.acquireActiveEntityWriteLock(metadataProvider.getLocks(), entityId.getDataverseName(),
                entityId.getEntityName());
        for (Dataset dataset : listener.getDatasets()) {
            lockUtil.modifyDatasetBegin(lockManager, metadataProvider.getLocks(), dataset.getDataverseName(),
                    dataset.getDatasetName());
        }
    }

    protected void releasePostRecoveryLocks() {
        metadataProvider.getLocks().reset();
    }

    private boolean canStartRecovery() {
        return clusterStateManager.getState() == ClusterState.ACTIVE
                || clusterStateManager.getState() == ClusterState.REBALANCE_REQUIRED;
    }
}
