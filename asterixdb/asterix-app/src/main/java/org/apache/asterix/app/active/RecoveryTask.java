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
import org.apache.asterix.active.IRetryPolicy;
import org.apache.asterix.active.IRetryPolicyFactory;
import org.apache.asterix.active.NoRetryPolicyFactory;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataLockUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RecoveryTask {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Level level = Level.INFO;
    private final ActiveEntityEventsListener listener;
    private volatile boolean cancelRecovery = false;
    private final IRetryPolicyFactory retryPolicyFactory;
    private final MetadataProvider metadataProvider;
    private final IClusterStateManager clusterStateManager;
    private Exception failure;

    public RecoveryTask(ICcApplicationContext appCtx, ActiveEntityEventsListener listener,
            IRetryPolicyFactory retryPolicyFactory) {
        this.listener = listener;
        this.retryPolicyFactory = retryPolicyFactory;
        this.metadataProvider = new MetadataProvider(appCtx, null);
        this.clusterStateManager = appCtx.getClusterStateManager();
    }

    public Callable<Void> recover() {
        if (retryPolicyFactory == NoRetryPolicyFactory.INSTANCE) {
            return () -> {
                return null;
            };
        }
        IRetryPolicy policy = retryPolicyFactory.create(listener);
        return () -> {
            String nameBefore = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName("RecoveryTask (" + listener.getEntityId() + ")");
                doRecover(policy);
            } finally {
                Thread.currentThread().setName(nameBefore);
            }
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
                        listener.setState(ActivityState.PERMANENTLY_FAILED);
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

    protected Void doRecover(IRetryPolicy policy)
            throws AlgebricksException, HyracksDataException, InterruptedException {
        LOGGER.log(level, "Actual Recovery task has started");
        if (listener.getState() != ActivityState.TEMPORARILY_FAILED) {
            LOGGER.log(level, "but its state is not temp failure and so we're just returning");
            return null;
        }
        LOGGER.log(level, "calling the policy");
        while (policy.retry()) {
            synchronized (listener) {
                if (cancelRecovery) {
                    return null;
                }
                while (clusterStateManager.getState() != ClusterState.ACTIVE) {
                    if (cancelRecovery) {
                        return null;
                    }
                    wait();
                }
            }
            IMetadataLockManager lockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
            lockManager.acquireActiveEntityWriteLock(metadataProvider.getLocks(),
                    listener.getEntityId().getDataverse() + '.' + listener.getEntityId().getEntityName());
            for (Dataset dataset : listener.getDatasets()) {
                lockManager.acquireDataverseReadLock(metadataProvider.getLocks(), dataset.getDataverseName());
                lockManager.acquireDatasetExclusiveModificationLock(metadataProvider.getLocks(),
                        DatasetUtil.getFullyQualifiedName(dataset));
            }
            synchronized (listener) {
                try {
                    if (cancelRecovery) {
                        return null;
                    }
                    listener.setState(ActivityState.RECOVERING);
                    listener.doStart(metadataProvider);
                    return null;
                } catch (Exception e) {
                    LOGGER.log(level, "Attempt to revive " + listener.getEntityId() + " failed", e);
                    listener.setState(ActivityState.TEMPORARILY_FAILED);
                    failure = e;
                } finally {
                    metadataProvider.getLocks().reset();
                }
                listener.notifyAll();
            }
        }
        // Recovery task is essntially over now either through failure or through cancellation(stop)
        synchronized (listener) {
            listener.notifyAll();
            if (listener.getState() != ActivityState.TEMPORARILY_FAILED) {
                return null;
            }
        }
        IMetadataLockManager lockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
        try {
            lockManager.acquireActiveEntityWriteLock(metadataProvider.getLocks(),
                    listener.getEntityId().getDataverse() + '.' + listener.getEntityId().getEntityName());
            for (Dataset dataset : listener.getDatasets()) {
                MetadataLockUtil.modifyDatasetBegin(lockManager, metadataProvider.getLocks(), dataset.getDatasetName(),
                        DatasetUtil.getFullyQualifiedName(dataset));
            }
            synchronized (listener) {
                if (cancelRecovery) {
                    return null;
                }
                if (listener.getState() == ActivityState.TEMPORARILY_FAILED) {
                    listener.setState(ActivityState.PERMANENTLY_FAILED);
                }
                listener.notifyAll();
            }
        } finally {
            metadataProvider.getLocks().reset();
        }
        return null;
    }

    public Exception getFailure() {
        return failure;
    }
}
