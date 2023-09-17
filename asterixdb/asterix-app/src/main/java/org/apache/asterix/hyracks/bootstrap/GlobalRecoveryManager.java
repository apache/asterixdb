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
package org.apache.asterix.hyracks.bootstrap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.message.StorageCleanupRequestMessage;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.cluster.IGlobalRecoveryManager;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

public class GlobalRecoveryManager implements IGlobalRecoveryManager {

    private static final Logger LOGGER = LogManager.getLogger();
    protected final IStorageComponentProvider componentProvider;
    protected final ICCServiceContext serviceCtx;
    protected final IHyracksClientConnection hcc;
    protected volatile boolean recoveryCompleted;
    protected volatile boolean recovering;
    protected Future<?> recoveryFuture;

    public GlobalRecoveryManager(ICCServiceContext serviceCtx, IHyracksClientConnection hcc,
            IStorageComponentProvider componentProvider) {
        this.serviceCtx = serviceCtx;
        this.hcc = hcc;
        this.componentProvider = componentProvider;
    }

    @Override
    public Set<IClusterManagementWork> notifyNodeFailure(Collection<String> deadNodeIds) {
        return Collections.emptySet();
    }

    @Override
    public Set<IClusterManagementWork> notifyNodeJoin(String joinedNodeId) {
        return Collections.emptySet();
    }

    private void executeHyracksJob(JobSpecification spec) throws Exception {
        spec.setMaxReattempts(0);
        JobId jobId = hcc.startJob(spec);
        hcc.waitForCompletion(jobId);
    }

    @Override
    public void startGlobalRecovery(ICcApplicationContext appCtx) {
        if (!recoveryCompleted && !recovering) {
            synchronized (this) {
                if (!recovering) {
                    recovering = true;
                    /*
                     * Perform recovery on a different thread to avoid deadlocks in
                     * {@link org.apache.asterix.common.cluster.IClusterStateManager}
                     */
                    recoveryFuture = serviceCtx.getControllerService().getExecutor().submit(() -> {
                        try {
                            recover(appCtx);
                        } catch (Throwable e) {
                            try {
                                LOGGER.fatal("Global recovery failed. Shutting down...", e);
                            } catch (Throwable ignore) {
                                // ignoring exception trying to log, just do the halt
                            }
                            ExitUtil.exit(ExitUtil.EC_FAILED_TO_RECOVER);
                        }
                    });
                }
            }
        }
    }

    protected void recover(ICcApplicationContext appCtx) throws Exception {
        LOGGER.info("Starting Global Recovery");
        MetadataManager.INSTANCE.init();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        rollbackIncompleteAtomicTransactions(appCtx);
        if (appCtx.getStorageProperties().isStorageGlobalCleanup()) {
            int storageGlobalCleanupTimeout = appCtx.getStorageProperties().getStorageGlobalCleanupTimeout();
            performGlobalStorageCleanup(mdTxnCtx, storageGlobalCleanupTimeout);
        }
        mdTxnCtx = doRecovery(appCtx, mdTxnCtx);
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        recoveryCompleted = true;
        recovering = false;
        synchronized (this) {
            recoveryFuture = null;
        }
        LOGGER.info("Global Recovery Completed. Refreshing cluster state...");
        appCtx.getClusterStateManager().refreshState();
    }

    protected void rollbackIncompleteAtomicTransactions(ICcApplicationContext appCtx) throws Exception {
        appCtx.getGlobalTxManager().rollback();
    }

    protected void performGlobalStorageCleanup(MetadataTransactionContext mdTxnCtx, int storageGlobalCleanupTimeoutSecs)
            throws Exception {
        List<Dataverse> dataverses = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
        IntOpenHashSet validDatasetIds = new IntOpenHashSet();
        for (Dataverse dataverse : dataverses) {
            List<Dataset> dataverseDatasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx,
                    dataverse.getDatabaseName(), dataverse.getDataverseName());
            dataverseDatasets.stream().filter(DatasetUtil::isNotView).mapToInt(Dataset::getDatasetId)
                    .forEach(validDatasetIds::add);
        }
        ICcApplicationContext ccAppCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
        final List<String> ncs = new ArrayList<>(ccAppCtx.getClusterStateManager().getParticipantNodes());
        CCMessageBroker messageBroker = (CCMessageBroker) ccAppCtx.getServiceContext().getMessageBroker();
        long reqId = messageBroker.newRequestId();
        List<StorageCleanupRequestMessage> requests = new ArrayList<>();
        for (int i = 0; i < ncs.size(); i++) {
            requests.add(new StorageCleanupRequestMessage(reqId, validDatasetIds));
        }
        messageBroker.sendSyncRequestToNCs(reqId, ncs, requests,
                TimeUnit.SECONDS.toMillis(storageGlobalCleanupTimeoutSecs), false);
    }

    protected MetadataTransactionContext doRecovery(ICcApplicationContext appCtx, MetadataTransactionContext mdTxnCtx)
            throws Exception {
        // Loop over datasets
        for (Dataverse dataverse : MetadataManager.INSTANCE.getDataverses(mdTxnCtx)) {
            // Fixes ASTERIXDB-2386 by caching the dataverse during recovery
            MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse.getDatabaseName(), dataverse.getDataverseName());
        }
        return mdTxnCtx;
    }

    @Override
    public void notifyStateChange(ClusterState newState) {
        synchronized (this) {
            if (recovering && newState == ClusterState.UNUSABLE && recoveryFuture != null) {
                // interrupt the recovery attempt since cluster became unusable during global recovery
                recoveryFuture.cancel(true);
            }
        }
        if (newState != ClusterState.ACTIVE && newState != ClusterState.RECOVERING) {
            recoveryCompleted = false;
        }
    }

    @Override
    public boolean isRecoveryCompleted() {
        return recoveryCompleted;
    }

}
