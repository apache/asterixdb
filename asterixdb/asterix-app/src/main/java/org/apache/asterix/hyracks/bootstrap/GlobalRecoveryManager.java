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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.cluster.IGlobalRecoveryManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.config.DatasetConfig.TransactionState;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.ExternalIndexingOperations;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GlobalRecoveryManager implements IGlobalRecoveryManager {

    private static final Logger LOGGER = LogManager.getLogger();
    protected final IStorageComponentProvider componentProvider;
    protected final ICCServiceContext serviceCtx;
    protected final IHyracksClientConnection hcc;
    protected volatile boolean recoveryCompleted;
    protected volatile boolean recovering;

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
                    /**
                     * Perform recovery on a different thread to avoid deadlocks in
                     * {@link org.apache.asterix.common.cluster.IClusterStateManager}
                     */
                    serviceCtx.getControllerService().getExecutor().submit(() -> {
                        try {
                            recover(appCtx);
                        } catch (HyracksDataException e) {
                            LOGGER.log(Level.ERROR, "Global recovery failed. Shutting down...", e);
                            ExitUtil.exit(ExitUtil.EC_FAILED_TO_RECOVER);
                        }
                    });
                }
            }
        }
    }

    protected void recover(ICcApplicationContext appCtx) throws HyracksDataException {
        try {
            LOGGER.info("Starting Global Recovery");
            MetadataManager.INSTANCE.init();
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            mdTxnCtx = doRecovery(appCtx, mdTxnCtx);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            recoveryCompleted = true;
            recovering = false;
            LOGGER.info("Global Recovery Completed. Refreshing cluster state...");
            appCtx.getClusterStateManager().refreshState();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    protected MetadataTransactionContext doRecovery(ICcApplicationContext appCtx, MetadataTransactionContext mdTxnCtx)
            throws Exception {
        // Loop over datasets
        for (Dataverse dataverse : MetadataManager.INSTANCE.getDataverses(mdTxnCtx)) {
            mdTxnCtx = recoverDatasets(appCtx, mdTxnCtx, dataverse);
            // Fixes ASTERIXDB-2386 by caching the dataverse during recovery
            MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse.getDataverseName());
        }
        return mdTxnCtx;
    }

    @Override
    public void notifyStateChange(ClusterState newState) {
        if (newState != ClusterState.ACTIVE && newState != ClusterState.RECOVERING) {
            recoveryCompleted = false;
        }
    }

    private MetadataTransactionContext recoverDatasets(ICcApplicationContext appCtx,
            MetadataTransactionContext mdTxnCtx, Dataverse dataverse) throws Exception {
        if (!dataverse.getDataverseName().equals(MetadataConstants.METADATA_DATAVERSE_NAME)) {
            MetadataProvider metadataProvider = new MetadataProvider(appCtx, dataverse);
            try {
                List<Dataset> datasets =
                        MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dataverse.getDataverseName());
                for (Dataset dataset : datasets) {
                    if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
                        // External dataset
                        // Get indexes
                        List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx,
                                dataset.getDataverseName(), dataset.getDatasetName());
                        // Get the state of the dataset
                        ExternalDatasetDetails dsd = (ExternalDatasetDetails) dataset.getDatasetDetails();
                        TransactionState datasetState = dsd.getState();
                        if (!indexes.isEmpty()) {
                            if (datasetState == TransactionState.BEGIN) {
                                List<ExternalFile> files =
                                        MetadataManager.INSTANCE.getDatasetExternalFiles(mdTxnCtx, dataset);
                                // if persumed abort, roll backward
                                // 1. delete all pending files
                                for (ExternalFile file : files) {
                                    if (file.getPendingOp() != ExternalFilePendingOp.NO_OP) {
                                        MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                                    }
                                }
                            }
                            // 2. clean artifacts in NCs
                            metadataProvider.setMetadataTxnContext(mdTxnCtx);
                            JobSpecification jobSpec =
                                    ExternalIndexingOperations.buildAbortOp(dataset, indexes, metadataProvider);
                            executeHyracksJob(jobSpec);
                            // 3. correct the dataset state
                            ((ExternalDatasetDetails) dataset.getDatasetDetails()).setState(TransactionState.COMMIT);
                            MetadataManager.INSTANCE.updateDataset(mdTxnCtx, dataset);
                            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                        } else if (datasetState == TransactionState.READY_TO_COMMIT) {
                            List<ExternalFile> files =
                                    MetadataManager.INSTANCE.getDatasetExternalFiles(mdTxnCtx, dataset);
                            // if ready to commit, roll forward
                            // 1. commit indexes in NCs
                            metadataProvider.setMetadataTxnContext(mdTxnCtx);
                            JobSpecification jobSpec =
                                    ExternalIndexingOperations.buildRecoverOp(dataset, indexes, metadataProvider);
                            executeHyracksJob(jobSpec);
                            // 2. add pending files in metadata
                            for (ExternalFile file : files) {
                                if (file.getPendingOp() == ExternalFilePendingOp.ADD_OP) {
                                    MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                                    file.setPendingOp(ExternalFilePendingOp.NO_OP);
                                    MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
                                } else if (file.getPendingOp() == ExternalFilePendingOp.DROP_OP) {
                                    // find original file
                                    for (ExternalFile originalFile : files) {
                                        if (originalFile.getFileName().equals(file.getFileName())) {
                                            MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                                            MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, originalFile);
                                            break;
                                        }
                                    }
                                } else if (file.getPendingOp() == ExternalFilePendingOp.APPEND_OP) {
                                    // find original file
                                    for (ExternalFile originalFile : files) {
                                        if (originalFile.getFileName().equals(file.getFileName())) {
                                            MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                                            MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, originalFile);
                                            originalFile.setSize(file.getSize());
                                            MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, originalFile);
                                        }
                                    }
                                }
                                // 3. correct the dataset state
                                ((ExternalDatasetDetails) dataset.getDatasetDetails())
                                        .setState(TransactionState.COMMIT);
                                MetadataManager.INSTANCE.updateDataset(mdTxnCtx, dataset);
                                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                            }
                        }
                    }
                }
            } finally {
                metadataProvider.getLocks().unlock();
            }
        }
        return mdTxnCtx;
    }

    @Override
    public boolean isRecoveryCompleted() {
        return recoveryCompleted;
    }

}
