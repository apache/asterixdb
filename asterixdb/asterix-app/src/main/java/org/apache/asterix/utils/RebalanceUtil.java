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
package org.apache.asterix.utils;

import static org.apache.asterix.app.translator.QueryTranslator.abort;
import static org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.dataflow.data.nontagged.MissingWriterFactory;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.IActiveEntityController;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.rebalance.IDatasetRebalanceCallback;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A utility class for the rebalance operation.
 */
public class RebalanceUtil {
    private static final Logger LOGGER = LogManager.getLogger();

    private RebalanceUtil() {

    }

    /**
     * Rebalances an existing dataset to a list of target nodes.
     *
     * @param dataverseName,
     *            the dataverse name.
     * @param datasetName,
     *            the dataset name.
     * @param targetNcNames,
     *            the list of target nodes.
     * @param metadataProvider,
     *            the metadata provider.
     * @param hcc,
     *            the reusable hyracks connection.
     * @throws Exception
     */
    public static void rebalance(String dataverseName, String datasetName, Set<String> targetNcNames,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc,
            IDatasetRebalanceCallback datasetRebalanceCallback) throws Exception {
        Dataset sourceDataset;
        Dataset targetDataset;
        // Executes the first Metadata transaction.
        // Generates the rebalance target files. While doing that, hold read locks on the dataset so
        // that no one can drop the rebalance source dataset.
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            // The source dataset.
            sourceDataset = metadataProvider.findDataset(dataverseName, datasetName);

            // If the source dataset doesn't exist, then it's a no-op.
            if (sourceDataset == null) {
                return;
            }

            Set<String> sourceNodes = new HashSet<>(metadataProvider.findNodes(sourceDataset.getNodeGroupName()));

            // The the source nodes are identical to the target nodes.
            if (sourceNodes.equals(targetNcNames)) {
                return;
            }

            if (!targetNcNames.isEmpty()) {
                // Creates a node group for rebalance.
                String nodeGroupName = DatasetUtil.createNodeGroupForNewDataset(sourceDataset.getDataverseName(),
                        sourceDataset.getDatasetName(), sourceDataset.getRebalanceCount() + 1, targetNcNames,
                        metadataProvider);
                // The target dataset for rebalance.
                targetDataset = sourceDataset.getTargetDatasetForRebalance(nodeGroupName);

                LOGGER.info("Rebalancing dataset {} from node group {} with nodes {} to node group {} with nodes {}",
                        sourceDataset.getDatasetName(), sourceDataset.getNodeGroupName(), sourceNodes,
                        targetDataset.getNodeGroupName(), targetNcNames);
                // Rebalances the source dataset into the target dataset.
                rebalance(sourceDataset, targetDataset, metadataProvider, hcc, datasetRebalanceCallback);
            } else {
                targetDataset = null;
                // if this the last NC in the cluster, just drop the dataset
                purgeDataset(sourceDataset, metadataProvider, hcc);
            }
            // Complete the metadata transaction.
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }

        if (targetNcNames.isEmpty()) {
            // Nothing else to do since the dataset was dropped.
            return;
        }
        // Up to this point, since the bulk part of a rebalance operation is done,
        // the following two operations will retry after interrupt and finally rethrow InterruptedException,
        // which means that they will always succeed and could possibly throw InterruptedException as the last step.
        // TODO(yingyi): ASTERIXDB-1948, in case a crash happens, currently the system will either:
        // 1. (crash before metadata switch) think the rebalance is not done, and the target data files are leaked until
        // the next rebalance request.
        // 2. (crash after metadata switch) think the rebalance is done, and the source data files are leaked;
        runWithRetryAfterInterrupt(() -> {
            // Executes the 2nd Metadata transaction for switching the metadata entity.
            // It detaches the source dataset and attaches the target dataset to metadata's point of view.
            runMetadataTransaction(metadataProvider,
                    () -> rebalanceSwitch(sourceDataset, targetDataset, metadataProvider));
            // Executes the 3rd Metadata transaction to drop the source dataset files and the node group for
            // the source dataset.
            runMetadataTransaction(metadataProvider, () -> dropSourceDataset(sourceDataset, metadataProvider, hcc));
        });
        LOGGER.info("Dataset {} rebalance completed successfully", datasetName);
    }

    @FunctionalInterface
    private interface Work {
        void run() throws Exception;
    }

    // Runs works.run() and lets it sustain interrupts.
    private static void runWithRetryAfterInterrupt(Work work) throws Exception {
        int retryCount = 0;
        InterruptedException interruptedException = null;
        boolean done = false;
        do {
            try {
                work.run();
                done = true;
            } catch (Exception e) {
                Throwable rootCause = ExceptionUtils.getRootCause(e);
                if (rootCause instanceof java.lang.InterruptedException) {
                    interruptedException = (InterruptedException) rootCause;
                    // clear the interrupted state from the thread
                    Thread.interrupted();
                    LOGGER.log(Level.WARN, "Retry with attempt " + (++retryCount), e);
                    continue;
                }
                throw e;
            }
        } while (!done);

        // Rethrows the interrupted exception.
        if (interruptedException != null) {
            throw interruptedException;
        }
    }

    // Executes a metadata transaction.
    private static void runMetadataTransaction(MetadataProvider metadataProvider, Work work) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            // Performs the actual work.
            work.run();
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    // Rebalances from the source to the target.
    private static void rebalance(Dataset source, Dataset target, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IDatasetRebalanceCallback datasetRebalanceCallback) throws Exception {
        // Drops the target dataset files (if any) to make rebalance idempotent.
        dropDatasetFiles(target, metadataProvider, hcc);

        // Performs the specified operation before the target dataset is populated.
        datasetRebalanceCallback.beforeRebalance(metadataProvider, source, target, hcc);

        // Creates the rebalance target.
        createRebalanceTarget(target, metadataProvider, hcc);

        // Populates the data from the rebalance source to the rebalance target.
        populateDataToRebalanceTarget(source, target, metadataProvider, hcc);

        // Creates and loads indexes for the rebalance target.
        createAndLoadSecondaryIndexesForTarget(source, target, metadataProvider, hcc);

        // Performs the specified operation after the target dataset is populated.
        datasetRebalanceCallback.afterRebalance(metadataProvider, source, target, hcc);
    }

    // Switches the metadata entity from the source dataset to the target dataset.
    private static void rebalanceSwitch(Dataset source, Dataset target, MetadataProvider metadataProvider)
            throws AlgebricksException, RemoteException {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        // upgrade lock
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        ActiveNotificationHandler activeNotificationHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        IMetadataLockManager lockManager = appCtx.getMetadataLockManager();
        lockManager.upgradeDatasetLockToWrite(metadataProvider.getLocks(), DatasetUtil.getFullyQualifiedName(source));
        LOGGER.info("Updating dataset {} node group from {} to {}", source.getDatasetName(), source.getNodeGroupName(),
                target.getNodeGroupName());
        try {
            // Updates the dataset entry in the metadata storage
            MetadataManager.INSTANCE.updateDataset(mdTxnCtx, target);
            for (IActiveEntityEventsListener listener : activeNotificationHandler.getEventListeners()) {
                if (listener instanceof IActiveEntityController) {
                    IActiveEntityController controller = (IActiveEntityController) listener;
                    controller.replace(target);
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            LOGGER.info("dataset {} node group updated to {}", target.getDatasetName(), target.getNodeGroupName());
        } finally {
            lockManager.downgradeDatasetLockToExclusiveModify(metadataProvider.getLocks(),
                    DatasetUtil.getFullyQualifiedName(target));
        }
    }

    // Drops the source dataset.
    private static void dropSourceDataset(Dataset source, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc) throws Exception {
        // Drops the source dataset files. No need to lock the dataset entity here because the source dataset has
        // been detached at this point.
        dropDatasetFiles(source, metadataProvider, hcc);
        tryDropDatasetNodegroup(source, metadataProvider);
        MetadataManager.INSTANCE.commitTransaction(metadataProvider.getMetadataTxnContext());
    }

    // Drops the metadata entry of source dataset's node group.
    private static void tryDropDatasetNodegroup(Dataset source, MetadataProvider metadataProvider) throws Exception {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        String sourceNodeGroup = source.getNodeGroupName();
        appCtx.getMetadataLockManager().acquireNodeGroupWriteLock(metadataProvider.getLocks(), sourceNodeGroup);
        MetadataManager.INSTANCE.dropNodegroup(metadataProvider.getMetadataTxnContext(), sourceNodeGroup, true);
    }

    // Creates the files for the rebalance target dataset.
    private static void createRebalanceTarget(Dataset target, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc) throws Exception {
        JobSpecification spec = DatasetUtil.createDatasetJobSpec(target, metadataProvider);
        JobUtils.runJob(hcc, spec, true);
    }

    // Populates the data from the source dataset to the rebalance target dataset.
    private static void populateDataToRebalanceTarget(Dataset source, Dataset target, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc) throws Exception {
        JobSpecification spec = new JobSpecification();
        TxnId txnId = metadataProvider.getTxnIdFactory().create();
        JobEventListenerFactory jobEventListenerFactory = new JobEventListenerFactory(txnId, true);
        spec.setJobletEventListenerFactory(jobEventListenerFactory);

        // The pipeline starter.
        IOperatorDescriptor starter = DatasetUtil.createDummyKeyProviderOp(spec, source, metadataProvider);

        // Creates primary index scan op.
        IOperatorDescriptor primaryScanOp = DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, source);

        // Creates secondary BTree upsert op.
        IOperatorDescriptor upsertOp = createPrimaryIndexUpsertOp(spec, metadataProvider, source, target);

        // The final commit operator.
        IOperatorDescriptor commitOp = createUpsertCommitOp(spec, metadataProvider, target);

        // Connects empty-tuple-source and scan.
        spec.connect(new OneToOneConnectorDescriptor(spec), starter, 0, primaryScanOp, 0);

        // Connects scan and upsert.
        int numKeys = target.getPrimaryKeys().size();
        int[] keys = IntStream.range(0, numKeys).toArray();
        IConnectorDescriptor connectorDescriptor = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keys, target.getPrimaryHashFunctionFactories(metadataProvider)));
        spec.connect(connectorDescriptor, primaryScanOp, 0, upsertOp, 0);

        // Connects upsert and sink.
        spec.connect(new OneToOneConnectorDescriptor(spec), upsertOp, 0, commitOp, 0);

        // Executes the job.
        JobUtils.runJob(hcc, spec, true);
    }

    // Creates the primary index upsert operator for populating the target dataset.
    private static IOperatorDescriptor createPrimaryIndexUpsertOp(JobSpecification spec,
            MetadataProvider metadataProvider, Dataset source, Dataset target) throws AlgebricksException {
        int numKeys = source.getPrimaryKeys().size();
        int numValues = source.hasMetaPart() ? 2 : 1;
        int[] fieldPermutation = IntStream.range(0, numKeys + numValues).toArray();
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> upsertOpAndConstraints =
                DatasetUtil.createPrimaryIndexUpsertOp(spec, metadataProvider, target,
                        source.getPrimaryRecordDescriptor(metadataProvider), fieldPermutation,
                        MissingWriterFactory.INSTANCE);
        IOperatorDescriptor upsertOp = upsertOpAndConstraints.first;
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, upsertOp,
                upsertOpAndConstraints.second);
        return upsertOp;
    }

    // Creates the commit operator for populating the target dataset.
    private static IOperatorDescriptor createUpsertCommitOp(JobSpecification spec, MetadataProvider metadataProvider,
            Dataset target) throws AlgebricksException {
        int[] primaryKeyFields = getPrimaryKeyPermutationForUpsert(target);
        return new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { target.getCommitRuntimeFactory(metadataProvider, primaryKeyFields, true) },
                new RecordDescriptor[] { target.getPrimaryRecordDescriptor(metadataProvider) });
    }

    // Drops dataset files of a given dataset.
    private static void dropDatasetFiles(Dataset dataset, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc) throws Exception {
        List<JobSpecification> jobs = new ArrayList<>();
        List<Index> indexes = metadataProvider.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName());
        for (Index index : indexes) {
            jobs.add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, dataset,
                    EnumSet.of(DropOption.IF_EXISTS, DropOption.WAIT_ON_IN_USE), null));
        }
        for (JobSpecification jobSpec : jobs) {
            JobUtils.runJob(hcc, jobSpec, true);
        }
    }

    // Creates and loads all secondary indexes for the rebalance target dataset.
    private static void createAndLoadSecondaryIndexesForTarget(Dataset source, Dataset target,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc) throws Exception {
        for (Index index : metadataProvider.getDatasetIndexes(source.getDataverseName(), source.getDatasetName())) {
            if (!index.isSecondaryIndex()) {
                continue;
            }
            // Creates the secondary index.
            JobSpecification indexCreationJobSpec =
                    IndexUtil.buildSecondaryIndexCreationJobSpec(target, index, metadataProvider, null);
            JobUtils.runJob(hcc, indexCreationJobSpec, true);

            // Loads the secondary index.
            JobSpecification indexLoadingJobSpec =
                    IndexUtil.buildSecondaryIndexLoadingJobSpec(target, index, metadataProvider, null);
            JobUtils.runJob(hcc, indexLoadingJobSpec, true);
        }
    }

    // Gets the primary key permutation for upserts.
    private static int[] getPrimaryKeyPermutationForUpsert(Dataset dataset) {
        // upsertIndicatorVar + prev record
        int f = 2;
        // add the previous meta second
        if (dataset.hasMetaPart()) {
            f++;
        }
        // add the previous filter third
        int numFilterFields = DatasetUtil.getFilterField(dataset) == null ? 0 : 1;
        if (numFilterFields > 0) {
            f++;
        }
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int[] pkIndexes = new int[numPrimaryKeys];
        for (int i = 0; i < pkIndexes.length; i++) {
            pkIndexes[i] = f;
            f++;
        }
        return pkIndexes;
    }

    private static void purgeDataset(Dataset dataset, MetadataProvider metadataProvider, IHyracksClientConnection hcc)
            throws Exception {
        runWithRetryAfterInterrupt(() -> {
            // drop dataset files
            dropDatasetFiles(dataset, metadataProvider, hcc);

            // drop dataset entry from metadata
            runMetadataTransaction(metadataProvider,
                    () -> MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(),
                            dataset.getDataverseName(), dataset.getDatasetName()));
            MetadataManager.INSTANCE.commitTransaction(metadataProvider.getMetadataTxnContext());
            // try to drop the dataset's node group
            runMetadataTransaction(metadataProvider, () -> tryDropDatasetNodegroup(dataset, metadataProvider));
            MetadataManager.INSTANCE.commitTransaction(metadataProvider.getMetadataTxnContext());
        });
    }
}
