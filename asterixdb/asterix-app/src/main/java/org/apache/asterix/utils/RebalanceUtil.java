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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.dataflow.data.nontagged.MissingWriterFactory;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataManagerUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.lock.LockList;
import org.apache.asterix.metadata.lock.MetadataLockManager;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.transaction.management.service.transaction.JobIdFactory;
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

/**
 * A utility class for the rebalance operation.
 */
public class RebalanceUtil {

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
            MetadataProvider metadataProvider, IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Dataset sourceDataset;
        Dataset targetDataset;
        // Generates the rebalance target files. While doing that, hold read locks on the dataset so
        // that no one can drop the rebalance source dataset.
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

            // Creates a node group for rebalance.
            String nodeGroupName = DatasetUtil.createNodeGroupForNewDataset(sourceDataset.getDataverseName(),
                    sourceDataset.getDatasetName(), sourceDataset.getRebalanceCount() + 1, targetNcNames,
                    metadataProvider);

            // The target dataset for rebalance.
            targetDataset = new Dataset(sourceDataset, true, nodeGroupName);

            // Rebalances the source dataset into the target dataset.
            rebalance(sourceDataset, targetDataset, metadataProvider, hcc);

            // Complete the metadata transaction.
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().reset();
        }

        // Starts another transaction for switching the metadata entity.
        mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            // Atomically switches the rebalance target to become the source dataset.
            rebalanceSwitch(sourceDataset, targetDataset, metadataProvider, hcc);

            // Complete the metadata transaction.
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().reset();
        }
    }

    // Rebalances from the source to the target.
    private static void rebalance(Dataset source, Dataset target, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc) throws Exception {
        // Creates the rebalance target.
        createRebalanceTarget(target, metadataProvider, hcc);

        // Populates the data from the rebalance source to the rebalance target.
        populateDataToRebalanceTarget(source, target, metadataProvider, hcc);

        // Creates and loads indexes for the rebalance target.
        createAndLoadSecondaryIndexesForTarget(source, target, metadataProvider, hcc);
    }

    private static void rebalanceSwitch(Dataset source, Dataset target, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();

        // Acquires the metadata write lock for the source/target dataset.
        writeLockDataset(metadataProvider.getLocks(), source);

        Dataset sourceDataset = MetadataManagerUtil.findDataset(mdTxnCtx, source.getDataverseName(),
                source.getDatasetName());
        if (sourceDataset == null) {
            // The dataset has already been dropped.
            // In this case, we should drop the generated target dataset files.
            dropDatasetFiles(target, metadataProvider, hcc);
            return;
        }

        // Drops the source dataset files.
        dropDatasetFiles(source, metadataProvider, hcc);

        // Updates the dataset entry in the metadata storage
        MetadataManager.INSTANCE.updateDataset(mdTxnCtx, target);

        // Drops the metadata entry of source dataset's node group.
        String sourceNodeGroup = source.getNodeGroupName();
        MetadataLockManager.INSTANCE.acquireNodeGroupWriteLock(metadataProvider.getLocks(), sourceNodeGroup);
        MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, sourceNodeGroup, true);
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
        JobId jobId = JobIdFactory.generateJobId();
        JobEventListenerFactory jobEventListenerFactory = new JobEventListenerFactory(jobId, true);
        spec.setJobletEventListenerFactory(jobEventListenerFactory);

        // The pipeline starter.
        IOperatorDescriptor starter = DatasetUtil.createDummyKeyProviderOp(spec, source, metadataProvider);

        // Creates primary index scan op.
        IOperatorDescriptor primaryScanOp = DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, source, jobId);

        // Creates secondary BTree upsert op.
        IOperatorDescriptor upsertOp = createPrimaryIndexUpsertOp(spec, metadataProvider, source, target);

        // The final commit operator.
        IOperatorDescriptor commitOp = createUpsertCommitOp(spec, metadataProvider, jobId, target);

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
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> upsertOpAndConstraints = DatasetUtil
                .createPrimaryIndexUpsertOp(spec, metadataProvider, target,
                        source.getPrimaryRecordDescriptor(metadataProvider), fieldPermutation,
                        MissingWriterFactory.INSTANCE);
        IOperatorDescriptor upsertOp = upsertOpAndConstraints.first;
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, upsertOp,
                upsertOpAndConstraints.second);
        return upsertOp;
    }

    // Creates the commit operator for populating the target dataset.
    private static IOperatorDescriptor createUpsertCommitOp(JobSpecification spec, MetadataProvider metadataProvider,
            JobId jobId, Dataset target) throws AlgebricksException {
        int numKeys = target.getPrimaryKeys().size();
        int[] primaryKeyFields = IntStream.range(0, numKeys).toArray();
        return new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] {
                        target.getCommitRuntimeFactory(metadataProvider, jobId, primaryKeyFields, true) },
                new RecordDescriptor[] { target.getPrimaryRecordDescriptor(metadataProvider) });
    }

    private static void dropDatasetFiles(Dataset dataset, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc) throws Exception {
        List<JobSpecification> jobs = new ArrayList<>();
        List<Index> indexes = metadataProvider.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName());
        for (Index index : indexes) {
            jobs.add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, dataset));
        }
        for (JobSpecification jobSpec : jobs) {
            JobUtils.runJob(hcc, jobSpec, true);
        }
    }

    // Acquires a read lock for the dataverse and a write lock for the dataset, in order to populate the dataset.
    private static void writeLockDataset(LockList locks, Dataset dataset) throws AsterixException {
        MetadataLockManager.INSTANCE.acquireDataverseReadLock(locks, dataset.getDataverseName());
        MetadataLockManager.INSTANCE.acquireDatasetWriteLock(locks,
                dataset.getDataverseName() + "." + dataset.getDatasetName());
    }

    // Creates and loads all secondary indexes for the rebalance target dataset.
    private static void createAndLoadSecondaryIndexesForTarget(Dataset source, Dataset target,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc) throws Exception {
        for (Index index : metadataProvider.getDatasetIndexes(source.getDataverseName(), source.getDatasetName())) {
            if (!index.isSecondaryIndex()) {
                continue;
            }
            // Creates the secondary index.
            JobSpecification indexCreationJobSpec = IndexUtil.buildSecondaryIndexCreationJobSpec(target, index,
                    metadataProvider);
            JobUtils.runJob(hcc, indexCreationJobSpec, true);

            // Loads the secondary index.
            JobSpecification indexLoadingJobSpec = IndexUtil.buildSecondaryIndexLoadingJobSpec(target, index,
                    metadataProvider);
            JobUtils.runJob(hcc, indexLoadingJobSpec, true);
        }
    }
}
