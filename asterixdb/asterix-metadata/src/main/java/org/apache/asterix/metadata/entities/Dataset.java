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

package org.apache.asterix.metadata.entities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveLifecycleListener;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.NoOpFrameOperationCallbackFactory;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeWithBuddyIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMInvertedIndexIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMRTreeIOOperationCallbackFactory;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.common.utils.JobUtils.ProgressState;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.metadata.declared.BTreeResourceFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.lock.ExternalDatasetsRegistry;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.ExternalIndexingOperations;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.metadata.utils.InvertedIndexResourceFactoryProvider;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.metadata.utils.RTreeResourceFactoryProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback.Operation;
import org.apache.asterix.transaction.management.opcallbacks.LockThenSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexInstantSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.TempDatasetPrimaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.TempDatasetSecondaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.UpsertOperationCallbackFactory;
import org.apache.asterix.transaction.management.resource.DatasetLocalResourceFactory;
import org.apache.asterix.transaction.management.runtime.CommitRuntimeFactory;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.common.IResourceFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Metadata describing a dataset.
 */
public class Dataset implements IMetadataEntity<Dataset>, IDataset {

    /*
     * Constants
     */
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(Dataset.class.getName());
    //TODO: Remove Singletons
    private static final BTreeResourceFactoryProvider bTreeResourceFactoryProvider =
            BTreeResourceFactoryProvider.INSTANCE;
    private static final RTreeResourceFactoryProvider rTreeResourceFactoryProvider =
            RTreeResourceFactoryProvider.INSTANCE;
    private static final InvertedIndexResourceFactoryProvider invertedIndexResourceFactoryProvider =
            InvertedIndexResourceFactoryProvider.INSTANCE;
    /*
     * Members
     */
    private final int datasetId;
    private final String dataverseName;
    private final String datasetName;
    private final String recordTypeDataverseName;
    private final String recordTypeName;
    private final String nodeGroupName;
    private final String compactionPolicyFactory;
    private final Map<String, String> hints;
    private final Map<String, String> compactionPolicyProperties;
    private final DatasetType datasetType;
    private final IDatasetDetails datasetDetails;
    private final String metaTypeDataverseName;
    private final String metaTypeName;
    private int pendingOp;
    /*
     * Transient (For caching)
     */

    public Dataset(String dataverseName, String datasetName, String recordTypeDataverseName, String recordTypeName,
            String nodeGroupName, String compactionPolicy, Map<String, String> compactionPolicyProperties,
            IDatasetDetails datasetDetails, Map<String, String> hints, DatasetType datasetType, int datasetId,
            int pendingOp) {
        this(dataverseName, datasetName, recordTypeDataverseName, recordTypeName, /*metaTypeDataverseName*/null,
                /*metaTypeName*/null, nodeGroupName, compactionPolicy, compactionPolicyProperties, datasetDetails,
                hints, datasetType, datasetId, pendingOp);
    }

    public Dataset(String dataverseName, String datasetName, String itemTypeDataverseName, String itemTypeName,
            String metaItemTypeDataverseName, String metaItemTypeName, String nodeGroupName, String compactionPolicy,
            Map<String, String> compactionPolicyProperties, IDatasetDetails datasetDetails, Map<String, String> hints,
            DatasetType datasetType, int datasetId, int pendingOp) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.recordTypeName = itemTypeName;
        this.recordTypeDataverseName = itemTypeDataverseName;
        this.metaTypeDataverseName = metaItemTypeDataverseName;
        this.metaTypeName = metaItemTypeName;
        this.nodeGroupName = nodeGroupName;
        this.compactionPolicyFactory = compactionPolicy;
        this.compactionPolicyProperties = compactionPolicyProperties;
        this.datasetType = datasetType;
        this.datasetDetails = datasetDetails;
        this.datasetId = datasetId;
        this.pendingOp = pendingOp;
        this.hints = hints;
    }

    public Dataset(Dataset dataset) {
        this(dataset.dataverseName, dataset.datasetName, dataset.recordTypeDataverseName, dataset.recordTypeName,
                dataset.metaTypeDataverseName, dataset.metaTypeName, dataset.nodeGroupName,
                dataset.compactionPolicyFactory, dataset.compactionPolicyProperties, dataset.datasetDetails,
                dataset.hints, dataset.datasetType, dataset.datasetId, dataset.pendingOp);
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getItemTypeName() {
        return recordTypeName;
    }

    public String getItemTypeDataverseName() {
        return recordTypeDataverseName;
    }

    public String getNodeGroupName() {
        return nodeGroupName;
    }

    public String getCompactionPolicy() {
        return compactionPolicyFactory;
    }

    public Map<String, String> getCompactionPolicyProperties() {
        return compactionPolicyProperties;
    }

    public DatasetType getDatasetType() {
        return datasetType;
    }

    public IDatasetDetails getDatasetDetails() {
        return datasetDetails;
    }

    public Map<String, String> getHints() {
        return hints;
    }

    public int getDatasetId() {
        return datasetId;
    }

    public int getPendingOp() {
        return pendingOp;
    }

    public String getMetaItemTypeDataverseName() {
        return metaTypeDataverseName;
    }

    public String getMetaItemTypeName() {
        return metaTypeName;
    }

    public boolean hasMetaPart() {
        return metaTypeDataverseName != null && metaTypeName != null;
    }

    public void setPendingOp(int pendingOp) {
        this.pendingOp = pendingOp;
    }

    @Override
    public Dataset addToCache(MetadataCache cache) {
        return cache.addDatasetIfNotExists(this);
    }

    @Override
    public Dataset dropFromCache(MetadataCache cache) {
        return cache.dropDataset(this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Dataset)) {
            return false;
        }
        Dataset otherDataset = (Dataset) other;
        return Objects.equals(dataverseName, otherDataset.dataverseName)
                && Objects.equals(datasetName, otherDataset.datasetName);
    }

    public boolean allow(ILogicalOperator topOp, byte operation) {//NOSONAR: this method is meant to be extended
        return !hasMetaPart();
    }

    /**
     * Drop this dataset
     *
     * @param metadataProvider
     *            metadata provider that can be used to get metadata info and runtimes
     * @param mdTxnCtx
     *            the transaction context
     * @param jobsToExecute
     *            a list of jobs to be executed as part of the drop operation
     * @param bActiveTxn
     *            whether the metadata transaction is ongoing
     * @param progress
     *            a mutable progress state used for error handling during the drop operation
     * @param hcc
     *            a client connection to hyracks master for job execution
     * @throws Exception
     *             if an error occur during the drop process or if the dataset can't be dropped for any reason
     */
    public void drop(MetadataProvider metadataProvider, MutableObject<MetadataTransactionContext> mdTxnCtx,
            List<JobSpecification> jobsToExecute, MutableBoolean bActiveTxn, MutableObject<ProgressState> progress,
            IHyracksClientConnection hcc) throws Exception {
        Map<FeedConnectionId, Pair<JobSpecification, Boolean>> disconnectJobList = new HashMap<>();
        if (getDatasetType() == DatasetType.INTERNAL) {
            // prepare job spec(s) that would disconnect any active feeds involving the dataset.
            ActiveLifecycleListener activeListener =
                    (ActiveLifecycleListener) metadataProvider.getApplicationContext().getActiveLifecycleListener();
            IActiveEntityEventsListener[] activeListeners = activeListener.getNotificationHandler().getEventListeners();
            for (IActiveEntityEventsListener listener : activeListeners) {
                if (listener.isEntityUsingDataset(this)) {
                    throw new CompilationException(ErrorCode.COMPILATION_CANT_DROP_ACTIVE_DATASET,
                            RecordUtil.toFullyQualifiedName(dataverseName, datasetName),
                            listener.getEntityId().toString());
                }
            }
            // #. prepare jobs to drop the datatset and the indexes in NC
            List<Index> indexes =
                    MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx.getValue(), dataverseName, datasetName);
            for (int j = 0; j < indexes.size(); j++) {
                if (indexes.get(j).isSecondaryIndex()) {
                    jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(indexes.get(j), metadataProvider, this));
                }
            }
            jobsToExecute.add(DatasetUtil.dropDatasetJobSpec(this, metadataProvider));
            // #. mark the existing dataset as PendingDropOp
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx.getValue(),
                    new Dataset(dataverseName, datasetName, getItemTypeDataverseName(), getItemTypeName(),
                            getMetaItemTypeDataverseName(), getMetaItemTypeName(), getNodeGroupName(),
                            getCompactionPolicy(), getCompactionPolicyProperties(), getDatasetDetails(), getHints(),
                            getDatasetType(), getDatasetId(), MetadataUtil.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
            bActiveTxn.setValue(false);
            progress.setValue(ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA);

            // # disconnect the feeds
            for (Pair<JobSpecification, Boolean> p : disconnectJobList.values()) {
                JobUtils.runJob(hcc, p.first, true);
            }

            // #. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                JobUtils.runJob(hcc, jobSpec, true);
            }

            mdTxnCtx.setValue(MetadataManager.INSTANCE.beginTransaction());
            bActiveTxn.setValue(true);
            metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
        } else {
            // External dataset
            ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(this);
            // #. prepare jobs to drop the datatset and the indexes in NC
            List<Index> indexes =
                    MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx.getValue(), dataverseName, datasetName);
            for (int j = 0; j < indexes.size(); j++) {
                if (ExternalIndexingOperations.isFileIndex(indexes.get(j))) {
                    jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(indexes.get(j), metadataProvider, this));
                } else {
                    jobsToExecute.add(DatasetUtil.buildDropFilesIndexJobSpec(metadataProvider, this));
                }
            }

            // #. mark the existing dataset as PendingDropOp
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx.getValue(),
                    new Dataset(dataverseName, datasetName, getItemTypeDataverseName(), getItemTypeName(),
                            getNodeGroupName(), getCompactionPolicy(), getCompactionPolicyProperties(),
                            getDatasetDetails(), getHints(), getDatasetType(), getDatasetId(),
                            MetadataUtil.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
            bActiveTxn.setValue(false);
            progress.setValue(ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA);

            // #. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                JobUtils.runJob(hcc, jobSpec, true);
            }
            if (!indexes.isEmpty()) {
                ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(this);
            }
            mdTxnCtx.setValue(MetadataManager.INSTANCE.beginTransaction());
            bActiveTxn.setValue(true);
            metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
        }

        // #. finally, delete the dataset.
        MetadataManager.INSTANCE.dropDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
        // Drop the associated nodegroup
        String nodegroup = getNodeGroupName();
        if (!nodegroup.equalsIgnoreCase(MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME)) {
            MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx.getValue(), dataverseName + ":" + datasetName);
        }
    }

    /**
     * Create the index dataflow helper factory for a particular index on the dataset
     *
     * @param mdProvider
     *            metadata provider to get metadata information, components, and runtimes
     * @param index
     *            the index to get the dataflow helper factory for
     * @param recordType
     *            the record type for the dataset
     * @param metaType
     *            the meta type for the dataset
     * @param mergePolicyFactory
     *            the merge policy factory of the dataset
     * @param mergePolicyProperties
     *            the merge policy properties for the dataset
     * @return indexDataflowHelperFactory
     *         an instance of {@link org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory}
     * @throws AlgebricksException
     *             if dataflow helper factory could not be created
     */
    public IResourceFactory getResourceFactory(MetadataProvider mdProvider, Index index, ARecordType recordType,
            ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties)
            throws AlgebricksException {
        ITypeTraits[] filterTypeTraits = DatasetUtil.computeFilterTypeTraits(this, recordType);
        IBinaryComparatorFactory[] filterCmpFactories = DatasetUtil.computeFilterBinaryComparatorFactories(this,
                recordType, mdProvider.getStorageComponentProvider().getComparatorFactoryProvider());
        IResourceFactory resourceFactory;
        switch (index.getIndexType()) {
            case BTREE:
                resourceFactory = bTreeResourceFactoryProvider.getResourceFactory(mdProvider, this, index, recordType,
                        metaType, mergePolicyFactory, mergePolicyProperties, filterTypeTraits, filterCmpFactories);
                break;
            case RTREE:
                resourceFactory = rTreeResourceFactoryProvider.getResourceFactory(mdProvider, this, index, recordType,
                        metaType, mergePolicyFactory, mergePolicyProperties, filterTypeTraits, filterCmpFactories);
                break;
            case LENGTH_PARTITIONED_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case SINGLE_PARTITION_WORD_INVIX:
                resourceFactory = invertedIndexResourceFactoryProvider.getResourceFactory(mdProvider, this, index,
                        recordType, metaType, mergePolicyFactory, mergePolicyProperties, filterTypeTraits,
                        filterCmpFactories);
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE,
                        index.getIndexType().toString());
        }
        return new DatasetLocalResourceFactory(datasetId, resourceFactory);
    }

    /**
     * Get the IO Operation callback factory for the index which belongs to this dataset
     *
     * @param index
     *            the index
     * @return ioOperationCallbackFactory
     *         an instance of {@link org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory}
     *         to be used with IO operations
     * @throws AlgebricksException
     *             if the factory could not be created for the index/dataset combination
     */
    public ILSMIOOperationCallbackFactory getIoOperationCallbackFactory(Index index) throws AlgebricksException {
        switch (index.getIndexType()) {
            case BTREE:
                return getDatasetType() == DatasetType.EXTERNAL
                        && !index.getIndexName().equals(IndexingConstants.getFilesIndexName(getDatasetName()))
                                ? LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE
                                : LSMBTreeIOOperationCallbackFactory.INSTANCE;
            case RTREE:
                return LSMRTreeIOOperationCallbackFactory.INSTANCE;
            case LENGTH_PARTITIONED_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case SINGLE_PARTITION_WORD_INVIX:
                return LSMInvertedIndexIOOperationCallbackFactory.INSTANCE;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE,
                        index.getIndexType().toString());
        }
    }

    /**
     * get the IndexOperationTrackerFactory for a particular index on the dataset
     *
     * @param index
     *            the index
     * @return an instance of {@link org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory}
     */
    public ILSMOperationTrackerFactory getIndexOperationTrackerFactory(Index index) {
        return index.isPrimaryIndex() ? new PrimaryIndexOperationTrackerFactory(getDatasetId())
                : new SecondaryIndexOperationTrackerFactory(getDatasetId());
    }

    /**
     * Get search callback factory for this dataset with the passed index and operation
     *
     * @param index
     *            the index
     * @param jobId
     *            the job id being compiled
     * @param op
     *            the operation this search is part of
     * @param primaryKeyFields
     *            the primary key fields indexes for locking purposes
     * @return
     *         an instance of {@link org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory}
     * @throws AlgebricksException
     *             if the callback factory could not be created
     */
    public ISearchOperationCallbackFactory getSearchCallbackFactory(IStorageComponentProvider storageComponentProvider,
            Index index, JobId jobId, IndexOperation op, int[] primaryKeyFields) throws AlgebricksException {
        if (getDatasetDetails().isTemp()) {
            return NoOpOperationCallbackFactory.INSTANCE;
        } else if (index.isPrimaryIndex()) {
            /**
             * Due to the read-committed isolation level,
             * we may acquire very short duration lock(i.e., instant lock) for readers.
             */
            return (op == IndexOperation.UPSERT)
                    ? new LockThenSearchOperationCallbackFactory(jobId, getDatasetId(), primaryKeyFields,
                            storageComponentProvider.getTransactionSubsystemProvider(), ResourceType.LSM_BTREE)
                    : new PrimaryIndexInstantSearchOperationCallbackFactory(jobId, getDatasetId(), primaryKeyFields,
                            storageComponentProvider.getTransactionSubsystemProvider(), ResourceType.LSM_BTREE);
        }
        return new SecondaryIndexSearchOperationCallbackFactory();
    }

    /**
     * Get the modification callback factory associated with this dataset, the passed index, and operation.
     *
     * @param index
     *            the index
     * @param jobId
     *            the job id of the job being compiled
     * @param op
     *            the operation performed for this callback
     * @param primaryKeyFields
     *            the indexes of the primary keys (used for lock operations)
     * @return
     *         an instance of {@link org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory}
     * @throws AlgebricksException
     *             If the callback factory could not be created
     */
    public IModificationOperationCallbackFactory getModificationCallbackFactory(
            IStorageComponentProvider componentProvider, Index index, JobId jobId, IndexOperation op,
            int[] primaryKeyFields) throws AlgebricksException {
        if (getDatasetDetails().isTemp()) {
            return op == IndexOperation.DELETE || op == IndexOperation.INSERT || op == IndexOperation.UPSERT
                    ? index.isPrimaryIndex()
                            ? new TempDatasetPrimaryIndexModificationOperationCallbackFactory(jobId, datasetId,
                                    primaryKeyFields, componentProvider.getTransactionSubsystemProvider(),
                                    Operation.get(op), index.resourceType())
                            : new TempDatasetSecondaryIndexModificationOperationCallbackFactory(jobId, getDatasetId(),
                                    primaryKeyFields, componentProvider.getTransactionSubsystemProvider(),
                                    Operation.get(op), index.resourceType())
                    : NoOpOperationCallbackFactory.INSTANCE;
        } else if (index.isPrimaryIndex()) {
            return op == IndexOperation.UPSERT
                    ? new UpsertOperationCallbackFactory(jobId, getDatasetId(), primaryKeyFields,
                            componentProvider.getTransactionSubsystemProvider(), Operation.get(op),
                            index.resourceType())
                    : op == IndexOperation.DELETE || op == IndexOperation.INSERT
                            ? new PrimaryIndexModificationOperationCallbackFactory(jobId, getDatasetId(),
                                    primaryKeyFields, componentProvider.getTransactionSubsystemProvider(),
                                    Operation.get(op), index.resourceType())
                            : NoOpOperationCallbackFactory.INSTANCE;
        } else {
            return op == IndexOperation.DELETE || op == IndexOperation.INSERT || op == IndexOperation.UPSERT
                    ? new SecondaryIndexModificationOperationCallbackFactory(jobId, getDatasetId(), primaryKeyFields,
                            componentProvider.getTransactionSubsystemProvider(), Operation.get(op),
                            index.resourceType())
                    : NoOpOperationCallbackFactory.INSTANCE;
        }
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(toMap());
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.WARNING, "Unable to convert map to json String", e);
            return dataverseName + "." + datasetName;
        }
    }

    public Map<String, Object> toMap() {
        Map<String, Object> tree = new HashMap<>();
        tree.put("datasetId", Integer.toString(datasetId));
        tree.put("dataverseName", dataverseName);
        tree.put("datasetName", datasetName);
        tree.put("recordTypeDataverseName", recordTypeDataverseName);
        tree.put("recordTypeName", recordTypeName);
        tree.put("nodeGroupName", nodeGroupName);
        tree.put("compactionPolicyFactory", compactionPolicyFactory);
        tree.put("hints", hints);
        tree.put("compactionPolicyProperties", compactionPolicyProperties);
        tree.put("datasetType", datasetType.name());
        tree.put("datasetDetails", datasetDetails.toString());
        tree.put("metaTypeDataverseName", metaTypeDataverseName);
        tree.put("metaTypeName", metaTypeName);
        tree.put("pendingOp", MetadataUtil.pendingOpToString(pendingOp));
        return tree;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverseName, datasetName);
    }

    public IPushRuntimeFactory getCommitRuntimeFactory(JobId jobId, int[] primaryKeyFields,
            MetadataProvider metadataProvider, int[] datasetPartitions, boolean isSink) {
        return new CommitRuntimeFactory(jobId, datasetId, primaryKeyFields,
                metadataProvider.isTemporaryDatasetWriteJob(), metadataProvider.isWriteTransaction(), datasetPartitions,
                isSink);
    }

    /**
     * Get the index dataflow helper factory for the dataset's primary index
     *
     * @param mdProvider
     *            an instance of metadata provider that is used to fetch metadata information
     * @throws AlgebricksException
     */
    public IResourceFactory getResourceFactory(MetadataProvider mdProvider) throws AlgebricksException {
        if (getDatasetType() != DatasetType.INTERNAL) {
            throw new AlgebricksException(ErrorCode.ASTERIX,
                    ErrorCode.COMPILATION_DATASET_TYPE_DOES_NOT_HAVE_PRIMARY_INDEX, getDatasetType());
        }
        Index index = mdProvider.getIndex(getDataverseName(), getDatasetName(), getDatasetName());
        ARecordType recordType = (ARecordType) mdProvider.findType(getItemTypeDataverseName(), getItemTypeName());
        ARecordType metaType = (ARecordType) mdProvider.findType(getMetaItemTypeDataverseName(), getMetaItemTypeName());
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(this, mdProvider.getMetadataTxnContext());
        return getResourceFactory(mdProvider, index, recordType, metaType, compactionInfo.first, compactionInfo.second);
    }

    public IFrameOperationCallbackFactory getFrameOpCallbackFactory() {
        return NoOpFrameOperationCallbackFactory.INSTANCE;
    }

    public boolean isTemp() {
        return getDatasetDetails().isTemp();
    }

    @Override
    public List<List<String>> getPrimaryKeys() {
        if (getDatasetType() == DatasetType.EXTERNAL) {
            return IndexingConstants.getRIDKeys(((ExternalDatasetDetails) getDatasetDetails()).getProperties());
        }
        return ((InternalDatasetDetails) getDatasetDetails()).getPartitioningKey();
    }

    public ITypeTraits[] getPrimaryTypeTraits(MetadataProvider metadataProvider, ARecordType recordType,
            ARecordType metaType) throws AlgebricksException {
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        ITypeTraitProvider ttProvider = storageComponentProvider.getTypeTraitProvider();
        List<List<String>> partitioningKeys = getPrimaryKeys();
        int numPrimaryKeys = partitioningKeys.size();
        ITypeTraits[] typeTraits = new ITypeTraits[numPrimaryKeys + 1 + (hasMetaPart() ? 1 : 0)];
        List<Integer> indicators = null;
        if (hasMetaPart()) {
            indicators = ((InternalDatasetDetails) getDatasetDetails()).getKeySourceIndicator();
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            IAType keyType = datasetType == DatasetType.EXTERNAL ? IndexingConstants.getFieldType(i)
                    : (indicators == null || indicators.get(i) == 0)
                            ? recordType.getSubFieldType(partitioningKeys.get(i))
                            : metaType.getSubFieldType(partitioningKeys.get(i));
            typeTraits[i] = ttProvider.getTypeTrait(keyType);
        }
        typeTraits[numPrimaryKeys] = ttProvider.getTypeTrait(recordType);
        if (hasMetaPart()) {
            typeTraits[numPrimaryKeys + 1] = ttProvider.getTypeTrait(metaType);
        }
        return typeTraits;
    }

    public IBinaryComparatorFactory[] getPrimaryComparatorFactories(MetadataProvider metadataProvider,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        IStorageComponentProvider storageComponentProvider = metadataProvider.getStorageComponentProvider();
        IBinaryComparatorFactoryProvider cmpFactoryProvider = storageComponentProvider.getComparatorFactoryProvider();
        List<List<String>> partitioningKeys = getPrimaryKeys();
        int numPrimaryKeys = partitioningKeys.size();
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[numPrimaryKeys];
        List<Integer> indicators = null;
        if (hasMetaPart()) {
            indicators = ((InternalDatasetDetails) getDatasetDetails()).getKeySourceIndicator();
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            IAType keyType =
                    (indicators == null || indicators.get(i) == 0) ? recordType.getSubFieldType(partitioningKeys.get(i))
                            : metaType.getSubFieldType(partitioningKeys.get(i));
            cmpFactories[i] = cmpFactoryProvider.getBinaryComparatorFactory(keyType, true);
        }
        return cmpFactories;
    }

    @Override
    public int[] getPrimaryBloomFilterFields() {
        List<List<String>> partitioningKeys = getPrimaryKeys();
        int numPrimaryKeys = partitioningKeys.size();
        int[] bloomFilterKeyFields = new int[numPrimaryKeys];
        for (int i = 0; i < numPrimaryKeys; i++) {
            bloomFilterKeyFields[i] = i;
        }
        return bloomFilterKeyFields;
    }
}
