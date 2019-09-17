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
import java.util.stream.IntStream;

import org.apache.asterix.common.api.IDatasetInfoProvider;
import org.apache.asterix.common.api.ILSMComponentIdGeneratorFactory;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicyFactory;
import org.apache.asterix.common.context.DatasetInfoProvider;
import org.apache.asterix.common.context.DatasetLSMComponentIdGeneratorFactory;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.NoOpFrameOperationCallbackFactory;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.ioopcallbacks.LSMIndexIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMIndexPageWriteCallbackFactory;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.common.utils.JobUtils.ProgressState;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
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
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.metadata.utils.RTreeResourceFactoryProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.compression.CompressionManager;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback.Operation;
import org.apache.asterix.transaction.management.opcallbacks.LockThenSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexInstantSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexInstanctSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.UpsertOperationCallbackFactory;
import org.apache.asterix.transaction.management.resource.DatasetLocalResourceFactory;
import org.apache.asterix.transaction.management.runtime.CommitRuntimeFactory;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger LOGGER = LogManager.getLogger();
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
    private final long rebalanceCount;
    private int pendingOp;
    private final String compressionScheme;

    public Dataset(String dataverseName, String datasetName, String recordTypeDataverseName, String recordTypeName,
            String nodeGroupName, String compactionPolicy, Map<String, String> compactionPolicyProperties,
            IDatasetDetails datasetDetails, Map<String, String> hints, DatasetType datasetType, int datasetId,
            int pendingOp) {
        this(dataverseName, datasetName, recordTypeDataverseName, recordTypeName, /*metaTypeDataverseName*/null,
                /*metaTypeName*/null, nodeGroupName, compactionPolicy, compactionPolicyProperties, datasetDetails,
                hints, datasetType, datasetId, pendingOp, CompressionManager.NONE);
    }

    public Dataset(String dataverseName, String datasetName, String itemTypeDataverseName, String itemTypeName,
            String metaItemTypeDataverseName, String metaItemTypeName, String nodeGroupName, String compactionPolicy,
            Map<String, String> compactionPolicyProperties, IDatasetDetails datasetDetails, Map<String, String> hints,
            DatasetType datasetType, int datasetId, int pendingOp, String compressionScheme) {
        this(dataverseName, datasetName, itemTypeDataverseName, itemTypeName, metaItemTypeDataverseName,
                metaItemTypeName, nodeGroupName, compactionPolicy, compactionPolicyProperties, datasetDetails, hints,
                datasetType, datasetId, pendingOp, 0L, compressionScheme);
    }

    public Dataset(Dataset dataset) {
        this(dataset.dataverseName, dataset.datasetName, dataset.recordTypeDataverseName, dataset.recordTypeName,
                dataset.metaTypeDataverseName, dataset.metaTypeName, dataset.nodeGroupName,
                dataset.compactionPolicyFactory, dataset.compactionPolicyProperties, dataset.datasetDetails,
                dataset.hints, dataset.datasetType, dataset.datasetId, dataset.pendingOp, dataset.rebalanceCount,
                dataset.compressionScheme);
    }

    public Dataset(String dataverseName, String datasetName, String itemTypeDataverseName, String itemTypeName,
            String metaItemTypeDataverseName, String metaItemTypeName, String nodeGroupName, String compactionPolicy,
            Map<String, String> compactionPolicyProperties, IDatasetDetails datasetDetails, Map<String, String> hints,
            DatasetType datasetType, int datasetId, int pendingOp, long rebalanceCount, String compressionScheme) {
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
        this.rebalanceCount = rebalanceCount;
        this.compressionScheme = compressionScheme;
    }

    @Override
    public String getDataverseName() {
        return dataverseName;
    }

    @Override
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

    public long getRebalanceCount() {
        return rebalanceCount;
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

    public boolean allow(ILogicalOperator topOp, byte operation) {
        if (!hasMetaPart()) {
            return true;
        }
        if (topOp.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        ILogicalOperator op = topOp.getInputs().get(0).getValue();
        while ((!op.getInputs().isEmpty())
                && op.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.UNNEST) {
            op = op.getInputs().get(0).getValue();
        }
        if (op.getInputs().isEmpty()) {
            return false;
        }
        UnnestOperator unnestOp = (UnnestOperator) op.getInputs().get(0).getValue();
        if (unnestOp.getExpressionRef().getValue().getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression functionCall =
                (AbstractFunctionCallExpression) unnestOp.getExpressionRef().getValue();
        if (functionCall.getFunctionIdentifier() != BuiltinFunctions.FEED_COLLECT) {
            return false;
        }
        return operation == DatasetUtil.OP_UPSERT;
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
     * @param sourceLoc
     * @throws Exception
     *             if an error occur during the drop process or if the dataset can't be dropped for any reason
     */
    public void drop(MetadataProvider metadataProvider, MutableObject<MetadataTransactionContext> mdTxnCtx,
            List<JobSpecification> jobsToExecute, MutableBoolean bActiveTxn, MutableObject<ProgressState> progress,
            IHyracksClientConnection hcc, boolean dropCorrespondingNodeGroup, SourceLocation sourceLoc)
            throws Exception {
        Map<FeedConnectionId, Pair<JobSpecification, Boolean>> disconnectJobList = new HashMap<>();
        if (getDatasetType() == DatasetType.INTERNAL) {
            // #. prepare jobs to drop the datatset and the indexes in NC
            List<Index> indexes =
                    MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx.getValue(), dataverseName, datasetName);
            for (int j = 0; j < indexes.size(); j++) {
                if (indexes.get(j).isSecondaryIndex()) {
                    jobsToExecute
                            .add(IndexUtil.buildDropIndexJobSpec(indexes.get(j), metadataProvider, this, sourceLoc));
                }
            }
            jobsToExecute.add(DatasetUtil.dropDatasetJobSpec(this, metadataProvider));
            // #. mark the existing dataset as PendingDropOp
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx.getValue(),
                    new Dataset(dataverseName, datasetName, getItemTypeDataverseName(), getItemTypeName(),
                            getMetaItemTypeDataverseName(), getMetaItemTypeName(), getNodeGroupName(),
                            getCompactionPolicy(), getCompactionPolicyProperties(), getDatasetDetails(), getHints(),
                            getDatasetType(), getDatasetId(), MetadataUtil.PENDING_DROP_OP, getCompressionScheme()));

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
                    jobsToExecute
                            .add(IndexUtil.buildDropIndexJobSpec(indexes.get(j), metadataProvider, this, sourceLoc));
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

        // Drops the associated nodegroup if it is no longer used by any other dataset.
        if (dropCorrespondingNodeGroup) {
            metadataProvider.getApplicationContext().getMetadataLockManager()
                    .acquireNodeGroupWriteLock(metadataProvider.getLocks(), nodeGroupName);
            MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx.getValue(), nodeGroupName, true);
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
                resourceFactory = BTreeResourceFactoryProvider.INSTANCE.getResourceFactory(mdProvider, this, index,
                        recordType, metaType, mergePolicyFactory, mergePolicyProperties, filterTypeTraits,
                        filterCmpFactories);
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
    @SuppressWarnings("squid:S1172")
    public ILSMIOOperationCallbackFactory getIoOperationCallbackFactory(Index index) throws AlgebricksException {
        return new LSMIndexIOOperationCallbackFactory(getComponentIdGeneratorFactory(), getDatasetInfoProvider());
    }

    public ILSMPageWriteCallbackFactory getPageWriteCallbackFactory() throws AlgebricksException {
        return new LSMIndexPageWriteCallbackFactory();
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

    public ILSMComponentIdGeneratorFactory getComponentIdGeneratorFactory() {
        return new DatasetLSMComponentIdGeneratorFactory(getDatasetId());
    }

    public IDatasetInfoProvider getDatasetInfoProvider() {
        return new DatasetInfoProvider(getDatasetId());
    }

    /**
     * Get search callback factory for this dataset with the passed index and operation
     *
     * @param storageComponentProvider
     *            storage component provider
     * @param index
     *            the index
     * @param op
     *            the operation this search is part of
     * @param primaryKeyFields
     *            the primary key fields indexes for locking purposes
     * @param primaryKeyFieldsInSecondaryIndex
     *            the primary key fields indexes in the given secondary index (used for index-only plan)
     * @param proceedIndexOnlyPlan
     *            the given plan is an index-only plan? (used for index-only plan)
     * @return
     *         an instance of {@link org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory}
     * @throws AlgebricksException
     *             if the callback factory could not be created
     */
    public ISearchOperationCallbackFactory getSearchCallbackFactory(IStorageComponentProvider storageComponentProvider,
            Index index, IndexOperation op, int[] primaryKeyFields, int[] primaryKeyFieldsInSecondaryIndex,
            boolean proceedIndexOnlyPlan) throws AlgebricksException {
        if (index.isPrimaryIndex()) {
            /**
             * Due to the read-committed isolation level,
             * we may acquire very short duration lock(i.e., instant lock) for readers.
             */
            return (op == IndexOperation.UPSERT || op == IndexOperation.INSERT)
                    ? new LockThenSearchOperationCallbackFactory(getDatasetId(), primaryKeyFields,
                            storageComponentProvider.getTransactionSubsystemProvider(), ResourceType.LSM_BTREE)
                    : new PrimaryIndexInstantSearchOperationCallbackFactory(getDatasetId(), primaryKeyFields,
                            storageComponentProvider.getTransactionSubsystemProvider(), ResourceType.LSM_BTREE);
        } else if (proceedIndexOnlyPlan) {
            // Index-only plan case: we need to execute instantTryLock on PK during a secondary-index search.
            // TODO: ResourceType is never used in the Callbackfactory. Should we keep it?
            return new SecondaryIndexInstanctSearchOperationCallbackFactory(getDatasetId(),
                    primaryKeyFieldsInSecondaryIndex, storageComponentProvider.getTransactionSubsystemProvider(),
                    index.resourceType());
        } else if (index.getKeyFieldNames().isEmpty()) {
            // this is the case where the index is secondary primary index and locking is required
            // since the secondary primary index replaces the dataset index (which locks)
            return new PrimaryIndexInstantSearchOperationCallbackFactory(getDatasetId(), primaryKeyFields,
                    storageComponentProvider.getTransactionSubsystemProvider(), ResourceType.LSM_BTREE);
        }
        return new SecondaryIndexSearchOperationCallbackFactory();
    }

    /**
     * Get search callback factory for this dataset with the passed index and operation
     *
     * @param storageComponentProvider
     *            storage component provider
     * @param index
     *            the index
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
            Index index, IndexOperation op, int[] primaryKeyFields) throws AlgebricksException {
        return getSearchCallbackFactory(storageComponentProvider, index, op, primaryKeyFields, null, false);
    }

    /**
     * Get the modification callback factory associated with this dataset, the passed index, and operation.
     *
     * @param index
     *            the index
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
            IStorageComponentProvider componentProvider, Index index, IndexOperation op, int[] primaryKeyFields)
            throws AlgebricksException {
        if (index.isPrimaryIndex()) {
            return op == IndexOperation.UPSERT || op == IndexOperation.INSERT
                    ? new UpsertOperationCallbackFactory(getDatasetId(),
                            primaryKeyFields, componentProvider.getTransactionSubsystemProvider(), Operation.get(op),
                            index.resourceType())
                    : op == IndexOperation.DELETE ? new PrimaryIndexModificationOperationCallbackFactory(getDatasetId(),
                            primaryKeyFields, componentProvider.getTransactionSubsystemProvider(), Operation.get(op),
                            index.resourceType()) : NoOpOperationCallbackFactory.INSTANCE;
        } else {
            return op == IndexOperation.DELETE || op == IndexOperation.INSERT || op == IndexOperation.UPSERT
                    ? new SecondaryIndexModificationOperationCallbackFactory(getDatasetId(), primaryKeyFields,
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
            LOGGER.log(Level.WARN, "Unable to convert map to json String", e);
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
        tree.put("rebalanceCount", rebalanceCount);
        tree.put("compressionScheme", compressionScheme);
        return tree;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverseName, datasetName);
    }

    /**
     * Gets the commit runtime factory for inserting/upserting/deleting operations on this dataset.
     *
     * @param metadataProvider,
     *            the metadata provider.
     * @param primaryKeyFieldPermutation,
     *            the primary key field permutation according to the input.
     * @param isSink,
     *            whether this commit runtime is the last operator in the pipeline.
     * @return the commit runtime factory for inserting/upserting/deleting operations on this dataset.
     * @throws AlgebricksException
     */
    public IPushRuntimeFactory getCommitRuntimeFactory(MetadataProvider metadataProvider,
            int[] primaryKeyFieldPermutation, boolean isSink) throws AlgebricksException {
        int[] datasetPartitions = getDatasetPartitions(metadataProvider);
        return new CommitRuntimeFactory(datasetId, primaryKeyFieldPermutation, metadataProvider.isWriteTransaction(),
                datasetPartitions, isSink);
    }

    public IFrameOperationCallbackFactory getFrameOpCallbackFactory(MetadataProvider mdProvider) {
        return NoOpFrameOperationCallbackFactory.INSTANCE;
    }

    public boolean isCorrelated() {
        return CorrelatedPrefixMergePolicyFactory.NAME.equals(compactionPolicyFactory);
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

    /**
     * Gets the record descriptor for primary records of this dataset.
     *
     * @param metadataProvider,
     *            the metadata provider.
     * @return the record descriptor for primary records of this dataset.
     * @throws AlgebricksException
     */
    public RecordDescriptor getPrimaryRecordDescriptor(MetadataProvider metadataProvider) throws AlgebricksException {
        List<List<String>> partitioningKeys = getPrimaryKeys();
        int numPrimaryKeys = partitioningKeys.size();
        ISerializerDeserializer[] primaryRecFields =
                new ISerializerDeserializer[numPrimaryKeys + 1 + (hasMetaPart() ? 1 : 0)];
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[numPrimaryKeys + 1 + (hasMetaPart() ? 1 : 0)];
        ISerializerDeserializerProvider serdeProvider = metadataProvider.getDataFormat().getSerdeProvider();
        List<Integer> indicators = null;
        if (hasMetaPart()) {
            indicators = ((InternalDatasetDetails) getDatasetDetails()).getKeySourceIndicator();
        }
        ARecordType itemType = (ARecordType) metadataProvider.findType(this);
        ARecordType metaType = (ARecordType) metadataProvider.findMetaType(this);

        // Set the serde/traits for primary keys
        for (int i = 0; i < numPrimaryKeys; i++) {
            IAType keyType =
                    datasetType == DatasetType.EXTERNAL ? IndexingConstants.getFieldType(i)
                            : (indicators == null || indicators.get(i) == 0)
                                    ? itemType.getSubFieldType(partitioningKeys.get(i))
                                    : metaType.getSubFieldType(partitioningKeys.get(i));
            primaryRecFields[i] = serdeProvider.getSerializerDeserializer(keyType);
            primaryTypeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(keyType);
        }

        // Set the serde for the record field
        primaryRecFields[numPrimaryKeys] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType);
        primaryTypeTraits[numPrimaryKeys] = TypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        if (hasMetaPart()) {
            // Set the serde and traits for the meta record field
            primaryRecFields[numPrimaryKeys + 1] =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaType);
            primaryTypeTraits[numPrimaryKeys + 1] = TypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        }
        return new RecordDescriptor(primaryRecFields, primaryTypeTraits);
    }

    /**
     * Gets the comparator factories for the primary key fields of this dataset.
     *
     * @param metadataProvider,
     *            the metadata provider.
     * @return the comparator factories for the primary key fields of this dataset.
     * @throws AlgebricksException
     */
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
            IAType keyType = datasetType == DatasetType.EXTERNAL ? IndexingConstants.getFieldType(i)
                    : (indicators == null || indicators.get(i) == 0)
                            ? recordType.getSubFieldType(partitioningKeys.get(i))
                            : metaType.getSubFieldType(partitioningKeys.get(i));
            cmpFactories[i] = cmpFactoryProvider.getBinaryComparatorFactory(keyType, true);
        }
        return cmpFactories;
    }

    /**
     * Gets the hash function factories for the primary key fields of this dataset.
     *
     * @param metadataProvider,
     *            the metadata provider.
     * @return the hash function factories for the primary key fields of this dataset.
     * @throws AlgebricksException
     */
    public IBinaryHashFunctionFactory[] getPrimaryHashFunctionFactories(MetadataProvider metadataProvider)
            throws AlgebricksException {
        ARecordType recordType = (ARecordType) metadataProvider.findType(this);
        ARecordType metaType = (ARecordType) metadataProvider.findMetaType(this);
        List<List<String>> partitioningKeys = getPrimaryKeys();
        int numPrimaryKeys = partitioningKeys.size();
        IBinaryHashFunctionFactory[] hashFuncFactories = new IBinaryHashFunctionFactory[numPrimaryKeys];
        List<Integer> indicators = null;
        if (hasMetaPart()) {
            indicators = ((InternalDatasetDetails) getDatasetDetails()).getKeySourceIndicator();
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            IAType keyType = datasetType == DatasetType.EXTERNAL ? IndexingConstants.getFieldType(i)
                    : (indicators == null || indicators.get(i) == 0)
                            ? recordType.getSubFieldType(partitioningKeys.get(i))
                            : metaType.getSubFieldType(partitioningKeys.get(i));
            hashFuncFactories[i] = BinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(keyType);
        }
        return hashFuncFactories;
    }

    @Override
    public int[] getPrimaryBloomFilterFields() {
        List<List<String>> partitioningKeys = getPrimaryKeys();
        int numPrimaryKeys = partitioningKeys.size();
        return IntStream.range(0, numPrimaryKeys).toArray();
    }

    // Gets the target dataset for the purpose of rebalance.
    public Dataset getTargetDatasetForRebalance(String targetNodeGroupName) {
        return new Dataset(this.dataverseName, this.datasetName, this.recordTypeDataverseName, this.recordTypeName,
                this.metaTypeDataverseName, this.metaTypeName, targetNodeGroupName, this.compactionPolicyFactory,
                this.compactionPolicyProperties, this.datasetDetails, this.hints, this.datasetType,
                DatasetIdFactory.generateAlternatingDatasetId(this.datasetId), this.pendingOp, this.rebalanceCount + 1,
                this.compressionScheme);
    }

    // Gets an array of partition numbers for this dataset.
    protected int[] getDatasetPartitions(MetadataProvider metadataProvider) throws AlgebricksException {
        FileSplit[] splitsForDataset =
                metadataProvider.splitsForIndex(metadataProvider.getMetadataTxnContext(), this, getDatasetName());
        int[] partitions = new int[splitsForDataset.length];
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] = StoragePathUtil.getPartitionNumFromRelativePath(splitsForDataset[i].getPath());
        }
        return partitions;
    }

    public String getFullyQualifiedName() {
        return dataverseName + '.' + datasetName;
    }

    public String getCompressionScheme() {
        return compressionScheme;
    }
}
