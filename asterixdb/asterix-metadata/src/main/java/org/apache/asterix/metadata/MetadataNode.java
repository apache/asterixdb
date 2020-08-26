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

package org.apache.asterix.metadata;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.dataflow.LSMIndexUtil;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager.AtomicityLevel;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.api.ExtensionMetadataDataset;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;
import org.apache.asterix.metadata.api.IExtensionMetadataSearchKey;
import org.apache.asterix.metadata.api.IMetadataEntityTupleTranslator;
import org.apache.asterix.metadata.api.IMetadataExtension;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.api.IMetadataNode;
import org.apache.asterix.metadata.api.IValueExtractor;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.metadata.entitytupletranslators.CompactionPolicyTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatasetTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatasourceAdapterTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatatypeTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DataverseTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.ExternalFileTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedConnectionTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedPolicyTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FunctionTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.IndexTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.LibraryTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.MetadataTupleTranslatorProvider;
import org.apache.asterix.metadata.entitytupletranslators.NodeGroupTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.NodeTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.SynonymTupleTranslator;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.asterix.metadata.valueextractors.MetadataEntityValueExtractor;
import org.apache.asterix.metadata.valueextractors.TupleCopyValueExtractor;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractComplexType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback.Operation;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexModificationOperationCallback;
import org.apache.asterix.transaction.management.opcallbacks.UpsertOperationCallback;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetadataNode implements IMetadataNode {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    // shared between core and extension
    private transient IDatasetLifecycleManager datasetLifecycleManager;
    private transient ITransactionSubsystem transactionSubsystem;
    private int metadataStoragePartition;
    private transient CachingTxnIdFactory txnIdFactory;
    // core only
    private transient MetadataTupleTranslatorProvider tupleTranslatorProvider;
    // extension only
    private Map<ExtensionMetadataDatasetId, ExtensionMetadataDataset<?>> extensionDatasets;

    public static final MetadataNode INSTANCE = new MetadataNode();

    private MetadataNode() {
        super();
    }

    public void initialize(INcApplicationContext runtimeContext,
            MetadataTupleTranslatorProvider tupleTranslatorProvider, List<IMetadataExtension> metadataExtensions,
            int partitionId) {
        this.tupleTranslatorProvider = tupleTranslatorProvider;
        this.transactionSubsystem = runtimeContext.getTransactionSubsystem();
        this.datasetLifecycleManager = runtimeContext.getDatasetLifecycleManager();
        this.metadataStoragePartition = partitionId;
        if (metadataExtensions != null) {
            extensionDatasets = new HashMap<>();
            for (IMetadataExtension metadataExtension : metadataExtensions) {
                for (ExtensionMetadataDataset<?> extensionIndex : metadataExtension.getExtensionIndexes()) {
                    extensionDatasets.put(extensionIndex.getId(), extensionIndex);
                }
            }
        }
        this.txnIdFactory = new CachingTxnIdFactory(runtimeContext);
    }

    public int getMetadataStoragePartition() {
        return metadataStoragePartition;
    }

    @Override
    public void beginTransaction(TxnId transactionId) {
        TransactionOptions options = new TransactionOptions(AtomicityLevel.ATOMIC);
        transactionSubsystem.getTransactionManager().beginTransaction(transactionId, options);
    }

    @SuppressWarnings("squid:S1181")
    @Override
    public void commitTransaction(TxnId txnId) {
        try {
            transactionSubsystem.getTransactionManager().commitTransaction(txnId);
        } catch (Throwable th) {
            // Metadata node should abort all Metadata transactions on re-start
            LOGGER.fatal("Failure committing a metadata transaction", th);
            ExitUtil.halt(ExitUtil.EC_FAILED_TO_COMMIT_METADATA_TXN);
        }
    }

    @SuppressWarnings("squid:S1181")
    @Override
    public void abortTransaction(TxnId txnId) {
        try {
            transactionSubsystem.getTransactionManager().abortTransaction(txnId);
        } catch (Throwable th) {
            // Metadata node should abort all uncommitted transactions on re-start
            LOGGER.fatal("Failure committing a metadata transaction", th);
            ExitUtil.halt(ExitUtil.EC_FAILED_TO_ABORT_METADATA_TXN);
        }
    }

    // TODO(amoudi): make all metadata operations go through the generic methods
    /**
     * Add entity to index
     *
     * @param txnId
     * @param entity
     * @param tupleTranslator
     * @param index
     * @throws AlgebricksException
     */
    private <T> void addEntity(TxnId txnId, T entity, IMetadataEntityTupleTranslator<T> tupleTranslator,
            IMetadataIndex index) throws AlgebricksException {
        try {
            ITupleReference tuple = tupleTranslator.getTupleFromMetadataEntity(entity);
            insertTupleIntoIndex(txnId, index, tuple);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    /**
     * Upsert entity to index
     *
     * @param txnId
     * @param entity
     * @param tupleTranslator
     * @param index
     * @throws AlgebricksException
     */
    private <T> void upsertEntity(TxnId txnId, T entity, IMetadataEntityTupleTranslator<T> tupleTranslator,
            IMetadataIndex index) throws AlgebricksException {
        try {
            ITupleReference tuple = tupleTranslator.getTupleFromMetadataEntity(entity);
            upsertTupleIntoIndex(txnId, index, tuple);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    /**
     * Delete entity from index
     *
     * @param txnId
     * @param entity
     * @param tupleTranslator
     * @param index
     * @throws AlgebricksException
     */
    private <T> void deleteEntity(TxnId txnId, T entity, IMetadataEntityTupleTranslator<T> tupleTranslator,
            IMetadataIndex index) throws AlgebricksException {
        try {
            ITupleReference tuple = tupleTranslator.getTupleFromMetadataEntity(entity);
            deleteTupleFromIndex(txnId, index, tuple);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    /**
     * retrieve all entities that matches the searchKey
     *
     * @param txnId
     * @param searchKey
     * @param tupleTranslator
     * @param index
     * @return
     * @throws AlgebricksException
     */
    private <T> List<T> getEntities(TxnId txnId, ITupleReference searchKey,
            IMetadataEntityTupleTranslator<T> tupleTranslator, IMetadataIndex index) throws AlgebricksException {
        try {
            IValueExtractor<T> valueExtractor = new MetadataEntityValueExtractor<>(tupleTranslator);
            List<T> results = new ArrayList<>();
            searchIndex(txnId, index, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public <T extends IExtensionMetadataEntity> void addEntity(TxnId txnId, T entity) throws AlgebricksException {
        ExtensionMetadataDataset<T> index = (ExtensionMetadataDataset<T>) extensionDatasets.get(entity.getDatasetId());
        if (index == null) {
            throw new AlgebricksException("Metadata Extension Index: " + entity.getDatasetId() + " was not found");
        }
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator(true);
        addEntity(txnId, entity, tupleTranslator, index);
    }

    @Override
    public <T extends IExtensionMetadataEntity> void upsertEntity(TxnId txnId, T entity) throws AlgebricksException {
        ExtensionMetadataDataset<T> index = (ExtensionMetadataDataset<T>) extensionDatasets.get(entity.getDatasetId());
        if (index == null) {
            throw new AlgebricksException("Metadata Extension Index: " + entity.getDatasetId() + " was not found");
        }
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator(true);
        upsertEntity(txnId, entity, tupleTranslator, index);
    }

    @Override
    public <T extends IExtensionMetadataEntity> void deleteEntity(TxnId txnId, T entity) throws AlgebricksException {
        ExtensionMetadataDataset<T> index = (ExtensionMetadataDataset<T>) extensionDatasets.get(entity.getDatasetId());
        if (index == null) {
            throw new AlgebricksException("Metadata Extension Index: " + entity.getDatasetId() + " was not found");
        }
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator(true);
        deleteEntity(txnId, entity, tupleTranslator, index);
    }

    @Override
    public <T extends IExtensionMetadataEntity> List<T> getEntities(TxnId txnId, IExtensionMetadataSearchKey searchKey)
            throws AlgebricksException {
        ExtensionMetadataDataset<T> index =
                (ExtensionMetadataDataset<T>) extensionDatasets.get(searchKey.getDatasetId());
        if (index == null) {
            throw new AlgebricksException("Metadata Extension Index: " + searchKey.getDatasetId() + " was not found");
        }
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator(false);
        return getEntities(txnId, searchKey.getSearchKey(), tupleTranslator, index);
    }

    @Override
    public void addDataverse(TxnId txnId, Dataverse dataverse) throws AlgebricksException {
        try {
            DataverseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataverseTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(dataverse);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException(
                        "A dataverse with this name " + dataverse.getDataverseName() + " already exists.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void addDataset(TxnId txnId, Dataset dataset) throws AlgebricksException {
        try {
            // Insert into the 'dataset' dataset.
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(true);
            ITupleReference datasetTuple = tupleReaderWriter.getTupleFromMetadataEntity(dataset);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                // Add the primary index for the dataset.
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                Index primaryIndex = new Index(dataset.getDataverseName(), dataset.getDatasetName(),
                        dataset.getDatasetName(), IndexType.BTREE, id.getPrimaryKey(), id.getKeySourceIndicator(),
                        id.getPrimaryKeyType(), false, false, true, dataset.getPendingOp());

                addIndex(txnId, primaryIndex);
            }
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("A dataset with this name " + dataset.getDatasetName()
                        + " already exists in dataverse '" + dataset.getDataverseName() + "'.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void addIndex(TxnId txnId, Index index) throws AlgebricksException {
        try {
            IndexTupleTranslator tupleWriter = tupleTranslatorProvider.getIndexTupleTranslator(txnId, this, true);
            ITupleReference tuple = tupleWriter.getTupleFromMetadataEntity(index);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, tuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("An index with name '" + index.getIndexName() + "' already exists.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void addNode(TxnId txnId, Node node) throws AlgebricksException {
        try {
            NodeTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getNodeTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(node);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.NODE_DATASET, tuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("A node with name '" + node.getNodeName() + "' already exists.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void modifyNodeGroup(TxnId txnId, NodeGroup nodeGroup, Operation modificationOp) throws AlgebricksException {
        try {
            NodeGroupTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getNodeGroupTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(nodeGroup);
            modifyMetadataIndex(modificationOp, txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, tuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException(
                        "A nodegroup with name '" + nodeGroup.getNodeGroupName() + "' already exists.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void addDatatype(TxnId txnId, Datatype datatype) throws AlgebricksException {
        try {
            DatatypeTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getDataTypeTupleTranslator(txnId, this, true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(datatype);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException(
                        "A datatype with name '" + datatype.getDatatypeName() + "' already exists.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void addFunction(TxnId txnId, Function function) throws AlgebricksException {
        try {
            // Insert into the 'function' dataset.
            FunctionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFunctionTupleTranslator(txnId, this, true);
            ITupleReference functionTuple = tupleReaderWriter.getTupleFromMetadataEntity(function);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET, functionTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("A function with this name " + function.getSignature()
                        + " already exists in dataverse '" + function.getDataverseName() + "'.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    private void insertTupleIntoIndex(TxnId txnId, IMetadataIndex metadataIndex, ITupleReference tuple)
            throws HyracksDataException {
        modifyMetadataIndex(Operation.INSERT, txnId, metadataIndex, tuple);
    }

    private void upsertTupleIntoIndex(TxnId txnId, IMetadataIndex metadataIndex, ITupleReference tuple)
            throws HyracksDataException {
        modifyMetadataIndex(Operation.UPSERT, txnId, metadataIndex, tuple);
    }

    private void modifyMetadataIndex(Operation op, TxnId txnId, IMetadataIndex metadataIndex, ITupleReference tuple)
            throws HyracksDataException {
        String resourceName = metadataIndex.getFile().getRelativePath();
        ILSMIndex lsmIndex = (ILSMIndex) datasetLifecycleManager.get(resourceName);
        datasetLifecycleManager.open(resourceName);
        try {
            ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(txnId);
            IModificationOperationCallback modCallback = createIndexModificationCallback(op, txnCtx, metadataIndex);
            IIndexAccessParameters iap = new IndexAccessParameters(modCallback, NoOpOperationCallback.INSTANCE);
            ILSMIndexAccessor indexAccessor = lsmIndex.createAccessor(iap);
            txnCtx.setWriteTxn(true);
            txnCtx.register(metadataIndex.getResourceId(),
                    StoragePathUtil.getPartitionNumFromRelativePath(resourceName), lsmIndex, modCallback,
                    metadataIndex.isPrimaryIndex());
            LSMIndexUtil.checkAndSetFirstLSN((AbstractLSMIndex) lsmIndex, transactionSubsystem.getLogManager());
            switch (op) {
                case INSERT:
                    indexAccessor.forceInsert(tuple);
                    break;
                case DELETE:
                    indexAccessor.forceDelete(tuple);
                    break;
                case UPSERT:
                    indexAccessor.forceUpsert(tuple);
                    break;
                default:
                    throw new IllegalStateException("Unknown operation type: " + op);
            }
        } finally {
            datasetLifecycleManager.close(resourceName);
        }
    }

    private IModificationOperationCallback createIndexModificationCallback(Operation indexOp,
            ITransactionContext txnCtx, IMetadataIndex metadataIndex) {
        switch (indexOp) {
            case INSERT:
            case DELETE:
                /*
                 * Regardless of the index type (primary or secondary index), secondary index modification
                 * callback is given. This is still correct since metadata index operation doesn't require
                 * any lock from ConcurrentLockMgr.
                 */
                return new SecondaryIndexModificationOperationCallback(metadataIndex.getDatasetId(),
                        metadataIndex.getPrimaryKeyIndexes(), txnCtx, transactionSubsystem.getLockManager(),
                        transactionSubsystem, metadataIndex.getResourceId(), metadataStoragePartition,
                        ResourceType.LSM_BTREE, indexOp);
            case UPSERT:
                return new UpsertOperationCallback(metadataIndex.getDatasetId(), metadataIndex.getPrimaryKeyIndexes(),
                        txnCtx, transactionSubsystem.getLockManager(), transactionSubsystem,
                        metadataIndex.getResourceId(), metadataStoragePartition, ResourceType.LSM_BTREE, indexOp);
            default:
                throw new IllegalStateException("Unknown operation type: " + indexOp);
        }
    }

    @Override
    public void dropDataverse(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        try {
            confirmDataverseCanBeDeleted(txnId, dataverseName);

            // Drop all synonyms in this dataverse.
            List<Synonym> dataverseSynonyms = getDataverseSynonyms(txnId, dataverseName);
            for (Synonym synonym : dataverseSynonyms) {
                dropSynonym(txnId, dataverseName, synonym.getSynonymName());
            }

            // Drop all feeds and connections in this dataverse.
            // Feeds may depend on datatypes and adapters
            List<Feed> dataverseFeeds = getDataverseFeeds(txnId, dataverseName);
            for (Feed feed : dataverseFeeds) {
                List<FeedConnection> feedConnections = getFeedConnections(txnId, dataverseName, feed.getFeedName());
                for (FeedConnection feedConnection : feedConnections) {
                    dropFeedConnection(txnId, dataverseName, feed.getFeedName(), feedConnection.getDatasetName());
                }
                dropFeed(txnId, dataverseName, feed.getFeedName());
            }

            // Drop all feed ingestion policies in this dataverse.
            List<FeedPolicyEntity> feedPolicies = getDataverseFeedPolicies(txnId, dataverseName);
            for (FeedPolicyEntity feedPolicy : feedPolicies) {
                dropFeedPolicy(txnId, dataverseName, feedPolicy.getPolicyName());
            }

            // Drop all functions in this dataverse.
            // Functions may depend on datatypes and libraries
            // As a side effect, acquires an S lock on the 'Function' dataset on behalf of txnId.
            List<Function> dataverseFunctions = getDataverseFunctions(txnId, dataverseName);
            for (Function function : dataverseFunctions) {
                dropFunction(txnId, function.getSignature(), true);
            }

            // Drop all adapters in this dataverse.
            // Adapters depend on libraries.
            // As a side effect, acquires an S lock on the 'Adapter' dataset on behalf of txnId.
            List<DatasourceAdapter> dataverseAdapters = getDataverseAdapters(txnId, dataverseName);
            for (DatasourceAdapter adapter : dataverseAdapters) {
                dropAdapter(txnId, dataverseName, adapter.getAdapterIdentifier().getName());
            }

            // Drop all libraries in this dataverse.
            List<Library> dataverseLibraries = getDataverseLibraries(txnId, dataverseName);
            for (Library lib : dataverseLibraries) {
                dropLibrary(txnId, lib.getDataverseName(), lib.getName());
            }

            // Drop all datasets and indexes in this dataverse.
            // Datasets depend on datatypes
            List<Dataset> dataverseDatasets = getDataverseDatasets(txnId, dataverseName);
            for (Dataset ds : dataverseDatasets) {
                dropDataset(txnId, dataverseName, ds.getDatasetName(), true);
            }

            // Drop all types in this dataverse.
            // As a side effect, acquires an S lock on the 'datatype' dataset on behalf of txnId.
            List<Datatype> dataverseDatatypes = getDataverseDatatypes(txnId, dataverseName);
            for (Datatype dataverseDatatype : dataverseDatatypes) {
                forceDropDatatype(txnId, dataverseName, dataverseDatatype.getDatatypeName());
            }

            // Delete the dataverse entry from the 'dataverse' dataset.
            // As a side effect, acquires an S lock on the 'dataverse' dataset on behalf of txnId.
            ITupleReference searchKey = createTuple(dataverseName);
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException("Cannot drop dataverse '" + dataverseName + "' because it doesn't exist.",
                        e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public boolean isDataverseNotEmpty(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        return !getDataverseDatatypes(txnId, dataverseName).isEmpty()
                || !getDataverseDatasets(txnId, dataverseName).isEmpty()
                || !getDataverseLibraries(txnId, dataverseName).isEmpty()
                || !getDataverseAdapters(txnId, dataverseName).isEmpty()
                || !getDataverseFunctions(txnId, dataverseName).isEmpty()
                || !getDataverseFeedPolicies(txnId, dataverseName).isEmpty()
                || !getDataverseFeeds(txnId, dataverseName).isEmpty()
                || !getDataverseSynonyms(txnId, dataverseName).isEmpty();
    }

    @Override
    public void dropDataset(TxnId txnId, DataverseName dataverseName, String datasetName) throws AlgebricksException {
        dropDataset(txnId, dataverseName, datasetName, false);
    }

    public void dropDataset(TxnId txnId, DataverseName dataverseName, String datasetName, boolean force)
            throws AlgebricksException {
        if (!force) {
            confirmDatasetCanBeDeleted(txnId, dataverseName, datasetName);
        }

        Dataset dataset = getDataset(txnId, dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Cannot drop dataset '" + datasetName + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'datasets' dataset.
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'dataset' dataset.
            ITupleReference datasetTuple = null;
            try {
                datasetTuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey);

                // Delete entry(s) from the 'indexes' dataset.
                List<Index> datasetIndexes = getDatasetIndexes(txnId, dataverseName, datasetName);
                if (datasetIndexes != null) {
                    for (Index index : datasetIndexes) {
                        dropIndex(txnId, dataverseName, datasetName, index.getIndexName());
                    }
                }

                if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
                    // Delete External Files
                    // As a side effect, acquires an S lock on the 'ExternalFile' dataset
                    // on behalf of txnId.
                    List<ExternalFile> datasetFiles = getExternalFiles(txnId, dataset);
                    if (datasetFiles != null && !datasetFiles.isEmpty()) {
                        // Drop all external files in this dataset.
                        for (ExternalFile file : datasetFiles) {
                            dropExternalFile(txnId, dataverseName, file.getDatasetName(), file.getFileNumber());
                        }
                    }
                }
            } catch (HyracksDataException hde) {
                // ignore this exception and continue deleting all relevant
                // artifacts.
                if (!hde.getComponent().equals(ErrorCode.HYRACKS)
                        || hde.getErrorCode() != ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                    throw new AlgebricksException(hde);
                }
            } finally {
                deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void dropIndex(TxnId txnId, DataverseName dataverseName, String datasetName, String indexName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName, indexName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'index' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, tuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException(
                        "Cannot drop index '" + datasetName + "." + indexName + "' because it doesn't exist.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public boolean dropNodegroup(TxnId txnId, String nodeGroupName, boolean failSilently) throws AlgebricksException {
        List<String> datasetNames = getDatasetNamesPartitionedOnThisNodeGroup(txnId, nodeGroupName);
        if (!datasetNames.isEmpty()) {
            if (failSilently) {
                return false;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("Nodegroup '" + nodeGroupName
                    + "' cannot be dropped; it was used for partitioning these datasets:");
            for (int i = 0; i < datasetNames.size(); i++) {
                sb.append("\n" + (i + 1) + "- " + datasetNames.get(i) + ".");
            }
            throw new AlgebricksException(sb.toString());
        }
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'nodegroup' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, tuple);
            return true;
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException("Cannot drop nodegroup '" + nodeGroupName + "' because it doesn't exist",
                        e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void dropDatatype(TxnId txnId, DataverseName dataverseName, String datatypeName) throws AlgebricksException {
        dropDatatype(txnId, dataverseName, datatypeName, false);
    }

    private void dropDatatype(TxnId txnId, DataverseName dataverseName, String datatypeName, boolean force)
            throws AlgebricksException {
        if (!force) {
            confirmDatatypeIsUnused(txnId, dataverseName, datatypeName);
        }
        // Delete the datatype entry, including all it's nested anonymous types.
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey);
            // Get nested types
            List<String> nestedTypes = getNestedComplexDatatypeNamesForThisDatatype(txnId, dataverseName, datatypeName);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
            for (String nestedType : nestedTypes) {
                Datatype dt = getDatatype(txnId, dataverseName, nestedType);
                if (dt != null && dt.getIsAnonymous()) {
                    dropDatatype(txnId, dataverseName, dt.getDatatypeName());
                }
            }
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException("Cannot drop type '" + datatypeName + "' because it doesn't exist", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    private void forceDropDatatype(TxnId txnId, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException("Cannot drop type '" + datatypeName + "' because it doesn't exist", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    private void deleteTupleFromIndex(TxnId txnId, IMetadataIndex metadataIndex, ITupleReference tuple)
            throws HyracksDataException {
        modifyMetadataIndex(Operation.DELETE, txnId, metadataIndex, tuple);
    }

    @Override
    public List<Dataverse> getDataverses(TxnId txnId) throws AlgebricksException {
        try {
            DataverseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, null, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public Dataverse getDataverse(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DataverseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public List<Dataset> getDataverseDatasets(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataset> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public List<Feed> getDataverseFeeds(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(false);
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Feed> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.FEED_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public List<Library> getDataverseLibraries(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            LibraryTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getLibraryTupleTranslator(false);
            IValueExtractor<Library> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Library> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.LIBRARY_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    private List<Datatype> getDataverseDatatypes(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatatypeTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getDataTypeTupleTranslator(txnId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public Dataset getDataset(TxnId txnId, DataverseName dataverseName, String datasetName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
            List<Dataset> results = new ArrayList<>();
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    public List<Dataset> getAllDatasets(TxnId txnId) throws AlgebricksException {
        try {
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataset> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, null, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    public List<Function> getAllFunctions(TxnId txnId) throws AlgebricksException {
        return getFunctionsImpl(txnId, null);
    }

    public List<Datatype> getAllDatatypes(TxnId txnId) throws AlgebricksException {
        try {
            DatatypeTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getDataTypeTupleTranslator(txnId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, null, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    public List<DatasourceAdapter> getAllAdapters(TxnId txnId) throws AlgebricksException {
        try {
            DatasourceAdapterTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getAdapterTupleTranslator(false);
            List<DatasourceAdapter> results = new ArrayList<>();
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, null, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    public List<FeedConnection> getAllFeedConnections(TxnId txnId) throws AlgebricksException {
        try {
            FeedConnectionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFeedConnectionTupleTranslator(false);
            List<FeedConnection> results = new ArrayList<>();
            IValueExtractor<FeedConnection> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, null, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    private void confirmDataverseCanBeDeleted(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        // If a dataset from a DIFFERENT dataverse
        // uses a type from this dataverse
        // throw an error
        List<Dataset> datasets = getAllDatasets(txnId);
        for (Dataset set : datasets) {
            if (set.getDataverseName().equals(dataverseName)) {
                continue;
            }
            if (set.getItemTypeDataverseName().equals(dataverseName)) {
                throw new AlgebricksException("Cannot drop dataverse. Type "
                        + TypeUtil.getFullyQualifiedDisplayName(set.getItemTypeDataverseName(), set.getItemTypeName())
                        + " used by dataset " + DatasetUtil.getFullyQualifiedDisplayName(set));
            }
            if (set.getMetaItemTypeDataverseName() != null
                    && set.getMetaItemTypeDataverseName().equals(dataverseName)) {
                throw new AlgebricksException("Cannot drop dataverse. Type "
                        + TypeUtil.getFullyQualifiedDisplayName(set.getMetaItemTypeDataverseName(),
                                set.getMetaItemTypeName())
                        + " used by dataset " + DatasetUtil.getFullyQualifiedDisplayName(set));
            }
        }

        // If a function from a DIFFERENT dataverse
        // uses datasets, functions or datatypes from this dataverse
        // throw an error
        List<Function> functions = getAllFunctions(txnId);
        for (Function function : functions) {
            if (function.getDataverseName().equals(dataverseName)) {
                continue;
            }
            for (Triple<DataverseName, String, String> datasetDependency : function.getDependencies().get(0)) {
                if (datasetDependency.first.equals(dataverseName)) {
                    throw new AlgebricksException("Cannot drop dataverse. Function " + function.getSignature()
                            + " depends on dataset " + DatasetUtil.getFullyQualifiedDisplayName(datasetDependency.first,
                                    datasetDependency.second));
                }
            }
            for (Triple<DataverseName, String, String> functionDependency : function.getDependencies().get(1)) {
                if (functionDependency.first.equals(dataverseName)) {
                    throw new AlgebricksException("Cannot drop dataverse. Function " + function.getSignature()
                            + " depends on function " + new FunctionSignature(functionDependency.first,
                                    functionDependency.second, Integer.parseInt(functionDependency.third)));
                }
            }
            for (Triple<DataverseName, String, String> type : function.getDependencies().get(2)) {
                if (type.first.equals(dataverseName)) {
                    throw new AlgebricksException("Cannot drop dataverse. Function " + function.getSignature()
                            + " depends on type " + TypeUtil.getFullyQualifiedDisplayName(type.first, type.second));
                }
            }
        }

        // If a feed connection from a DIFFERENT dataverse applies
        // a function from this dataverse then throw an error
        List<FeedConnection> feedConnections = getAllFeedConnections(txnId);
        for (FeedConnection feedConnection : feedConnections) {
            if (dataverseName.equals(feedConnection.getDataverseName())) {
                continue;
            }
            for (FunctionSignature functionSignature : feedConnection.getAppliedFunctions()) {
                if (dataverseName.equals(functionSignature.getDataverseName())) {
                    throw new AlgebricksException(
                            "Cannot drop dataverse. Feed connection " + feedConnection.getDataverseName() + "."
                                    + feedConnection.getFeedName() + " depends on function " + functionSignature);
                }
            }
        }
    }

    private void confirmFunctionCanBeDeleted(TxnId txnId, FunctionSignature signature) throws AlgebricksException {
        // If any other function uses this function, throw an error
        List<Function> functions = getAllFunctions(txnId);
        for (Function function : functions) {
            for (Triple<DataverseName, String, String> functionalDependency : function.getDependencies().get(1)) {
                if (functionalDependency.first.equals(signature.getDataverseName())
                        && functionalDependency.second.equals(signature.getName())
                        && functionalDependency.third.equals(Integer.toString(signature.getArity()))) {
                    throw new AlgebricksException(
                            "Cannot drop function " + signature + " being used by function " + function.getSignature());
                }
            }
        }

        // if any other feed connection uses this function, throw an error
        List<FeedConnection> feedConnections = getAllFeedConnections(txnId);
        for (FeedConnection feedConnection : feedConnections) {
            if (feedConnection.containsFunction(signature)) {
                throw new AlgebricksException("Cannot drop function " + signature + " being used by feed connection "
                        + feedConnection.getDatasetName() + "." + feedConnection.getFeedName());
            }
        }
    }

    private void confirmDatasetCanBeDeleted(TxnId txnId, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        // If any function uses this type, throw an error
        List<Function> functions = getAllFunctions(txnId);
        for (Function function : functions) {
            for (Triple<DataverseName, String, String> datasetDependency : function.getDependencies().get(0)) {
                if (datasetDependency.first.equals(dataverseName) && datasetDependency.second.equals(datasetName)) {
                    throw new AlgebricksException("Cannot drop dataset "
                            + DatasetUtil.getFullyQualifiedDisplayName(dataverseName, datasetName)
                            + " being used by function " + function.getSignature());
                }
            }
        }
    }

    private void confirmLibraryCanBeDeleted(TxnId txnId, DataverseName dataverseName, String libraryName)
            throws AlgebricksException {
        confirmLibraryIsUnusedByFunctions(txnId, dataverseName, libraryName);
        confirmLibraryIsUnusedByAdapters(txnId, dataverseName, libraryName);
    }

    private void confirmLibraryIsUnusedByFunctions(TxnId txnId, DataverseName dataverseName, String libraryName)
            throws AlgebricksException {
        List<Function> functions = getAllFunctions(txnId);
        for (Function function : functions) {
            if (libraryName.equals(function.getLibraryName())
                    && dataverseName.equals(function.getLibraryDataverseName())) {
                throw new AlgebricksException("Cannot drop library " + dataverseName + '.' + libraryName
                        + " being used by funciton " + function.getSignature());
            }
        }
    }

    private void confirmLibraryIsUnusedByAdapters(TxnId txnId, DataverseName dataverseName, String libraryName)
            throws AlgebricksException {
        List<DatasourceAdapter> adapters = getAllAdapters(txnId);
        for (DatasourceAdapter adapter : adapters) {
            if (libraryName.equals(adapter.getLibraryName())
                    && dataverseName.equals(adapter.getLibraryDataverseName())) {
                throw new AlgebricksException("Cannot drop library " + dataverseName + '.' + libraryName
                        + " being used by adapter " + adapter.getAdapterIdentifier().getDataverseName() + '.'
                        + adapter.getAdapterIdentifier().getName());
            }
        }
    }

    private void confirmDatatypeIsUnused(TxnId txnId, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        confirmDatatypeIsUnusedByDatatypes(txnId, dataverseName, datatypeName);
        confirmDatatypeIsUnusedByDatasets(txnId, dataverseName, datatypeName);
        confirmDatatypeIsUnusedByFunctions(txnId, dataverseName, datatypeName);
    }

    private void confirmDatatypeIsUnusedByDatasets(TxnId txnId, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        // If any dataset uses this type, throw an error
        List<Dataset> datasets = getAllDatasets(txnId);
        for (Dataset set : datasets) {
            if (set.getItemTypeName().equals(datatypeName) && set.getItemTypeDataverseName().equals(dataverseName)) {
                throw new AlgebricksException(
                        "Cannot drop type " + TypeUtil.getFullyQualifiedDisplayName(dataverseName, datatypeName)
                                + " being used by dataset " + DatasetUtil.getFullyQualifiedDisplayName(set));
            }
        }
    }

    private void confirmDatatypeIsUnusedByDatatypes(TxnId txnId, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        // If any datatype uses this type, throw an error
        // TODO: Currently this loads all types into memory. This will need to be fixed
        // for large numbers of types
        Datatype dataTypeToBeDropped = getDatatype(txnId, dataverseName, datatypeName);
        assert dataTypeToBeDropped != null;
        IAType typeToBeDropped = dataTypeToBeDropped.getDatatype();
        List<Datatype> datatypes = getAllDatatypes(txnId);
        for (Datatype dataType : datatypes) {
            // skip types in different dataverses as well as the type to be dropped itself
            if (!dataType.getDataverseName().equals(dataverseName)
                    || dataType.getDatatype().getTypeName().equals(datatypeName)) {
                continue;
            }
            AbstractComplexType recType = (AbstractComplexType) dataType.getDatatype();
            if (recType.containsType(typeToBeDropped)) {
                throw new AlgebricksException("Cannot drop type "
                        + TypeUtil.getFullyQualifiedDisplayName(dataverseName, datatypeName) + " being used by type "
                        + TypeUtil.getFullyQualifiedDisplayName(dataverseName, recType.getTypeName()));
            }
        }
    }

    private void confirmDatatypeIsUnusedByFunctions(TxnId txnId, DataverseName dataverseName, String dataTypeName)
            throws AlgebricksException {
        // If any function uses this type, throw an error
        List<Function> functions = getAllFunctions(txnId);
        for (Function function : functions) {
            for (Triple<DataverseName, String, String> datasetDependency : function.getDependencies().get(2)) {
                if (datasetDependency.first.equals(dataverseName) && datasetDependency.second.equals(dataTypeName)) {
                    throw new AlgebricksException(
                            "Cannot drop type " + TypeUtil.getFullyQualifiedDisplayName(dataverseName, dataTypeName)
                                    + " is being used by function " + function.getSignature());
                }
            }
        }
    }

    private List<String> getNestedComplexDatatypeNamesForThisDatatype(TxnId txnId, DataverseName dataverseName,
            String datatypeName) throws AlgebricksException {
        // Return all field types that aren't builtin types
        Datatype parentType = getDatatype(txnId, dataverseName, datatypeName);

        List<IAType> subTypes = null;
        if (parentType.getDatatype().getTypeTag() == ATypeTag.OBJECT) {
            ARecordType recType = (ARecordType) parentType.getDatatype();
            subTypes = Arrays.asList(recType.getFieldTypes());
        } else if (parentType.getDatatype().getTypeTag() == ATypeTag.UNION) {
            AUnionType recType = (AUnionType) parentType.getDatatype();
            subTypes = recType.getUnionList();
        }

        List<String> nestedTypes = new ArrayList<>();
        if (subTypes != null) {
            for (IAType subType : subTypes) {
                if (!(subType instanceof BuiltinType)) {
                    nestedTypes.add(subType.getTypeName());
                }
            }
        }
        return nestedTypes;
    }

    private List<String> getDatasetNamesPartitionedOnThisNodeGroup(TxnId txnId, String nodegroup)
            throws AlgebricksException {
        // this needs to scan the datasets and return the datasets that use this
        // nodegroup
        List<String> nodeGroupDatasets = new ArrayList<>();
        List<Dataset> datasets = getAllDatasets(txnId);
        for (Dataset set : datasets) {
            if (set.getNodeGroupName().equals(nodegroup)) {
                nodeGroupDatasets.add(set.getDatasetName());
            }
        }
        return nodeGroupDatasets;

    }

    @Override
    public Index getIndex(TxnId txnId, DataverseName dataverseName, String datasetName, String indexName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName, indexName);
            IndexTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getIndexTupleTranslator(txnId, this, false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Index> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public List<Index> getDatasetIndexes(TxnId txnId, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            IndexTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getIndexTupleTranslator(txnId, this, false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Index> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public Datatype getDatatype(TxnId txnId, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            DatatypeTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getDataTypeTupleTranslator(txnId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public NodeGroup getNodeGroup(TxnId txnId, String nodeGroupName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            NodeGroupTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getNodeGroupTupleTranslator(false);
            IValueExtractor<NodeGroup> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<NodeGroup> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public Function getFunction(TxnId txnId, FunctionSignature functionSignature) throws AlgebricksException {
        List<Function> functions = getFunctionsImpl(txnId, createTuple(functionSignature.getDataverseName(),
                functionSignature.getName(), Integer.toString(functionSignature.getArity())));
        return functions.isEmpty() ? null : functions.get(0);
    }

    @Override
    public List<Function> getDataverseFunctions(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        return getFunctionsImpl(txnId, createTuple(dataverseName));
    }

    private List<Function> getFunctionsImpl(TxnId txnId, ITupleReference searchKey) throws AlgebricksException {
        try {
            FunctionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFunctionTupleTranslator(txnId, this, false);
            List<Function> results = new ArrayList<>();
            IValueExtractor<Function> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void dropFunction(TxnId txnId, FunctionSignature functionSignature) throws AlgebricksException {
        dropFunction(txnId, functionSignature, false);
    }

    private void dropFunction(TxnId txnId, FunctionSignature functionSignature, boolean force)
            throws AlgebricksException {
        if (!force) {
            confirmFunctionCanBeDeleted(txnId, functionSignature);
        }
        try {
            // Delete entry from the 'function' dataset.
            ITupleReference searchKey = createTuple(functionSignature.getDataverseName(), functionSignature.getName(),
                    Integer.toString(functionSignature.getArity()));
            // Searches the index for the tuple to be deleted. Acquires an S lock on the 'function' dataset.
            ITupleReference functionTuple =
                    getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET, functionTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException(
                        "Cannot drop function '" + functionSignature + "' because it doesn't exist", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    private ITupleReference getTupleToBeDeleted(TxnId txnId, IMetadataIndex metadataIndex, ITupleReference searchKey)
            throws AlgebricksException, HyracksDataException {
        IValueExtractor<ITupleReference> valueExtractor = new TupleCopyValueExtractor(metadataIndex.getTypeTraits());
        List<ITupleReference> results = new ArrayList<>();
        searchIndex(txnId, metadataIndex, searchKey, valueExtractor, results);
        if (results.isEmpty()) {
            throw HyracksDataException.create(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY);
        }
        // There should be exactly one result returned from the search.
        return results.get(0);
    }

    // Debugging Method
    private String printMetadata() {
        StringBuilder sb = new StringBuilder();
        try {
            RangePredicate rangePred;
            IMetadataIndex index = MetadataPrimaryIndexes.DATAVERSE_DATASET;
            String resourceName = index.getFile().toString();
            IIndex indexInstance = datasetLifecycleManager.get(resourceName);
            datasetLifecycleManager.open(resourceName);
            IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            IIndexCursor rangeCursor = indexAccessor.createSearchCursor(false);
            try {
                rangePred = new RangePredicate(null, null, true, true, null, null);
                indexAccessor.search(rangeCursor, rangePred);
                try {
                    while (rangeCursor.hasNext()) {
                        rangeCursor.next();
                        sb.append(TupleUtils.printTuple(rangeCursor.getTuple(),
                                new ISerializerDeserializer[] { SerializerDeserializerProvider.INSTANCE
                                        .getSerializerDeserializer(BuiltinType.ASTRING) }));
                    }
                } finally {
                    rangeCursor.close();
                }
                datasetLifecycleManager.close(resourceName);
                indexInstance = datasetLifecycleManager.get(resourceName);
                datasetLifecycleManager.open(resourceName);
                indexAccessor = indexInstance.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                rangeCursor = indexAccessor.createSearchCursor(false);
                rangePred = new RangePredicate(null, null, true, true, null, null);
                indexAccessor.search(rangeCursor, rangePred);
                try {
                    while (rangeCursor.hasNext()) {
                        rangeCursor.next();
                        sb.append(TupleUtils.printTuple(rangeCursor.getTuple(),
                                new ISerializerDeserializer[] {
                                        SerializerDeserializerProvider.INSTANCE
                                                .getSerializerDeserializer(BuiltinType.ASTRING),
                                        SerializerDeserializerProvider.INSTANCE
                                                .getSerializerDeserializer(BuiltinType.ASTRING) }));
                    }
                } finally {
                    rangeCursor.close();
                }
                datasetLifecycleManager.close(resourceName);
                indexInstance = datasetLifecycleManager.get(resourceName);
                datasetLifecycleManager.open(resourceName);
                indexAccessor = indexInstance.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                rangeCursor = indexAccessor.createSearchCursor(false);
                rangePred = new RangePredicate(null, null, true, true, null, null);
                indexAccessor.search(rangeCursor, rangePred);
                try {
                    while (rangeCursor.hasNext()) {
                        rangeCursor.next();
                        sb.append(TupleUtils.printTuple(rangeCursor.getTuple(), new ISerializerDeserializer[] {
                                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING),
                                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING),
                                SerializerDeserializerProvider.INSTANCE
                                        .getSerializerDeserializer(BuiltinType.ASTRING) }));
                    }
                } finally {
                    rangeCursor.close();
                }
            } finally {
                rangeCursor.destroy();
            }
            datasetLifecycleManager.close(resourceName);
        } catch (Exception e) {
            LOGGER.error(e);
        }
        return sb.toString();
    }

    private <T> void searchIndex(TxnId txnId, IMetadataIndex index, ITupleReference searchKey,
            IValueExtractor<T> valueExtractor, List<T> results) throws AlgebricksException, HyracksDataException {
        IBinaryComparatorFactory[] comparatorFactories = index.getKeyBinaryComparatorFactory();
        if (index.getFile() == null) {
            throw new AlgebricksException("No file for Index " + index.getDataverseName() + "." + index.getIndexName());
        }
        String resourceName = index.getFile().getRelativePath();
        IIndex indexInstance = datasetLifecycleManager.get(resourceName);
        datasetLifecycleManager.open(resourceName);
        IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        try {
            IBinaryComparator[] searchCmps = null;
            MultiComparator searchCmp = null;
            if (searchKey != null) {
                searchCmps = new IBinaryComparator[searchKey.getFieldCount()];
                for (int i = 0; i < searchKey.getFieldCount(); i++) {
                    searchCmps[i] = comparatorFactories[i].createBinaryComparator();
                }
                searchCmp = new MultiComparator(searchCmps);
            }
            RangePredicate rangePred = new RangePredicate(searchKey, searchKey, true, true, searchCmp, searchCmp);
            search(indexAccessor, rangePred, results, valueExtractor, txnId);
        } finally {
            indexAccessor.destroy();
        }
        datasetLifecycleManager.close(resourceName);
    }

    private <T> void search(IIndexAccessor indexAccessor, RangePredicate rangePred, List<T> results,
            IValueExtractor<T> valueExtractor, TxnId txnId) throws HyracksDataException, AlgebricksException {
        IIndexCursor rangeCursor = indexAccessor.createSearchCursor(false);
        try {
            indexAccessor.search(rangeCursor, rangePred);
            try {
                while (rangeCursor.hasNext()) {
                    rangeCursor.next();
                    T result = valueExtractor.getValue(txnId, rangeCursor.getTuple());
                    if (result != null) {
                        results.add(result);
                    }
                }
            } finally {
                rangeCursor.close();
            }
        } finally {
            rangeCursor.destroy();
        }
    }

    @Override
    public void initializeDatasetIdFactory(TxnId txnId) throws AlgebricksException {
        int mostRecentDatasetId;
        try {
            String resourceName = MetadataPrimaryIndexes.DATASET_DATASET.getFile().getRelativePath();
            IIndex indexInstance = datasetLifecycleManager.get(resourceName);
            datasetLifecycleManager.open(resourceName);
            try {
                mostRecentDatasetId = getMostRecentDatasetIdFromStoredDatasetIndex(indexInstance, txnId);
            } finally {
                datasetLifecycleManager.close(resourceName);
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        DatasetIdFactory.initialize(mostRecentDatasetId);
    }

    private int getMostRecentDatasetIdFromStoredDatasetIndex(IIndex indexInstance, TxnId txnId)
            throws HyracksDataException, AlgebricksException {
        DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
        IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        int mostRecentDatasetId = MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID;
        IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        try {
            IIndexCursor rangeCursor = indexAccessor.createSearchCursor(false);
            try {
                indexAccessor.search(rangeCursor, rangePred);
                try {
                    while (rangeCursor.hasNext()) {
                        rangeCursor.next();
                        final ITupleReference ref = rangeCursor.getTuple();
                        final Dataset ds = valueExtractor.getValue(txnId, ref);
                        int datasetId = Math.max(ds.getDatasetId(),
                                DatasetIdFactory.generateAlternatingDatasetId(ds.getDatasetId()));
                        if (mostRecentDatasetId < datasetId) {
                            mostRecentDatasetId = datasetId;
                        }
                    }
                } finally {
                    rangeCursor.close();
                }
            } finally {
                rangeCursor.destroy();
            }
        } finally {
            indexAccessor.destroy();
        }
        return mostRecentDatasetId;
    }

    public static ITupleReference createTuple(DataverseName dataverseName, String... rest) {
        return createTuple(dataverseName.getCanonicalForm(), rest);
    }

    public static ITupleReference createTuple(String first, String... rest) {
        try {
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1 + rest.length);
            ISerializerDeserializer<AString> stringSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
            AMutableString aString = new AMutableString(first);
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
            for (String s : rest) {
                aString.setValue(s);
                stringSerde.serialize(aString, tupleBuilder.getDataOutput());
                tupleBuilder.addFieldEndOffset();
            }
            ArrayTupleReference tuple = new ArrayTupleReference();
            tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            return tuple;
        } catch (HyracksDataException e) {
            // This should never happen
            throw new IllegalStateException("Failed to create search tuple", e);
        }
    }

    public static ITupleReference createTuple() {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(0);
        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    @Override
    public void addAdapter(TxnId txnId, DatasourceAdapter adapter) throws AlgebricksException {
        try {
            // Insert into the 'Adapter' dataset.
            DatasourceAdapterTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getAdapterTupleTranslator(true);
            ITupleReference adapterTuple = tupleReaderWriter.getTupleFromMetadataEntity(adapter);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, adapterTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("A adapter with this name " + adapter.getAdapterIdentifier().getName()
                        + " already exists in dataverse '" + adapter.getAdapterIdentifier().getDataverseName() + "'.",
                        e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void dropAdapter(TxnId txnId, DataverseName dataverseName, String adapterName) throws AlgebricksException {
        DatasourceAdapter adapter = getAdapter(txnId, dataverseName, adapterName);
        if (adapter == null) {
            throw new AlgebricksException("Cannot drop adapter '" + adapter + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'Adapter' dataset.
            ITupleReference searchKey = createTuple(dataverseName, adapterName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'Adapter' dataset.
            ITupleReference datasetTuple =
                    getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, datasetTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException("Cannot drop adapter '" + adapterName + " since it doesn't exist", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public DatasourceAdapter getAdapter(TxnId txnId, DataverseName dataverseName, String adapterName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, adapterName);
            DatasourceAdapterTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getAdapterTupleTranslator(false);
            List<DatasourceAdapter> results = new ArrayList<>();
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void addCompactionPolicy(TxnId txnId, CompactionPolicy compactionPolicy) throws AlgebricksException {
        try {
            // Insert into the 'CompactionPolicy' dataset.
            CompactionPolicyTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getCompactionPolicyTupleTranslator(true);
            ITupleReference compactionPolicyTuple = tupleReaderWriter.getTupleFromMetadataEntity(compactionPolicy);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET, compactionPolicyTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("A compcation policy with this name " + compactionPolicy.getPolicyName()
                        + " already exists in dataverse '" + compactionPolicy.getPolicyName() + "'.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public CompactionPolicy getCompactionPolicy(TxnId txnId, DataverseName dataverseName, String policyName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, policyName);
            CompactionPolicyTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getCompactionPolicyTupleTranslator(false);
            List<CompactionPolicy> results = new ArrayList<>();
            IValueExtractor<CompactionPolicy> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public List<DatasourceAdapter> getDataverseAdapters(TxnId txnId, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatasourceAdapterTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getAdapterTupleTranslator(false);
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<DatasourceAdapter> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void addLibrary(TxnId txnId, Library library) throws AlgebricksException {
        try {
            // Insert into the 'Library' dataset.
            LibraryTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getLibraryTupleTranslator(true);
            ITupleReference libraryTuple = tupleReaderWriter.getTupleFromMetadataEntity(library);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.LIBRARY_DATASET, libraryTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("A library with this name " + library.getDataverseName()
                        + " already exists in dataverse '" + library.getDataverseName() + "'.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void dropLibrary(TxnId txnId, DataverseName dataverseName, String libraryName) throws AlgebricksException {
        dropLibrary(txnId, dataverseName, libraryName, false);
    }

    private void dropLibrary(TxnId txnId, DataverseName dataverseName, String libraryName, boolean force)
            throws AlgebricksException {
        if (!force) {
            confirmLibraryCanBeDeleted(txnId, dataverseName, libraryName);
        }
        try {
            // Delete entry from the 'Library' dataset.
            ITupleReference searchKey = createTuple(dataverseName, libraryName);
            // Searches the index for the tuple to be deleted. Acquires an S lock on the 'Library' dataset.
            ITupleReference datasetTuple =
                    getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.LIBRARY_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.LIBRARY_DATASET, datasetTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException("Cannot drop library '" + libraryName + "' because it doesn't exist", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public Library getLibrary(TxnId txnId, DataverseName dataverseName, String libraryName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, libraryName);
            LibraryTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getLibraryTupleTranslator(false);
            List<Library> results = new ArrayList<>();
            IValueExtractor<Library> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.LIBRARY_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public int getMostRecentDatasetId() throws RemoteException {
        return DatasetIdFactory.getMostRecentDatasetId();
    }

    @Override
    public void addFeedPolicy(TxnId txnId, FeedPolicyEntity feedPolicy) throws AlgebricksException {
        try {
            // Insert into the 'FeedPolicy' dataset.
            FeedPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedPolicyTupleTranslator(true);
            ITupleReference feedPolicyTuple = tupleReaderWriter.getTupleFromMetadataEntity(feedPolicy);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, feedPolicyTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("A feed policy with this name " + feedPolicy.getPolicyName()
                        + " already exists in dataverse '" + feedPolicy.getPolicyName() + "'.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public FeedPolicyEntity getFeedPolicy(TxnId txnId, DataverseName dataverseName, String policyName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, policyName);
            FeedPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedPolicyTupleTranslator(false);
            List<FeedPolicyEntity> results = new ArrayList<>();
            IValueExtractor<FeedPolicyEntity> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void addFeedConnection(TxnId txnId, FeedConnection feedConnection) throws AlgebricksException {
        try {
            FeedConnectionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFeedConnectionTupleTranslator(true);
            ITupleReference feedConnTuple = tupleReaderWriter.getTupleFromMetadataEntity(feedConnection);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, feedConnTuple);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public List<FeedConnection> getFeedConnections(TxnId txnId, DataverseName dataverseName, String feedName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, feedName);
            FeedConnectionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFeedConnectionTupleTranslator(false);
            List<FeedConnection> results = new ArrayList<>();
            IValueExtractor<FeedConnection> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public FeedConnection getFeedConnection(TxnId txnId, DataverseName dataverseName, String feedName,
            String datasetName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, feedName, datasetName);
            FeedConnectionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFeedConnectionTupleTranslator(false);
            List<FeedConnection> results = new ArrayList<>();
            IValueExtractor<FeedConnection> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void dropFeedConnection(TxnId txnId, DataverseName dataverseName, String feedName, String datasetName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, feedName, datasetName);
            ITupleReference tuple =
                    getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, tuple);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void addFeed(TxnId txnId, Feed feed) throws AlgebricksException {
        try {
            // Insert into the 'Feed' dataset.
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(true);
            ITupleReference feedTuple = tupleReaderWriter.getTupleFromMetadataEntity(feed);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.FEED_DATASET, feedTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("A feed with this name " + feed.getFeedName()
                        + " already exists in dataverse '" + feed.getDataverseName() + "'.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public Feed getFeed(TxnId txnId, DataverseName dataverseName, String feedName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, feedName);
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(false);
            List<Feed> results = new ArrayList<>();
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.FEED_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public List<Feed> getFeeds(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(false);
            List<Feed> results = new ArrayList<>();
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.FEED_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void dropFeed(TxnId txnId, DataverseName dataverseName, String feedName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, feedName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'nodegroup' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.FEED_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.FEED_DATASET, tuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException("Cannot drop feed '" + feedName + "' because it doesn't exist", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void dropFeedPolicy(TxnId txnId, DataverseName dataverseName, String policyName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, policyName);
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, tuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException("Unknown feed policy " + policyName, e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public List<FeedPolicyEntity> getDataverseFeedPolicies(TxnId txnId, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            FeedPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedPolicyTupleTranslator(false);
            IValueExtractor<FeedPolicyEntity> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<FeedPolicyEntity> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void addExternalFile(TxnId txnId, ExternalFile externalFile) throws AlgebricksException {
        try {
            // Insert into the 'externalFiles' dataset.
            ExternalFileTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getExternalFileTupleTranslator(true);
            ITupleReference externalFileTuple = tupleReaderWriter.getTupleFromMetadataEntity(externalFile);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, externalFileTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("An externalFile with this number " + externalFile.getFileNumber()
                        + " already exists in dataset '" + externalFile.getDatasetName() + "' in dataverse '"
                        + externalFile.getDataverseName() + "'.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public List<ExternalFile> getExternalFiles(TxnId txnId, Dataset dataset) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataset.getDataverseName(), dataset.getDatasetName());
            ExternalFileTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getExternalFileTupleTranslator(false);
            IValueExtractor<ExternalFile> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<ExternalFile> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void dropExternalFile(TxnId txnId, DataverseName dataverseName, String datasetName, int fileNumber)
            throws AlgebricksException {
        try {
            // Delete entry from the 'ExternalFile' dataset.
            ITupleReference searchKey = createExternalFileSearchTuple(dataverseName, datasetName, fileNumber);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'ExternalFile' dataset.
            ITupleReference datasetTuple =
                    getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, datasetTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException("Couldn't drop externalFile.", e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void dropExternalFiles(TxnId txnId, Dataset dataset) throws AlgebricksException {
        List<ExternalFile> files = getExternalFiles(txnId, dataset);
        // loop through files and delete them
        for (int i = 0; i < files.size(); i++) {
            dropExternalFile(txnId, files.get(i).getDataverseName(), files.get(i).getDatasetName(),
                    files.get(i).getFileNumber());
        }
    }

    // This method is used to create a search tuple for external data file since the
    // search tuple has an int value
    public ITupleReference createExternalFileSearchTuple(DataverseName dataverseName, String datasetName,
            int fileNumber) throws HyracksDataException {
        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
        ISerializerDeserializer<AInt32> intSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

        AMutableString aString = new AMutableString("");
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);

        // dataverse field
        aString.setValue(dataverseName.getCanonicalForm());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // dataset field
        aString.setValue(datasetName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // file number field
        intSerde.serialize(new AInt32(fileNumber), tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    @Override
    public ExternalFile getExternalFile(TxnId txnId, DataverseName dataverseName, String datasetName,
            Integer fileNumber) throws AlgebricksException {
        try {
            ITupleReference searchKey = createExternalFileSearchTuple(dataverseName, datasetName, fileNumber);
            ExternalFileTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getExternalFileTupleTranslator(false);
            IValueExtractor<ExternalFile> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<ExternalFile> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void addSynonym(TxnId txnId, Synonym synonym) throws AlgebricksException {
        try {
            // Insert into the 'Synonym' dataset.
            SynonymTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getSynonymTupleTranslator(true);
            ITupleReference synonymTuple = tupleReaderWriter.getTupleFromMetadataEntity(synonym);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.SYNONYM_DATASET, synonymTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS) && e.getErrorCode() == ErrorCode.DUPLICATE_KEY) {
                throw new AlgebricksException("A synonym with name '" + synonym.getSynonymName() + "' already exists.",
                        e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void dropSynonym(TxnId txnId, DataverseName dataverseName, String synonymName) throws AlgebricksException {
        Synonym synonym = getSynonym(txnId, dataverseName, synonymName);
        if (synonym == null) {
            throw new AlgebricksException("Cannot drop synonym '" + synonym + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'Synonym' dataset.
            ITupleReference searchKey = createTuple(dataverseName, synonymName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'Synonym' dataset.
            ITupleReference synonymTuple =
                    getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.SYNONYM_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.SYNONYM_DATASET, synonymTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException("Cannot drop synonym '" + synonymName, e);
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public Synonym getSynonym(TxnId txnId, DataverseName dataverseName, String synonymName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, synonymName);
            SynonymTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getSynonymTupleTranslator(false);
            List<Synonym> results = new ArrayList<>();
            IValueExtractor<Synonym> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.SYNONYM_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public List<Synonym> getDataverseSynonyms(TxnId txnId, DataverseName dataverseName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            SynonymTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getSynonymTupleTranslator(false);
            IValueExtractor<Synonym> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Synonym> results = new ArrayList<>();
            searchIndex(txnId, MetadataPrimaryIndexes.SYNONYM_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void updateDataset(TxnId txnId, Dataset dataset) throws AlgebricksException {
        try {
            // This method will delete previous entry of the dataset and insert the new one
            // Delete entry from the 'datasets' dataset.
            ITupleReference searchKey = createTuple(dataset.getDataverseName(), dataset.getDatasetName());
            // Searches the index for the tuple to be deleted. Acquires an S lock on the 'dataset' dataset.
            ITupleReference datasetTuple =
                    getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            // Insert into the 'dataset' dataset.
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(true);
            datasetTuple = tupleReaderWriter.getTupleFromMetadataEntity(dataset);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
        } catch (HyracksDataException e) {
            if (e.getComponent().equals(ErrorCode.HYRACKS)
                    && e.getErrorCode() == ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                throw new AlgebricksException(
                        "Cannot drop dataset '" + dataset.getDatasetName() + "' because it doesn't exist");
            } else {
                throw new AlgebricksException(e);
            }
        }
    }

    @Override
    public void updateLibrary(TxnId txnId, Library library) throws AlgebricksException {
        dropLibrary(txnId, library.getDataverseName(), library.getName(), true);
        addLibrary(txnId, library);
    }

    @Override
    public void updateFunction(TxnId txnId, Function function) throws AlgebricksException {
        dropFunction(txnId, function.getSignature(), true);
        addFunction(txnId, function);
    }

    @Override
    public void updateDatatype(TxnId txnId, Datatype datatype) throws AlgebricksException {
        dropDatatype(txnId, datatype.getDataverseName(), datatype.getDatatypeName(), true);
        addDatatype(txnId, datatype);
    }

    public ITxnIdFactory getTxnIdFactory() {
        return txnIdFactory;
    }
}
