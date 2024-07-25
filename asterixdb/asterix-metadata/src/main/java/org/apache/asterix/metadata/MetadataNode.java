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

import static org.apache.asterix.common.api.IIdentifierMapper.Modifier.PLURAL;
import static org.apache.asterix.common.exceptions.ErrorCode.ADAPTER_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.CANNOT_DROP_DATABASE_DEPENDENT_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.CANNOT_DROP_DATAVERSE_DEPENDENT_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.CANNOT_DROP_OBJECT_DEPENDENT_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.COMPACTION_POLICY_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.DATABASE_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.DATASET_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.DATAVERSE_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_FILE_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.FEED_CONNECTION_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.FEED_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.FEED_POLICY_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.FULL_TEXT_DEFAULT_CONFIG_CANNOT_BE_DELETED_OR_CREATED;
import static org.apache.asterix.common.exceptions.ErrorCode.FUNCTION_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.INDEX_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.LIBRARY_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.METADATA_ERROR;
import static org.apache.asterix.common.exceptions.ErrorCode.NODEGROUP_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.NODE_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.SYNONYM_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.TYPE_EXISTS;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_ADAPTER;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_DATABASE;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_DATAVERSE;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_EXTERNAL_FILE;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_FEED;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_FEED_CONNECTION;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_FEED_POLICY;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_FUNCTION;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_INDEX;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_LIBRARY;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_NODEGROUP;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_SYNONYM;
import static org.apache.asterix.common.exceptions.ErrorCode.UNKNOWN_TYPE;
import static org.apache.asterix.common.utils.IdentifierUtil.dataset;

import java.io.PrintStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.dataflow.LSMIndexUtil;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager.AtomicityLevel;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.formats.nontagged.CleanJSONPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.NullIntrospector;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.api.ExtensionMetadataDataset;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;
import org.apache.asterix.metadata.api.IExtensionMetadataSearchKey;
import org.apache.asterix.metadata.api.IMetadataEntityTupleTranslator;
import org.apache.asterix.metadata.api.IMetadataExtension;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.api.IMetadataNode;
import org.apache.asterix.metadata.api.IValueExtractor;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.bootstrap.MetadataIndexesProvider;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Database;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.DependencyKind;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.FullTextConfigMetadataEntity;
import org.apache.asterix.metadata.entities.FullTextFilterMetadataEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.metadata.entities.ViewDetails;
import org.apache.asterix.metadata.entitytupletranslators.CompactionPolicyTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatabaseTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatasetTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatasourceAdapterTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatatypeTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DataverseTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.ExternalFileTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedConnectionTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedPolicyTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FullTextConfigMetadataEntityTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FullTextFilterMetadataEntityTupleTranslator;
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
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.AbstractComplexType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.fulltext.FullTextConfigDescriptor;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback.Operation;
import org.apache.asterix.transaction.management.opcallbacks.NoOpModificationOpCallback;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexModificationOperationCallback;
import org.apache.asterix.transaction.management.opcallbacks.UpsertOperationCallback;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
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
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Strings;

public class MetadataNode implements IMetadataNode {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    // shared between core and extension
    private transient IDatasetLifecycleManager datasetLifecycleManager;
    private transient ITransactionSubsystem transactionSubsystem;
    private int metadataStoragePartition;
    private transient CachingTxnIdFactory txnIdFactory;
    // core only
    private transient MetadataIndexesProvider mdIndexesProvider;
    private transient MetadataTupleTranslatorProvider tupleTranslatorProvider;
    // extension only
    private Map<ExtensionMetadataDatasetId, ExtensionMetadataDataset<?>> extensionDatasets;
    private final ReentrantLock metadataModificationLock = new ReentrantLock(true);
    private boolean atomicNoWAL;

    public static final MetadataNode INSTANCE = new MetadataNode();

    private MetadataNode() {
        super();
    }

    public void initialize(INcApplicationContext runtimeContext, MetadataIndexesProvider metadataIndexesProvider,
            MetadataTupleTranslatorProvider tupleTranslatorProvider, List<IMetadataExtension> metadataExtensions,
            int partitionId) {
        this.mdIndexesProvider = metadataIndexesProvider;
        this.tupleTranslatorProvider = tupleTranslatorProvider;
        this.transactionSubsystem = runtimeContext.getTransactionSubsystem();
        this.datasetLifecycleManager = runtimeContext.getDatasetLifecycleManager();
        this.metadataStoragePartition = partitionId;
        if (metadataExtensions != null) {
            extensionDatasets = new HashMap<>();
            for (IMetadataExtension metadataExtension : metadataExtensions) {
                for (ExtensionMetadataDataset<?> extensionIndex : metadataExtension
                        .getExtensionIndexes(metadataIndexesProvider)) {
                    extensionDatasets.put(extensionIndex.getId(), extensionIndex);
                }
            }
        }
        this.txnIdFactory = new CachingTxnIdFactory(runtimeContext);
        atomicNoWAL = runtimeContext.isCloudDeployment();
    }

    public int getMetadataStoragePartition() {
        return metadataStoragePartition;
    }

    @Override
    public void beginTransaction(TxnId transactionId) {
        AtomicityLevel lvl = atomicNoWAL ? AtomicityLevel.ATOMIC_NO_WAL : AtomicityLevel.ATOMIC;
        TransactionOptions options = new TransactionOptions(lvl);
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
        ExtensionMetadataDataset<T> index = getExtensionMetadataDataset(entity.getDatasetId());
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator(true);
        addEntity(txnId, entity, tupleTranslator, index);
    }

    @Override
    public <T extends IExtensionMetadataEntity> void upsertEntity(TxnId txnId, T entity) throws AlgebricksException {
        ExtensionMetadataDataset<T> index = getExtensionMetadataDataset(entity.getDatasetId());
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator(true);
        upsertEntity(txnId, entity, tupleTranslator, index);
    }

    @Override
    public <T extends IExtensionMetadataEntity> void deleteEntity(TxnId txnId, T entity) throws AlgebricksException {
        ExtensionMetadataDataset<T> index = getExtensionMetadataDataset(entity.getDatasetId());
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator(true);
        deleteEntity(txnId, entity, tupleTranslator, index);
    }

    @Override
    public <T extends IExtensionMetadataEntity> List<T> getEntities(TxnId txnId, IExtensionMetadataSearchKey searchKey)
            throws AlgebricksException {
        ExtensionMetadataDataset<T> index = getExtensionMetadataDataset(searchKey.getDatasetId());
        IMetadataEntityTupleTranslator<T> tupleTranslator = index.getTupleTranslator(false);
        return getEntities(txnId, searchKey.getSearchKey(mdIndexesProvider), tupleTranslator, index);
    }

    @Override
    public JsonNode getEntitiesAsJson(TxnId txnId, IMetadataIndex metadataIndex, int payloadPosition)
            throws AlgebricksException, RemoteException {
        try {
            return getJsonNodes(txnId, metadataIndex, payloadPosition);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    private <T extends IExtensionMetadataEntity> ExtensionMetadataDataset<T> getExtensionMetadataDataset(
            ExtensionMetadataDatasetId datasetId) throws AlgebricksException {
        ExtensionMetadataDataset<T> index = (ExtensionMetadataDataset<T>) extensionDatasets.get(datasetId);
        if (index == null) {
            throw new AsterixException(METADATA_ERROR, "Metadata Extension Index: " + datasetId + " was not found");
        }
        return index;
    }

    @Override
    public void addDatabase(TxnId txnId, Database database) throws AlgebricksException, RemoteException {
        try {
            if (!mdIndexesProvider.isUsingDatabase()) {
                return;
            }
            DatabaseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatabaseTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(database);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getDatabaseEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(DATABASE_EXISTS, e, database.getDatabaseName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void addDataverse(TxnId txnId, Dataverse dataverse) throws AlgebricksException {
        try {
            DataverseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataverseTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(dataverse);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getDataverseEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                //TODO(DB): consider adding the database name to the error message?
                throw new AsterixException(DATAVERSE_EXISTS, e, dataverse.getDataverseName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void addDataset(TxnId txnId, Dataset dataset) throws AlgebricksException {
        try {
            // Insert into the 'dataset' dataset.
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(true);
            ITupleReference datasetTuple = tupleReaderWriter.getTupleFromMetadataEntity(dataset);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getDatasetEntity().getIndex(), datasetTuple);
            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                // Add the primary index for the dataset.
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                Index primaryIndex = Index.createPrimaryIndex(dataset.getDatabaseName(), dataset.getDataverseName(),
                        dataset.getDatasetName(), id.getPrimaryKey(), id.getKeySourceIndicator(),
                        id.getPrimaryKeyType(), dataset.getPendingOp());

                addIndex(txnId, primaryIndex);
            }
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(DATASET_EXISTS, e, dataset.getDatasetName(), dataset.getDataverseName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void addIndex(TxnId txnId, Index index) throws AlgebricksException {
        try {
            IndexTupleTranslator tupleWriter = tupleTranslatorProvider.getIndexTupleTranslator(txnId, this, true);
            ITupleReference tuple = tupleWriter.getTupleFromMetadataEntity(index);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getIndexEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(INDEX_EXISTS, e, index.getIndexName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void addNode(TxnId txnId, Node node) throws AlgebricksException {
        try {
            NodeTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getNodeTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(node);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getNodeEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(NODE_EXISTS, e, node.getNodeName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void modifyNodeGroup(TxnId txnId, NodeGroup nodeGroup, Operation modificationOp) throws AlgebricksException {
        try {
            NodeGroupTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getNodeGroupTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(nodeGroup);
            modifyMetadataIndex(modificationOp, txnId, mdIndexesProvider.getNodeGroupEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(NODEGROUP_EXISTS, e, nodeGroup.getNodeGroupName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void addDatatype(TxnId txnId, Datatype datatype) throws AlgebricksException {
        try {
            DatatypeTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getDataTypeTupleTranslator(txnId, this, true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(datatype);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getDatatypeEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(TYPE_EXISTS, e, datatype.getDatatypeName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
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
            insertTupleIntoIndex(txnId, mdIndexesProvider.getFunctionEntity().getIndex(), functionTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(FUNCTION_EXISTS, e, function.getName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void addFullTextFilter(TxnId txnId, FullTextFilterMetadataEntity filterMetadataEntity)
            throws RemoteException, AlgebricksException {
        insertFullTextFilterMetadataEntityToCatalog(txnId, filterMetadataEntity);
    }

    @Override
    public FullTextFilterMetadataEntity getFullTextFilter(TxnId txnId, String database, DataverseName dataverseName,
            String filterName) throws AlgebricksException {
        try {
            FullTextFilterMetadataEntityTupleTranslator translator =
                    tupleTranslatorProvider.getFullTextFilterTupleTranslator(true);
            ITupleReference searchKey = createTuple(database, dataverseName, filterName);
            IValueExtractor<FullTextFilterMetadataEntity> valueExtractor =
                    new MetadataEntityValueExtractor<>(translator);
            List<FullTextFilterMetadataEntity> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getFullTextFilterEntity().getIndex(), searchKey, valueExtractor,
                    results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void dropFullTextFilter(TxnId txnId, String database, DataverseName dataverseName, String filterName)
            throws AlgebricksException {
        dropFullTextFilterDescriptor(txnId, database, dataverseName, filterName, false);
    }

    private void dropFullTextFilterDescriptor(TxnId txnId, String database, DataverseName dataverseName,
            String filterName, boolean force) throws AlgebricksException {
        if (!force) {
            confirmFullTextFilterCanBeDeleted(txnId, database, dataverseName, filterName);
        }
        try {
            ITupleReference key = createTuple(database, dataverseName, filterName);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getFullTextFilterEntity().getIndex(), key);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private void insertFullTextConfigMetadataEntityToCatalog(TxnId txnId, FullTextConfigMetadataEntity config)
            throws AlgebricksException {
        try {
            FullTextConfigMetadataEntityTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFullTextConfigTupleTranslator(true);
            ITupleReference configTuple = tupleReaderWriter.getTupleFromMetadataEntity(config);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getFullTextConfigEntity().getIndex(), configTuple);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private void insertFullTextFilterMetadataEntityToCatalog(TxnId txnId, FullTextFilterMetadataEntity filter)
            throws AlgebricksException {
        try {
            FullTextFilterMetadataEntityTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFullTextFilterTupleTranslator(true);
            ITupleReference filterTuple = tupleReaderWriter.getTupleFromMetadataEntity(filter);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getFullTextFilterEntity().getIndex(), filterTuple);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void addFullTextConfig(TxnId txnId, FullTextConfigMetadataEntity config)
            throws AlgebricksException, RemoteException {
        insertFullTextConfigMetadataEntityToCatalog(txnId, config);
    }

    @Override
    public FullTextConfigMetadataEntity getFullTextConfig(TxnId txnId, String database, DataverseName dataverseName,
            String configName) throws AlgebricksException {
        FullTextConfigMetadataEntityTupleTranslator translator =
                tupleTranslatorProvider.getFullTextConfigTupleTranslator(true);

        ITupleReference searchKey;
        List<FullTextConfigMetadataEntity> results = new ArrayList<>();
        try {
            searchKey = createTuple(database, dataverseName, configName);
            IValueExtractor<FullTextConfigMetadataEntity> valueExtractor =
                    new MetadataEntityValueExtractor<>(translator);
            searchIndex(txnId, mdIndexesProvider.getFullTextConfigEntity().getIndex(), searchKey, valueExtractor,
                    results);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }

        if (results.isEmpty()) {
            return null;
        }

        return results.get(0);
    }

    @Override
    public void dropFullTextConfig(TxnId txnId, String database, DataverseName dataverseName, String configName)
            throws AlgebricksException {
        dropFullTextConfigDescriptor(txnId, database, dataverseName, configName, false);
    }

    private void dropFullTextConfigDescriptor(TxnId txnId, String database, DataverseName dataverseName,
            String configName, boolean force) throws AlgebricksException {
        if (!force) {
            confirmFullTextConfigCanBeDeleted(txnId, database, dataverseName, configName);
        }

        try {
            ITupleReference key = createTuple(database, dataverseName, configName);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getFullTextConfigEntity().getIndex(), key);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
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

    private void deleteTupleFromIndex(TxnId txnId, IMetadataIndex metadataIndex, ITupleReference tuple)
            throws HyracksDataException {
        modifyMetadataIndex(Operation.DELETE, txnId, metadataIndex, tuple);
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
            txnCtx.acquireExclusiveWriteLock(metadataModificationLock);
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
                if (!txnCtx.hasWAL()) {
                    return new NoOpModificationOpCallback(metadataIndex.getDatasetId(),
                            metadataIndex.getPrimaryKeyIndexes(), txnCtx, transactionSubsystem.getLockManager(),
                            transactionSubsystem, metadataIndex.getResourceId(), metadataStoragePartition,
                            ResourceType.LSM_BTREE, indexOp);
                }
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
                if (!txnCtx.hasWAL()) {
                    return new NoOpModificationOpCallback(metadataIndex.getDatasetId(),
                            metadataIndex.getPrimaryKeyIndexes(), txnCtx, transactionSubsystem.getLockManager(),
                            transactionSubsystem, metadataIndex.getResourceId(), metadataStoragePartition,
                            ResourceType.LSM_BTREE, indexOp);
                }
                return new UpsertOperationCallback(metadataIndex.getDatasetId(), metadataIndex.getPrimaryKeyIndexes(),
                        txnCtx, transactionSubsystem.getLockManager(), transactionSubsystem,
                        metadataIndex.getResourceId(), metadataStoragePartition, ResourceType.LSM_BTREE, indexOp);
            default:
                throw new IllegalStateException("Unknown operation type: " + indexOp);
        }
    }

    @Override
    public void dropDatabase(TxnId txnId, String databaseName) throws AlgebricksException, RemoteException {
        try {
            if (!mdIndexesProvider.isUsingDatabase()) {
                return;
            }
            //TODO(DB): review
            confirmDatabaseCanBeDeleted(txnId, databaseName);

            // Drop all feeds and connections in this database.
            // Feeds may depend on datatypes and adapters
            dropDatabaseFeeds(txnId, databaseName);

            // Drop all feed ingestion policies in this database.
            List<FeedPolicyEntity> feedPolicies = getDatabaseFeedPolicies(txnId, databaseName);
            for (FeedPolicyEntity feedPolicy : feedPolicies) {
                dropFeedPolicy(txnId, databaseName, feedPolicy.getDataverseName(), feedPolicy.getPolicyName());
            }

            // Drop all functions in this database.
            // Functions may depend on libraries, datasets, functions, datatypes, synonyms
            // As a side effect, acquires an S lock on the 'Function' dataset on behalf of txnId.
            List<Function> databaseFunctions = getDatabaseFunctions(txnId, databaseName);
            for (Function function : databaseFunctions) {
                dropFunction(txnId, function.getSignature(), true);
            }

            // Drop all adapters in this database.
            // Adapters depend on libraries.
            // As a side effect, acquires an S lock on the 'Adapter' dataset on behalf of txnId.
            List<DatasourceAdapter> databaseAdapters = getDatabaseAdapters(txnId, databaseName);
            for (DatasourceAdapter adapter : databaseAdapters) {
                dropAdapter(txnId, databaseName, adapter.getAdapterIdentifier().getDataverseName(),
                        adapter.getAdapterIdentifier().getName());
            }

            // Drop all libraries in this database.
            List<Library> databaseLibraries = getDatabaseLibraries(txnId, databaseName);
            for (Library lib : databaseLibraries) {
                dropLibrary(txnId, lib.getDatabaseName(), lib.getDataverseName(), lib.getName(), true);
            }

            // Drop all synonyms in this database.
            List<Synonym> databaseSynonyms = getDatabaseSynonyms(txnId, databaseName);
            for (Synonym synonym : databaseSynonyms) {
                dropSynonym(txnId, databaseName, synonym.getDataverseName(), synonym.getSynonymName(), true);
            }

            // Drop all datasets and indexes in this database.
            // Datasets depend on datatypes
            List<Dataset> databaseDatasets = getDatabaseDatasets(txnId, databaseName);
            for (Dataset ds : databaseDatasets) {
                dropDataset(txnId, databaseName, ds.getDataverseName(), ds.getDatasetName(), true);
            }

            // Drop full-text configs in this database.
            // Note that full-text configs are utilized by the index, and we need to always drop index first
            // and then full-text config
            List<FullTextConfigMetadataEntity> configMetadataEntities = getDatabaseFullTextConfigs(txnId, databaseName);
            for (FullTextConfigMetadataEntity configMetadataEntity : configMetadataEntities) {
                dropFullTextConfigDescriptor(txnId, databaseName,
                        configMetadataEntity.getFullTextConfig().getDataverseName(),
                        configMetadataEntity.getFullTextConfig().getName(), true);
            }

            // Drop full-text filters in this database.
            // Note that full-text filters are utilized by the full-text configs,
            // and we need to always drop full-text configs first
            // and then full-text filter
            List<FullTextFilterMetadataEntity> filters = getDatabaseFullTextFilters(txnId, databaseName);
            for (FullTextFilterMetadataEntity filter : filters) {
                dropFullTextFilterDescriptor(txnId, databaseName, filter.getFullTextFilter().getDataverseName(),
                        filter.getFullTextFilter().getName(), true);
            }

            // Drop all types in this database.
            // As a side effect, acquires an S lock on the 'datatype' dataset on behalf of txnId.
            List<Datatype> databaseDatatypes = getDatabaseDatatypes(txnId, databaseName);
            for (Datatype dataType : databaseDatatypes) {
                forceDropDatatype(txnId, databaseName, dataType.getDataverseName(), dataType.getDatatypeName());
            }

            List<Dataverse> databaseDataverses = getDatabaseDataverses(txnId, databaseName);
            for (Dataverse dataverse : databaseDataverses) {
                forceDropDataverse(txnId, databaseName, dataverse.getDataverseName());
            }

            // delete the database entry from the 'Database' collection
            // as a side effect, acquires an S lock on the 'Database' collection on behalf of txnId
            ITupleReference searchKey = createTuple(databaseName);
            ITupleReference tuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getDatabaseEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getDatabaseEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_DATABASE, e, databaseName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    private void dropDatabaseFeeds(TxnId txnId, String databaseName) throws AlgebricksException {
        List<Feed> databaseFeeds = getDatabaseFeeds(txnId, databaseName);
        for (Feed feed : databaseFeeds) {
            List<FeedConnection> feedConnections =
                    getFeedConnections(txnId, databaseName, feed.getDataverseName(), feed.getFeedName());
            for (FeedConnection feedConnection : feedConnections) {
                dropFeedConnection(txnId, databaseName, feedConnection.getDataverseName(), feed.getFeedName(),
                        feedConnection.getDatasetName());
            }
            dropFeed(txnId, databaseName, feed.getDataverseName(), feed.getFeedName());
        }
    }

    @Override
    public void dropDataverse(TxnId txnId, String database, DataverseName dataverseName) throws AlgebricksException {
        try {
            confirmDataverseCanBeDeleted(txnId, database, dataverseName);

            // Drop all feeds and connections in this dataverse.
            // Feeds may depend on datatypes and adapters
            List<Feed> dataverseFeeds = getDataverseFeeds(txnId, database, dataverseName);
            for (Feed feed : dataverseFeeds) {
                List<FeedConnection> feedConnections =
                        getFeedConnections(txnId, database, dataverseName, feed.getFeedName());
                for (FeedConnection feedConnection : feedConnections) {
                    dropFeedConnection(txnId, database, dataverseName, feed.getFeedName(),
                            feedConnection.getDatasetName());
                }
                dropFeed(txnId, database, dataverseName, feed.getFeedName());
            }

            // Drop all feed ingestion policies in this dataverse.
            List<FeedPolicyEntity> feedPolicies = getDataverseFeedPolicies(txnId, database, dataverseName);
            for (FeedPolicyEntity feedPolicy : feedPolicies) {
                dropFeedPolicy(txnId, database, dataverseName, feedPolicy.getPolicyName());
            }

            // Drop all functions in this dataverse.
            // Functions may depend on libraries, datasets, functions, datatypes, synonyms
            // As a side effect, acquires an S lock on the 'Function' dataset on behalf of txnId.
            List<Function> dataverseFunctions = getDataverseFunctions(txnId, database, dataverseName);
            for (Function function : dataverseFunctions) {
                dropFunction(txnId, function.getSignature(), true);
            }

            // Drop all adapters in this dataverse.
            // Adapters depend on libraries.
            // As a side effect, acquires an S lock on the 'Adapter' dataset on behalf of txnId.
            List<DatasourceAdapter> dataverseAdapters = getDataverseAdapters(txnId, database, dataverseName);
            for (DatasourceAdapter adapter : dataverseAdapters) {
                dropAdapter(txnId, database, dataverseName, adapter.getAdapterIdentifier().getName());
            }

            // Drop all libraries in this dataverse.
            //TODO(DB): should may be check the library dependency above
            List<Library> dataverseLibraries = getDataverseLibraries(txnId, database, dataverseName);
            for (Library lib : dataverseLibraries) {
                dropLibrary(txnId, lib.getDatabaseName(), lib.getDataverseName(), lib.getName());
            }

            // Drop all synonyms in this dataverse.
            List<Synonym> dataverseSynonyms = getDataverseSynonyms(txnId, database, dataverseName);
            for (Synonym synonym : dataverseSynonyms) {
                dropSynonym(txnId, database, dataverseName, synonym.getSynonymName(), true);
            }

            // Drop all datasets and indexes in this dataverse.
            // Datasets depend on datatypes
            List<Dataset> dataverseDatasets = getDataverseDatasets(txnId, database, dataverseName);
            for (Dataset ds : dataverseDatasets) {
                dropDataset(txnId, database, dataverseName, ds.getDatasetName(), true);
            }

            // Drop full-text configs in this dataverse.
            // Note that full-text configs are utilized by the index, and we need to always drop index first
            // and then full-text config
            List<FullTextConfigMetadataEntity> configMetadataEntities =
                    getDataverseFullTextConfigs(txnId, database, dataverseName);
            for (FullTextConfigMetadataEntity configMetadataEntity : configMetadataEntities) {
                dropFullTextConfigDescriptor(txnId, database, dataverseName,
                        configMetadataEntity.getFullTextConfig().getName(), true);
            }

            // Drop full-text filters in this dataverse.
            // Note that full-text filters are utilized by the full-text configs,
            // and we need to always drop full-text configs first
            // and then full-text filter
            List<FullTextFilterMetadataEntity> filters = getDataverseFullTextFilters(txnId, database, dataverseName);
            for (FullTextFilterMetadataEntity filter : filters) {
                dropFullTextFilterDescriptor(txnId, database, dataverseName, filter.getFullTextFilter().getName(),
                        true);
            }

            // Drop all types in this dataverse.
            // As a side effect, acquires an S lock on the 'datatype' dataset on behalf of txnId.
            List<Datatype> dataverseDatatypes = getDataverseDatatypes(txnId, database, dataverseName);
            for (Datatype dataverseDatatype : dataverseDatatypes) {
                forceDropDatatype(txnId, database, dataverseName, dataverseDatatype.getDatatypeName());
            }

            // Delete the dataverse entry from the 'dataverse' dataset.
            // As a side effect, acquires an S lock on the 'dataverse' dataset on behalf of txnId.
            forceDropDataverse(txnId, database, dataverseName);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_DATAVERSE, e,
                        MetadataUtil.dataverseName(database, dataverseName, mdIndexesProvider.isUsingDatabase()));
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    private void forceDropDataverse(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, HyracksDataException {
        ITupleReference searchKey = createTuple(database, dataverseName);
        ITupleReference tuple =
                getTupleToBeDeleted(txnId, mdIndexesProvider.getDataverseEntity().getIndex(), searchKey);
        deleteTupleFromIndex(txnId, mdIndexesProvider.getDataverseEntity().getIndex(), tuple);
    }

    @Override
    public boolean isDataverseNotEmpty(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        return !getDataverseDatatypes(txnId, database, dataverseName).isEmpty()
                || !getDataverseDatasets(txnId, database, dataverseName).isEmpty()
                || !getDataverseLibraries(txnId, database, dataverseName).isEmpty()
                || !getDataverseAdapters(txnId, database, dataverseName).isEmpty()
                || !getDataverseFunctions(txnId, database, dataverseName).isEmpty()
                || !getDataverseFeedPolicies(txnId, database, dataverseName).isEmpty()
                || !getDataverseFeeds(txnId, database, dataverseName).isEmpty()
                || !getDataverseSynonyms(txnId, database, dataverseName).isEmpty()
                || !getDataverseFullTextConfigs(txnId, database, dataverseName).isEmpty()
                || !getDataverseFullTextFilters(txnId, database, dataverseName).isEmpty();
    }

    @Override
    public void dropDataset(TxnId txnId, String database, DataverseName dataverseName, String datasetName,
            boolean force) throws AlgebricksException {
        Dataset dataset = getDataset(txnId, database, dataverseName, datasetName);
        if (dataset == null) {
            throw new AsterixException(UNKNOWN_DATASET_IN_DATAVERSE, datasetName,
                    MetadataUtil.dataverseName(database, dataverseName, mdIndexesProvider.isUsingDatabase()));
        }
        if (!force) {
            String datasetTypeDisplayName = DatasetUtil.getDatasetTypeDisplayName(dataset.getDatasetType());
            confirmDatasetCanBeDeleted(txnId, datasetTypeDisplayName, database, dataverseName, datasetName);
        }

        try {
            // Delete entry from the 'datasets' dataset.
            ITupleReference searchKey = createTuple(database, dataverseName, datasetName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'dataset' dataset.
            ITupleReference datasetTuple = null;
            try {
                datasetTuple = getTupleToBeDeleted(txnId, mdIndexesProvider.getDatasetEntity().getIndex(), searchKey);

                switch (dataset.getDatasetType()) {
                    case INTERNAL:
                        // Delete entry(s) from the 'indexes' dataset.
                        List<Index> datasetIndexes = getDatasetIndexes(txnId, database, dataverseName, datasetName);
                        if (datasetIndexes != null) {
                            for (Index index : datasetIndexes) {
                                dropIndex(txnId, database, dataverseName, datasetName, index.getIndexName());
                            }
                        }
                        break;
                    case EXTERNAL:
                        // Delete entry(s) from the 'indexes' dataset.
                        datasetIndexes = getDatasetIndexes(txnId, database, dataverseName, datasetName);
                        if (datasetIndexes != null) {
                            for (Index index : datasetIndexes) {
                                dropIndex(txnId, database, dataverseName, datasetName, index.getIndexName());
                            }
                        }
                        // Delete External Files
                        // As a side effect, acquires an S lock on the 'ExternalFile' dataset
                        // on behalf of txnId.
                        List<ExternalFile> datasetFiles = getExternalFiles(txnId, dataset);
                        if (datasetFiles != null && !datasetFiles.isEmpty()) {
                            // Drop all external files in this dataset.
                            for (ExternalFile file : datasetFiles) {
                                dropExternalFile(txnId, database, dataverseName, file.getDatasetName(),
                                        file.getFileNumber());
                            }
                        }
                        break;
                    case VIEW:
                        break;
                }
            } catch (HyracksDataException hde) {
                // ignore this exception and continue deleting all relevant artifacts
                if (!hde.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                    throw new AsterixException(METADATA_ERROR, hde, hde.getMessage());
                }
            } finally {
                deleteTupleFromIndex(txnId, mdIndexesProvider.getDatasetEntity().getIndex(), datasetTuple);
            }
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void dropIndex(TxnId txnId, String database, DataverseName dataverseName, String datasetName,
            String indexName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, datasetName, indexName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'index' dataset.
            ITupleReference tuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getIndexEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getIndexEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_INDEX, e, indexName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public boolean dropNodegroup(TxnId txnId, String nodeGroupName, boolean failSilently) throws AlgebricksException {
        List<Dataset> datasets = getDatasetsPartitionedOnThisNodeGroup(txnId, nodeGroupName);
        if (!datasets.isEmpty()) {
            if (failSilently) {
                return false;
            }
            throw new AsterixException(CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, "node group", nodeGroupName,
                    dataset(PLURAL),
                    datasets.stream().map(DatasetUtil::getFullyQualifiedDisplayName).collect(Collectors.joining(", ")));
        }
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'nodegroup' dataset.
            ITupleReference tuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getNodeGroupEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getNodeGroupEntity().getIndex(), tuple);
            return true;
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_NODEGROUP, e, nodeGroupName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void dropDatatype(TxnId txnId, String database, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        dropDatatype(txnId, database, dataverseName, datatypeName, false);
    }

    private void dropDatatype(TxnId txnId, String database, DataverseName dataverseName, String datatypeName,
            boolean force) throws AlgebricksException {
        if (!force) {
            confirmDatatypeIsUnused(txnId, database, dataverseName, datatypeName);
        }
        // Delete the datatype entry, including all it's nested anonymous types.
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getDatatypeEntity().getIndex(), searchKey);
            // Get nested types
            List<String> nestedTypes =
                    getNestedComplexDatatypeNamesForThisDatatype(txnId, database, dataverseName, datatypeName);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getDatatypeEntity().getIndex(), tuple);
            for (String nestedType : nestedTypes) {
                Datatype dt = getDatatype(txnId, database, dataverseName, nestedType);
                if (dt != null && dt.getIsAnonymous()) {
                    dropDatatype(txnId, database, dataverseName, dt.getDatatypeName());
                }
            }
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_TYPE, e, datatypeName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    private void forceDropDatatype(TxnId txnId, String database, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getDatatypeEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getDatatypeEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_TYPE, e, datatypeName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public List<Database> getDatabases(TxnId txnId) throws AlgebricksException, RemoteException {
        try {
            DatabaseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatabaseTupleTranslator(false);
            IValueExtractor<Database> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Database> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDatabaseEntity().getIndex(), null, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public List<Dataverse> getDataverses(TxnId txnId) throws AlgebricksException {
        try {
            DataverseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDataverseEntity().getIndex(), null, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public Database getDatabase(TxnId txnId, String databaseName) throws AlgebricksException {
        try {
            if (!mdIndexesProvider.isUsingDatabase()) {
                return defaultDatabase(databaseName);
            }
            ITupleReference searchKey = createTuple(databaseName);
            DatabaseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatabaseTupleTranslator(false);
            IValueExtractor<Database> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Database> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDatabaseEntity().getIndex(), searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public Dataverse getDataverse(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName);
            DataverseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDataverseEntity().getIndex(), searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public List<Dataset> getDataverseDatasets(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName);
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataset> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDatasetEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private List<Dataverse> getDatabaseDataverses(TxnId txnId, String database) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database);
            DataverseTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDataverseEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public List<Dataset> getDatabaseDatasets(TxnId txnId, String database) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database);
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataset> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDatasetEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public List<Feed> getDataverseFeeds(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName);
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(false);
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Feed> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getFeedEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private List<Feed> getDatabaseFeeds(TxnId txnId, String database) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database);
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(false);
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Feed> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getFeedEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public List<Library> getDataverseLibraries(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName);
            LibraryTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getLibraryTupleTranslator(false);
            IValueExtractor<Library> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Library> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getLibraryEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public List<Library> getDatabaseLibraries(TxnId txnId, String database) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database);
            LibraryTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getLibraryTupleTranslator(false);
            IValueExtractor<Library> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Library> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getLibraryEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private List<Datatype> getDataverseDatatypes(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName);
            DatatypeTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getDataTypeTupleTranslator(txnId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDatatypeEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private List<Datatype> getDatabaseDatatypes(TxnId txnId, String database) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database);
            DatatypeTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getDataTypeTupleTranslator(txnId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDatatypeEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private List<FullTextConfigMetadataEntity> getDataverseFullTextConfigs(TxnId txnId, String database,
            DataverseName dataverseName) throws AlgebricksException {
        ITupleReference searchKey = createTuple(database, dataverseName);
        FullTextConfigMetadataEntityTupleTranslator tupleReaderWriter =
                tupleTranslatorProvider.getFullTextConfigTupleTranslator(true);
        IValueExtractor<FullTextConfigMetadataEntity> valueExtractor =
                new MetadataEntityValueExtractor<>(tupleReaderWriter);
        List<FullTextConfigMetadataEntity> results = new ArrayList<>();
        try {
            searchIndex(txnId, mdIndexesProvider.getFullTextConfigEntity().getIndex(), searchKey, valueExtractor,
                    results);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
        return results;
    }

    private List<FullTextConfigMetadataEntity> getDatabaseFullTextConfigs(TxnId txnId, String database)
            throws AlgebricksException {
        ITupleReference searchKey = createTuple(database);
        FullTextConfigMetadataEntityTupleTranslator tupleReaderWriter =
                tupleTranslatorProvider.getFullTextConfigTupleTranslator(true);
        IValueExtractor<FullTextConfigMetadataEntity> valueExtractor =
                new MetadataEntityValueExtractor<>(tupleReaderWriter);
        List<FullTextConfigMetadataEntity> results = new ArrayList<>();
        try {
            searchIndex(txnId, mdIndexesProvider.getFullTextConfigEntity().getIndex(), searchKey, valueExtractor,
                    results);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
        return results;
    }

    private List<FullTextFilterMetadataEntity> getDataverseFullTextFilters(TxnId txnId, String database,
            DataverseName dataverseName) throws AlgebricksException {
        ITupleReference searchKey = createTuple(database, dataverseName);
        FullTextFilterMetadataEntityTupleTranslator tupleReaderWriter =
                tupleTranslatorProvider.getFullTextFilterTupleTranslator(true);
        IValueExtractor<FullTextFilterMetadataEntity> valueExtractor =
                new MetadataEntityValueExtractor<>(tupleReaderWriter);
        List<FullTextFilterMetadataEntity> results = new ArrayList<>();
        try {
            searchIndex(txnId, mdIndexesProvider.getFullTextFilterEntity().getIndex(), searchKey, valueExtractor,
                    results);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
        return results;
    }

    private List<FullTextFilterMetadataEntity> getDatabaseFullTextFilters(TxnId txnId, String database)
            throws AlgebricksException {
        ITupleReference searchKey = createTuple(database);
        FullTextFilterMetadataEntityTupleTranslator tupleReaderWriter =
                tupleTranslatorProvider.getFullTextFilterTupleTranslator(true);
        IValueExtractor<FullTextFilterMetadataEntity> valueExtractor =
                new MetadataEntityValueExtractor<>(tupleReaderWriter);
        List<FullTextFilterMetadataEntity> results = new ArrayList<>();
        try {
            searchIndex(txnId, mdIndexesProvider.getFullTextFilterEntity().getIndex(), searchKey, valueExtractor,
                    results);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
        return results;
    }

    @Override
    public Dataset getDataset(TxnId txnId, String database, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, datasetName);
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
            List<Dataset> results = new ArrayList<>();
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getDatasetEntity().getIndex(), searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    public List<Dataset> getAllDatasets(TxnId txnId) throws AlgebricksException {
        try {
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Dataset> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDatasetEntity().getIndex(), null, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
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
            searchIndex(txnId, mdIndexesProvider.getDatatypeEntity().getIndex(), null, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    public List<DatasourceAdapter> getAllAdapters(TxnId txnId) throws AlgebricksException {
        try {
            DatasourceAdapterTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getAdapterTupleTranslator(false);
            List<DatasourceAdapter> results = new ArrayList<>();
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getDatasourceAdapterEntity().getIndex(), null, valueExtractor,
                    results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    public List<FeedConnection> getAllFeedConnections(TxnId txnId) throws AlgebricksException {
        try {
            FeedConnectionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFeedConnectionTupleTranslator(false);
            List<FeedConnection> results = new ArrayList<>();
            IValueExtractor<FeedConnection> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getFeedConnectionEntity().getIndex(), null, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private void confirmDatabaseCanBeDeleted(TxnId txnId, String database) throws AlgebricksException {
        // if a dataset/view from a DIFFERENT database uses a type from this database throw an error
        ensureNoDatasetUsingDatabase(txnId, database);

        // if a function from a DIFFERENT database uses datasets, functions, datatypes, or synonyms from this database
        // throw an error
        ensureNoFunctionUsingDatabase(txnId, database);

        // if a feed connection from a DIFFERENT database applies a function from this database then throw an error
        ensureNoFeedUsingDatabase(txnId, database);

        // if an adapter from a DIFFERENT database uses a library from this database then throw an error
        ensureNoAdapterUsingDatabase(txnId, database);
    }

    private void ensureNoAdapterUsingDatabase(TxnId txnId, String database) throws AlgebricksException {
        List<DatasourceAdapter> adapters = getAllAdapters(txnId);
        for (DatasourceAdapter adapter : adapters) {
            if (database.equals(adapter.getAdapterIdentifier().getDatabaseName())) {
                // skip adapters in self database
                continue;
            }
            if (database.equals(adapter.getLibraryDatabaseName())) {
                throw new AsterixException(CANNOT_DROP_DATABASE_DEPENDENT_EXISTS, "library",
                        MetadataUtil.getFullyQualifiedDisplayName(adapter.getLibraryDatabaseName(),
                                adapter.getLibraryDataverseName(), adapter.getLibraryName()),
                        "adapter",
                        MetadataUtil.getFullyQualifiedDisplayName(adapter.getAdapterIdentifier().getDataverseName(),
                                adapter.getAdapterIdentifier().getName()));
            }
        }
    }

    private void ensureNoFeedUsingDatabase(TxnId txnId, String database) throws AlgebricksException {
        List<FeedConnection> feedConnections = getAllFeedConnections(txnId);
        for (FeedConnection feedConnection : feedConnections) {
            if (database.equals(feedConnection.getDatabaseName())) {
                continue;
            }
            for (FunctionSignature functionSignature : feedConnection.getAppliedFunctions()) {
                if (database.equals(functionSignature.getDatabaseName())) {
                    throw new AsterixException(CANNOT_DROP_DATABASE_DEPENDENT_EXISTS, "function", functionSignature,
                            "feed connection",
                            MetadataUtil.getFullyQualifiedDisplayName(feedConnection.getDatabaseName(),
                                    feedConnection.getDataverseName(), feedConnection.getFeedName()));
                }
            }
        }
    }

    private void ensureNoFunctionUsingDatabase(TxnId txnId, String database) throws AlgebricksException {
        List<Function> functions = getAllFunctions(txnId);
        for (Function otherFunction : functions) {
            if (otherFunction.getDatabaseName().equals(database)) {
                continue;
            }
            List<DependencyKind> dependenciesSchema = Function.DEPENDENCIES_SCHEMA;
            List<List<DependencyFullyQualifiedName>> dependencies = otherFunction.getDependencies();
            for (int i = 0, n = dependencies.size(); i < n; i++) {
                for (DependencyFullyQualifiedName dependency : dependencies.get(i)) {
                    if (dependency.getDatabaseName().equals(database)) {
                        DependencyKind dependencyKind = dependenciesSchema.get(i);
                        throw new AsterixException(CANNOT_DROP_DATABASE_DEPENDENT_EXISTS, dependencyKind,
                                dependencyKind.getDependencyDisplayName(dependency), "function",
                                otherFunction.getSignature());
                    }
                }
            }
            if (database.equals(otherFunction.getLibraryDatabaseName())) {
                throw new AsterixException(CANNOT_DROP_DATABASE_DEPENDENT_EXISTS, "library",
                        MetadataUtil.getFullyQualifiedDisplayName(otherFunction.getLibraryDatabaseName(),
                                otherFunction.getLibraryDataverseName(), otherFunction.getLibraryName()),
                        "function", otherFunction.getSignature());
            }
        }
    }

    private void ensureNoDatasetUsingDatabase(TxnId txnId, String database) throws AlgebricksException {
        List<Dataset> datasets = getAllDatasets(txnId);
        for (Dataset otherDataset : datasets) {
            if (otherDataset.getDatabaseName().equals(database)) {
                continue;
            }
            if (otherDataset.getItemTypeDatabaseName().equals(database)) {
                //TODO(DB): fix display to include the database conditionally
                throw new AsterixException(CANNOT_DROP_DATABASE_DEPENDENT_EXISTS, "type",
                        MetadataUtil.getFullyQualifiedDisplayName(otherDataset.getItemTypeDatabaseName(),
                                otherDataset.getItemTypeDataverseName(), otherDataset.getItemTypeName()),
                        dataset(), DatasetUtil.getFullyQualifiedDisplayName(otherDataset));
            }
            if (otherDataset.hasMetaPart() && otherDataset.getMetaItemTypeDatabaseName().equals(database)) {
                throw new AsterixException(CANNOT_DROP_DATABASE_DEPENDENT_EXISTS, "type",
                        MetadataUtil.getFullyQualifiedDisplayName(otherDataset.getItemTypeDatabaseName(),
                                otherDataset.getMetaItemTypeDataverseName(), otherDataset.getMetaItemTypeName()),
                        dataset(), DatasetUtil.getFullyQualifiedDisplayName(otherDataset));
            }
            if (otherDataset.getDatasetType() == DatasetType.VIEW) {
                ViewDetails viewDetails = (ViewDetails) otherDataset.getDatasetDetails();
                List<DependencyKind> dependenciesSchema = ViewDetails.DEPENDENCIES_SCHEMA;
                List<List<DependencyFullyQualifiedName>> dependencies = viewDetails.getDependencies();
                for (int i = 0, n = dependencies.size(); i < n; i++) {
                    for (DependencyFullyQualifiedName dependency : dependencies.get(i)) {
                        if (dependency.getDatabaseName().equals(database)) {
                            DependencyKind dependencyKind = dependenciesSchema.get(i);
                            throw new AsterixException(CANNOT_DROP_DATABASE_DEPENDENT_EXISTS, dependencyKind,
                                    dependencyKind.getDependencyDisplayName(dependency), "view",
                                    DatasetUtil.getFullyQualifiedDisplayName(otherDataset));
                        }
                    }
                }
            }
        }
    }

    private void confirmDataverseCanBeDeleted(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        // If a dataset from a DIFFERENT dataverse uses a type from this dataverse throw an error
        List<Dataset> datasets = getAllDatasets(txnId);
        for (Dataset dataset : datasets) {
            if (dataset.getDataverseName().equals(dataverseName) && dataset.getDatabaseName().equals(database)) {
                continue;
            }
            if (dataset.getItemTypeDataverseName().equals(dataverseName)
                    && dataset.getItemTypeDatabaseName().equals(database)) {
                throw new AsterixException(CANNOT_DROP_DATAVERSE_DEPENDENT_EXISTS, "type",
                        TypeUtil.getFullyQualifiedDisplayName(dataset.getItemTypeDataverseName(),
                                dataset.getItemTypeName()),
                        dataset(), DatasetUtil.getFullyQualifiedDisplayName(dataset));
            }
            if (dataset.hasMetaPart() && dataset.getMetaItemTypeDataverseName().equals(dataverseName)
                    && dataset.getMetaItemTypeDatabaseName().equals(database)) {
                throw new AsterixException(CANNOT_DROP_DATAVERSE_DEPENDENT_EXISTS, "type",
                        TypeUtil.getFullyQualifiedDisplayName(dataset.getMetaItemTypeDataverseName(),
                                dataset.getMetaItemTypeName()),
                        dataset(), DatasetUtil.getFullyQualifiedDisplayName(dataset));
            }
            if (dataset.getDatasetType() == DatasetType.VIEW) {
                ViewDetails viewDetails = (ViewDetails) dataset.getDatasetDetails();
                List<DependencyKind> dependenciesSchema = ViewDetails.DEPENDENCIES_SCHEMA;
                List<List<DependencyFullyQualifiedName>> dependencies = viewDetails.getDependencies();
                for (int i = 0, n = dependencies.size(); i < n; i++) {
                    for (DependencyFullyQualifiedName dependency : dependencies.get(i)) {
                        if (dependency.getDataverseName().equals(dataverseName)
                                && dependency.getDatabaseName().equals(database)) {
                            DependencyKind dependencyKind = dependenciesSchema.get(i);
                            throw new AsterixException(CANNOT_DROP_DATAVERSE_DEPENDENT_EXISTS, dependencyKind,
                                    dependencyKind.getDependencyDisplayName(dependency), "view",
                                    DatasetUtil.getFullyQualifiedDisplayName(dataset));
                        }
                    }
                }
            }
        }

        // If a function from a DIFFERENT dataverse uses datasets, functions, datatypes, or synonyms from this dataverse
        // throw an error
        List<Function> functions = getAllFunctions(txnId);
        for (Function function : functions) {
            if (function.getDataverseName().equals(dataverseName) && function.getDatabaseName().equals(database)) {
                continue;
            }
            List<DependencyKind> dependenciesSchema = Function.DEPENDENCIES_SCHEMA;
            List<List<DependencyFullyQualifiedName>> dependencies = function.getDependencies();
            for (int i = 0, n = dependencies.size(); i < n; i++) {
                for (DependencyFullyQualifiedName dependency : dependencies.get(i)) {
                    if (dependency.getDataverseName().equals(dataverseName)
                            && dependency.getDatabaseName().equals(database)) {
                        DependencyKind dependencyKind = dependenciesSchema.get(i);
                        throw new AsterixException(CANNOT_DROP_DATAVERSE_DEPENDENT_EXISTS, dependencyKind,
                                dependencyKind.getDependencyDisplayName(dependency), "function",
                                function.getSignature());
                    }
                }
            }
        }

        // If a feed connection from a DIFFERENT dataverse applies a function from this dataverse then throw an error
        List<FeedConnection> feedConnections = getAllFeedConnections(txnId);
        for (FeedConnection feedConnection : feedConnections) {
            if (dataverseName.equals(feedConnection.getDataverseName())
                    && database.equals(feedConnection.getDatabaseName())) {
                continue;
            }
            for (FunctionSignature functionSignature : feedConnection.getAppliedFunctions()) {
                if (dataverseName.equals(functionSignature.getDataverseName())
                        && database.equals(functionSignature.getDatabaseName())) {
                    throw new AsterixException(CANNOT_DROP_DATAVERSE_DEPENDENT_EXISTS, "function", functionSignature,
                            "feed connection", MetadataUtil.getFullyQualifiedDisplayName(
                                    feedConnection.getDataverseName(), feedConnection.getFeedName()));
                }
            }
        }
        //TODO(DB): should check entities depending on libraries in this dataverse
    }

    private void confirmFunctionCanBeDeleted(TxnId txnId, FunctionSignature signature) throws AlgebricksException {
        confirmFunctionIsUnusedByViews(txnId, signature);
        confirmFunctionIsUnusedByFunctions(txnId, signature);

        // if any other feed connection uses this function, throw an error
        List<FeedConnection> feedConnections = getAllFeedConnections(txnId);
        for (FeedConnection feedConnection : feedConnections) {
            if (feedConnection.containsFunction(signature)) {
                throw new AsterixException(CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, "function", signature,
                        "feed connection", MetadataUtil.getFullyQualifiedDisplayName(feedConnection.getDataverseName(),
                                feedConnection.getFeedName()));
            }
        }
    }

    private void confirmFunctionIsUnusedByViews(TxnId txnId, FunctionSignature signature) throws AlgebricksException {
        String functionDatabase = signature.getDatabaseName();
        confirmObjectIsUnusedByViews(txnId, "function", DependencyKind.FUNCTION, functionDatabase,
                signature.getDataverseName(), signature.getName(), Integer.toString(signature.getArity()));
    }

    private void confirmFunctionIsUnusedByFunctions(TxnId txnId, FunctionSignature signature)
            throws AlgebricksException {
        String functionDatabase = signature.getDatabaseName();
        confirmObjectIsUnusedByFunctions(txnId, "function", DependencyKind.FUNCTION, functionDatabase,
                signature.getDataverseName(), signature.getName(), Integer.toString(signature.getArity()));
    }

    private void confirmObjectIsUnusedByFunctions(TxnId txnId, String objectKindDisplayName,
            DependencyKind dependencyKind, String database, DataverseName dataverseName, String objectName,
            String objectArg) throws AlgebricksException {
        // If any function uses this object, throw an error
        List<Function> functions = getAllFunctions(txnId);
        confirmObjectIsUnusedByFunctionsImpl(functions, objectKindDisplayName, dependencyKind, database, dataverseName,
                objectName, objectArg);
    }

    private void confirmObjectIsUnusedByFunctionsImpl(List<Function> allFunctions, String objectKindDisplayName,
            DependencyKind dependencyKind, String database, DataverseName dataverseName, String objectName,
            String objectArg) throws AlgebricksException {
        int functionDependencyIdx = Function.DEPENDENCIES_SCHEMA.indexOf(dependencyKind);
        if (functionDependencyIdx < 0) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE);
        }
        for (Function function : allFunctions) {
            List<List<DependencyFullyQualifiedName>> functionDependencies = function.getDependencies();
            if (functionDependencyIdx < functionDependencies.size()) {
                List<DependencyFullyQualifiedName> functionObjectDependencies =
                        functionDependencies.get(functionDependencyIdx);
                if (functionObjectDependencies != null) {
                    for (DependencyFullyQualifiedName dependency : functionObjectDependencies) {
                        if (dependency.getDataverseName().equals(dataverseName)
                                && dependency.getDatabaseName().equals(database)
                                && dependency.getSubName1().equals(objectName)
                                && (objectArg == null || objectArg.equals(dependency.getSubName2()))) {
                            throw new AsterixException(CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, objectKindDisplayName,
                                    dependencyKind.getDependencyDisplayName(dependency), "function",
                                    function.getSignature());
                        }
                    }
                }
            }
        }
    }

    private void confirmObjectIsUnusedByViews(TxnId txnId, String objectKindDisplayName, DependencyKind dependencyKind,
            String database, DataverseName dataverseName, String objectName, String objectArg)
            throws AlgebricksException {
        // If any function uses this object, throw an error
        List<Dataset> datasets = getAllDatasets(txnId);
        confirmObjectIsUnusedByViewsImpl(datasets, objectKindDisplayName, dependencyKind, database, dataverseName,
                objectName, objectArg);
    }

    private void confirmObjectIsUnusedByViewsImpl(List<Dataset> allDatasets, String objectKindDisplayName,
            DependencyKind dependencyKind, String database, DataverseName dataverseName, String objectName,
            String objectArg) throws AlgebricksException {
        int viewDependencyIdx = ViewDetails.DEPENDENCIES_SCHEMA.indexOf(dependencyKind);
        if (viewDependencyIdx < 0) {
            throw new AlgebricksException(ErrorCode.ILLEGAL_STATE);
        }
        for (Dataset dataset : allDatasets) {
            if (dataset.getDatasetType() == DatasetType.VIEW) {
                ViewDetails viewDetails = (ViewDetails) dataset.getDatasetDetails();
                List<List<DependencyFullyQualifiedName>> viewDependencies = viewDetails.getDependencies();
                if (viewDependencyIdx < viewDependencies.size()) {
                    List<DependencyFullyQualifiedName> viewObjectDependencies = viewDependencies.get(viewDependencyIdx);
                    if (viewObjectDependencies != null) {
                        for (DependencyFullyQualifiedName dependency : viewObjectDependencies) {
                            if (dependency.getDataverseName().equals(dataverseName)
                                    && dependency.getDatabaseName().equals(database)
                                    && dependency.getSubName1().equals(objectName)
                                    && (objectArg == null || objectArg.equals(dependency.getSubName2()))) {
                                throw new AsterixException(CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, objectKindDisplayName,
                                        dependencyKind.getDependencyDisplayName(dependency), "view",
                                        DatasetUtil.getFullyQualifiedDisplayName(dataset));
                            }
                        }
                    }
                }
            }
        }
    }

    private void confirmFullTextConfigCanBeDeleted(TxnId txnId, String database,
            DataverseName dataverseNameFullTextConfig, String configName) throws AlgebricksException {
        if (Strings.isNullOrEmpty(configName)) {
            throw new MetadataException(FULL_TEXT_DEFAULT_CONFIG_CANNOT_BE_DELETED_OR_CREATED);
        }

        // If any index uses this full-text config, throw an error
        List<Dataset> datasets = getAllDatasets(txnId);
        for (Dataset dataset : datasets) {
            List<Index> indexes = getDatasetIndexes(txnId, dataset.getDatabaseName(), dataset.getDataverseName(),
                    dataset.getDatasetName());
            for (Index index : indexes) {
                // ToDo: to support index to access full-text config in another dataverse,
                //   we may need to include the dataverse of the full-text config in the index.getFullTextConfigDataverse()
                //   and instead of checking index.getDataverseName(), we need to check index.getFullTextConfigDataverse()
                //   to see if it is the same as the dataverse of the full-text config
                if (Index.IndexCategory.of(index.getIndexType()) == Index.IndexCategory.TEXT) {
                    String indexConfigName = ((Index.TextIndexDetails) index.getIndexDetails()).getFullTextConfigName();
                    if (index.getDataverseName().equals(dataverseNameFullTextConfig)
                            && index.getDatabaseName().equals(database) && !Strings.isNullOrEmpty(indexConfigName)
                            && indexConfigName.equals(configName)) {
                        throw new AsterixException(CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, "full-text config",
                                MetadataUtil.getFullyQualifiedDisplayName(dataverseNameFullTextConfig, configName),
                                "index", DatasetUtil.getFullyQualifiedDisplayName(index.getDataverseName(),
                                        index.getDatasetName()) + "." + index.getIndexName());
                    }
                }
            }
        }
    }

    private void confirmDatasetCanBeDeleted(TxnId txnId, String datasetTypeDisplayName, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        confirmDatasetIsUnusedByFunctions(txnId, datasetTypeDisplayName, database, dataverseName, datasetName);
        confirmDatasetIsUnusedByViews(txnId, datasetTypeDisplayName, database, dataverseName, datasetName);
    }

    private void confirmDatasetIsUnusedByFunctions(TxnId txnId, String datasetKindDisplayName, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        confirmObjectIsUnusedByFunctions(txnId, datasetKindDisplayName, DependencyKind.DATASET, database, dataverseName,
                datasetName, null);
    }

    private void confirmDatasetIsUnusedByViews(TxnId txnId, String datasetKindDisplayName, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        confirmObjectIsUnusedByViews(txnId, datasetKindDisplayName, DependencyKind.DATASET, database, dataverseName,
                datasetName, null);
    }

    private void confirmLibraryCanBeDeleted(TxnId txnId, String database, DataverseName dataverseName,
            String libraryName) throws AlgebricksException {
        confirmLibraryIsUnusedByFunctions(txnId, database, dataverseName, libraryName);
        confirmLibraryIsUnusedByAdapters(txnId, database, dataverseName, libraryName);
    }

    private void confirmLibraryIsUnusedByFunctions(TxnId txnId, String database, DataverseName dataverseName,
            String libraryName) throws AlgebricksException {
        List<Function> functions = getAllFunctions(txnId);
        for (Function function : functions) {
            if (libraryName.equals(function.getLibraryName()) && database.equals(function.getLibraryDatabaseName())
                    && dataverseName.equals(function.getLibraryDataverseName())) {
                throw new AsterixException(CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, "library",
                        MetadataUtil.getFullyQualifiedDisplayName(dataverseName, libraryName), "function",
                        function.getSignature());
            }
        }
    }

    private void confirmLibraryIsUnusedByAdapters(TxnId txnId, String database, DataverseName dataverseName,
            String libraryName) throws AlgebricksException {
        List<DatasourceAdapter> adapters = getAllAdapters(txnId);
        for (DatasourceAdapter adapter : adapters) {
            if (libraryName.equals(adapter.getLibraryName()) && database.equals(adapter.getLibraryDatabaseName())
                    && dataverseName.equals(adapter.getLibraryDataverseName())) {
                throw new AsterixException(CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, "library",
                        MetadataUtil.getFullyQualifiedDisplayName(dataverseName, libraryName), "adapter",
                        MetadataUtil.getFullyQualifiedDisplayName(adapter.getAdapterIdentifier().getDataverseName(),
                                adapter.getAdapterIdentifier().getName()));
            }
        }
    }

    private void confirmDatatypeIsUnused(TxnId txnId, String database, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        confirmDatatypeIsUnusedByDatatypes(txnId, database, dataverseName, datatypeName);
        confirmDatatypeIsUnusedByDatasets(txnId, database, dataverseName, datatypeName);
        confirmDatatypeIsUnusedByFunctions(txnId, database, dataverseName, datatypeName);
    }

    private void confirmDatatypeIsUnusedByDatasets(TxnId txnId, String database, DataverseName dataverseName,
            String datatypeName) throws AlgebricksException {
        // If any dataset uses this type, throw an error
        List<Dataset> datasets = getAllDatasets(txnId);
        for (Dataset dataset : datasets) {
            if ((dataset.getItemTypeName().equals(datatypeName)
                    && dataset.getItemTypeDataverseName().equals(dataverseName))
                    && dataset.getItemTypeDatabaseName().equals(database)
                    || ((dataset.hasMetaPart() && dataset.getMetaItemTypeName().equals(datatypeName)
                            && dataset.getMetaItemTypeDataverseName().equals(dataverseName)
                            && dataset.getMetaItemTypeDatabaseName().equals(database)))) {
                throw new AsterixException(CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, "type",
                        TypeUtil.getFullyQualifiedDisplayName(dataverseName, datatypeName), dataset(),
                        DatasetUtil.getFullyQualifiedDisplayName(dataset));
            }
        }

        // additionally, if a view uses this type, throw an error
        // Note: for future use. currently views don't have any type dependencies
        confirmObjectIsUnusedByViewsImpl(datasets, null, DependencyKind.TYPE, database, dataverseName, datatypeName,
                null);
    }

    private void confirmDatatypeIsUnusedByDatatypes(TxnId txnId, String database, DataverseName dataverseName,
            String datatypeName) throws AlgebricksException {
        // If any datatype uses this type, throw an error
        // TODO: Currently this loads all types into memory. This will need to be fixed
        // for large numbers of types
        Datatype dataTypeToBeDropped = getDatatype(txnId, database, dataverseName, datatypeName);
        assert dataTypeToBeDropped != null;
        IAType typeToBeDropped = dataTypeToBeDropped.getDatatype();
        List<Datatype> datatypes = getAllDatatypes(txnId);
        for (Datatype dataType : datatypes) {

            // skip types in different dataverses as well as the type to be dropped itself
            //TODO(DB): review this
            if (!dataType.getDataverseName().equals(dataverseName) || !dataType.getDatabaseName().equals(database)
                    || dataType.getDatatype().getTypeName().equals(datatypeName)) {
                continue;
            }
            AbstractComplexType recType = (AbstractComplexType) dataType.getDatatype();
            if (recType.containsType(typeToBeDropped)) {
                throw new AsterixException(CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, "type",
                        TypeUtil.getFullyQualifiedDisplayName(dataverseName, datatypeName), "type",
                        TypeUtil.getFullyQualifiedDisplayName(dataverseName, recType.getTypeName()));
            }
        }
    }

    private void confirmDatatypeIsUnusedByFunctions(TxnId txnId, String database, DataverseName dataverseName,
            String dataTypeName) throws AlgebricksException {
        confirmObjectIsUnusedByFunctions(txnId, "datatype", DependencyKind.TYPE, database, dataverseName, dataTypeName,
                null);
    }

    private void confirmFullTextFilterCanBeDeleted(TxnId txnId, String database, DataverseName dataverseName,
            String fullTextFilterName) throws AlgebricksException {
        List<FullTextConfigMetadataEntity> configMetadataEntities =
                getDataverseFullTextConfigs(txnId, database, dataverseName);
        for (FullTextConfigMetadataEntity configMetadataEntity : configMetadataEntities) {
            FullTextConfigDescriptor config = configMetadataEntity.getFullTextConfig();
            for (String filterName : config.getFilterNames()) {
                if (filterName.equals(fullTextFilterName)) {
                    throw new AsterixException(CANNOT_DROP_OBJECT_DEPENDENT_EXISTS, "full-text filter",
                            TypeUtil.getFullyQualifiedDisplayName(dataverseName, fullTextFilterName),
                            "full-text config", TypeUtil.getFullyQualifiedDisplayName(dataverseName, config.getName()));
                }
            }
        }
    }

    private List<String> getNestedComplexDatatypeNamesForThisDatatype(TxnId txnId, String database,
            DataverseName dataverseName, String datatypeName) throws AlgebricksException {
        // Return all field types that aren't builtin types
        Datatype parentType = getDatatype(txnId, database, dataverseName, datatypeName);

        List<IAType> subTypes = null;
        if (parentType.getDatatype().getTypeTag() == ATypeTag.OBJECT) {
            ARecordType recType = (ARecordType) parentType.getDatatype();
            subTypes = Arrays.asList(recType.getFieldTypes());
        } else if (parentType.getDatatype().getTypeTag() == ATypeTag.UNION) {
            AUnionType unionType = (AUnionType) parentType.getDatatype();
            subTypes = unionType.getUnionList();
        } else if (parentType.getDatatype().getTypeTag() == ATypeTag.ARRAY
                || parentType.getDatatype().getTypeTag() == ATypeTag.MULTISET) {
            AbstractCollectionType collType = (AbstractCollectionType) parentType.getDatatype();
            subTypes = List.of(collType.getItemType());
        }

        List<String> nestedTypes = new ArrayList<>();
        if (subTypes != null) {
            for (IAType subType : subTypes) {
                IAType actualType = TypeComputeUtils.getActualType(subType);
                if (!(actualType instanceof BuiltinType)) {
                    nestedTypes.add(actualType.getTypeName());
                }
            }
        }
        return nestedTypes;
    }

    private List<Dataset> getDatasetsPartitionedOnThisNodeGroup(TxnId txnId, String nodegroup)
            throws AlgebricksException {
        // this needs to scan the datasets and return the datasets that use this nodegroup
        List<Dataset> nodeGroupDatasets = new ArrayList<>();
        List<Dataset> datasets = getAllDatasets(txnId);
        for (Dataset set : datasets) {
            if (set.getNodeGroupName().equals(nodegroup)) {
                nodeGroupDatasets.add(set);
            }
        }
        return nodeGroupDatasets;
    }

    @Override
    public Index getIndex(TxnId txnId, String database, DataverseName dataverseName, String datasetName,
            String indexName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, datasetName, indexName);
            IndexTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getIndexTupleTranslator(txnId, this, false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Index> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getIndexEntity().getIndex(), searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public List<Index> getDatasetIndexes(TxnId txnId, String database, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, datasetName);
            IndexTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getIndexTupleTranslator(txnId, this, false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Index> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getIndexEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public Datatype getDatatype(TxnId txnId, String database, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, datatypeName);
            DatatypeTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getDataTypeTupleTranslator(txnId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDatatypeEntity().getIndex(), searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public NodeGroup getNodeGroup(TxnId txnId, String nodeGroupName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            NodeGroupTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getNodeGroupTupleTranslator(false);
            IValueExtractor<NodeGroup> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<NodeGroup> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getNodeGroupEntity().getIndex(), searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public Function getFunction(TxnId txnId, FunctionSignature functionSignature) throws AlgebricksException {
        String functionDatabase = functionSignature.getDatabaseName();
        List<Function> functions =
                getFunctionsImpl(txnId, createTuple(functionDatabase, functionSignature.getDataverseName(),
                        functionSignature.getName(), Integer.toString(functionSignature.getArity())));
        return functions.isEmpty() ? null : functions.get(0);
    }

    @Override
    public List<Function> getDataverseFunctions(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        return getFunctionsImpl(txnId, createTuple(database, dataverseName));
    }

    private List<Function> getDatabaseFunctions(TxnId txnId, String database) throws AlgebricksException {
        return getFunctionsImpl(txnId, createTuple(database));
    }

    private List<Function> getFunctionsImpl(TxnId txnId, ITupleReference searchKey) throws AlgebricksException {
        try {
            FunctionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFunctionTupleTranslator(txnId, this, false);
            List<Function> results = new ArrayList<>();
            IValueExtractor<Function> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getFunctionEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
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
            String functionDatabase = functionSignature.getDatabaseName();
            ITupleReference searchKey = createTuple(functionDatabase, functionSignature.getDataverseName(),
                    functionSignature.getName(), Integer.toString(functionSignature.getArity()));
            // Searches the index for the tuple to be deleted. Acquires an S lock on the 'function' dataset.
            ITupleReference functionTuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getFunctionEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getFunctionEntity().getIndex(), functionTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_FUNCTION, e, functionSignature.toString());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    private ITupleReference getTupleToBeDeleted(TxnId txnId, IMetadataIndex metadataIndex, ITupleReference searchKey)
            throws AlgebricksException, HyracksDataException {
        IValueExtractor<ITupleReference> valueExtractor = new TupleCopyValueExtractor(metadataIndex.getTypeTraits(),
                TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.ANULL), NullIntrospector.INSTANCE);
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
            IMetadataIndex index = mdIndexesProvider.getDataverseEntity().getIndex();
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

    private void setAtomicOpContext(IIndexAccessor accessor) {
        Map<String, Object> indexAccessorOpContextParameters = new HashMap<>();
        indexAccessorOpContextParameters.put(HyracksConstants.ATOMIC_OP_CONTEXT, true);
        ((ILSMIndexAccessor) accessor).getOpContext().setParameters(indexAccessorOpContextParameters);
    }

    private <T> void searchIndex(TxnId txnId, IMetadataIndex index, ITupleReference searchKey,
            IValueExtractor<T> valueExtractor, List<T> results) throws AlgebricksException, HyracksDataException {
        IBinaryComparatorFactory[] comparatorFactories = index.getKeyBinaryComparatorFactory();
        if (index.getFile() == null) {
            throw new AsterixException(METADATA_ERROR,
                    "No file for Index " + index.getDataverseName() + "." + index.getIndexName());
        }
        String resourceName = index.getFile().getRelativePath();
        IIndex indexInstance = datasetLifecycleManager.get(resourceName);
        datasetLifecycleManager.open(resourceName);
        IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        if (atomicNoWAL) {
            setAtomicOpContext(indexAccessor);
        }
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
            String resourceName = mdIndexesProvider.getDatasetEntity().getIndex().getFile().getRelativePath();
            IIndex indexInstance = datasetLifecycleManager.get(resourceName);
            datasetLifecycleManager.open(resourceName);
            try {
                mostRecentDatasetId = getMostRecentDatasetIdFromStoredDatasetIndex(indexInstance, txnId);
            } finally {
                datasetLifecycleManager.close(resourceName);
            }
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
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
        //TODO(DB): remove this method after
        return createTuple(dataverseName.getCanonicalForm(), rest);
    }

    private ITupleReference createTuple(String databaseName, DataverseName dataverseName, String... rest) {
        if (mdIndexesProvider.isUsingDatabase()) {
            return createDatabaseTuple(databaseName, dataverseName, rest);
        } else {
            return createTuple(dataverseName.getCanonicalForm(), rest);
        }
    }

    public static ITupleReference createDatabaseTuple(String databaseName, DataverseName dataverseName,
            String... rest) {
        try {
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2 + rest.length);
            ISerializerDeserializer<AString> stringSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
            AMutableString aString = new AMutableString(databaseName);
            tupleBuilder.addField(stringSerde, aString);
            aString.setValue(dataverseName.getCanonicalForm());
            tupleBuilder.addField(stringSerde, aString);
            for (String s : rest) {
                aString.setValue(s);
                tupleBuilder.addField(stringSerde, aString);
            }
            ArrayTupleReference tuple = new ArrayTupleReference();
            tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            return tuple;
        } catch (HyracksDataException e) {
            // This should never happen
            throw new IllegalStateException("Failed to create search tuple", e);
        }
    }

    public static ITupleReference createTuple(String first, String... rest) {
        try {
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1 + rest.length);
            ISerializerDeserializer<AString> stringSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
            AMutableString aString = new AMutableString(first);
            tupleBuilder.addField(stringSerde, aString);
            for (String s : rest) {
                aString.setValue(s);
                tupleBuilder.addField(stringSerde, aString);
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
            insertTupleIntoIndex(txnId, mdIndexesProvider.getDatasourceAdapterEntity().getIndex(), adapterTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(ADAPTER_EXISTS, e, adapter.getAdapterIdentifier().getName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void dropAdapter(TxnId txnId, String database, DataverseName dataverseName, String adapterName)
            throws AlgebricksException {
        try {
            // Delete entry from the 'Adapter' dataset.
            ITupleReference searchKey = createTuple(database, dataverseName, adapterName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'Adapter' dataset.
            ITupleReference datasetTuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getDatasourceAdapterEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getDatasourceAdapterEntity().getIndex(), datasetTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_ADAPTER, e, adapterName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public DatasourceAdapter getAdapter(TxnId txnId, String database, DataverseName dataverseName, String adapterName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, adapterName);
            DatasourceAdapterTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getAdapterTupleTranslator(false);
            List<DatasourceAdapter> results = new ArrayList<>();
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getDatasourceAdapterEntity().getIndex(), searchKey, valueExtractor,
                    results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void addCompactionPolicy(TxnId txnId, CompactionPolicy compactionPolicy) throws AlgebricksException {
        try {
            // Insert into the 'CompactionPolicy' dataset.
            CompactionPolicyTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getCompactionPolicyTupleTranslator(true);
            ITupleReference compactionPolicyTuple = tupleReaderWriter.getTupleFromMetadataEntity(compactionPolicy);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getCompactionPolicyEntity().getIndex(),
                    compactionPolicyTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(COMPACTION_POLICY_EXISTS, e, compactionPolicy.getPolicyName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public CompactionPolicy getCompactionPolicy(TxnId txnId, String database, DataverseName dataverseName,
            String policyName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, policyName);
            CompactionPolicyTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getCompactionPolicyTupleTranslator(false);
            List<CompactionPolicy> results = new ArrayList<>();
            IValueExtractor<CompactionPolicy> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getCompactionPolicyEntity().getIndex(), searchKey, valueExtractor,
                    results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public List<DatasourceAdapter> getDataverseAdapters(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName);
            DatasourceAdapterTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getAdapterTupleTranslator(false);
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<DatasourceAdapter> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDatasourceAdapterEntity().getIndex(), searchKey, valueExtractor,
                    results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private List<DatasourceAdapter> getDatabaseAdapters(TxnId txnId, String database) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database);
            DatasourceAdapterTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getAdapterTupleTranslator(false);
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<DatasourceAdapter> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getDatasourceAdapterEntity().getIndex(), searchKey, valueExtractor,
                    results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void addLibrary(TxnId txnId, Library library) throws AlgebricksException {
        try {
            // Insert into the 'Library' dataset.
            LibraryTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getLibraryTupleTranslator(true);
            ITupleReference libraryTuple = tupleReaderWriter.getTupleFromMetadataEntity(library);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getLibraryEntity().getIndex(), libraryTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(LIBRARY_EXISTS, e, library.getName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void dropLibrary(TxnId txnId, String database, DataverseName dataverseName, String libraryName)
            throws AlgebricksException {
        dropLibrary(txnId, database, dataverseName, libraryName, false);
    }

    private void dropLibrary(TxnId txnId, String database, DataverseName dataverseName, String libraryName,
            boolean force) throws AlgebricksException {
        if (!force) {
            confirmLibraryCanBeDeleted(txnId, database, dataverseName, libraryName);
        }
        try {
            // Delete entry from the 'Library' dataset.
            ITupleReference searchKey = createTuple(database, dataverseName, libraryName);
            // Searches the index for the tuple to be deleted. Acquires an S lock on the 'Library' dataset.
            ITupleReference datasetTuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getLibraryEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getLibraryEntity().getIndex(), datasetTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_LIBRARY, e, libraryName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public Library getLibrary(TxnId txnId, String database, DataverseName dataverseName, String libraryName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, libraryName);
            LibraryTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getLibraryTupleTranslator(false);
            List<Library> results = new ArrayList<>();
            IValueExtractor<Library> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getLibraryEntity().getIndex(), searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
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
            insertTupleIntoIndex(txnId, mdIndexesProvider.getFeedPolicyEntity().getIndex(), feedPolicyTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(FEED_POLICY_EXISTS, e, feedPolicy.getPolicyName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public FeedPolicyEntity getFeedPolicy(TxnId txnId, String database, DataverseName dataverseName, String policyName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, policyName);
            FeedPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedPolicyTupleTranslator(false);
            List<FeedPolicyEntity> results = new ArrayList<>();
            IValueExtractor<FeedPolicyEntity> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getFeedPolicyEntity().getIndex(), searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void addFeedConnection(TxnId txnId, FeedConnection feedConnection) throws AlgebricksException {
        try {
            FeedConnectionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFeedConnectionTupleTranslator(true);
            ITupleReference feedConnTuple = tupleReaderWriter.getTupleFromMetadataEntity(feedConnection);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getFeedConnectionEntity().getIndex(), feedConnTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(FEED_CONNECTION_EXISTS, e, feedConnection.getFeedName(),
                        feedConnection.getDatasetName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public List<FeedConnection> getFeedConnections(TxnId txnId, String database, DataverseName dataverseName,
            String feedName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, feedName);
            FeedConnectionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFeedConnectionTupleTranslator(false);
            List<FeedConnection> results = new ArrayList<>();
            IValueExtractor<FeedConnection> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getFeedConnectionEntity().getIndex(), searchKey, valueExtractor,
                    results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public FeedConnection getFeedConnection(TxnId txnId, String database, DataverseName dataverseName, String feedName,
            String datasetName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, feedName, datasetName);
            FeedConnectionTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getFeedConnectionTupleTranslator(false);
            List<FeedConnection> results = new ArrayList<>();
            IValueExtractor<FeedConnection> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getFeedConnectionEntity().getIndex(), searchKey, valueExtractor,
                    results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void dropFeedConnection(TxnId txnId, String database, DataverseName dataverseName, String feedName,
            String datasetName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, feedName, datasetName);
            ITupleReference tuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getFeedConnectionEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getFeedConnectionEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_FEED_CONNECTION, e, feedName, datasetName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void addFeed(TxnId txnId, Feed feed) throws AlgebricksException {
        try {
            // Insert into the 'Feed' dataset.
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(true);
            ITupleReference feedTuple = tupleReaderWriter.getTupleFromMetadataEntity(feed);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getFeedEntity().getIndex(), feedTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(FEED_EXISTS, e, feed.getFeedName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public Feed getFeed(TxnId txnId, String database, DataverseName dataverseName, String feedName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, feedName);
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(false);
            List<Feed> results = new ArrayList<>();
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getFeedEntity().getIndex(), searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public List<Feed> getFeeds(TxnId txnId, String database, DataverseName dataverseName) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName);
            FeedTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedTupleTranslator(false);
            List<Feed> results = new ArrayList<>();
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getFeedEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void dropFeed(TxnId txnId, String database, DataverseName dataverseName, String feedName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, feedName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'nodegroup' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, mdIndexesProvider.getFeedEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getFeedEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_FEED, e, feedName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void dropFeedPolicy(TxnId txnId, String database, DataverseName dataverseName, String policyName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, policyName);
            ITupleReference tuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getFeedPolicyEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getFeedPolicyEntity().getIndex(), tuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_FEED_POLICY, e, policyName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public List<FeedPolicyEntity> getDataverseFeedPolicies(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName);
            FeedPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedPolicyTupleTranslator(false);
            IValueExtractor<FeedPolicyEntity> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<FeedPolicyEntity> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getFeedPolicyEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private List<FeedPolicyEntity> getDatabaseFeedPolicies(TxnId txnId, String database) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database);
            FeedPolicyTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getFeedPolicyTupleTranslator(false);
            IValueExtractor<FeedPolicyEntity> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<FeedPolicyEntity> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getFeedPolicyEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void addExternalFile(TxnId txnId, ExternalFile externalFile) throws AlgebricksException {
        try {
            // Insert into the 'externalFiles' dataset.
            ExternalFileTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getExternalFileTupleTranslator(true);
            ITupleReference externalFileTuple = tupleReaderWriter.getTupleFromMetadataEntity(externalFile);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getExternalFileEntity().getIndex(), externalFileTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(EXTERNAL_FILE_EXISTS, e, externalFile.getFileNumber(),
                        externalFile.getDatasetName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public List<ExternalFile> getExternalFiles(TxnId txnId, Dataset dataset) throws AlgebricksException {
        try {
            ITupleReference searchKey =
                    createTuple(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName());
            ExternalFileTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getExternalFileTupleTranslator(false);
            IValueExtractor<ExternalFile> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<ExternalFile> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getExternalFileEntity().getIndex(), searchKey, valueExtractor,
                    results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void dropExternalFile(TxnId txnId, String database, DataverseName dataverseName, String datasetName,
            int fileNumber) throws AlgebricksException {
        try {
            // Delete entry from the 'ExternalFile' dataset.
            ITupleReference searchKey = createExternalFileSearchTuple(database, dataverseName, datasetName, fileNumber);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'ExternalFile' dataset.
            ITupleReference datasetTuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getExternalFileEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getExternalFileEntity().getIndex(), datasetTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_EXTERNAL_FILE, e, fileNumber, datasetName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void dropExternalFiles(TxnId txnId, Dataset dataset) throws AlgebricksException {
        List<ExternalFile> files = getExternalFiles(txnId, dataset);
        // loop through files and delete them
        for (int i = 0; i < files.size(); i++) {
            dropExternalFile(txnId, files.get(i).getDatabaseName(), files.get(i).getDataverseName(),
                    files.get(i).getDatasetName(), files.get(i).getFileNumber());
        }
    }

    // This method is used to create a search tuple for external data file since the
    // search tuple has an int value
    private ITupleReference createExternalFileSearchTuple(String database, DataverseName dataverseName,
            String datasetName, int fileNumber) throws HyracksDataException {
        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
        ISerializerDeserializer<AInt32> intSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

        AMutableString aString = new AMutableString("");
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);

        // database field
        if (mdIndexesProvider.isUsingDatabase()) {
            aString.setValue(database);
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
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
    public ExternalFile getExternalFile(TxnId txnId, String database, DataverseName dataverseName, String datasetName,
            Integer fileNumber) throws AlgebricksException {
        try {
            ITupleReference searchKey = createExternalFileSearchTuple(database, dataverseName, datasetName, fileNumber);
            ExternalFileTupleTranslator tupleReaderWriter =
                    tupleTranslatorProvider.getExternalFileTupleTranslator(false);
            IValueExtractor<ExternalFile> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<ExternalFile> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getExternalFileEntity().getIndex(), searchKey, valueExtractor,
                    results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void addSynonym(TxnId txnId, Synonym synonym) throws AlgebricksException {
        try {
            // Insert into the 'Synonym' dataset.
            SynonymTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getSynonymTupleTranslator(true);
            ITupleReference synonymTuple = tupleReaderWriter.getTupleFromMetadataEntity(synonym);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getSynonymEntity().getIndex(), synonymTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.DUPLICATE_KEY)) {
                throw new AsterixException(SYNONYM_EXISTS, e, synonym.getSynonymName());
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void dropSynonym(TxnId txnId, String database, DataverseName dataverseName, String synonymName)
            throws AlgebricksException {
        dropSynonym(txnId, database, dataverseName, synonymName, false);
    }

    private void dropSynonym(TxnId txnId, String database, DataverseName dataverseName, String synonymName,
            boolean force) throws AlgebricksException {
        if (!force) {
            confirmSynonymCanBeDeleted(txnId, database, dataverseName, synonymName);
        }

        try {
            // Delete entry from the 'Synonym' dataset.
            ITupleReference searchKey = createTuple(database, dataverseName, synonymName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'Synonym' dataset.
            ITupleReference synonymTuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getSynonymEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getSynonymEntity().getIndex(), synonymTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_SYNONYM, e, synonymName);
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    private void confirmSynonymCanBeDeleted(TxnId txnId, String database, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        confirmSynonymIsUnusedByFunctions(txnId, database, dataverseName, synonymName);
        confirmSynonymIsUnusedByViews(txnId, database, dataverseName, synonymName);
    }

    private void confirmSynonymIsUnusedByFunctions(TxnId txnId, String database, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        confirmObjectIsUnusedByFunctions(txnId, "synonym", DependencyKind.SYNONYM, database, dataverseName, synonymName,
                null);
    }

    private void confirmSynonymIsUnusedByViews(TxnId txnId, String database, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        confirmObjectIsUnusedByViews(txnId, "synonym", DependencyKind.SYNONYM, database, dataverseName, synonymName,
                null);
    }

    @Override
    public Synonym getSynonym(TxnId txnId, String database, DataverseName dataverseName, String synonymName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName, synonymName);
            SynonymTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getSynonymTupleTranslator(false);
            List<Synonym> results = new ArrayList<>();
            IValueExtractor<Synonym> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            searchIndex(txnId, mdIndexesProvider.getSynonymEntity().getIndex(), searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public List<Synonym> getDataverseSynonyms(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database, dataverseName);
            SynonymTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getSynonymTupleTranslator(false);
            IValueExtractor<Synonym> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Synonym> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getSynonymEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    private List<Synonym> getDatabaseSynonyms(TxnId txnId, String database) throws AlgebricksException {
        try {
            ITupleReference searchKey = createTuple(database);
            SynonymTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getSynonymTupleTranslator(false);
            IValueExtractor<Synonym> valueExtractor = new MetadataEntityValueExtractor<>(tupleReaderWriter);
            List<Synonym> results = new ArrayList<>();
            searchIndex(txnId, mdIndexesProvider.getSynonymEntity().getIndex(), searchKey, valueExtractor, results);
            return results;
        } catch (HyracksDataException e) {
            throw new AsterixException(METADATA_ERROR, e, e.getMessage());
        }
    }

    @Override
    public void updateDataset(TxnId txnId, Dataset dataset) throws AlgebricksException {
        try {
            // This method will delete previous entry of the dataset and insert the new one
            // Delete entry from the 'datasets' dataset.
            ITupleReference searchKey =
                    createTuple(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName());
            // Searches the index for the tuple to be deleted. Acquires an S lock on the 'dataset' dataset.
            ITupleReference datasetTuple =
                    getTupleToBeDeleted(txnId, mdIndexesProvider.getDatasetEntity().getIndex(), searchKey);
            deleteTupleFromIndex(txnId, mdIndexesProvider.getDatasetEntity().getIndex(), datasetTuple);
            // Insert into the 'dataset' dataset.
            DatasetTupleTranslator tupleReaderWriter = tupleTranslatorProvider.getDatasetTupleTranslator(true);
            datasetTuple = tupleReaderWriter.getTupleFromMetadataEntity(dataset);
            insertTupleIntoIndex(txnId, mdIndexesProvider.getDatasetEntity().getIndex(), datasetTuple);
        } catch (HyracksDataException e) {
            if (e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
                throw new AsterixException(UNKNOWN_DATASET_IN_DATAVERSE, e, dataset.getDatasetName(),
                        MetadataUtil.dataverseName(dataset.getDatabaseName(), dataset.getDataverseName(),
                                mdIndexesProvider.isUsingDatabase()));
            } else {
                throw new AsterixException(METADATA_ERROR, e, e.getMessage());
            }
        }
    }

    @Override
    public void updateLibrary(TxnId txnId, Library library) throws AlgebricksException {
        dropLibrary(txnId, library.getDatabaseName(), library.getDataverseName(), library.getName(), true);
        addLibrary(txnId, library);
    }

    @Override
    public void updateFunction(TxnId txnId, Function function) throws AlgebricksException {
        dropFunction(txnId, function.getSignature(), true);
        addFunction(txnId, function);
    }

    @Override
    public void updateDatatype(TxnId txnId, Datatype datatype) throws AlgebricksException {
        dropDatatype(txnId, datatype.getDatabaseName(), datatype.getDataverseName(), datatype.getDatatypeName(), true);
        addDatatype(txnId, datatype);
    }

    public ITxnIdFactory getTxnIdFactory() {
        return txnIdFactory;
    }

    private Database defaultDatabase(String databaseName) {
        //TODO(DB): review
        if (MetadataConstants.SYSTEM_DATABASE.equals(databaseName)) {
            return MetadataBuiltinEntities.SYSTEM_DATABASE;
        } else {
            return MetadataBuiltinEntities.DEFAULT_DATABASE;
        }
    }

    private ArrayNode getJsonNodes(TxnId txnId, IMetadataIndex mdIndex, int payloadPosition)
            throws AlgebricksException, HyracksDataException {
        IValueExtractor<JsonNode> valueExtractor = createValueExtractor(mdIndex, payloadPosition);
        List<JsonNode> results = new ArrayList<>();
        searchIndex(txnId, mdIndex, null, valueExtractor, results);
        ArrayNode array = JSONUtil.createArray();
        results.forEach(array::add);
        return array;
    }

    private static IValueExtractor<JsonNode> createValueExtractor(IMetadataIndex mdIndex, int payloadFieldIndex) {
        return new IValueExtractor<>() {

            final ARecordType payloadRecordType = mdIndex.getPayloadRecordType();
            final IPrinterFactory printerFactory =
                    CleanJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(payloadRecordType);
            final IPrinter printer = printerFactory.createPrinter();
            final ByteArrayAccessibleOutputStream outputStream = new ByteArrayAccessibleOutputStream();
            final PrintStream printStream = new PrintStream(outputStream);

            @Override
            public JsonNode getValue(TxnId txnId, ITupleReference tuple) {
                try {
                    byte[] serRecord = tuple.getFieldData(payloadFieldIndex);
                    int recordStartOffset = tuple.getFieldStart(payloadFieldIndex);
                    int recordLength = tuple.getFieldLength(payloadFieldIndex);

                    printer.init();
                    outputStream.reset();

                    printer.print(serRecord, recordStartOffset, recordLength, printStream);
                    printStream.flush();
                    return JSONUtil.readTree(outputStream.getByteArray(), 0, outputStream.getLength());
                } catch (Throwable th) {
                    return JSONUtil.createObject();
                }
            }
        };
    }
}
