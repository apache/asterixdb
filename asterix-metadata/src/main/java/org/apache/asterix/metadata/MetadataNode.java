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
import java.util.List;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.dataflow.AsterixLSMIndexUtil;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.api.IMetadataNode;
import org.apache.asterix.metadata.api.IValueExtractor;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataSecondaryIndexes;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.ExternalFile;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedPolicy;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entitytupletranslators.CompactionPolicyTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatasetTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatasourceAdapterTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DatatypeTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.DataverseTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.ExternalFileTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedPolicyTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FeedTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.FunctionTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.IndexTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.LibraryTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.NodeGroupTupleTranslator;
import org.apache.asterix.metadata.entitytupletranslators.NodeTupleTranslator;
import org.apache.asterix.metadata.valueextractors.DatasetNameValueExtractor;
import org.apache.asterix.metadata.valueextractors.DatatypeNameValueExtractor;
import org.apache.asterix.metadata.valueextractors.MetadataEntityValueExtractor;
import org.apache.asterix.metadata.valueextractors.NestedDatatypeNameValueExtractor;
import org.apache.asterix.metadata.valueextractors.TupleCopyValueExtractor;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexModificationOperationCallback;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexModificationOperationCallback;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.util.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManager;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;

public class MetadataNode implements IMetadataNode {
    private static final long serialVersionUID = 1L;

    private static final DatasetId METADATA_DATASET_ID = new DatasetId(MetadataPrimaryIndexes.METADATA_DATASET_ID);

    private IIndexLifecycleManager indexLifecycleManager;
    private ITransactionSubsystem transactionSubsystem;

    public static final MetadataNode INSTANCE = new MetadataNode();

    private MetadataNode() {
        super();
    }

    public void initialize(IAsterixAppRuntimeContext runtimeContext) {
        this.transactionSubsystem = runtimeContext.getTransactionSubsystem();
        this.indexLifecycleManager = runtimeContext.getIndexLifecycleManager();
    }

    @Override
    public void beginTransaction(JobId transactionId) throws ACIDException, RemoteException {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().beginTransaction(transactionId);
        txnCtx.setMetadataTransaction(true);
    }

    @Override
    public void commitTransaction(JobId jobId) throws RemoteException, ACIDException {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);
        transactionSubsystem.getTransactionManager().commitTransaction(txnCtx, new DatasetId(-1), -1);
    }

    @Override
    public void abortTransaction(JobId jobId) throws RemoteException, ACIDException {
        try {
            ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId,
                    false);
            transactionSubsystem.getTransactionManager().abortTransaction(txnCtx, new DatasetId(-1), -1);
        } catch (ACIDException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void lock(JobId jobId, byte lockMode) throws ACIDException, RemoteException {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);
        transactionSubsystem.getLockManager().lock(METADATA_DATASET_ID, -1, lockMode, txnCtx);
    }

    @Override
    public void unlock(JobId jobId, byte lockMode) throws ACIDException, RemoteException {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);
        transactionSubsystem.getLockManager().unlock(METADATA_DATASET_ID, -1, lockMode, txnCtx);
    }

    @Override
    public void addDataverse(JobId jobId, Dataverse dataverse) throws MetadataException, RemoteException {
        try {
            DataverseTupleTranslator tupleReaderWriter = new DataverseTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(dataverse);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A dataverse with this name " + dataverse.getDataverseName()
                    + " already exists.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addDataset(JobId jobId, Dataset dataset) throws MetadataException, RemoteException {
        try {
            // Insert into the 'dataset' dataset.
            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(true);
            ITupleReference datasetTuple = tupleReaderWriter.getTupleFromMetadataEntity(dataset);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            // Add an entry for the node group
            ITupleReference nodeGroupTuple = createTuple(dataset.getNodeGroupName(), dataset.getDataverseName(),
                    dataset.getDatasetName());
            insertTupleIntoIndex(jobId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, nodeGroupTuple);
            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                // Add the primary index for the dataset.
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                Index primaryIndex = new Index(dataset.getDataverseName(), dataset.getDatasetName(),
                        dataset.getDatasetName(), IndexType.BTREE, id.getPrimaryKey(), id.getPrimaryKeyType(), false,
                        true, dataset.getPendingOp());

                addIndex(jobId, primaryIndex);
            }
            // Add entry in datatype secondary index.
            ITupleReference dataTypeTuple = createTuple(dataset.getDataverseName(), dataset.getItemTypeName(),
                    dataset.getDatasetName());
            insertTupleIntoIndex(jobId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeTuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A dataset with this name " + dataset.getDatasetName()
                    + " already exists in dataverse '" + dataset.getDataverseName() + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addIndex(JobId jobId, Index index) throws MetadataException, RemoteException {
        try {
            IndexTupleTranslator tupleWriter = new IndexTupleTranslator(jobId, this, true);
            ITupleReference tuple = tupleWriter.getTupleFromMetadataEntity(index);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.INDEX_DATASET, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("An index with name '" + index.getIndexName() + "' already exists.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addNode(JobId jobId, Node node) throws MetadataException, RemoteException {
        try {
            NodeTupleTranslator tupleReaderWriter = new NodeTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(node);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.NODE_DATASET, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A node with name '" + node.getNodeName() + "' already exists.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addNodeGroup(JobId jobId, NodeGroup nodeGroup) throws MetadataException, RemoteException {
        try {
            NodeGroupTupleTranslator tupleReaderWriter = new NodeGroupTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(nodeGroup);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.NODEGROUP_DATASET, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A nodegroup with name '" + nodeGroup.getNodeGroupName() + "' already exists.",
                    e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addDatatype(JobId jobId, Datatype datatype) throws MetadataException, RemoteException {
        try {
            DatatypeTupleTranslator tupleReaderWriter = new DatatypeTupleTranslator(jobId, this, true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(datatype);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A datatype with name '" + datatype.getDatatypeName() + "' already exists.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addFunction(JobId jobId, Function function) throws MetadataException, RemoteException {
        try {
            // Insert into the 'function' dataset.
            FunctionTupleTranslator tupleReaderWriter = new FunctionTupleTranslator(true);
            ITupleReference functionTuple = tupleReaderWriter.getTupleFromMetadataEntity(function);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, functionTuple);

        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A function with this name " + function.getName() + " and arity "
                    + function.getArity() + " already exists in dataverse '" + function.getDataverseName() + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public void insertIntoDatatypeSecondaryIndex(JobId jobId, String dataverseName, String nestedTypeName,
            String topTypeName) throws Exception {
        ITupleReference tuple = createTuple(dataverseName, nestedTypeName, topTypeName);
        insertTupleIntoIndex(jobId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, tuple);
    }

    private void insertTupleIntoIndex(JobId jobId, IMetadataIndex metadataIndex, ITupleReference tuple)
            throws Exception {
        long resourceID = metadataIndex.getResourceID();
        ILSMIndex lsmIndex = (ILSMIndex) indexLifecycleManager.getIndex(resourceID);
        try {
            indexLifecycleManager.open(resourceID);

            // prepare a Callback for logging
            IModificationOperationCallback modCallback = createIndexModificationCallback(jobId, resourceID,
                    metadataIndex, lsmIndex, IndexOperation.INSERT);

            ILSMIndexAccessor indexAccessor = lsmIndex.createAccessor(modCallback, NoOpOperationCallback.INSTANCE);

            ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId,
                    false);
            txnCtx.setWriteTxn(true);
            txnCtx.registerIndexAndCallback(resourceID, lsmIndex, (AbstractOperationCallback) modCallback,
                    metadataIndex.isPrimaryIndex());

            AsterixLSMIndexUtil.checkAndSetFirstLSN((AbstractLSMIndex) lsmIndex, transactionSubsystem.getLogManager());

            // TODO: fix exceptions once new BTree exception model is in hyracks.
            indexAccessor.forceInsert(tuple);
        } catch (Exception e) {
            throw e;
        } finally {
            indexLifecycleManager.close(resourceID);
        }
    }

    private IModificationOperationCallback createIndexModificationCallback(JobId jobId, long resourceId,
            IMetadataIndex metadataIndex, ILSMIndex lsmIndex, IndexOperation indexOp) throws Exception {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);

        if (metadataIndex.isPrimaryIndex()) {
            return new PrimaryIndexModificationOperationCallback(metadataIndex.getDatasetId().getId(),
                    metadataIndex.getPrimaryKeyIndexes(), txnCtx, transactionSubsystem.getLockManager(),
                    transactionSubsystem, resourceId, ResourceType.LSM_BTREE, indexOp);
        } else {
            return new SecondaryIndexModificationOperationCallback(metadataIndex.getDatasetId().getId(),
                    metadataIndex.getPrimaryKeyIndexes(), txnCtx, transactionSubsystem.getLockManager(),
                    transactionSubsystem, resourceId, ResourceType.LSM_BTREE, indexOp);
        }
    }

    @Override
    public void dropDataverse(JobId jobId, String dataverseName) throws MetadataException, RemoteException {
        try {

            List<Dataset> dataverseDatasets;
            Dataset ds;
            dataverseDatasets = getDataverseDatasets(jobId, dataverseName);
            if (dataverseDatasets != null && dataverseDatasets.size() > 0) {
                // Drop all datasets in this dataverse.
                for (int i = 0; i < dataverseDatasets.size(); i++) {
                    ds = dataverseDatasets.get(i);
                    dropDataset(jobId, dataverseName, ds.getDatasetName());
                }
            }
            List<Datatype> dataverseDatatypes;
            // As a side effect, acquires an S lock on the 'datatype' dataset
            // on behalf of txnId.
            dataverseDatatypes = getDataverseDatatypes(jobId, dataverseName);
            if (dataverseDatatypes != null && dataverseDatatypes.size() > 0) {
                // Drop all types in this dataverse.
                for (int i = 0; i < dataverseDatatypes.size(); i++) {
                    forceDropDatatype(jobId, dataverseName, dataverseDatatypes.get(i).getDatatypeName());
                }
            }

            // As a side effect, acquires an S lock on the 'Function' dataset
            // on behalf of txnId.
            List<Function> dataverseFunctions = getDataverseFunctions(jobId, dataverseName);
            if (dataverseFunctions != null && dataverseFunctions.size() > 0) {
                // Drop all functions in this dataverse.
                for (Function function : dataverseFunctions) {
                    dropFunction(jobId, new FunctionSignature(dataverseName, function.getName(), function.getArity()));
                }
            }

            // As a side effect, acquires an S lock on the 'Adapter' dataset
            // on behalf of txnId.
            List<DatasourceAdapter> dataverseAdapters = getDataverseAdapters(jobId, dataverseName);
            if (dataverseAdapters != null && dataverseAdapters.size() > 0) {
                // Drop all functions in this dataverse.
                for (DatasourceAdapter adapter : dataverseAdapters) {
                    dropAdapter(jobId, dataverseName, adapter.getAdapterIdentifier().getName());
                }
            }

            List<Feed> dataverseFeeds;
            Feed feed;
            dataverseFeeds = getDataverseFeeds(jobId, dataverseName);
            if (dataverseFeeds != null && dataverseFeeds.size() > 0) {
                // Drop all datasets in this dataverse.
                for (int i = 0; i < dataverseFeeds.size(); i++) {
                    feed = dataverseFeeds.get(i);
                    dropFeed(jobId, dataverseName, feed.getFeedName());
                }
            }

            List<FeedPolicy> feedPolicies = getDataversePolicies(jobId, dataverseName);
            if (feedPolicies != null && feedPolicies.size() > 0) {
                // Drop all feed ingestion policies in this dataverse.
                for (FeedPolicy feedPolicy : feedPolicies) {
                    dropFeedPolicy(jobId, dataverseName, feedPolicy.getPolicyName());
                }
            }

            // Delete the dataverse entry from the 'dataverse' dataset.
            ITupleReference searchKey = createTuple(dataverseName);
            // As a side effect, acquires an S lock on the 'dataverse' dataset
            // on behalf of txnId.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop dataverse '" + dataverseName + "' because it doesn't exist.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropDataset(JobId jobId, String dataverseName, String datasetName) throws MetadataException,
            RemoteException {
        Dataset dataset;
        try {
            dataset = getDataset(jobId, dataverseName, datasetName);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        if (dataset == null) {
            throw new MetadataException("Cannot drop dataset '" + datasetName + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'datasets' dataset.
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'dataset' dataset.
            ITupleReference datasetTuple = null;
            try {
                datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey);

                // Delete entry from secondary index 'group'.
                ITupleReference groupNameSearchKey = createTuple(dataset.getNodeGroupName(), dataverseName, datasetName);
                // Searches the index for the tuple to be deleted. Acquires an S
                // lock on the GROUPNAME_ON_DATASET_INDEX index.
                try {
                    ITupleReference groupNameTuple = getTupleToBeDeleted(jobId,
                            MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, groupNameSearchKey);
                    deleteTupleFromIndex(jobId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, groupNameTuple);
                } catch (TreeIndexException tie) {
                    // ignore this exception and continue deleting all relevant
                    // artifacts.
                }

                // Delete entry from secondary index 'type'.
                ITupleReference dataTypeSearchKey = createTuple(dataverseName, dataset.getItemTypeName(), datasetName);
                // Searches the index for the tuple to be deleted. Acquires an S
                // lock on the DATATYPENAME_ON_DATASET_INDEX index.
                try {
                    ITupleReference dataTypeTuple = getTupleToBeDeleted(jobId,
                            MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeSearchKey);
                    deleteTupleFromIndex(jobId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeTuple);
                } catch (TreeIndexException tie) {
                    // ignore this exception and continue deleting all relevant
                    // artifacts.
                }

                // Delete entry(s) from the 'indexes' dataset.
                List<Index> datasetIndexes = getDatasetIndexes(jobId, dataverseName, datasetName);
                if (datasetIndexes != null) {
                    for (Index index : datasetIndexes) {
                        dropIndex(jobId, dataverseName, datasetName, index.getIndexName());
                    }
                }

                if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
                    // Delete External Files
                    // As a side effect, acquires an S lock on the 'ExternalFile' dataset
                    // on behalf of txnId.
                    List<ExternalFile> datasetFiles = getExternalFiles(jobId, dataset);
                    if (datasetFiles != null && datasetFiles.size() > 0) {
                        // Drop all external files in this dataset.
                        for (ExternalFile file : datasetFiles) {
                            dropExternalFile(jobId, dataverseName, file.getDatasetName(), file.getFileNumber());
                        }
                    }
                }
            } catch (TreeIndexException tie) {
                // ignore this exception and continue deleting all relevant
                // artifacts.
            } finally {
                deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            }

        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropIndex(JobId jobId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName, indexName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'index' dataset.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.INDEX_DATASET, tuple);
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop index '" + datasetName + "." + indexName
                    + "' because it doesn't exist.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropNodegroup(JobId jobId, String nodeGroupName) throws MetadataException, RemoteException {
        List<String> datasetNames;
        try {
            datasetNames = getDatasetNamesPartitionedOnThisNodeGroup(jobId, nodeGroupName);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        if (!datasetNames.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Nodegroup '" + nodeGroupName
                    + "' cannot be dropped; it was used for partitioning these datasets:");
            for (int i = 0; i < datasetNames.size(); i++)
                sb.append("\n" + (i + 1) + "- " + datasetNames.get(i) + ".");
            throw new MetadataException(sb.toString());
        }
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'nodegroup' dataset.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.NODEGROUP_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.NODEGROUP_DATASET, tuple);
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop nodegroup '" + nodeGroupName + "' because it doesn't exist", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropDatatype(JobId jobId, String dataverseName, String datatypeName) throws MetadataException,
            RemoteException {
        List<String> datasetNames;
        List<String> usedDatatypes;
        try {
            datasetNames = getDatasetNamesDeclaredByThisDatatype(jobId, dataverseName, datatypeName);
            usedDatatypes = getDatatypeNamesUsingThisDatatype(jobId, dataverseName, datatypeName);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        // Check whether type is being used by datasets.
        if (!datasetNames.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Cannot drop type '" + datatypeName + "'; it was used when creating these datasets:");
            for (int i = 0; i < datasetNames.size(); i++)
                sb.append("\n" + (i + 1) + "- " + datasetNames.get(i) + ".");
            throw new MetadataException(sb.toString());
        }
        // Check whether type is being used by other types.
        if (!usedDatatypes.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Cannot drop type '" + datatypeName + "'; it is used in these datatypes:");
            for (int i = 0; i < usedDatatypes.size(); i++)
                sb.append("\n" + (i + 1) + "- " + usedDatatypes.get(i) + ".");
            throw new MetadataException(sb.toString());
        }
        // Delete the datatype entry, including all it's nested types.
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey);
            // This call uses the secondary index on datatype. Get nested types
            // before deleting entry from secondary index.
            List<String> nestedTypes = getNestedDatatypeNames(jobId, dataverseName, datatypeName);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
            deleteFromDatatypeSecondaryIndex(jobId, dataverseName, datatypeName);
            for (String nestedType : nestedTypes) {
                Datatype dt = getDatatype(jobId, dataverseName, nestedType);
                if (dt != null && dt.getIsAnonymous()) {
                    dropDatatype(jobId, dataverseName, dt.getDatatypeName());
                }
            }
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop type '" + datatypeName + "' because it doesn't exist", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private void forceDropDatatype(JobId jobId, String dataverseName, String datatypeName) throws AsterixException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
            deleteFromDatatypeSecondaryIndex(jobId, dataverseName, datatypeName);
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new AsterixException("Cannot drop type '" + datatypeName + "' because it doesn't exist", e);
        } catch (AsterixException e) {
            throw e;
        } catch (Exception e) {
            throw new AsterixException(e);
        }
    }

    private void deleteFromDatatypeSecondaryIndex(JobId jobId, String dataverseName, String datatypeName)
            throws AsterixException {
        try {
            List<String> nestedTypes = getNestedDatatypeNames(jobId, dataverseName, datatypeName);
            for (String nestedType : nestedTypes) {
                ITupleReference searchKey = createTuple(dataverseName, nestedType, datatypeName);
                // Searches the index for the tuple to be deleted. Acquires an S
                // lock on the DATATYPENAME_ON_DATATYPE_INDEX index.
                ITupleReference tuple = getTupleToBeDeleted(jobId,
                        MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, searchKey);
                deleteTupleFromIndex(jobId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, tuple);
            }
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new AsterixException("Cannot drop type '" + datatypeName + "' because it doesn't exist", e);
        } catch (AsterixException e) {
            throw e;
        } catch (Exception e) {
            throw new AsterixException(e);
        }
    }

    private void deleteTupleFromIndex(JobId jobId, IMetadataIndex metadataIndex, ITupleReference tuple)
            throws Exception {
        long resourceID = metadataIndex.getResourceID();
        ILSMIndex lsmIndex = (ILSMIndex) indexLifecycleManager.getIndex(resourceID);
        try {
            indexLifecycleManager.open(resourceID);
            // prepare a Callback for logging
            IModificationOperationCallback modCallback = createIndexModificationCallback(jobId, resourceID,
                    metadataIndex, lsmIndex, IndexOperation.DELETE);
            ILSMIndexAccessor indexAccessor = lsmIndex.createAccessor(modCallback, NoOpOperationCallback.INSTANCE);

            ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId,
                    false);
            txnCtx.setWriteTxn(true);
            txnCtx.registerIndexAndCallback(resourceID, lsmIndex, (AbstractOperationCallback) modCallback,
                    metadataIndex.isPrimaryIndex());

            AsterixLSMIndexUtil.checkAndSetFirstLSN((AbstractLSMIndex) lsmIndex, transactionSubsystem.getLogManager());

            indexAccessor.forceDelete(tuple);
        } catch (Exception e) {
            throw e;
        } finally {
            indexLifecycleManager.close(resourceID);
        }
    }

    @Override
    public List<Dataverse> getDataverses(JobId jobId) throws MetadataException, RemoteException {
        try {
            DataverseTupleTranslator tupleReaderWriter = new DataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<Dataverse>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<Dataverse>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, null, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public Dataverse getDataverse(JobId jobId, String dataverseName) throws MetadataException, RemoteException {

        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DataverseTupleTranslator tupleReaderWriter = new DataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<Dataverse>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<Dataverse>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public List<Dataset> getDataverseDatasets(JobId jobId, String dataverseName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<Dataset>(tupleReaderWriter);
            List<Dataset> results = new ArrayList<Dataset>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<Feed> getDataverseFeeds(JobId jobId, String dataverseName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            FeedTupleTranslator tupleReaderWriter = new FeedTupleTranslator(false);
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<Feed>(tupleReaderWriter);
            List<Feed> results = new ArrayList<Feed>();
            searchIndex(jobId, MetadataPrimaryIndexes.FEED_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public List<Library> getDataverseLibraries(JobId jobId, String dataverseName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            LibraryTupleTranslator tupleReaderWriter = new LibraryTupleTranslator(false);
            IValueExtractor<Library> valueExtractor = new MetadataEntityValueExtractor<Library>(tupleReaderWriter);
            List<Library> results = new ArrayList<Library>();
            searchIndex(jobId, MetadataPrimaryIndexes.LIBRARY_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private List<Datatype> getDataverseDatatypes(JobId jobId, String dataverseName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatatypeTupleTranslator tupleReaderWriter = new DatatypeTupleTranslator(jobId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<Datatype>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<Datatype>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Dataset getDataset(JobId jobId, String dataverseName, String datasetName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(false);
            List<Dataset> results = new ArrayList<Dataset>();
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<Dataset>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private List<String> getDatasetNamesDeclaredByThisDatatype(JobId jobId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new DatasetNameValueExtractor();
            searchIndex(jobId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, searchKey, valueExtractor,
                    results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public List<String> getDatatypeNamesUsingThisDatatype(JobId jobId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new DatatypeNameValueExtractor(dataverseName, this);
            searchIndex(jobId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, searchKey, valueExtractor,
                    results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private List<String> getNestedDatatypeNames(JobId jobId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new NestedDatatypeNameValueExtractor(datatypeName);
            searchIndex(jobId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, searchKey, valueExtractor,
                    results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public List<String> getDatasetNamesPartitionedOnThisNodeGroup(JobId jobId, String nodegroup)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(nodegroup);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new DatasetNameValueExtractor();
            searchIndex(jobId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Index getIndex(JobId jobId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName, indexName);
            IndexTupleTranslator tupleReaderWriter = new IndexTupleTranslator(jobId, this, false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<Index>(tupleReaderWriter);
            List<Index> results = new ArrayList<Index>();
            searchIndex(jobId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<Index> getDatasetIndexes(JobId jobId, String dataverseName, String datasetName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            IndexTupleTranslator tupleReaderWriter = new IndexTupleTranslator(jobId, this, false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<Index>(tupleReaderWriter);
            List<Index> results = new ArrayList<Index>();
            searchIndex(jobId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Datatype getDatatype(JobId jobId, String dataverseName, String datatypeName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            DatatypeTupleTranslator tupleReaderWriter = new DatatypeTupleTranslator(jobId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<Datatype>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<Datatype>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            e.printStackTrace();
            throw new MetadataException(e);
        }
    }

    @Override
    public NodeGroup getNodeGroup(JobId jobId, String nodeGroupName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            NodeGroupTupleTranslator tupleReaderWriter = new NodeGroupTupleTranslator(false);
            IValueExtractor<NodeGroup> valueExtractor = new MetadataEntityValueExtractor<NodeGroup>(tupleReaderWriter);
            List<NodeGroup> results = new ArrayList<NodeGroup>();
            searchIndex(jobId, MetadataPrimaryIndexes.NODEGROUP_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Function getFunction(JobId jobId, FunctionSignature functionSignature) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(functionSignature.getNamespace(), functionSignature.getName(), ""
                    + functionSignature.getArity());
            FunctionTupleTranslator tupleReaderWriter = new FunctionTupleTranslator(false);
            List<Function> results = new ArrayList<Function>();
            IValueExtractor<Function> valueExtractor = new MetadataEntityValueExtractor<Function>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            e.printStackTrace();
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropFunction(JobId jobId, FunctionSignature functionSignature) throws MetadataException,
            RemoteException {

        Function function;
        try {
            function = getFunction(jobId, functionSignature);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        if (function == null) {
            throw new MetadataException("Cannot drop function '" + functionSignature.toString()
                    + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'function' dataset.
            ITupleReference searchKey = createTuple(functionSignature.getNamespace(), functionSignature.getName(), ""
                    + functionSignature.getArity());
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'function' dataset.
            ITupleReference functionTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET,
                    searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, functionTuple);

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("There is no function with the name " + functionSignature.getName()
                    + " and arity " + functionSignature.getArity(), e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private ITupleReference getTupleToBeDeleted(JobId jobId, IMetadataIndex metadataIndex, ITupleReference searchKey)
            throws Exception {
        IValueExtractor<ITupleReference> valueExtractor = new TupleCopyValueExtractor(metadataIndex.getTypeTraits());
        List<ITupleReference> results = new ArrayList<ITupleReference>();
        searchIndex(jobId, metadataIndex, searchKey, valueExtractor, results);
        if (results.isEmpty()) {
            // TODO: Temporarily a TreeIndexException to make it get caught by
            // caller in the appropriate catch block.
            throw new TreeIndexException("Could not find entry to be deleted.");
        }
        // There should be exactly one result returned from the search.
        return results.get(0);
    }

    // Debugging Method
    public String printMetadata() {

        StringBuilder sb = new StringBuilder();
        try {
            IMetadataIndex index = MetadataPrimaryIndexes.DATAVERSE_DATASET;
            long resourceID = index.getResourceID();
            IIndex indexInstance = indexLifecycleManager.getIndex(resourceID);
            indexLifecycleManager.open(resourceID);
            IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            ITreeIndexCursor rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor(false);

            RangePredicate rangePred = null;
            rangePred = new RangePredicate(null, null, true, true, null, null);
            indexAccessor.search(rangeCursor, rangePred);
            try {
                while (rangeCursor.hasNext()) {
                    rangeCursor.next();
                    sb.append(TupleUtils.printTuple(rangeCursor.getTuple(),
                            new ISerializerDeserializer[] { AqlSerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(BuiltinType.ASTRING) }));
                }
            } finally {
                rangeCursor.close();
            }
            indexLifecycleManager.close(resourceID);

            index = MetadataPrimaryIndexes.DATASET_DATASET;
            resourceID = index.getResourceID();
            indexInstance = indexLifecycleManager.getIndex(resourceID);
            indexLifecycleManager.open(resourceID);
            indexAccessor = indexInstance
                    .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor(false);

            rangePred = null;
            rangePred = new RangePredicate(null, null, true, true, null, null);
            indexAccessor.search(rangeCursor, rangePred);
            try {
                while (rangeCursor.hasNext()) {
                    rangeCursor.next();
                    sb.append(TupleUtils.printTuple(rangeCursor.getTuple(), new ISerializerDeserializer[] {
                            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING),
                            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING) }));
                }
            } finally {
                rangeCursor.close();
            }
            indexLifecycleManager.close(resourceID);

            index = MetadataPrimaryIndexes.INDEX_DATASET;
            resourceID = index.getResourceID();
            indexInstance = indexLifecycleManager.getIndex(resourceID);
            indexLifecycleManager.open(resourceID);
            indexAccessor = indexInstance
                    .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor(false);

            rangePred = null;
            rangePred = new RangePredicate(null, null, true, true, null, null);
            indexAccessor.search(rangeCursor, rangePred);
            try {
                while (rangeCursor.hasNext()) {
                    rangeCursor.next();
                    sb.append(TupleUtils.printTuple(rangeCursor.getTuple(), new ISerializerDeserializer[] {
                            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING),
                            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING),
                            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING) }));
                }
            } finally {
                rangeCursor.close();
            }
            indexLifecycleManager.close(resourceID);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    private <ResultType> void searchIndex(JobId jobId, IMetadataIndex index, ITupleReference searchKey,
            IValueExtractor<ResultType> valueExtractor, List<ResultType> results) throws Exception {
        IBinaryComparatorFactory[] comparatorFactories = index.getKeyBinaryComparatorFactory();
        long resourceID = index.getResourceID();
        IIndex indexInstance = indexLifecycleManager.getIndex(resourceID);
        indexLifecycleManager.open(resourceID);
        IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        ITreeIndexCursor rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor(false);

        IBinaryComparator[] searchCmps = null;
        MultiComparator searchCmp = null;
        RangePredicate rangePred = null;
        if (searchKey != null) {
            searchCmps = new IBinaryComparator[searchKey.getFieldCount()];
            for (int i = 0; i < searchKey.getFieldCount(); i++) {
                searchCmps[i] = comparatorFactories[i].createBinaryComparator();
            }
            searchCmp = new MultiComparator(searchCmps);
        }
        rangePred = new RangePredicate(searchKey, searchKey, true, true, searchCmp, searchCmp);
        indexAccessor.search(rangeCursor, rangePred);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ResultType result = valueExtractor.getValue(jobId, rangeCursor.getTuple());
                if (result != null) {
                    results.add(result);
                }
            }
        } finally {
            rangeCursor.close();
        }
        indexLifecycleManager.close(resourceID);
    }

    @Override
    public void initializeDatasetIdFactory(JobId jobId) throws MetadataException, RemoteException {
        int mostRecentDatasetId = MetadataPrimaryIndexes.FIRST_AVAILABLE_USER_DATASET_ID;
        long resourceID = MetadataPrimaryIndexes.DATASET_DATASET.getResourceID();
        try {
            IIndex indexInstance = indexLifecycleManager.getIndex(resourceID);
            indexLifecycleManager.open(resourceID);
            try {
                IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
                IIndexCursor rangeCursor = indexAccessor.createSearchCursor(false);

                DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(false);
                IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<Dataset>(tupleReaderWriter);
                RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);

                indexAccessor.search(rangeCursor, rangePred);
                int datasetId;

                try {
                    while (rangeCursor.hasNext()) {
                        rangeCursor.next();
                        final ITupleReference ref = rangeCursor.getTuple();
                        final Dataset ds = valueExtractor.getValue(jobId, ref);
                        datasetId = ds.getDatasetId();
                        if (mostRecentDatasetId < datasetId) {
                            mostRecentDatasetId = datasetId;
                        }
                    }
                } finally {
                    rangeCursor.close();
                }
            } finally {
                indexLifecycleManager.close(resourceID);
            }

        } catch (Exception e) {
            throw new MetadataException(e);
        }

        DatasetIdFactory.initialize(mostRecentDatasetId);
    }

    // TODO: Can use Hyrack's TupleUtils for this, once we switch to a newer
    // Hyracks version.
    public ITupleReference createTuple(String... fields) throws HyracksDataException {
        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);
        AMutableString aString = new AMutableString("");
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fields.length);
        for (String s : fields) {
            aString.setValue(s);
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    @Override
    public List<Function> getDataverseFunctions(JobId jobId, String dataverseName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            FunctionTupleTranslator tupleReaderWriter = new FunctionTupleTranslator(false);
            IValueExtractor<Function> valueExtractor = new MetadataEntityValueExtractor<Function>(tupleReaderWriter);
            List<Function> results = new ArrayList<Function>();
            searchIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addAdapter(JobId jobId, DatasourceAdapter adapter) throws MetadataException, RemoteException {
        try {
            // Insert into the 'Adapter' dataset.
            DatasourceAdapterTupleTranslator tupleReaderWriter = new DatasourceAdapterTupleTranslator(true);
            ITupleReference adapterTuple = tupleReaderWriter.getTupleFromMetadataEntity(adapter);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, adapterTuple);

        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A adapter with this name " + adapter.getAdapterIdentifier().getName()
                    + " already exists in dataverse '" + adapter.getAdapterIdentifier().getNamespace() + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public void dropAdapter(JobId jobId, String dataverseName, String adapterName) throws MetadataException,
            RemoteException {
        DatasourceAdapter adapter;
        try {
            adapter = getAdapter(jobId, dataverseName, adapterName);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        if (adapter == null) {
            throw new MetadataException("Cannot drop adapter '" + adapter + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'Adapter' dataset.
            ITupleReference searchKey = createTuple(dataverseName, adapterName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'Adapter' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId,
                    MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, datasetTuple);

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop adapter '" + adapterName, e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public DatasourceAdapter getAdapter(JobId jobId, String dataverseName, String adapterName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, adapterName);
            DatasourceAdapterTupleTranslator tupleReaderWriter = new DatasourceAdapterTupleTranslator(false);
            List<DatasourceAdapter> results = new ArrayList<DatasourceAdapter>();
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<DatasourceAdapter>(
                    tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addCompactionPolicy(JobId jobId, CompactionPolicy compactionPolicy) throws MetadataException,
            RemoteException {
        try {
            // Insert into the 'CompactionPolicy' dataset.
            CompactionPolicyTupleTranslator tupleReaderWriter = new CompactionPolicyTupleTranslator(true);
            ITupleReference compactionPolicyTuple = tupleReaderWriter.getTupleFromMetadataEntity(compactionPolicy);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET, compactionPolicyTuple);

        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("A compcation policy with this name " + compactionPolicy.getPolicyName()
                    + " already exists in dataverse '" + compactionPolicy.getPolicyName() + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public CompactionPolicy getCompactionPolicy(JobId jobId, String dataverse, String policyName)
            throws MetadataException, RemoteException {

        try {
            ITupleReference searchKey = createTuple(dataverse, policyName);
            CompactionPolicyTupleTranslator tupleReaderWriter = new CompactionPolicyTupleTranslator(false);
            List<CompactionPolicy> results = new ArrayList<CompactionPolicy>();
            IValueExtractor<CompactionPolicy> valueExtractor = new MetadataEntityValueExtractor<CompactionPolicy>(
                    tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<DatasourceAdapter> getDataverseAdapters(JobId jobId, String dataverseName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatasourceAdapterTupleTranslator tupleReaderWriter = new DatasourceAdapterTupleTranslator(false);
            IValueExtractor<DatasourceAdapter> valueExtractor = new MetadataEntityValueExtractor<DatasourceAdapter>(
                    tupleReaderWriter);
            List<DatasourceAdapter> results = new ArrayList<DatasourceAdapter>();
            searchIndex(jobId, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addLibrary(JobId jobId, Library library) throws MetadataException, RemoteException {
        try {
            // Insert into the 'Library' dataset.
            LibraryTupleTranslator tupleReaderWriter = new LibraryTupleTranslator(true);
            ITupleReference libraryTuple = tupleReaderWriter.getTupleFromMetadataEntity(library);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.LIBRARY_DATASET, libraryTuple);

        } catch (TreeIndexException e) {
            throw new MetadataException("A library with this name " + library.getDataverseName()
                    + " already exists in dataverse '" + library.getDataverseName() + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public void dropLibrary(JobId jobId, String dataverseName, String libraryName) throws MetadataException,
            RemoteException {
        Library library;
        try {
            library = getLibrary(jobId, dataverseName, libraryName);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        if (library == null) {
            throw new MetadataException("Cannot drop library '" + library + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'Library' dataset.
            ITupleReference searchKey = createTuple(dataverseName, libraryName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'Adapter' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.LIBRARY_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.LIBRARY_DATASET, datasetTuple);

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop library '" + libraryName, e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public Library getLibrary(JobId jobId, String dataverseName, String libraryName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, libraryName);
            LibraryTupleTranslator tupleReaderWriter = new LibraryTupleTranslator(false);
            List<Library> results = new ArrayList<Library>();
            IValueExtractor<Library> valueExtractor = new MetadataEntityValueExtractor<Library>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.LIBRARY_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public int getMostRecentDatasetId() throws MetadataException, RemoteException {
        return DatasetIdFactory.getMostRecentDatasetId();
    }

    @Override
    public void addFeedPolicy(JobId jobId, FeedPolicy feedPolicy) throws MetadataException, RemoteException {
        try {
            // Insert into the 'FeedPolicy' dataset.
            FeedPolicyTupleTranslator tupleReaderWriter = new FeedPolicyTupleTranslator(true);
            ITupleReference feedPolicyTuple = tupleReaderWriter.getTupleFromMetadataEntity(feedPolicy);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, feedPolicyTuple);

        } catch (TreeIndexException e) {
            throw new MetadataException("A feed policy with this name " + feedPolicy.getPolicyName()
                    + " already exists in dataverse '" + feedPolicy.getPolicyName() + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public FeedPolicy getFeedPolicy(JobId jobId, String dataverse, String policyName) throws MetadataException,
            RemoteException {

        try {
            ITupleReference searchKey = createTuple(dataverse, policyName);
            FeedPolicyTupleTranslator tupleReaderWriter = new FeedPolicyTupleTranslator(false);
            List<FeedPolicy> results = new ArrayList<FeedPolicy>();
            IValueExtractor<FeedPolicy> valueExtractor = new MetadataEntityValueExtractor<FeedPolicy>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addFeed(JobId jobId, Feed feed) throws MetadataException, RemoteException {
        try {
            // Insert into the 'Feed' dataset.
            FeedTupleTranslator tupleReaderWriter = new FeedTupleTranslator(true);
            ITupleReference feedTuple = tupleReaderWriter.getTupleFromMetadataEntity(feed);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.FEED_DATASET, feedTuple);

        } catch (TreeIndexException e) {
            throw new MetadataException("A feed with this name " + feed.getFeedName()
                    + " already exists in dataverse '" + feed.getDataverseName() + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Feed getFeed(JobId jobId, String dataverse, String feedName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverse, feedName);
            FeedTupleTranslator tupleReaderWriter = new FeedTupleTranslator(false);
            List<Feed> results = new ArrayList<Feed>();
            IValueExtractor<Feed> valueExtractor = new MetadataEntityValueExtractor<Feed>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.FEED_DATASET, searchKey, valueExtractor, results);
            if (!results.isEmpty()) {
                return results.get(0);
            }
            return null;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropFeed(JobId jobId, String dataverse, String feedName) throws MetadataException, RemoteException {

        try {
            ITupleReference searchKey = createTuple(dataverse, feedName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'nodegroup' dataset.
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.FEED_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.FEED_DATASET, tuple);
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop feed '" + feedName + "' because it doesn't exist", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public void dropFeedPolicy(JobId jobId, String dataverseName, String policyName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, policyName);
            ITupleReference tuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, tuple);
        } catch (TreeIndexException e) {
            throw new MetadataException("Unknown feed policy " + policyName, e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<FeedPolicy> getDataversePolicies(JobId jobId, String dataverse) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverse);
            FeedPolicyTupleTranslator tupleReaderWriter = new FeedPolicyTupleTranslator(false);
            IValueExtractor<FeedPolicy> valueExtractor = new MetadataEntityValueExtractor<FeedPolicy>(tupleReaderWriter);
            List<FeedPolicy> results = new ArrayList<FeedPolicy>();
            searchIndex(jobId, MetadataPrimaryIndexes.FEED_POLICY_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addExternalFile(JobId jobId, ExternalFile externalFile) throws MetadataException, RemoteException {
        try {
            // Insert into the 'externalFiles' dataset.
            ExternalFileTupleTranslator tupleReaderWriter = new ExternalFileTupleTranslator(true);
            ITupleReference externalFileTuple = tupleReaderWriter.getTupleFromMetadataEntity(externalFile);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, externalFileTuple);
        } catch (TreeIndexDuplicateKeyException e) {
            throw new MetadataException("An externalFile with this number " + externalFile.getFileNumber()
                    + " already exists in dataset '" + externalFile.getDatasetName() + "' in dataverse '"
                    + externalFile.getDataverseName() + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<ExternalFile> getExternalFiles(JobId jobId, Dataset dataset) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataset.getDataverseName(), dataset.getDatasetName());
            ExternalFileTupleTranslator tupleReaderWriter = new ExternalFileTupleTranslator(false);
            IValueExtractor<ExternalFile> valueExtractor = new MetadataEntityValueExtractor<ExternalFile>(
                    tupleReaderWriter);
            List<ExternalFile> results = new ArrayList<ExternalFile>();
            searchIndex(jobId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropExternalFile(JobId jobId, String dataverseName, String datasetName, int fileNumber)
            throws MetadataException, RemoteException {
        try {
            // Delete entry from the 'ExternalFile' dataset.
            ITupleReference searchKey = createExternalFileSearchTuple(dataverseName, datasetName, fileNumber);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'ExternalFile' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET,
                    searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, datasetTuple);
        } catch (TreeIndexException e) {
            throw new MetadataException("Couldn't drop externalFile.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropExternalFiles(JobId jobId, Dataset dataset) throws MetadataException, RemoteException {
        List<ExternalFile> files;
        try {
            files = getExternalFiles(jobId, dataset);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        try {
            //loop through files and delete them
            for (int i = 0; i < files.size(); i++) {
                dropExternalFile(jobId, files.get(i).getDataverseName(), files.get(i).getDatasetName(), files.get(i)
                        .getFileNumber());
            }
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    // This method is used to create a search tuple for external data file since the search tuple has an int value
    @SuppressWarnings("unchecked")
    public ITupleReference createExternalFileSearchTuple(String dataverseName, String datasetName, int fileNumber)
            throws HyracksDataException {
        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);
        ISerializerDeserializer<AInt32> intSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.AINT32);

        AMutableString aString = new AMutableString("");
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);

        //dataverse field
        aString.setValue(dataverseName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        //dataset field
        aString.setValue(datasetName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        //file number field
        intSerde.serialize(new AInt32(fileNumber), tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    @Override
    public ExternalFile getExternalFile(JobId jobId, String dataverseName, String datasetName, Integer fileNumber)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createExternalFileSearchTuple(dataverseName, datasetName, fileNumber);
            ExternalFileTupleTranslator tupleReaderWriter = new ExternalFileTupleTranslator(false);
            IValueExtractor<ExternalFile> valueExtractor = new MetadataEntityValueExtractor<ExternalFile>(
                    tupleReaderWriter);
            List<ExternalFile> results = new ArrayList<ExternalFile>();
            searchIndex(jobId, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void updateDataset(JobId jobId, Dataset dataset) throws MetadataException, RemoteException {
        try {
            // This method will delete previous entry of the dataset and insert the new one
            // Delete entry from the 'datasets' dataset.
            ITupleReference searchKey;
            searchKey = createTuple(dataset.getDataverseName(), dataset.getDatasetName());
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'dataset' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            // Previous tuple was deleted
            // Insert into the 'dataset' dataset.
            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(true);
            datasetTuple = tupleReaderWriter.getTupleFromMetadataEntity(dataset);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }
}
