/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.common.transactions.AbstractOperationCallback;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager.ResourceType;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.api.IMetadataIndex;
import edu.uci.ics.asterix.metadata.api.IMetadataNode;
import edu.uci.ics.asterix.metadata.api.IValueExtractor;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataSecondaryIndexes;
import edu.uci.ics.asterix.metadata.entities.CompactionPolicy;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Node;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.metadata.entitytupletranslators.CompactionPolicyTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.DatasetTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.DatasourceAdapterTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.DatatypeTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.DataverseTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.FunctionTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.IndexTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.NodeGroupTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.NodeTupleTranslator;
import edu.uci.ics.asterix.metadata.valueextractors.DatasetNameValueExtractor;
import edu.uci.ics.asterix.metadata.valueextractors.DatatypeNameValueExtractor;
import edu.uci.ics.asterix.metadata.valueextractors.MetadataEntityValueExtractor;
import edu.uci.ics.asterix.metadata.valueextractors.NestedDatatypeNameValueExtractor;
import edu.uci.ics.asterix.metadata.valueextractors.TupleCopyValueExtractor;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexModificationOperationCallback;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexModificationOperationCallback;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetIdFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

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
    public void unlock(JobId jobId) throws ACIDException, RemoteException {
        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);
        transactionSubsystem.getLockManager().unlock(METADATA_DATASET_ID, -1, txnCtx);
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
            if (dataset.getDatasetType() == DatasetType.INTERNAL || dataset.getDatasetType() == DatasetType.FEED) {
                // Add the primary index for the dataset.
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                Index primaryIndex = new Index(dataset.getDataverseName(), dataset.getDatasetName(),
                        dataset.getDatasetName(), IndexType.BTREE, id.getPrimaryKey(), true, dataset.getPendingOp());

                addIndex(jobId, primaryIndex);
                ITupleReference nodeGroupTuple = createTuple(id.getNodeGroupName(), dataset.getDataverseName(),
                        dataset.getDatasetName());
                insertTupleIntoIndex(jobId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, nodeGroupTuple);
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
            IndexTupleTranslator tupleWriter = new IndexTupleTranslator(true);
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
        indexLifecycleManager.open(resourceID);

        // prepare a Callback for logging
        IModificationOperationCallback modCallback = createIndexModificationCallback(jobId, resourceID, metadataIndex,
                lsmIndex, IndexOperation.INSERT);

        ILSMIndexAccessor indexAccessor = lsmIndex.createAccessor(modCallback, NoOpOperationCallback.INSTANCE);

        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);
        txnCtx.setWriteTxn(true);
        txnCtx.registerIndexAndCallback(resourceID, lsmIndex, (AbstractOperationCallback) modCallback,
                metadataIndex.isPrimaryIndex());

        // TODO: fix exceptions once new BTree exception model is in hyracks.
        indexAccessor.forceInsert(tuple);

        indexLifecycleManager.close(resourceID);
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

            dataverseDatasets = getDataverseDatasets(jobId, dataverseName);
            if (dataverseDatasets != null && dataverseDatasets.size() > 0) {
                // Drop all datasets in this dataverse.
                for (int i = 0; i < dataverseDatasets.size(); i++) {
                    dropDataset(jobId, dataverseName, dataverseDatasets.get(i).getDatasetName());
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
                    dropAdapter(jobId, dataverseName, adapter.getAdapterIdentifier().getAdapterName());
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
            try {
                ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATASET_DATASET,
                        searchKey);
                deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            } catch (TreeIndexException tie) {
                // ignore this exception and continue deleting all relevant
                // artifacts.
            }

            // Delete entry from secondary index 'group'.
            if (dataset.getDatasetType() == DatasetType.INTERNAL || dataset.getDatasetType() == DatasetType.FEED) {
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                ITupleReference groupNameSearchKey = createTuple(id.getNodeGroupName(), dataverseName, datasetName);
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
            if (dataset.getDatasetType() == DatasetType.INTERNAL || dataset.getDatasetType() == DatasetType.FEED) {
                List<Index> datasetIndexes = getDatasetIndexes(jobId, dataverseName, datasetName);
                if (datasetIndexes != null) {
                    for (Index index : datasetIndexes) {
                        dropIndex(jobId, dataverseName, datasetName, index.getIndexName());
                    }
                }
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
        indexLifecycleManager.open(resourceID);
        // prepare a Callback for logging
        IModificationOperationCallback modCallback = createIndexModificationCallback(jobId, resourceID, metadataIndex,
                lsmIndex, IndexOperation.DELETE);
        ILSMIndexAccessor indexAccessor = lsmIndex.createAccessor(modCallback, NoOpOperationCallback.INSTANCE);

        ITransactionContext txnCtx = transactionSubsystem.getTransactionManager().getTransactionContext(jobId, false);
        txnCtx.setWriteTxn(true);
        txnCtx.registerIndexAndCallback(resourceID, lsmIndex, (AbstractOperationCallback) modCallback,
                metadataIndex.isPrimaryIndex());

        indexAccessor.forceDelete(tuple);
        indexLifecycleManager.close(resourceID);
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
            IndexTupleTranslator tupleReaderWriter = new IndexTupleTranslator(false);
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
            IndexTupleTranslator tupleReaderWriter = new IndexTupleTranslator(false);
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
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET,
                    searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, datasetTuple);

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
            ITreeIndexCursor rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor();

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
            rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor();

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
            rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor();

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
        ITreeIndexCursor rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor();

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
            IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            IIndexCursor rangeCursor = indexAccessor.createSearchCursor();

            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<Dataset>(tupleReaderWriter);
            RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);

            indexAccessor.search(rangeCursor, rangePred);
            int datasetId;

            try {
                while (rangeCursor.hasNext()) {
                    rangeCursor.next();
                    ITupleReference ref = rangeCursor.getTuple();
                    Dataset ds = valueExtractor.getValue(jobId, rangeCursor.getTuple());
                    datasetId = ((Dataset) valueExtractor.getValue(jobId, rangeCursor.getTuple())).getDatasetId();
                    if (mostRecentDatasetId < datasetId) {
                        mostRecentDatasetId = datasetId;
                    }
                }
            } finally {
                rangeCursor.close();
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
            throw new MetadataException("A adapter with this name " + adapter.getAdapterIdentifier().getAdapterName()
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
    public int getMostRecentDatasetId() throws MetadataException, RemoteException {
        return DatasetIdFactory.getMostRecentDatasetId();
    }
}
