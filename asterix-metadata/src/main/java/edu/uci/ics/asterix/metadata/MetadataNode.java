/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.context.AsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.api.IMetadataIndex;
import edu.uci.ics.asterix.metadata.api.IMetadataNode;
import edu.uci.ics.asterix.metadata.api.IValueExtractor;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataSecondaryIndexes;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Node;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.metadata.entitytupletranslators.DatasetTupleTranslator;
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
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetId;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetIdFactory;
import edu.uci.ics.asterix.transaction.management.service.transaction.JobId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTreeRangeSearchCursor;

public class MetadataNode implements IMetadataNode {
    private static final long serialVersionUID = 1L;

    // TODO: Temporary transactional resource id for metadata.
    private static final byte[] metadataResourceId = MetadataNode.class.toString().getBytes();
    private static final DatasetId METADATA_DATASET_ID = new DatasetId(MetadataPrimaryIndexes.METADATA_DATASET_ID);

    private IIndexLifecycleManager indexLifecycleManager;
    private TransactionProvider transactionProvider;

    public static final MetadataNode INSTANCE = new MetadataNode();

    private MetadataNode() {
        super();
    }

    public void initialize(AsterixAppRuntimeContext runtimeContext) {
        this.transactionProvider = runtimeContext.getTransactionProvider();
        this.indexLifecycleManager = runtimeContext.getIndexLifecycleManager();
    }

    @Override
    public void beginTransaction(JobId transactionId) throws ACIDException, RemoteException {
        transactionProvider.getTransactionManager().beginTransaction(transactionId);
    }

    @Override
    public void commitTransaction(JobId jobId) throws RemoteException, ACIDException {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(jobId);
        transactionProvider.getTransactionManager().commitTransaction(txnCtx);
    }

    @Override
    public void abortTransaction(JobId jobId) throws RemoteException, ACIDException {
        try {
            TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(jobId);
            transactionProvider.getTransactionManager().abortTransaction(txnCtx);
        } catch (ACIDException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void lock(JobId jobId, byte lockMode) throws ACIDException, RemoteException {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(jobId);
        transactionProvider.getLockManager().lock(METADATA_DATASET_ID, -1, lockMode, txnCtx);
    }

    @Override
    public void unlock(JobId jobId) throws ACIDException, RemoteException {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(jobId);
        transactionProvider.getLockManager().unlock(METADATA_DATASET_ID, -1, txnCtx);
    }

    @Override
    public void addDataverse(JobId jobId, Dataverse dataverse) throws MetadataException, RemoteException {
        try {
            DataverseTupleTranslator tupleReaderWriter = new DataverseTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(dataverse);
            insertTupleIntoIndex(jobId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);
        } catch (BTreeDuplicateKeyException e) {
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
                        dataset.getDatasetName(), IndexType.BTREE, id.getPrimaryKey(), true);
                addIndex(jobId, primaryIndex);
                ITupleReference nodeGroupTuple = createTuple(id.getNodeGroupName(), dataset.getDataverseName(),
                        dataset.getDatasetName());
                insertTupleIntoIndex(jobId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, nodeGroupTuple);
            }
            // Add entry in datatype secondary index.
            ITupleReference dataTypeTuple = createTuple(dataset.getDataverseName(), dataset.getItemTypeName(),
                    dataset.getDatasetName());
            insertTupleIntoIndex(jobId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeTuple);
        } catch (BTreeDuplicateKeyException e) {
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
        } catch (BTreeDuplicateKeyException e) {
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
        } catch (BTreeDuplicateKeyException e) {
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
        } catch (BTreeDuplicateKeyException e) {
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
        } catch (BTreeDuplicateKeyException e) {
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

        } catch (BTreeDuplicateKeyException e) {
            throw new MetadataException("A dataset with this name " + function.getFunctionName() + " and arity "
                    + function.getFunctionArity() + " already exists in dataverse '" + function.getDataverseName()
                    + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public void insertIntoDatatypeSecondaryIndex(JobId jobId, String dataverseName, String nestedTypeName,
            String topTypeName) throws Exception {
        ITupleReference tuple = createTuple(dataverseName, nestedTypeName, topTypeName);
        insertTupleIntoIndex(jobId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, tuple);
    }

    private void insertTupleIntoIndex(JobId jobId, IMetadataIndex index, ITupleReference tuple) throws Exception {
        long resourceID = index.getResourceID();
        IIndex indexInstance = indexLifecycleManager.getIndex(resourceID);
        indexLifecycleManager.open(resourceID);
        IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(jobId);
        transactionProvider.getLockManager().lock(index.getDatasetId(), -1, LockMode.X, txnCtx);
        // TODO: fix exceptions once new BTree exception model is in hyracks.
        indexAccessor.insert(tuple);
        index.getTreeLogger().generateLogRecord(transactionProvider, txnCtx, IndexOperation.INSERT, tuple);
        indexLifecycleManager.close(resourceID);
    }

    @Override
    public void dropDataverse(JobId jobId, String dataverseName) throws MetadataException, RemoteException {
        try {
            List<Dataset> dataverseDatasets;
            // As a side effect, acquires an S lock on the 'dataset' dataset
            // on behalf of txnId.
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
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            // Delete entry from secondary index 'group'.
            if (dataset.getDatasetType() == DatasetType.INTERNAL || dataset.getDatasetType() == DatasetType.FEED) {
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                ITupleReference groupNameSearchKey = createTuple(id.getNodeGroupName(), dataverseName, datasetName);
                // Searches the index for the tuple to be deleted. Acquires an S
                // lock on the GROUPNAME_ON_DATASET_INDEX index.
                ITupleReference groupNameTuple = getTupleToBeDeleted(jobId,
                        MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, groupNameSearchKey);
                deleteTupleFromIndex(jobId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, groupNameTuple);
            }
            // Delete entry from secondary index 'type'.
            ITupleReference dataTypeSearchKey = createTuple(dataverseName, dataset.getItemTypeName(), datasetName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the DATATYPENAME_ON_DATASET_INDEX index.
            ITupleReference dataTypeTuple = getTupleToBeDeleted(jobId,
                    MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeSearchKey);
            deleteTupleFromIndex(jobId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeTuple);
            // Delete entry(s) from the 'indexes' dataset.
            if (dataset.getDatasetType() == DatasetType.INTERNAL || dataset.getDatasetType() == DatasetType.FEED) {
                List<Index> datasetIndexes = getDatasetIndexes(jobId, dataverseName, datasetName);
                for (Index index : datasetIndexes) {
                    dropIndex(jobId, dataverseName, datasetName, index.getIndexName());
                }
            }
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop dataset '" + datasetName + "' because it doesn't exist.", e);
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
            // This call uses the secondary index on datatype. Get nested types before deleting entry from secondary index.
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

    private void deleteTupleFromIndex(JobId jobId, IMetadataIndex index, ITupleReference tuple) throws Exception {
        long resourceID = index.getResourceID();
        IIndex indexInstance = indexLifecycleManager.getIndex(resourceID);
        indexLifecycleManager.open(resourceID);
        IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(jobId);
        // This lock is actually an upgrade, because a deletion must be preceded
        // by a search, in order to be able to undo an aborted deletion.
        // The transaction with txnId will have an S lock on the
        // resource. Note that lock converters have a higher priority than
        // regular waiters in the LockManager.
        transactionProvider.getLockManager().lock(index.getDatasetId(), -1, LockMode.X, txnCtx);
        indexAccessor.delete(tuple);
        index.getTreeLogger().generateLogRecord(transactionProvider, txnCtx, IndexOperation.DELETE, tuple);
        indexLifecycleManager.close(resourceID);
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
            ITupleReference searchKey = createTuple(dataverseName, dataverseName);
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
    public Function getFunction(JobId jobId, String dataverseName, String functionName, int arity)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, functionName, "" + arity);
            FunctionTupleTranslator tupleReaderWriter = new FunctionTupleTranslator(false);
            List<Function> results = new ArrayList<Function>();
            IValueExtractor<Function> valueExtractor = new MetadataEntityValueExtractor<Function>(tupleReaderWriter);
            searchIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropFunction(JobId jobId, String dataverseName, String functionName, int arity)
            throws MetadataException, RemoteException {
        Function function;
        try {
            function = getFunction(jobId, dataverseName, functionName, arity);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        if (function == null) {
            throw new MetadataException("Cannot drop function '" + functionName + " and arity " + arity
                    + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'function' dataset.
            ITupleReference searchKey = createTuple(dataverseName, functionName, "" + arity);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'function' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET,
                    searchKey);
            deleteTupleFromIndex(jobId, MetadataPrimaryIndexes.FUNCTION_DATASET, datasetTuple);

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop function '" + functionName + " and arity " + arity
                    + "' because it doesn't exist.", e);
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

    private <ResultType> void searchIndex(JobId jobId, IMetadataIndex index, ITupleReference searchKey,
            IValueExtractor<ResultType> valueExtractor, List<ResultType> results) throws Exception {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(jobId);
        transactionProvider.getLockManager().lock(index.getDatasetId(), -1, LockMode.S, txnCtx);
        IBinaryComparatorFactory[] comparatorFactories = index.getKeyBinaryComparatorFactory();
        long resourceID = index.getResourceID();
        IIndex indexInstance = indexLifecycleManager.getIndex(resourceID);
        indexLifecycleManager.open(resourceID);
        IIndexAccessor indexAccessor = indexInstance.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        ITreeIndexCursor rangeCursor = new LSMBTreeRangeSearchCursor();
        IBinaryComparator[] searchCmps = new IBinaryComparator[searchKey.getFieldCount()];
        for (int i = 0; i < searchKey.getFieldCount(); i++) {
            searchCmps[i] = comparatorFactories[i].createBinaryComparator();
        }
        MultiComparator searchCmp = new MultiComparator(searchCmps);
        RangePredicate rangePred = new RangePredicate(searchKey, searchKey, true, true, searchCmp, searchCmp);
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
        IIndex indexInstance = indexLifecycleManager.getIndex(resourceID);
        try {
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
}
