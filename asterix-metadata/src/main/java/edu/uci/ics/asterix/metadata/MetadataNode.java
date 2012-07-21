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
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class MetadataNode implements IMetadataNode {
    private static final long serialVersionUID = 1L;

    // TODO: Temporary transactional resource id for metadata.
    private static final byte[] metadataResourceId = MetadataNode.class.toString().getBytes();

    private IndexRegistry<IIndex> indexRegistry;
    private TransactionProvider transactionProvider;

    public static final MetadataNode INSTANCE = new MetadataNode();

    private MetadataNode() {
        super();
    }

    public void initialize(AsterixAppRuntimeContext runtimeContext) {
        this.transactionProvider = runtimeContext.getTransactionProvider();
        this.indexRegistry = runtimeContext.getIndexRegistry();
    }

    @Override
    public void beginTransaction(long transactionId) throws ACIDException, RemoteException {
        transactionProvider.getTransactionManager().beginTransaction(transactionId);
    }

    @Override
    public void commitTransaction(long txnId) throws RemoteException, ACIDException {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        transactionProvider.getTransactionManager().commitTransaction(txnCtx);
    }

    @Override
    public void abortTransaction(long txnId) throws RemoteException, ACIDException {
        try {
            TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
            transactionProvider.getTransactionManager().abortTransaction(txnCtx);
        } catch (ACIDException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public boolean lock(long txnId, int lockMode) throws ACIDException, RemoteException {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        return transactionProvider.getLockManager().lock(txnCtx, metadataResourceId, lockMode);
    }

    @Override
    public boolean unlock(long txnId) throws ACIDException, RemoteException {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        return transactionProvider.getLockManager().unlock(txnCtx, metadataResourceId);
    }

    @Override
    public void addDataverse(long txnId, Dataverse dataverse) throws MetadataException, RemoteException {
        try {
            DataverseTupleTranslator tupleReaderWriter = new DataverseTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(dataverse);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);
        } catch (BTreeDuplicateKeyException e) {
            throw new MetadataException("A dataverse with this name " + dataverse.getDataverseName()
                    + " already exists.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addDataset(long txnId, Dataset dataset) throws MetadataException, RemoteException {
        try {
            // Insert into the 'dataset' dataset.
            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(true);
            ITupleReference datasetTuple = tupleReaderWriter.getTupleFromMetadataEntity(dataset);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            if (dataset.getDatasetType() == DatasetType.INTERNAL || dataset.getDatasetType() == DatasetType.FEED) {
                // Add the primary index for the dataset.
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                Index primaryIndex = new Index(dataset.getDataverseName(), dataset.getDatasetName(),
                        dataset.getDatasetName(), IndexType.BTREE, id.getPrimaryKey(), true);
                addIndex(txnId, primaryIndex);
                ITupleReference nodeGroupTuple = createTuple(id.getNodeGroupName(), dataset.getDataverseName(),
                        dataset.getDatasetName());
                insertTupleIntoIndex(txnId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, nodeGroupTuple);
            }
            // Add entry in datatype secondary index.
            ITupleReference dataTypeTuple = createTuple(dataset.getDataverseName(), dataset.getItemTypeName(),
                    dataset.getDatasetName());
            insertTupleIntoIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeTuple);
        } catch (BTreeDuplicateKeyException e) {
            throw new MetadataException("A dataset with this name " + dataset.getDatasetName()
                    + " already exists in dataverse '" + dataset.getDataverseName() + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addIndex(long txnId, Index index) throws MetadataException, RemoteException {
        try {
            IndexTupleTranslator tupleWriter = new IndexTupleTranslator(true);
            ITupleReference tuple = tupleWriter.getTupleFromMetadataEntity(index);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, tuple);
        } catch (BTreeDuplicateKeyException e) {
            throw new MetadataException("An index with name '" + index.getIndexName() + "' already exists.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addNode(long txnId, Node node) throws MetadataException, RemoteException {
        try {
            NodeTupleTranslator tupleReaderWriter = new NodeTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(node);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.NODE_DATASET, tuple);
        } catch (BTreeDuplicateKeyException e) {
            throw new MetadataException("A node with name '" + node.getNodeName() + "' already exists.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addNodeGroup(long txnId, NodeGroup nodeGroup) throws MetadataException, RemoteException {
        try {
            NodeGroupTupleTranslator tupleReaderWriter = new NodeGroupTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(nodeGroup);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, tuple);
        } catch (BTreeDuplicateKeyException e) {
            throw new MetadataException("A nodegroup with name '" + nodeGroup.getNodeGroupName() + "' already exists.",
                    e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addDatatype(long txnId, Datatype datatype) throws MetadataException, RemoteException {
        try {
            DatatypeTupleTranslator tupleReaderWriter = new DatatypeTupleTranslator(txnId, this, true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(datatype);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
        } catch (BTreeDuplicateKeyException e) {
            throw new MetadataException("A datatype with name '" + datatype.getDatatypeName() + "' already exists.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addFunction(long txnId, Function function) throws MetadataException, RemoteException {
        try {
            // Insert into the 'function' dataset.
            FunctionTupleTranslator tupleReaderWriter = new FunctionTupleTranslator(true);
            ITupleReference functionTuple = tupleReaderWriter.getTupleFromMetadataEntity(function);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET, functionTuple);

        } catch (BTreeDuplicateKeyException e) {
            throw new MetadataException("A dataset with this name " + function.getFunctionName() + " and arity "
                    + function.getFunctionArity() + " already exists in dataverse '" + function.getDataverseName()
                    + "'.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public void insertIntoDatatypeSecondaryIndex(long txnId, String dataverseName, String nestedTypeName,
            String topTypeName) throws Exception {
        ITupleReference tuple = createTuple(dataverseName, nestedTypeName, topTypeName);
        insertTupleIntoIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, tuple);
    }

    private void insertTupleIntoIndex(long txnId, IMetadataIndex index, ITupleReference tuple) throws Exception {
        int fileId = index.getFileId();
        BTree btree = (BTree) indexRegistry.get(fileId);
        btree.open(fileId);
        ITreeIndexAccessor indexAccessor = btree.createAccessor();
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        transactionProvider.getLockManager().lock(txnCtx, index.getResourceId(), LockMode.EXCLUSIVE);
        // TODO: fix exceptions once new BTree exception model is in hyracks.
        indexAccessor.insert(tuple);
        index.getTreeLogger().generateLogRecord(transactionProvider, txnCtx, IndexOp.INSERT, tuple);
    }

    @Override
    public void dropDataverse(long txnId, String dataverseName) throws MetadataException, RemoteException {
        try {
            List<Dataset> dataverseDatasets;
            // As a side effect, acquires an S lock on the 'dataset' dataset
            // on behalf of txnId.
            dataverseDatasets = getDataverseDatasets(txnId, dataverseName);
            if (dataverseDatasets != null && dataverseDatasets.size() > 0) {
                // Drop all datasets in this dataverse.
                for (int i = 0; i < dataverseDatasets.size(); i++) {
                    dropDataset(txnId, dataverseName, dataverseDatasets.get(i).getDatasetName());
                }
            }
            List<Datatype> dataverseDatatypes;
            // As a side effect, acquires an S lock on the 'datatype' dataset
            // on behalf of txnId.
            dataverseDatatypes = getDataverseDatatypes(txnId, dataverseName);
            if (dataverseDatatypes != null && dataverseDatatypes.size() > 0) {
                // Drop all types in this dataverse.
                for (int i = 0; i < dataverseDatatypes.size(); i++) {
                    forceDropDatatype(txnId, dataverseName, dataverseDatatypes.get(i).getDatatypeName());
                }
            }
            // Delete the dataverse entry from the 'dataverse' dataset.
            ITupleReference searchKey = createTuple(dataverseName);
            // As a side effect, acquires an S lock on the 'dataverse' dataset
            // on behalf of txnId.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop dataverse '" + dataverseName + "' because it doesn't exist.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropDataset(long txnId, String dataverseName, String datasetName) throws MetadataException,
            RemoteException {
        Dataset dataset;
        try {
            dataset = getDataset(txnId, dataverseName, datasetName);
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
            ITupleReference datasetTuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            // Delete entry from secondary index 'group'.
            if (dataset.getDatasetType() == DatasetType.INTERNAL || dataset.getDatasetType() == DatasetType.FEED) {
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                ITupleReference groupNameSearchKey = createTuple(id.getNodeGroupName(), dataverseName, datasetName);
                // Searches the index for the tuple to be deleted. Acquires an S
                // lock on the GROUPNAME_ON_DATASET_INDEX index.
                ITupleReference groupNameTuple = getTupleToBeDeleted(txnId,
                        MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, groupNameSearchKey);
                deleteTupleFromIndex(txnId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, groupNameTuple);
            }
            // Delete entry from secondary index 'type'.
            ITupleReference dataTypeSearchKey = createTuple(dataverseName, dataset.getItemTypeName(), datasetName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the DATATYPENAME_ON_DATASET_INDEX index.
            ITupleReference dataTypeTuple = getTupleToBeDeleted(txnId,
                    MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeSearchKey);
            deleteTupleFromIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeTuple);
            // Delete entry(s) from the 'indexes' dataset.
            if (dataset.getDatasetType() == DatasetType.INTERNAL || dataset.getDatasetType() == DatasetType.FEED) {
                List<Index> datasetIndexes = getDatasetIndexes(txnId, dataverseName, datasetName);
                for (Index index : datasetIndexes) {
                    dropIndex(txnId, dataverseName, datasetName, index.getIndexName());
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
    public void dropIndex(long txnId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName, indexName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'index' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, tuple);
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
    public void dropNodegroup(long txnId, String nodeGroupName) throws MetadataException, RemoteException {
        List<String> datasetNames;
        try {
            datasetNames = getDatasetNamesPartitionedOnThisNodeGroup(txnId, nodeGroupName);
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
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, tuple);
            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop nodegroup '" + nodeGroupName + "' because it doesn't exist", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropDatatype(long txnId, String dataverseName, String datatypeName) throws MetadataException,
            RemoteException {
        List<String> datasetNames;
        List<String> usedDatatypes;
        try {
            datasetNames = getDatasetNamesDeclaredByThisDatatype(txnId, dataverseName, datatypeName);
            usedDatatypes = getDatatypeNamesUsingThisDatatype(txnId, dataverseName, datatypeName);
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
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
            deleteFromDatatypeSecondaryIndex(txnId, dataverseName, datatypeName);
            List<String> nestedTypes = getNestedDatatypeNames(txnId, dataverseName, datatypeName);
            for (String nestedType : nestedTypes) {
                Datatype dt = getDatatype(txnId, dataverseName, nestedType);
                if (dt != null && dt.getIsAnonymous()) {
                    dropDatatype(txnId, dataverseName, dt.getDatatypeName());
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

    private void forceDropDatatype(long txnId, String dataverseName, String datatypeName) throws AsterixException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
            deleteFromDatatypeSecondaryIndex(txnId, dataverseName, datatypeName);
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

    private void deleteFromDatatypeSecondaryIndex(long txnId, String dataverseName, String datatypeName)
            throws AsterixException {
        try {
            List<String> nestedTypes = getNestedDatatypeNames(txnId, dataverseName, datatypeName);
            for (String nestedType : nestedTypes) {
                ITupleReference searchKey = createTuple(dataverseName, nestedType, datatypeName);
                // Searches the index for the tuple to be deleted. Acquires an S
                // lock on the DATATYPENAME_ON_DATATYPE_INDEX index.
                ITupleReference tuple = getTupleToBeDeleted(txnId,
                        MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, searchKey);
                deleteTupleFromIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, tuple);
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

    private void deleteTupleFromIndex(long txnId, IMetadataIndex index, ITupleReference tuple) throws Exception {
        int fileId = index.getFileId();
        BTree btree = (BTree) indexRegistry.get(fileId);
        btree.open(fileId);

        ITreeIndexAccessor indexAccessor = btree.createAccessor();
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        // This lock is actually an upgrade, because a deletion must be preceded
        // by a search, in order to be able to undo an aborted deletion.
        // The transaction with txnId will have an S lock on the
        // resource. Note that lock converters have a higher priority than
        // regular waiters in the LockManager.
        transactionProvider.getLockManager().lock(txnCtx, index.getResourceId(), LockMode.EXCLUSIVE);
        indexAccessor.delete(tuple);
        index.getTreeLogger().generateLogRecord(transactionProvider, txnCtx, IndexOp.DELETE, tuple);
    }

    @Override
    public Dataverse getDataverse(long txnId, String dataverseName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DataverseTupleTranslator tupleReaderWriter = new DataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<Dataverse>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<Dataverse>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public List<Dataset> getDataverseDatasets(long txnId, String dataverseName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<Dataset>(tupleReaderWriter);
            List<Dataset> results = new ArrayList<Dataset>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private List<Datatype> getDataverseDatatypes(long txnId, String dataverseName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatatypeTupleTranslator tupleReaderWriter = new DatatypeTupleTranslator(txnId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<Datatype>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<Datatype>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Dataset getDataset(long txnId, String dataverseName, String datasetName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(false);
            List<Dataset> results = new ArrayList<Dataset>();
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<Dataset>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private List<String> getDatasetNamesDeclaredByThisDatatype(long txnId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, dataverseName);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new DatasetNameValueExtractor();
            searchIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, searchKey, valueExtractor,
                    results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public List<String> getDatatypeNamesUsingThisDatatype(long txnId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new DatatypeNameValueExtractor(dataverseName, this);
            searchIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, searchKey, valueExtractor,
                    results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private List<String> getNestedDatatypeNames(long txnId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new NestedDatatypeNameValueExtractor(datatypeName);
            searchIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, searchKey, valueExtractor,
                    results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public List<String> getDatasetNamesPartitionedOnThisNodeGroup(long txnId, String nodegroup)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(nodegroup);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new DatasetNameValueExtractor();
            searchIndex(txnId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Index getIndex(long txnId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName, indexName);
            IndexTupleTranslator tupleReaderWriter = new IndexTupleTranslator(false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<Index>(tupleReaderWriter);
            List<Index> results = new ArrayList<Index>();
            searchIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<Index> getDatasetIndexes(long txnId, String dataverseName, String datasetName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            IndexTupleTranslator tupleReaderWriter = new IndexTupleTranslator(false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<Index>(tupleReaderWriter);
            List<Index> results = new ArrayList<Index>();
            searchIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Datatype getDatatype(long txnId, String dataverseName, String datatypeName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            DatatypeTupleTranslator tupleReaderWriter = new DatatypeTupleTranslator(txnId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<Datatype>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<Datatype>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public NodeGroup getNodeGroup(long txnId, String nodeGroupName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            NodeGroupTupleTranslator tupleReaderWriter = new NodeGroupTupleTranslator(false);
            IValueExtractor<NodeGroup> valueExtractor = new MetadataEntityValueExtractor<NodeGroup>(tupleReaderWriter);
            List<NodeGroup> results = new ArrayList<NodeGroup>();
            searchIndex(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Function getFunction(long txnId, String dataverseName, String functionName, int arity)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, functionName, "" + arity);
            FunctionTupleTranslator tupleReaderWriter = new FunctionTupleTranslator(false);
            List<Function> results = new ArrayList<Function>();
            IValueExtractor<Function> valueExtractor = new MetadataEntityValueExtractor<Function>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropFunction(long txnId, String dataverseName, String functionName, int arity)
            throws MetadataException, RemoteException {
        Function function;
        try {
            function = getFunction(txnId, dataverseName, functionName, arity);
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
            ITupleReference datasetTuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET,
                    searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET, datasetTuple);

            // TODO: Change this to be a BTree specific exception, e.g.,
            // BTreeKeyDoesNotExistException.
        } catch (TreeIndexException e) {
            throw new MetadataException("Cannot drop function '" + functionName + " and arity " + arity
                    + "' because it doesn't exist.", e);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private ITupleReference getTupleToBeDeleted(long txnId, IMetadataIndex metadataIndex, ITupleReference searchKey)
            throws Exception {
        IValueExtractor<ITupleReference> valueExtractor = new TupleCopyValueExtractor(metadataIndex.getTypeTraits());
        List<ITupleReference> results = new ArrayList<ITupleReference>();
        searchIndex(txnId, metadataIndex, searchKey, valueExtractor, results);
        if (results.isEmpty()) {
            // TODO: Temporarily a TreeIndexException to make it get caught by
            // caller in the appropriate catch block.
            throw new TreeIndexException("Could not find entry to be deleted.");
        }
        // There should be exactly one result returned from the search.
        return results.get(0);
    }

    private <ResultType> void searchIndex(long txnId, IMetadataIndex index, ITupleReference searchKey,
            IValueExtractor<ResultType> valueExtractor, List<ResultType> results) throws Exception {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        transactionProvider.getLockManager().lock(txnCtx, index.getResourceId(), LockMode.SHARED);
        IBinaryComparatorFactory[] comparatorFactories = index.getKeyBinaryComparatorFactory();
        int fileId = index.getFileId();
        BTree btree = (BTree) indexRegistry.get(fileId);
        btree.open(fileId);
        ITreeIndexFrame leafFrame = btree.getLeafFrameFactory().createFrame();
        ITreeIndexAccessor indexAccessor = btree.createAccessor();
        ITreeIndexCursor rangeCursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) leafFrame, false);
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
                ResultType result = valueExtractor.getValue(txnId, rangeCursor.getTuple());
                if (result != null) {
                    results.add(result);
                }
            }
        } finally {
            rangeCursor.close();
        }
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
