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

package edu.uci.ics.asterix.metadata.api;

import java.rmi.RemoteException;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.CompactionPolicy;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.Node;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;

/**
 * A metadata manager provides user access to Asterix metadata (e.g., types,
 * datasets, indexes, etc.). A metadata manager satisfies requests by contacting
 * the metadata node which is responsible for the storage-level details. This
 * interface describes the operations that a metadata manager must support.
 * Every operation is meant to be performed in the context of a transaction on
 * the metadata node against the metadata. It is the responsibility of the user
 * to begin a transaction, lock the metadata, and commit or abort a metadata
 * transaction using the appropriate methods declared below. Lock acquisition at
 * finer levels is the responsibility of the metadata node, not the metadata
 * manager or its user.
 */
public interface IMetadataManager {

    /**
     * Initializes the metadata manager, e.g., finds the remote metadata node.
     * 
     * @throws RemoteException
     *             If an error occurred while contacting the proxy for finding
     *             the metadata node.
     */
    public void init() throws RemoteException, MetadataException;

    /**
     * Begins a transaction on the metadata node.
     * 
     * @return A globally unique transaction id.
     * @throws ACIDException
     * @throws RemoteException
     */
    public MetadataTransactionContext beginTransaction() throws ACIDException, RemoteException;

    /**
     * Commits a remote transaction on the metadata node.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @throws ACIDException
     * @throws RemoteException
     */
    public void commitTransaction(MetadataTransactionContext ctx) throws ACIDException, RemoteException;

    /**
     * Aborts a remote transaction running on the metadata node.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @throws ACIDException
     * @throws RemoteException
     */
    public void abortTransaction(MetadataTransactionContext ctx) throws ACIDException, RemoteException;

    /**
     * Locks the metadata in given mode. The lock acquisition is delegated to
     * the metadata node. This method blocks until the lock can be acquired.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param lockMode
     *            Desired lockode.
     * @throws ACIDException
     * @throws RemoteException
     */
    public void lock(MetadataTransactionContext ctx, byte lockMode) throws ACIDException, RemoteException;

    /**
     * Releases all locks on the metadata held by the given transaction id.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @throws ACIDException
     * @throws RemoteException
     */
    public void unlock(MetadataTransactionContext ctx) throws ACIDException, RemoteException;

    /**
     * Inserts a new dataverse into the metadata.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverse
     *            Dataverse instance to be inserted.
     * @throws MetadataException
     *             For example, if the dataverse already exists.
     */
    public void addDataverse(MetadataTransactionContext ctx, Dataverse dataverse) throws MetadataException;

    /**
     * Retrieves all dataverses
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @return A list of dataverse instances.
     * @throws MetadataException
     */
    List<Dataverse> getDataverses(MetadataTransactionContext ctx) throws MetadataException;

    /**
     * Retrieves a dataverse with given name.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse to retrieve.
     * @return A dataverse instance.
     * @throws MetadataException
     *             For example, if the dataverse does not exist.
     */
    public Dataverse getDataverse(MetadataTransactionContext ctx, String dataverseName) throws MetadataException;

    /**
     * Retrieves all datasets belonging to the given dataverse.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse of which to find all datasets.
     * @return A list of dataset instances.
     * @throws MetadataException
     *             For example, if the dataverse does not exist.
     */
    public List<Dataset> getDataverseDatasets(MetadataTransactionContext ctx, String dataverseName)
            throws MetadataException;

    /**
     * Deletes the dataverse with given name, and all it's associated datasets,
     * indexes, and types.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @return A list of dataset instances.
     * @throws MetadataException
     *             For example, if the dataverse does not exist.
     */
    public void dropDataverse(MetadataTransactionContext ctx, String dataverseName) throws MetadataException;

    /**
     * Inserts a new dataset into the metadata.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataset
     *            Dataset instance to be inserted.
     * @throws MetadataException
     *             For example, if the dataset already exists.
     */
    public void addDataset(MetadataTransactionContext ctx, Dataset dataset) throws MetadataException;

    /**
     * Retrieves a dataset within a given dataverse.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Dataverse name to look for the dataset.
     * @param datasetName
     *            Name of dataset to be retrieved.
     * @return A dataset instance.
     * @throws MetadataException
     *             For example, if the dataset does not exist.
     */
    public Dataset getDataset(MetadataTransactionContext ctx, String dataverseName, String datasetName)
            throws MetadataException;

    /**
     * Retrieves all indexes of a dataset.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse which holds the given dataset.
     * @param datasetName
     *            Name of dataset for which to retrieve all indexes.
     * @return A list of Index instances.
     * @throws MetadataException
     *             For example, if the dataset and/or dataverse does not exist.
     */
    public List<Index> getDatasetIndexes(MetadataTransactionContext ctx, String dataverseName, String datasetName)
            throws MetadataException;

    /**
     * Deletes the dataset with given name, and all it's associated indexes.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse which holds the given dataset.
     * @param datasetName
     *            Name of dataset to delete.
     * @throws MetadataException
     *             For example, if the dataset and/or dataverse does not exist.
     */
    public void dropDataset(MetadataTransactionContext ctx, String dataverseName, String datasetName)
            throws MetadataException;

    /**
     * Inserts an index into the metadata. The index itself knows its name, and
     * which dataset it belongs to.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param index
     *            Index instance to be inserted.
     * @throws MetadataException
     *             For example, if the index already exists.
     */
    public void addIndex(MetadataTransactionContext ctx, Index index) throws MetadataException;

    /**
     * Retrieves the index with given name, in given dataverse and dataset.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of the datavers holding the given dataset.
     * @param datasetName
     *            Name of the dataset holding the index.
     * @indexName Name of the index to retrieve.
     * @return An Index instance.
     * @throws MetadataException
     *             For example, if the index does not exist.
     */
    public Index getIndex(MetadataTransactionContext ctx, String dataverseName, String datasetName, String indexName)
            throws MetadataException;

    /**
     * Deletes the index with given name, in given dataverse and dataset.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of the datavers holding the given dataset.
     * @param datasetName
     *            Name of the dataset holding the index.
     * @indexName Name of the index to retrieve.
     * @throws MetadataException
     *             For example, if the index does not exist.
     */
    public void dropIndex(MetadataTransactionContext ctx, String dataverseName, String datasetName, String indexName)
            throws MetadataException;

    /**
     * Inserts a datatype.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param datatype
     *            Datatype instance to be inserted.
     * @throws MetadataException
     *             For example, if the datatype already exists.
     */
    public void addDatatype(MetadataTransactionContext ctx, Datatype datatype) throws MetadataException;

    /**
     * Retrieves the datatype with given name in given dataverse.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse holding the datatype.
     * @param datatypeName
     *            Name of datatype to be retrieved.
     * @return A datatype instance.
     * @throws MetadataException
     *             For example, if the datatype does not exist.
     */
    public Datatype getDatatype(MetadataTransactionContext ctx, String dataverseName, String datatypeName)
            throws MetadataException;

    /**
     * Deletes the given datatype in given dataverse.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse holding the datatype.
     * @param datatypeName
     *            Name of datatype to be deleted.
     * @throws MetadataException
     *             For example, if there are still datasets using the type to be
     *             deleted.
     */
    public void dropDatatype(MetadataTransactionContext ctx, String dataverseName, String datatypeName)
            throws MetadataException;

    /**
     * Inserts a node group.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param nodeGroup
     *            Node group instance to insert.
     * @throws MetadataException
     *             For example, if the node group already exists.
     */
    public void addNodegroup(MetadataTransactionContext ctx, NodeGroup nodeGroup) throws MetadataException;

    /**
     * Retrieves a node group.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param nodeGroupName
     *            Name of node group to be retrieved.
     * @throws MetadataException
     *             For example, if the node group does not exist.
     */
    public NodeGroup getNodegroup(MetadataTransactionContext ctx, String nodeGroupName) throws MetadataException;

    /**
     * Deletes a node group.
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param nodeGroupName
     *            Name of node group to be deleted.
     * @throws MetadataException
     *             For example, there are still datasets partitioned on the node
     *             group to be deleted.
     */
    public void dropNodegroup(MetadataTransactionContext ctx, String nodeGroupName) throws MetadataException;

    /**
     * Inserts a node (machine).
     * 
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param node
     *            Node instance to be inserted.
     * @throws MetadataException
     *             For example, if the node already exists.
     */
    public void addNode(MetadataTransactionContext ctx, Node node) throws MetadataException;

    /**
     * @param mdTxnCtx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param function
     *            An instance of type Function that represents the function
     *            being added
     * @throws MetadataException
     */
    public void addFunction(MetadataTransactionContext mdTxnCtx, Function function) throws MetadataException;

    /**
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param functionSignature
     *            the functions signature (unique to the function)
     * @return
     * @throws MetadataException
     */

    public Function getFunction(MetadataTransactionContext ctx, FunctionSignature functionSignature)
            throws MetadataException;

    /**
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param functionSignature
     *            the functions signature (unique to the function)
     * @throws MetadataException
     */
    public void dropFunction(MetadataTransactionContext ctx, FunctionSignature functionSignature)
            throws MetadataException;

    /**
     * @param mdTxnCtx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param function
     *            An instance of type Adapter that represents the adapter being
     *            added
     * @throws MetadataException
     */
    public void addAdapter(MetadataTransactionContext mdTxnCtx, DatasourceAdapter adapter) throws MetadataException;

    /**
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            the dataverse associated with the adapter being searched
     * @param Name
     *            name of the adapter
     * @return
     * @throws MetadataException
     */
    public DatasourceAdapter getAdapter(MetadataTransactionContext ctx, String dataverseName, String name)
            throws MetadataException;

    /**
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            the dataverse associated with the adapter being dropped
     * @param name
     *            name of the adapter
     * @throws MetadataException
     */
    public void dropAdapter(MetadataTransactionContext ctx, String dataverseName, String name) throws MetadataException;

    /**
     * @param ctx
     * @param policy
     * @throws MetadataException
     */
    public void addCompactionPolicy(MetadataTransactionContext ctx, CompactionPolicy policy) throws MetadataException;

    /**
     * @param ctx
     * @param dataverse
     * @param policyName
     * @return
     * @throws MetadataException
     */
    public CompactionPolicy getCompactionPolicy(MetadataTransactionContext ctx, String dataverse, String policyName)
            throws MetadataException;

    /**
     * @param ctx
     * @param dataverseName
     * @return
     * @throws MetadataException
     */
    public List<Function> getDataverseFunctions(MetadataTransactionContext ctx, String dataverseName)
            throws MetadataException;

    public void initializeDatasetIdFactory(MetadataTransactionContext ctx) throws MetadataException;

    public int getMostRecentDatasetId() throws MetadataException;

    public void acquireWriteLatch();

    public void releaseWriteLatch();

    public void acquireReadLatch();

    public void releaseReadLatch();

}
