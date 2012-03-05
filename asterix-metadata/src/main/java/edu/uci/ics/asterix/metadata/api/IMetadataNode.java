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

package edu.uci.ics.asterix.metadata.api;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.Node;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;

/**
 * A metadata node stores metadata in its local storage structures (currently
 * BTrees). A metadata node services requests on behalf of the (possibly remote)
 * metadata manager by executing local transactions against its local storage.
 * This interface mirrors the methods in IMetadataManager.
 * Users wanting to lock/access metadata shall always go through the
 * MetadataManager, and should never call methods on the MetadataNode directly
 * for any reason.
 */
public interface IMetadataNode extends Remote, Serializable {

    /**
     * Begins a local transaction against the metadata.
     * 
     * @throws ACIDException
     * @throws RemoteException
     */
    public void beginTransaction(long txnId) throws ACIDException, RemoteException;

    /**
     * Commits a local transaction against the metadata.
     * 
     * @throws ACIDException
     * @throws RemoteException
     */
    public void commitTransaction(long txnId) throws ACIDException, RemoteException;

    /**
     * Aborts a local transaction against the metadata.
     * 
     * @throws ACIDException
     * @throws RemoteException
     */
    public void abortTransaction(long txnId) throws ACIDException, RemoteException;

    /**
     * Locally locks the entire metadata in given mode on behalf of given
     * transaction id.
     * 
     * @throws ACIDException
     * @throws RemoteException
     */
    public boolean lock(long txnId, int lockMode) throws ACIDException, RemoteException;

    /**
     * Releases all local locks of given transaction id.
     * 
     * @throws ACIDException
     * @throws RemoteException
     */
    public boolean unlock(long txnId) throws ACIDException, RemoteException;

    /**
     * Inserts a new dataverse into the metadata, acquiring local locks on
     * behalf of the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverse
     *            Dataverse instance to be inserted.
     * @throws MetadataException
     *             For example, if the dataverse already exists.
     * @throws RemoteException
     */
    public void addDataverse(long txnId, Dataverse dataverse) throws MetadataException, RemoteException;

    /**
     * Retrieves a dataverse with given name, acquiring local locks on behalf of
     * the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse to retrieve.
     * @return A dataverse instance.
     * @throws MetadataException
     *             For example, if the dataverse does not exist.
     * @throws RemoteException
     */
    public Dataverse getDataverse(long txnId, String dataverseName) throws MetadataException, RemoteException;

    /**
     * Retrieves all datasets belonging to the given dataverse, acquiring local
     * locks on behalf of the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse of which to find all datasets.
     * @return A list of dataset instances.
     * @throws MetadataException
     *             For example, if the dataverse does not exist. RemoteException
     */
    public List<Dataset> getDataverseDatasets(long txnId, String dataverseName) throws MetadataException,
            RemoteException;

    /**
     * Deletes the dataverse with given name, and all it's associated datasets,
     * indexes, and types, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @return A list of dataset instances.
     * @throws MetadataException
     *             For example, if the dataverse does not exist.
     * @throws RemoteException
     */
    public void dropDataverse(long txnId, String dataverseName) throws MetadataException, RemoteException;

    /**
     * Inserts a new dataset into the metadata, acquiring local locks on behalf
     * of the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataset
     *            Dataset instance to be inserted.
     * @throws MetadataException
     *             For example, if the dataset already exists.
     * @throws RemoteException
     */
    public void addDataset(long txnId, Dataset dataset) throws MetadataException, RemoteException;

    /**
     * Retrieves a dataset within a given dataverse, acquiring local locks on
     * behalf of the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Dataverse name to look for the dataset.
     * @param datasetName
     *            Name of dataset to be retrieved.
     * @return A dataset instance.
     * @throws MetadataException
     *             For example, if the dataset does not exist.
     * @throws RemoteException
     */
    public Dataset getDataset(long txnId, String dataverseName, String datasetName) throws MetadataException,
            RemoteException;

    /**
     * Retrieves all indexes of a dataset, acquiring local locks on behalf of
     * the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse which holds the given dataset.
     * @param datasetName
     *            Name of dataset for which to retrieve all indexes.
     * @return A list of Index instances.
     * @throws MetadataException
     *             For example, if the dataset and/or dataverse does not exist.
     * @throws RemoteException
     */
    public List<Index> getDatasetIndexes(long txnId, String dataverseName, String datasetName)
            throws MetadataException, RemoteException;

    /**
     * Deletes the dataset with given name, and all it's associated indexes,
     * acquiring local locks on behalf of the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse which holds the given dataset.
     * @param datasetName
     *            Name of dataset to delete.
     * @throws MetadataException
     *             For example, if the dataset and/or dataverse does not exist.
     * @throws RemoteException
     */
    public void dropDataset(long txnId, String dataverseName, String datasetName) throws MetadataException,
            RemoteException;

    /**
     * Inserts an index into the metadata, acquiring local locks on behalf of
     * the given transaction id. The index itself knows its name, and which
     * dataset it belongs to.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param index
     *            Index instance to be inserted.
     * @throws MetadataException
     *             For example, if the index already exists.
     * @throws RemoteException
     */
    public void addIndex(long txnId, Index index) throws MetadataException, RemoteException;

    /**
     * Retrieves the index with given name, in given dataverse and dataset,
     * acquiring local locks on behalf of the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the datavers holding the given dataset.
     * @param datasetName
     *            Name of the dataset holding the index.
     * @indexName Name of the index to retrieve.
     * @return An Index instance.
     * @throws MetadataException
     *             For example, if the index does not exist.
     * @throws RemoteException
     */
    public Index getIndex(long txnId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException;

    /**
     * Deletes the index with given name, in given dataverse and dataset,
     * acquiring local locks on behalf of the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the datavers holding the given dataset.
     * @param datasetName
     *            Name of the dataset holding the index.
     * @indexName Name of the index to retrieve.
     * @throws MetadataException
     *             For example, if the index does not exist.
     * @throws RemoteException
     */
    public void dropIndex(long txnId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException;

    /**
     * Inserts a datatype, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param datatype
     *            Datatype instance to be inserted.
     * @throws MetadataException
     *             For example, if the datatype already exists.
     * @throws RemoteException
     */
    public void addDatatype(long txnId, Datatype datatype) throws MetadataException, RemoteException;

    /**
     * Retrieves the datatype with given name in given dataverse, acquiring
     * local locks on behalf of the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse holding the datatype.
     * @param datatypeName
     *            Name of datatype to be retrieved.
     * @return A datatype instance.
     * @throws MetadataException
     *             For example, if the datatype does not exist.
     * @throws RemoteException
     */
    public Datatype getDatatype(long txnId, String dataverseName, String datatypeName) throws MetadataException,
            RemoteException;

    /**
     * Deletes the given datatype in given dataverse, acquiring local locks on
     * behalf of the given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse holding the datatype.
     * @param datatypeName
     *            Name of datatype to be deleted.
     * @throws MetadataException
     *             For example, if there are still datasets using the type to be
     *             deleted.
     * @throws RemoteException
     */
    public void dropDatatype(long txnId, String dataverseName, String datatypeName) throws MetadataException,
            RemoteException;

    /**
     * Inserts a node group, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param nodeGroup
     *            Node group instance to insert.
     * @throws MetadataException
     *             For example, if the node group already exists.
     * @throws RemoteException
     */
    public void addNodeGroup(long txnId, NodeGroup nodeGroup) throws MetadataException, RemoteException;

    /**
     * Retrieves a node group, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param nodeGroupName
     *            Name of node group to be retrieved.
     * @throws MetadataException
     *             For example, if the node group does not exist.
     * @throws RemoteException
     */
    public NodeGroup getNodeGroup(long txnId, String nodeGroupName) throws MetadataException, RemoteException;

    /**
     * Deletes a node group, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param nodeGroupName
     *            Name of node group to be deleted.
     * @throws MetadataException
     *             For example, there are still datasets partitioned on the node
     *             group to be deleted.
     * @throws RemoteException
     */
    public void dropNodegroup(long txnId, String nodeGroupName) throws MetadataException, RemoteException;

    /**
     * Inserts a node (compute node), acquiring local locks on behalf of the
     * given transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param node
     *            Node instance to be inserted.
     * @throws MetadataException
     *             For example, if the node already exists.
     * @throws RemoteException
     */
    public void addNode(long txnId, Node node) throws MetadataException, RemoteException;
    
    /**
	 * 
	 * @param txnId
	 *            A globally unique id for an active metadata transaction.
	 * @param dataverseName
	 *            dataverse asociated with the function that is to be deleted.
	 * @param functionName
	 *            Name of function to be deleted.
	 * @param arity
	 *            Arity of the function to be deleted
	 * @return
	 * @throws MetadataException
	 * @throws RemoteException
	 */
	public Function getFunction(long txnId, String dataverseName,
			String functionName, int arity) throws MetadataException,
			RemoteException;

	/**
	 * Deletes a function , acquiring local locks on behalf of the given
	 * transaction id.
	 * 
	 * @param txnId
	 *            A globally unique id for an active metadata transaction.
	 * @param dataverseName
	 *            dataverse asociated with the function that is to be deleted.
	 * @param functionName
	 *            Name of function to be deleted.
	 * @param arity
	 *            Arity of the function to be deleted
	 * @throws MetadataException
	 *             For example, there are still datasets partitioned on the node
	 *             group to be deleted.
	 * @throws RemoteException
	 */
	public void dropFunction(long txnId, String dataverseName,
			String functionName, int arity) throws MetadataException,
			RemoteException;

	/**
	 * 
	 * @param txnId
	 *            A globally unique id for an active metadata transaction.
	 * @param function
	 *            Function to be inserted
	 * @throws MetadataException
	 *             for example, if the function already exists or refers to an
	 *             unknown function
	 * @throws RemoteException
	 */
	public void addFunction(long txnId, Function function)
			throws MetadataException, RemoteException;
}
