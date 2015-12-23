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

package org.apache.asterix.metadata.api;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedPolicy;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;

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
    public void beginTransaction(JobId jobId) throws ACIDException, RemoteException;

    /**
     * Commits a local transaction against the metadata.
     * 
     * @throws ACIDException
     * @throws RemoteException
     */
    public void commitTransaction(JobId jobId) throws ACIDException, RemoteException;

    /**
     * Aborts a local transaction against the metadata.
     * 
     * @throws ACIDException
     * @throws RemoteException
     */
    public void abortTransaction(JobId jobId) throws ACIDException, RemoteException;

    /**
     * Locally locks the entire metadata in given mode on behalf of given
     * transaction id.
     * 
     * @throws ACIDException
     * @throws RemoteException
     */
    public void lock(JobId jobId, byte lockMode) throws ACIDException, RemoteException;

    /**
     * Releases all local locks of given transaction id.
     * 
     * @throws ACIDException
     * @throws RemoteException
     */
    public void unlock(JobId jobId, byte lockMode) throws ACIDException, RemoteException;

    /**
     * Inserts a new dataverse into the metadata, acquiring local locks on
     * behalf of the given transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataverse
     *            Dataverse instance to be inserted.
     * @throws MetadataException
     *             For example, if the dataverse already exists.
     * @throws RemoteException
     */
    public void addDataverse(JobId jobId, Dataverse dataverse) throws MetadataException, RemoteException;

    /**
     * Retrieves all dataverses, acquiring local locks on behalf of
     * the given transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @return A list of dataverse instances.
     * @throws MetadataException
     *             For example, if the dataverse does not exist.
     * @throws RemoteException
     */
    public List<Dataverse> getDataverses(JobId jobId) throws MetadataException, RemoteException;

    /**
     * Retrieves a dataverse with given name, acquiring local locks on behalf of
     * the given transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse to retrieve.
     * @return A dataverse instance.
     * @throws MetadataException
     *             For example, if the dataverse does not exist.
     * @throws RemoteException
     */
    public Dataverse getDataverse(JobId jobId, String dataverseName) throws MetadataException, RemoteException;

    /**
     * Retrieves all datasets belonging to the given dataverse, acquiring local
     * locks on behalf of the given transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse of which to find all datasets.
     * @return A list of dataset instances.
     * @throws MetadataException
     *             For example, if the dataverse does not exist. RemoteException
     */
    public List<Dataset> getDataverseDatasets(JobId jobId, String dataverseName) throws MetadataException,
            RemoteException;

    /**
     * Deletes the dataverse with given name, and all it's associated datasets,
     * indexes, and types, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @return A list of dataset instances.
     * @throws MetadataException
     *             For example, if the dataverse does not exist.
     * @throws RemoteException
     */
    public void dropDataverse(JobId jobId, String dataverseName) throws MetadataException, RemoteException;

    /**
     * Inserts a new dataset into the metadata, acquiring local locks on behalf
     * of the given transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataset
     *            Dataset instance to be inserted.
     * @throws MetadataException
     *             For example, if the dataset already exists.
     * @throws RemoteException
     */
    public void addDataset(JobId jobId, Dataset dataset) throws MetadataException, RemoteException;

    /**
     * Retrieves a dataset within a given dataverse, acquiring local locks on
     * behalf of the given transaction id.
     * 
     * @param jobId
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
    public Dataset getDataset(JobId jobId, String dataverseName, String datasetName) throws MetadataException,
            RemoteException;

    /**
     * Retrieves all indexes of a dataset, acquiring local locks on behalf of
     * the given transaction id.
     * 
     * @param jobId
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
    public List<Index> getDatasetIndexes(JobId jobId, String dataverseName, String datasetName)
            throws MetadataException, RemoteException;

    /**
     * Deletes the dataset with given name, and all it's associated indexes,
     * acquiring local locks on behalf of the given transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse which holds the given dataset.
     * @param datasetName
     *            Name of dataset to delete.
     * @throws MetadataException
     *             For example, if the dataset and/or dataverse does not exist.
     * @throws RemoteException
     */
    public void dropDataset(JobId jobId, String dataverseName, String datasetName) throws MetadataException,
            RemoteException;

    /**
     * Inserts an index into the metadata, acquiring local locks on behalf of
     * the given transaction id. The index itself knows its name, and which
     * dataset it belongs to.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param index
     *            Index instance to be inserted.
     * @throws MetadataException
     *             For example, if the index already exists.
     * @throws RemoteException
     */
    public void addIndex(JobId jobId, Index index) throws MetadataException, RemoteException;

    /**
     * Retrieves the index with given name, in given dataverse and dataset,
     * acquiring local locks on behalf of the given transaction id.
     * 
     * @param jobId
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
    public Index getIndex(JobId jobId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException;

    /**
     * Deletes the index with given name, in given dataverse and dataset,
     * acquiring local locks on behalf of the given transaction id.
     * 
     * @param jobId
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
    public void dropIndex(JobId jobId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException;

    /**
     * Inserts a datatype, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param datatype
     *            Datatype instance to be inserted.
     * @throws MetadataException
     *             For example, if the datatype already exists.
     * @throws RemoteException
     */
    public void addDatatype(JobId jobId, Datatype datatype) throws MetadataException, RemoteException;

    /**
     * Retrieves the datatype with given name in given dataverse, acquiring
     * local locks on behalf of the given transaction id.
     * 
     * @param jobId
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
    public Datatype getDatatype(JobId jobId, String dataverseName, String datatypeName) throws MetadataException,
            RemoteException;

    /**
     * Deletes the given datatype in given dataverse, acquiring local locks on
     * behalf of the given transaction id.
     * 
     * @param jobId
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
    public void dropDatatype(JobId jobId, String dataverseName, String datatypeName) throws MetadataException,
            RemoteException;

    /**
     * Inserts a node group, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param nodeGroup
     *            Node group instance to insert.
     * @throws MetadataException
     *             For example, if the node group already exists.
     * @throws RemoteException
     */
    public void addNodeGroup(JobId jobId, NodeGroup nodeGroup) throws MetadataException, RemoteException;

    /**
     * Retrieves a node group, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param nodeGroupName
     *            Name of node group to be retrieved.
     * @throws MetadataException
     *             For example, if the node group does not exist.
     * @throws RemoteException
     */
    public NodeGroup getNodeGroup(JobId jobId, String nodeGroupName) throws MetadataException, RemoteException;

    /**
     * Deletes a node group, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param nodeGroupName
     *            Name of node group to be deleted.
     * @throws MetadataException
     *             For example, there are still datasets partitioned on the node
     *             group to be deleted.
     * @throws RemoteException
     */
    public void dropNodegroup(JobId jobId, String nodeGroupName) throws MetadataException, RemoteException;

    /**
     * Inserts a node (compute node), acquiring local locks on behalf of the
     * given transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param node
     *            Node instance to be inserted.
     * @throws MetadataException
     *             For example, if the node already exists.
     * @throws RemoteException
     */
    public void addNode(JobId jobId, Node node) throws MetadataException, RemoteException;

    /**
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param functionSignature
     *            An instance of functionSignature representing the function
     * @return
     * @throws MetadataException
     * @throws RemoteException
     */
    public Function getFunction(JobId jobId, FunctionSignature functionSignature) throws MetadataException,
            RemoteException;

    /**
     * Deletes a function, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param functionSignature
     *            An instance of functionSignature representing the function
     * @throws MetadataException
     *             For example, there are still datasets partitioned on the node
     *             group to be deleted.
     * @throws RemoteException
     */
    public void dropFunction(JobId jobId, FunctionSignature functionSignature) throws MetadataException,
            RemoteException;

    /**
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param function
     *            Function to be inserted
     * @throws MetadataException
     *             for example, if the function already exists or refers to an
     *             unknown function
     * @throws RemoteException
     */
    public void addFunction(JobId jobId, Function function) throws MetadataException, RemoteException;

    /**
     * @param ctx
     * @param dataverseName
     * @return List<Function> A list containing the functions in the specified dataverse
     * @throws MetadataException
     * @throws RemoteException
     */
    public List<Function> getDataverseFunctions(JobId jobId, String dataverseName) throws MetadataException,
            RemoteException;

    /**
     * @param ctx
     * @param dataverseName
     * @return List<Adapter> A list containing the adapters in the specified dataverse
     * @throws MetadataException
     * @throws RemoteException
     */
    public List<DatasourceAdapter> getDataverseAdapters(JobId jobId, String dataverseName) throws MetadataException,
            RemoteException;

    /**
     * @param jobId
     * @param dataverseName
     * @param adapterName
     * @return
     * @throws MetadataException
     * @throws RemoteException
     */
    public DatasourceAdapter getAdapter(JobId jobId, String dataverseName, String adapterName)
            throws MetadataException, RemoteException;

    /**
     * Deletes a adapter , acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the adapter that is to be deleted.
     * @param adapterName
     *            Name of adapter to be deleted. MetadataException for example,
     *            if the adapter does not exists.
     * @throws RemoteException
     */
    public void dropAdapter(JobId jobId, String dataverseName, String adapterName) throws MetadataException,
            RemoteException;

    /**
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param adapter
     *            Adapter to be inserted
     * @throws MetadataException
     *             for example, if the adapter already exists.
     * @throws RemoteException
     */
    public void addAdapter(JobId jobId, DatasourceAdapter adapter) throws MetadataException, RemoteException;

    /**
     * @param jobId
     * @param compactionPolicy
     * @throws MetadataException
     * @throws RemoteException
     */
    public void addCompactionPolicy(JobId jobId, CompactionPolicy compactionPolicy) throws MetadataException,
            RemoteException;

    /**
     * @param jobId
     * @param dataverse
     * @param policy
     * @return
     * @throws MetadataException
     * @throws RemoteException
     */
    public CompactionPolicy getCompactionPolicy(JobId jobId, String dataverse, String policy) throws MetadataException,
            RemoteException;

    /**
     * @param jobId
     * @throws MetadataException
     * @throws RemoteException
     */
    public void initializeDatasetIdFactory(JobId jobId) throws MetadataException, RemoteException;

    /**
     * @return
     * @throws MetadataException
     * @throws RemoteException
     */
    public int getMostRecentDatasetId() throws MetadataException, RemoteException;

    /**
     * @param jobId
     * @param feed
     * @throws MetadataException
     * @throws RemoteException
     */
    public void addFeed(JobId jobId, Feed feed) throws MetadataException, RemoteException;

    /**
     * @param jobId
     * @param dataverse
     * @param feedName
     * @return
     * @throws MetadataException
     * @throws RemoteException
     */
    public Feed getFeed(JobId jobId, String dataverse, String feedName) throws MetadataException, RemoteException;

    /**
     * @param jobId
     * @param dataverse
     * @param feedName
     * @throws MetadataException
     * @throws RemoteException
     */
    public void dropFeed(JobId jobId, String dataverse, String feedName) throws MetadataException, RemoteException;

   
    /**
     * @param jobId
     * @param feedPolicy
     * @throws MetadataException
     * @throws RemoteException
     */
    public void addFeedPolicy(JobId jobId, FeedPolicy feedPolicy) throws MetadataException, RemoteException;

    /**
     * @param jobId
     * @param dataverse
     * @param policy
     * @return
     * @throws MetadataException
     * @throws RemoteException
     */
    public FeedPolicy getFeedPolicy(JobId jobId, String dataverse, String policy) throws MetadataException,
            RemoteException;

   
    /**
     * Removes a library , acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the adapter that is to be deleted.
     * @param libraryName
     *            Name of library to be deleted. MetadataException for example,
     *            if the library does not exists.
     * @throws RemoteException
     */
    public void dropLibrary(JobId jobId, String dataverseName, String libraryName) throws MetadataException,
            RemoteException;

    /**
     * Adds a library, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param library
     *            Library to be added
     * @throws MetadataException
     *             for example, if the library is already added.
     * @throws RemoteException
     */
    public void addLibrary(JobId jobId, Library library) throws MetadataException, RemoteException;

    /**
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the library that is to be retrieved.
     * @param libraryName
     *            name of the library that is to be retrieved
     * @return Library
     * @throws MetadataException
     * @throws RemoteException
     */
    public Library getLibrary(JobId jobId, String dataverseName, String libraryName) throws MetadataException,
            RemoteException;

    /**
     * Retireve libraries installed in a given dataverse.
     * 
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the library that is to be retrieved.
     * @return Library
     * @throws MetadataException
     * @throws RemoteException
     */
    public List<Library> getDataverseLibraries(JobId jobId, String dataverseName) throws MetadataException,
            RemoteException;

    /**
     * @param jobId
     * @param dataverseName
     * @return
     * @throws MetadataException
     * @throws RemoteException
     */
    public List<Feed> getDataverseFeeds(JobId jobId, String dataverseName) throws MetadataException, RemoteException;

    /**
     * delete a give feed (ingestion) policy
     * 
     * @param jobId
     * @param dataverseName
     * @param policyName
     * @return
     * @throws RemoteException
     * @throws MetadataException
     */
    public void dropFeedPolicy(JobId jobId, String dataverseName, String policyName) throws MetadataException,
            RemoteException;

    /**
     * @param jobId
     * @param dataverse
     * @return
     * @throws MetadataException
     * @throws RemoteException
     */
    public List<FeedPolicy> getDataversePolicies(JobId jobId, String dataverse) throws MetadataException,
            RemoteException;

    /**
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param externalFile
     *            An object representing the external file entity
     * @throws MetadataException
     *             for example, if the file already exists.
     * @throws RemoteException
     */
    public void addExternalFile(JobId jobId, ExternalFile externalFile) throws MetadataException, RemoteException;

    /**
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataset
     *            A dataset the files belongs to.
     * @throws MetadataException
     * @throws RemoteException
     */
    public List<ExternalFile> getExternalFiles(JobId jobId, Dataset dataset) throws MetadataException, RemoteException;

    /**
     * Deletes an externalFile , acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the external dataset that owns the file to be deleted.
     * @param datasetName
     *            Name of dataset owning the file to be deleted.
     * @param fileNumber
     *            the id number for the file to be deleted
     * @throws RemoteException
     */
    public void dropExternalFile(JobId jobId, String dataverseName, String datasetName, int fileNumber)
            throws MetadataException, RemoteException;

    /**
     * Deletes all external files belonging to a dataset, acquiring local locks on behalf of the given
     * transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataset
     *            An external dataset the files belong to.
     * @throws RemoteException
     */
    public void dropExternalFiles(JobId jobId, Dataset dataset) throws MetadataException, RemoteException;

    /**
     * Retrieves the file with given number, in given dataverse and dataset,
     * acquiring local locks on behalf of the given transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the datavers holding the given dataset.
     * @param datasetName
     *            Name of the dataset holding the index.
     * @param fileNumber
     *            Number of the file
     * @return An ExternalFile instance.
     * @throws MetadataException
     *             For example, if the index does not exist.
     * @throws RemoteException
     */
    public ExternalFile getExternalFile(JobId jobId, String dataverseName, String datasetName, Integer fileNumber)
            throws MetadataException, RemoteException;
    
    
    /**
     * update an existing dataset in the metadata, acquiring local locks on behalf
     * of the given transaction id.
     * 
     * @param jobId
     *            A globally unique id for an active metadata transaction.
     * @param dataset
     *            updated Dataset instance.
     * @throws MetadataException
     *             For example, if the dataset already exists.
     * @throws RemoteException
     */
    public void updateDataset(JobId jobId, Dataset dataset) throws MetadataException, RemoteException;

}
