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

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Database;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.FullTextConfigMetadataEntity;
import org.apache.asterix.metadata.entities.FullTextFilterMetadataEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A metadata node stores metadata in its local storage structures (currently
 * BTrees). A metadata node services requests on behalf of the (possibly remote)
 * metadata manager by executing local transactions against its local storage.
 * This interface mirrors the methods in IMetadataManager. Users wanting to
 * lock/access metadata shall always go through the MetadataManager, and should
 * never call methods on the MetadataNode directly for any reason.
 */
public interface IMetadataNode extends Remote, Serializable {

    /**
     * Begins a local transaction against the metadata.
     *
     * @throws RemoteException remote exception
     */
    void beginTransaction(TxnId txnId) throws RemoteException;

    /**
     * Commits a local transaction against the metadata.
     *
     * @throws RemoteException remote exception
     */
    void commitTransaction(TxnId txnId) throws RemoteException;

    /**
     * Aborts a local transaction against the metadata.
     *
     * @throws RemoteException remote exception
     */
    void abortTransaction(TxnId txnId) throws RemoteException;

    void addDatabase(TxnId txnId, Database database) throws AlgebricksException, RemoteException;

    void dropDatabase(TxnId txnId, String databaseName) throws AlgebricksException, RemoteException;

    Database getDatabase(TxnId txnId, String databaseName) throws AlgebricksException, RemoteException;

    /**
     * Inserts a new dataverse into the metadata, acquiring local locks on behalf of
     * the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverse
     *            Dataverse instance to be inserted.
     * @throws AlgebricksException
     *             For example, if the dataverse already exists.
     * @throws RemoteException remote exception
     */
    void addDataverse(TxnId txnId, Dataverse dataverse) throws AlgebricksException, RemoteException;

    /**
     * Deletes the dataverse with given name, and all it's associated datasets,
     * indexes, and types, acquiring local locks on behalf of the given transaction
     * id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse to drop.
     * @throws AlgebricksException
     *             For example, if the dataverse does not exist.
     */
    void dropDataverse(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    /**
     * Retrieves a dataverse with given name, acquiring local locks on behalf of the
     * given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse to retrieve.
     * @return A dataverse instance.
     * @throws AlgebricksException
     *             For example, if the dataverse does not exist.
     * @throws RemoteException remote exception
     */
    Dataverse getDataverse(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    List<Database> getDatabases(TxnId txnId) throws AlgebricksException, RemoteException;

    /**
     * Retrieves all dataverses, acquiring local locks on behalf of the given
     * transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @return A list of dataverse instances.
     * @throws AlgebricksException
     *             For example, if the dataverse does not exist.
     * @throws RemoteException remote exception
     */
    List<Dataverse> getDataverses(TxnId txnId) throws AlgebricksException, RemoteException;

    List<Dataset> getDatabaseDatasets(TxnId txnId, String database) throws AlgebricksException, RemoteException;

    /**
     * Retrieves all datasets belonging to the given dataverse, acquiring local
     * locks on behalf of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse of which to find all datasets.
     * @return A list of dataset instances.
     * @throws AlgebricksException
     *             For example, if the dataverse does not exist. RemoteException
     */
    List<Dataset> getDataverseDatasets(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    /**
     * Returns {@code true} if given dataverse is not empty
     * (i.e. contains any datatypes, datasets or any other entities).
     *  @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse
     */
    boolean isDataverseNotEmpty(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    /**
     * Inserts a new dataset into the metadata, acquiring local locks on behalf of
     * the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataset
     *            Dataset instance to be inserted.
     * @throws AlgebricksException
     *             For example, if the dataset already exists.
     * @throws RemoteException remote exception
     */
    void addDataset(TxnId txnId, Dataset dataset) throws AlgebricksException, RemoteException;

    /**
     * Retrieves a dataset within a given dataverse, acquiring local locks on behalf
     * of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Dataverse name to look for the dataset.
     * @param datasetName
     *            Name of dataset to be retrieved.
     * @return A dataset instance.
     * @throws AlgebricksException
     *             For example, if the dataset does not exist.
     * @throws RemoteException remote exception
     */
    Dataset getDataset(TxnId txnId, String database, DataverseName dataverseName, String datasetName)
            throws AlgebricksException, RemoteException;

    /**
     * Retrieves all indexes of a dataset, acquiring local locks on behalf of the
     * given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse which holds the given dataset.
     * @param datasetName
     *            Name of dataset for which to retrieve all indexes.
     * @return A list of Index instances.
     * @throws AlgebricksException
     *             For example, if the dataset and/or dataverse does not exist.
     * @throws RemoteException remote exception
     */
    List<Index> getDatasetIndexes(TxnId txnId, String database, DataverseName dataverseName, String datasetName)
            throws AlgebricksException, RemoteException;

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
     * @param force
     *            If true, forces drop the dataset. Setting it to true could make the metadata inconsistent.
     * @throws AlgebricksException
     *             For example, if the dataset and/or dataverse does not exist.
     * @throws RemoteException remote exception
     */
    void dropDataset(TxnId txnId, String database, DataverseName dataverseName, String datasetName, boolean force)
            throws AlgebricksException, RemoteException;

    /**
     * Inserts an index into the metadata, acquiring local locks on behalf of the
     * given transaction id. The index itself knows its name, and which dataset it
     * belongs to.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param index
     *            Index instance to be inserted.
     * @throws AlgebricksException
     *             For example, if the index already exists.
     * @throws RemoteException remote exception
     */
    void addIndex(TxnId txnId, Index index) throws AlgebricksException, RemoteException;

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
     * @param indexName
     *            Name of the index to retrieve.
     * @return An Index instance.
     * @throws AlgebricksException
     *             For example, if the index does not exist.
     * @throws RemoteException remote exception
     */
    Index getIndex(TxnId txnId, String database, DataverseName dataverseName, String datasetName, String indexName)
            throws AlgebricksException, RemoteException;

    /**
     * Deletes the index with given name, in given dataverse and dataset, acquiring
     * local locks on behalf of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the datavers holding the given dataset.
     * @param datasetName
     *            Name of the dataset holding the index.
     * @param indexName
     *            Name of the index to retrieve.
     * @throws AlgebricksException
     *             For example, if the index does not exist.
     * @throws RemoteException remote exception
     */
    void dropIndex(TxnId txnId, String database, DataverseName dataverseName, String datasetName, String indexName)
            throws AlgebricksException, RemoteException;

    /**
     * Inserts a datatype, acquiring local locks on behalf of the given transaction
     * id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param datatype
     *            Datatype instance to be inserted.
     * @throws AlgebricksException
     *             For example, if the datatype already exists.
     * @throws RemoteException remote exception
     */
    void addDatatype(TxnId txnId, Datatype datatype) throws AlgebricksException, RemoteException;

    /**
     * Retrieves the datatype with given name in given dataverse, acquiring local
     * locks on behalf of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse holding the datatype.
     * @param datatypeName
     *            Name of datatype to be retrieved.
     * @return A datatype instance.
     * @throws AlgebricksException
     *             For example, if the datatype does not exist.
     * @throws RemoteException remote exception
     */
    Datatype getDatatype(TxnId txnId, String database, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException, RemoteException;

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
     * @throws AlgebricksException
     *             For example, if there are still datasets using the type to be
     *             deleted.
     * @throws RemoteException remote exception
     */
    void dropDatatype(TxnId txnId, String database, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException, RemoteException;

    /**
     * Inserts a node group, acquiring local locks on behalf of the given
     * transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param nodeGroup
     *            Node group instance to insert.
     * @param modificationOp
     * @throws AlgebricksException
     *             For example, if the node group already exists.
     * @throws RemoteException remote exception
     */
    void modifyNodeGroup(TxnId txnId, NodeGroup nodeGroup,
            AbstractIndexModificationOperationCallback.Operation modificationOp)
            throws AlgebricksException, RemoteException;

    /**
     * Retrieves a node group, acquiring local locks on behalf of the given
     * transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param nodeGroupName
     *            Name of node group to be retrieved.
     * @throws AlgebricksException
     *             For example, if the node group does not exist.
     * @throws RemoteException remote exception
     */
    NodeGroup getNodeGroup(TxnId txnId, String nodeGroupName) throws AlgebricksException, RemoteException;

    /**
     * Deletes a node group, acquiring local locks on behalf of the given
     * transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param nodeGroupName
     *            Name of node group to be deleted.
     * @param failSilently
     *            true means it's a no-op if the node group cannot be dropped; false
     *            means it will throw an exception.
     * @return Whether the node group has been successfully dropped.
     * @throws AlgebricksException
     *             For example, there are still datasets partitioned on the node
     *             group to be deleted.
     * @throws RemoteException remote exception
     */
    boolean dropNodegroup(TxnId txnId, String nodeGroupName, boolean failSilently)
            throws AlgebricksException, RemoteException;

    /**
     * Inserts a node (compute node), acquiring local locks on behalf of the given
     * transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param node
     *            Node instance to be inserted.
     * @throws AlgebricksException
     *             For example, if the node already exists.
     * @throws RemoteException remote exception
     */
    void addNode(TxnId txnId, Node node) throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param functionSignature
     *            An instance of functionSignature representing the function
     * @return
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    Function getFunction(TxnId txnId, FunctionSignature functionSignature) throws AlgebricksException, RemoteException;

    /**
     * Retrieves all functions belonging to the given dataverse, acquiring local
     * locks on behalf of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse of which to find all functions.
     * @return A list of function instances.
     * @throws AlgebricksException
     *             For example, if the dataverse does not exist.
     * @throws RemoteException remote exception
     */
    List<Function> getDataverseFunctions(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    /**
     * Deletes a function, acquiring local locks on behalf of the given transaction
     * id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param functionSignature
     *            An instance of functionSignature representing the function
     * @throws AlgebricksException
     *             For example, there are still datasets partitioned on the node
     *             group to be deleted.
     * @throws RemoteException remote exception
     */
    void dropFunction(TxnId txnId, FunctionSignature functionSignature) throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param function
     *            Function to be inserted
     * @throws AlgebricksException
     *             for example, if the function already exists or refers to an
     *             unknown function
     * @throws RemoteException remote exception
     */
    void addFunction(TxnId txnId, Function function) throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     *            Metadata transaction id of an active metadata transaction.
     * @param filterMetadataEntity
     *            the full-text filter metadata entity to be added
     * @throws AlgebricksException
     *              For example, if the filter with the same name in the same dataverse already exists
     */
    void addFullTextFilter(TxnId txnId, FullTextFilterMetadataEntity filterMetadataEntity)
            throws RemoteException, AlgebricksException;

    /**
     * @param txnId
     *            Metadata transaction id of an active metadata transaction.
     * @param dataverseName
     *            the name of the dataverse where the full-text filter belongs
     * @param filterName
     *            the name of the full-text filter to be fetched
     * @throws AlgebricksException
     *              For example, if the filter doesn't exist
     */
    FullTextFilterMetadataEntity getFullTextFilter(TxnId txnId, String database, DataverseName dataverseName,
            String filterName) throws RemoteException, AlgebricksException;

    /**
     * @param txnId
     *            Metadata transaction id of an active metadata transaction.
     * @param dataverseName
     *            the name of the dataverse where the full-text filter belongs
     * @param filterName
     *            the name of the full-text filter to be dropped
     * @throws AlgebricksException
     *              For example, if ifExists is set to false and the filter doesn't exist
     */
    void dropFullTextFilter(TxnId txnId, String database, DataverseName dataverseName, String filterName)
            throws RemoteException, AlgebricksException;

    /**
     * @param txnId
     *            Metadata transaction id of an active metadata transaction.
     * @param configMetadataEntity
     *            the full-text config descriptor to be added
     * @throws AlgebricksException
     *              For example, if the config with the same name in the same dataverse already exists
     */
    void addFullTextConfig(TxnId txnId, FullTextConfigMetadataEntity configMetadataEntity)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     *            Metadata transaction id of an active metadata transaction.
     * @param dataverseName
     *            the name of the dataverse where the full-text filter belongs
     * @param configName
     *            the name of the full-text config to be fetched
     * @throws AlgebricksException
     *              For example, if the full-text config doesn't exist
     */
    FullTextConfigMetadataEntity getFullTextConfig(TxnId txnId, String database, DataverseName dataverseName,
            String configName) throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     *            Metadata transaction id of an active metadata transaction.
     * @param dataverseName
     *            the name of the dataverse where the full-text filter belongs
     * @param configName
     *            the name of the full-text config to be dropped
     * @throws AlgebricksException
     *              For example, if ifExists is set to false and the config doesn't exist
     */
    void dropFullTextConfig(TxnId txnId, String database, DataverseName dataverseName, String configName)
            throws RemoteException, AlgebricksException;

    /**
     * @param txnId
     * @param dataverseName
     * @return List<Adapter> A list containing the adapters in the specified
     *         dataverse
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    List<DatasourceAdapter> getDataverseAdapters(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @param dataverseName
     * @param adapterName
     * @return
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    DatasourceAdapter getAdapter(TxnId txnId, String database, DataverseName dataverseName, String adapterName)
            throws AlgebricksException, RemoteException;

    /**
     * Deletes a adapter , acquiring local locks on behalf of the given transaction
     * id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the adapter that is to be deleted.
     * @param adapterName
     *            Name of adapter to be deleted. AlgebricksException for example, if
     *            the adapter does not exists.
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    void dropAdapter(TxnId txnId, String database, DataverseName dataverseName, String adapterName)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param adapter
     *            Adapter to be inserted
     * @throws AlgebricksException
     *             for example, if the adapter already exists.
     * @throws RemoteException remote exception
     */
    void addAdapter(TxnId txnId, DatasourceAdapter adapter) throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @param compactionPolicy
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    void addCompactionPolicy(TxnId txnId, CompactionPolicy compactionPolicy)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @param dataverseName
     * @param policy
     * @return
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    CompactionPolicy getCompactionPolicy(TxnId txnId, String database, DataverseName dataverseName, String policy)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    void initializeDatasetIdFactory(TxnId txnId) throws AlgebricksException, RemoteException;

    /**
     * @return
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    int getMostRecentDatasetId() throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @param feed
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    void addFeed(TxnId txnId, Feed feed) throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @param dataverseName
     * @param feedName
     * @return
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    Feed getFeed(TxnId txnId, String database, DataverseName dataverseName, String feedName)
            throws AlgebricksException, RemoteException;

    List<Feed> getFeeds(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @param dataverseName
     * @param feedName
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    void dropFeed(TxnId txnId, String database, DataverseName dataverseName, String feedName)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @param feedPolicy
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    void addFeedPolicy(TxnId txnId, FeedPolicyEntity feedPolicy) throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @param dataverseName
     * @param policy
     * @return
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    FeedPolicyEntity getFeedPolicy(TxnId txnId, String database, DataverseName dataverseName, String policy)
            throws AlgebricksException, RemoteException;

    /**
     * Removes a library , acquiring local locks on behalf of the given transaction
     * id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the adapter that is to be deleted.
     * @param libraryName
     *            Name of library to be deleted. AlgebricksException for example, if
     *            the library does not exists.
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    void dropLibrary(TxnId txnId, String database, DataverseName dataverseName, String libraryName)
            throws AlgebricksException, RemoteException;

    /**
     * Adds a library, acquiring local locks on behalf of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param library
     *            Library to be added
     * @throws AlgebricksException
     *             for example, if the library is already added.
     * @throws RemoteException remote exception
     */
    void addLibrary(TxnId txnId, Library library) throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the library that is to be retrieved.
     * @param libraryName
     *            name of the library that is to be retrieved
     * @return Library
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    Library getLibrary(TxnId txnId, String database, DataverseName dataverseName, String libraryName)
            throws AlgebricksException, RemoteException;

    List<Library> getDatabaseLibraries(TxnId txnId, String database) throws AlgebricksException, RemoteException;

    /**
     * Retireve libraries installed in a given dataverse.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the library that is to be retrieved.
     * @return Library
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    List<Library> getDataverseLibraries(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @param dataverseName
     * @return
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    List<Feed> getDataverseFeeds(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    /**
     * delete a give feed (ingestion) policy
     *
     * @param txnId
     * @param dataverseName
     * @param policyName
     * @return
     * @throws RemoteException remote exception
     * @throws AlgebricksException
     */
    void dropFeedPolicy(TxnId txnId, String database, DataverseName dataverseName, String policyName)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     * @param dataverseName
     * @return
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    List<FeedPolicyEntity> getDataverseFeedPolicies(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param externalFile
     *            An object representing the external file entity
     * @throws AlgebricksException
     *             for example, if the file already exists.
     * @throws RemoteException remote exception
     */
    void addExternalFile(TxnId txnId, ExternalFile externalFile) throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataset
     *            A dataset the files belongs to.
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    List<ExternalFile> getExternalFiles(TxnId txnId, Dataset dataset) throws AlgebricksException, RemoteException;

    /**
     * Deletes an externalFile , acquiring local locks on behalf of the given
     * transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the external dataset that owns the file
     *            to be deleted.
     * @param datasetName
     *            Name of dataset owning the file to be deleted.
     * @param fileNumber
     *            the id number for the file to be deleted
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    void dropExternalFile(TxnId txnId, String database, DataverseName dataverseName, String datasetName, int fileNumber)
            throws AlgebricksException, RemoteException;

    /**
     * Deletes all external files belonging to a dataset, acquiring local locks on
     * behalf of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataset
     *            An external dataset the files belong to.
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    void dropExternalFiles(TxnId txnId, Dataset dataset) throws AlgebricksException, RemoteException;

    /**
     * Retrieves the file with given number, in given dataverse and dataset,
     * acquiring local locks on behalf of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            Name of the datavers holding the given dataset.
     * @param datasetName
     *            Name of the dataset holding the index.
     * @param fileNumber
     *            Number of the file
     * @return An ExternalFile instance.
     * @throws AlgebricksException
     *             For example, if the index does not exist.
     * @throws RemoteException remote exception
     */
    ExternalFile getExternalFile(TxnId txnId, String database, DataverseName dataverseName, String datasetName,
            Integer fileNumber) throws AlgebricksException, RemoteException;

    /**
     * Adds a synonym, acquiring local locks on behalf of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param synonym
     *            Synonym to be added
     * @throws AlgebricksException
     *             for example, if the synonym is already added.
     * @throws RemoteException remote exception
     */
    void addSynonym(TxnId txnId, Synonym synonym) throws AlgebricksException, RemoteException;

    /**
     * Removes a synonym, acquiring local locks on behalf of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the synonym that is to be deleted.
     * @param synonymName
     *            Name of synonym to be deleted. AlgebricksException for example, if
     *            the synonym does not exists.
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    void dropSynonym(TxnId txnId, String database, DataverseName dataverseName, String synonymName)
            throws AlgebricksException, RemoteException;

    /**
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the synonym that is to be retrieved.
     * @param synonymName
     *            name of the synonym that is to be retrieved
     * @return Synonym
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    Synonym getSynonym(TxnId txnId, String database, DataverseName dataverseName, String synonymName)
            throws AlgebricksException, RemoteException;

    /**
     * Retrieve synonyms installed in a given dataverse.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataverseName
     *            dataverse associated with synonyms that are to be retrieved.
     * @return list of synonyms
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    List<Synonym> getDataverseSynonyms(TxnId txnId, String database, DataverseName dataverseName)
            throws AlgebricksException, RemoteException;

    /**
     * update an existing dataset in the metadata, acquiring local locks on behalf
     * of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param dataset
     *            updated Dataset instance.
     * @throws AlgebricksException
     *             For example, if the dataset already exists.
     * @throws RemoteException remote exception
     */
    void updateDataset(TxnId txnId, Dataset dataset) throws AlgebricksException, RemoteException;

    /**
     * update an existing library in the metadata, acquiring local locks on behalf
     * of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param library
     *            updated Library instance.
     */
    void updateLibrary(TxnId txnId, Library library) throws AlgebricksException, RemoteException;

    /**
     * update an existing function in the metadata, acquiring local locks on behalf
     * of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param function
     *            updated Function instance.
     */
    void updateFunction(TxnId txnId, Function function) throws AlgebricksException, RemoteException;

    /**
     * update an existing datatype in the metadata, acquiring local locks on behalf
     * of the given transaction id.
     *
     * @param txnId
     *            A globally unique id for an active metadata transaction.
     * @param datatype
     *            updated Datatype instance.
     */
    void updateDatatype(TxnId txnId, Datatype datatype) throws AlgebricksException, RemoteException;

    /**
     * Adds an extension entity under the ongoing transaction job id
     *
     * @param txnId
     * @param entity
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    <T extends IExtensionMetadataEntity> void addEntity(TxnId txnId, T entity)
            throws AlgebricksException, RemoteException;

    /**
     * Upserts an extension entity under the ongoing transaction job id
     *
     * @param txnId
     * @param entity
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    <T extends IExtensionMetadataEntity> void upsertEntity(TxnId txnId, T entity)
            throws AlgebricksException, RemoteException;

    /**
     * Deletes an extension entity under the ongoing transaction job id
     *
     * @param txnId
     * @param entity
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    <T extends IExtensionMetadataEntity> void deleteEntity(TxnId txnId, T entity)
            throws AlgebricksException, RemoteException;

    /**
     * Gets a list of extension entities matching a search key under the ongoing
     * transaction
     *
     * @param txnId
     * @param searchKey
     * @return
     * @throws AlgebricksException
     * @throws RemoteException remote exception
     */
    <T extends IExtensionMetadataEntity> List<T> getEntities(TxnId txnId, IExtensionMetadataSearchKey searchKey)
            throws AlgebricksException, RemoteException;

    /**
     * Gets all the records of a metadata dataset as JSON.
     *
     * @param txnId transaction id
     * @param metadataIndex the metadata dataset
     * @param payloadPosition the position of the record in the tuple
     *
     * @return the metadata records as JSON
     *
     * @throws AlgebricksException AlgebricksException
     * @throws RemoteException RemoteException
     */
    JsonNode getEntitiesAsJson(TxnId txnId, IMetadataIndex metadataIndex, int payloadPosition)
            throws AlgebricksException, RemoteException;

    void addFeedConnection(TxnId txnId, FeedConnection feedConnection) throws AlgebricksException, RemoteException;

    FeedConnection getFeedConnection(TxnId txnId, String database, DataverseName dataverseName, String feedName,
            String datasetName) throws AlgebricksException, RemoteException;

    void dropFeedConnection(TxnId txnId, String database, DataverseName dataverseName, String feedName,
            String datasetName) throws AlgebricksException, RemoteException;

    List<FeedConnection> getFeedConnections(TxnId txnId, String database, DataverseName dataverseName, String feedName)
            throws AlgebricksException, RemoteException;
}
