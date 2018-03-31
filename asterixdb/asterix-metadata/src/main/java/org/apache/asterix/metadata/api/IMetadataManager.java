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

import java.rmi.RemoteException;
import java.util.List;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.IMetadataBootstrap;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.metadata.MetadataTransactionContext;
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
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

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
public interface IMetadataManager extends IMetadataBootstrap {

    /**
     * Begins a transaction on the metadata node.
     *
     * @return A globally unique transaction id.
     * @throws ACIDException
     * @throws RemoteException
     */
    MetadataTransactionContext beginTransaction() throws ACIDException, RemoteException;

    /**
     * Commits a remote transaction on the metadata node.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @throws ACIDException
     * @throws RemoteException
     */
    void commitTransaction(MetadataTransactionContext ctx) throws ACIDException, RemoteException;

    /**
     * Aborts a remote transaction running on the metadata node.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @throws ACIDException
     * @throws RemoteException
     */
    void abortTransaction(MetadataTransactionContext ctx) throws ACIDException, RemoteException;

    /**
     * Inserts a new dataverse into the metadata.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverse
     *            Dataverse instance to be inserted.
     * @throws AlgebricksException
     *             For example, if the dataverse already exists.
     */
    void addDataverse(MetadataTransactionContext ctx, Dataverse dataverse) throws AlgebricksException;

    /**
     * Retrieves all dataverses
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @return A list of dataverse instances.
     * @throws AlgebricksException
     */
    List<Dataverse> getDataverses(MetadataTransactionContext ctx) throws AlgebricksException;

    /**
     * Retrieves a dataverse with given name.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse to retrieve.
     * @return A dataverse instance.
     * @throws AlgebricksException
     *             For example, if the dataverse does not exist.
     */
    Dataverse getDataverse(MetadataTransactionContext ctx, String dataverseName) throws AlgebricksException;

    /**
     * Retrieves all datasets belonging to the given dataverse.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of the dataverse of which to find all datasets.
     * @return A list of dataset instances.
     * @throws AlgebricksException
     *             For example, if the dataverse does not exist.
     */
    List<Dataset> getDataverseDatasets(MetadataTransactionContext ctx, String dataverseName) throws AlgebricksException;

    /**
     * Deletes the dataverse with given name, and all it's associated datasets,
     * indexes, and types.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @return A list of dataset instances.
     * @throws AlgebricksException
     *             For example, if the dataverse does not exist.
     */
    void dropDataverse(MetadataTransactionContext ctx, String dataverseName) throws AlgebricksException;

    /**
     * Inserts a new dataset into the metadata.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataset
     *            Dataset instance to be inserted.
     * @throws AlgebricksException
     *             For example, if the dataset already exists.
     */
    void addDataset(MetadataTransactionContext ctx, Dataset dataset) throws AlgebricksException;

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
     * @throws AlgebricksException
     *             For example, if the dataset does not exist.
     */
    Dataset getDataset(MetadataTransactionContext ctx, String dataverseName, String datasetName)
            throws AlgebricksException;

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
     * @throws AlgebricksException
     *             For example, if the dataset and/or dataverse does not exist.
     */
    List<Index> getDatasetIndexes(MetadataTransactionContext ctx, String dataverseName, String datasetName)
            throws AlgebricksException;

    /**
     * Deletes the dataset with given name, and all it's associated indexes.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse which holds the given dataset.
     * @param datasetName
     *            Name of dataset to delete.
     * @throws AlgebricksException
     *             For example, if the dataset and/or dataverse does not exist.
     */
    void dropDataset(MetadataTransactionContext ctx, String dataverseName, String datasetName)
            throws AlgebricksException;

    /**
     * Inserts an index into the metadata. The index itself knows its name, and
     * which dataset it belongs to.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param index
     *            Index instance to be inserted.
     * @throws AlgebricksException
     *             For example, if the index already exists.
     */
    void addIndex(MetadataTransactionContext ctx, Index index) throws AlgebricksException;

    /**
     * Retrieves the index with given name, in given dataverse and dataset.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of the datavers holding the given dataset.
     * @param datasetName
     *            Name of the dataset holding the index.
     * @param indexName
     *            Name of the index to retrieve.
     * @return An Index instance.
     * @throws AlgebricksException
     *             For example, if the index does not exist.
     */
    Index getIndex(MetadataTransactionContext ctx, String dataverseName, String datasetName, String indexName)
            throws AlgebricksException;

    /**
     * Deletes the index with given name, in given dataverse and dataset.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of the datavers holding the given dataset.
     * @param datasetName
     *            Name of the dataset holding the index.
     * @param indexName
     *            Name of the index to retrieve.
     * @throws AlgebricksException
     *             For example, if the index does not exist.
     */
    void dropIndex(MetadataTransactionContext ctx, String dataverseName, String datasetName, String indexName)
            throws AlgebricksException;

    /**
     * Inserts a datatype.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param datatype
     *            Datatype instance to be inserted.
     * @throws AlgebricksException
     *             For example, if the datatype already exists.
     */
    void addDatatype(MetadataTransactionContext ctx, Datatype datatype) throws AlgebricksException;

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
     * @throws AlgebricksException
     *             For example, if the datatype does not exist.
     */
    Datatype getDatatype(MetadataTransactionContext ctx, String dataverseName, String datatypeName)
            throws AlgebricksException;

    /**
     * Deletes the given datatype in given dataverse.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            Name of dataverse holding the datatype.
     * @param datatypeName
     *            Name of datatype to be deleted.
     * @throws AlgebricksException
     *             For example, if there are still datasets using the type to be
     *             deleted.
     */
    void dropDatatype(MetadataTransactionContext ctx, String dataverseName, String datatypeName)
            throws AlgebricksException;

    /**
     * Inserts a new node group.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param nodeGroup
     *            Node group instance to insert.
     * @throws AlgebricksException
     *             For example, if the node group already exists
     */
    void addNodegroup(MetadataTransactionContext ctx, NodeGroup nodeGroup) throws AlgebricksException;

    /**
     * Inserts a new (or updates an existing) node group.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param nodeGroup
     *            Node group instance to insert or update.
     * @throws AlgebricksException
     */
    void upsertNodegroup(MetadataTransactionContext ctx, NodeGroup nodeGroup) throws AlgebricksException;

    /**
     * Retrieves a node group.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param nodeGroupName
     *            Name of node group to be retrieved.
     * @throws AlgebricksException
     *             For example, if the node group does not exist.
     */
    NodeGroup getNodegroup(MetadataTransactionContext ctx, String nodeGroupName) throws AlgebricksException;

    /**
     * Deletes a node group.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param nodeGroupName
     *            Name of node group to be deleted.
     * @param failSilently
     *            true means it's a no-op if the node group cannot be dropped; false
     *            means it will throw an exception.
     * @throws AlgebricksException
     *             For example, there are still datasets partitioned on the node
     *             group to be deleted.
     */
    void dropNodegroup(MetadataTransactionContext ctx, String nodeGroupName, boolean failSilently)
            throws AlgebricksException;

    /**
     * Inserts a node (machine).
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param node
     *            Node instance to be inserted.
     * @throws AlgebricksException
     *             For example, if the node already exists.
     */
    void addNode(MetadataTransactionContext ctx, Node node) throws AlgebricksException;

    /**
     * @param mdTxnCtx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param function
     *            An instance of type Function that represents the function being
     *            added
     * @throws AlgebricksException
     */
    void addFunction(MetadataTransactionContext mdTxnCtx, Function function) throws AlgebricksException;

    /**
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param functionSignature
     *            the functions signature (unique to the function)
     * @return
     * @throws AlgebricksException
     */

    Function getFunction(MetadataTransactionContext ctx, FunctionSignature functionSignature)
            throws AlgebricksException;

    List<Function> getFunctions(MetadataTransactionContext ctx, String dataverseName) throws AlgebricksException;

    /**
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param functionSignature
     *            the functions signature (unique to the function)
     * @throws AlgebricksException
     */
    void dropFunction(MetadataTransactionContext ctx, FunctionSignature functionSignature) throws AlgebricksException;

    /**
     * @param mdTxnCtx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param adapter
     *            An instance of type Adapter that represents the adapter being
     *            added
     * @throws AlgebricksException
     */
    void addAdapter(MetadataTransactionContext mdTxnCtx, DatasourceAdapter adapter) throws AlgebricksException;

    /**
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            the dataverse associated with the adapter being searched
     * @param name
     *            name of the adapter
     * @return
     * @throws AlgebricksException
     */
    DatasourceAdapter getAdapter(MetadataTransactionContext ctx, String dataverseName, String name)
            throws AlgebricksException;

    /**
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            the dataverse associated with the adapter being dropped
     * @param name
     *            name of the adapter
     * @throws AlgebricksException
     */
    void dropAdapter(MetadataTransactionContext ctx, String dataverseName, String name) throws AlgebricksException;

    /**
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            the dataverse whose associated adapters are being requested
     * @return
     * @throws AlgebricksException
     */
    List<DatasourceAdapter> getDataverseAdapters(MetadataTransactionContext ctx, String dataverseName)
            throws AlgebricksException;

    /**
     * @param ctx
     * @param policy
     * @throws AlgebricksException
     */
    void addCompactionPolicy(MetadataTransactionContext ctx, CompactionPolicy policy) throws AlgebricksException;

    /**
     * @param ctx
     * @param dataverse
     * @param policyName
     * @return
     * @throws AlgebricksException
     */
    CompactionPolicy getCompactionPolicy(MetadataTransactionContext ctx, String dataverse, String policyName)
            throws AlgebricksException;

    /**
     * @param ctx
     * @param dataverseName
     * @return
     * @throws AlgebricksException
     */
    List<Function> getDataverseFunctions(MetadataTransactionContext ctx, String dataverseName)
            throws AlgebricksException;

    /**
     * @param ctx
     * @param feed
     * @throws AlgebricksException
     */
    void addFeed(MetadataTransactionContext ctx, Feed feed) throws AlgebricksException;

    /**
     * @param ctx
     * @param dataverse
     * @param feedName
     * @return
     * @throws AlgebricksException
     */
    Feed getFeed(MetadataTransactionContext ctx, String dataverse, String feedName) throws AlgebricksException;

    List<Feed> getFeeds(MetadataTransactionContext ctx, String dataverse) throws AlgebricksException;

    /**
     * @param ctx
     * @param dataverse
     * @param feedName
     * @throws AlgebricksException
     */
    void dropFeed(MetadataTransactionContext ctx, String dataverse, String feedName) throws AlgebricksException;

    /**
     * @param ctx
     * @param policy
     * @throws AlgebricksException
     */
    void addFeedPolicy(MetadataTransactionContext ctx, FeedPolicyEntity policy) throws AlgebricksException;

    /**
     * @param ctx
     * @param dataverse
     * @param policyName
     * @throws AlgebricksException
     */
    void dropFeedPolicy(MetadataTransactionContext ctx, String dataverse, String policyName) throws AlgebricksException;

    /**
     * @param ctx
     * @param dataverse
     * @param policyName
     * @return
     * @throws AlgebricksException
     */
    FeedPolicyEntity getFeedPolicy(MetadataTransactionContext ctx, String dataverse, String policyName)
            throws AlgebricksException;

    void initializeDatasetIdFactory(MetadataTransactionContext ctx) throws AlgebricksException;

    int getMostRecentDatasetId() throws AlgebricksException;

    /**
     * Removes a library , acquiring local locks on behalf of the given transaction
     * id.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the adapter that is to be deleted.
     * @param libraryName
     *            Name of library to be deleted. AlgebricksException for example, if
     *            the library does not exists.
     * @throws AlgebricksException
     */
    void dropLibrary(MetadataTransactionContext ctx, String dataverseName, String libraryName)
            throws AlgebricksException;

    /**
     * Adds a library, acquiring local locks on behalf of the given transaction id.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param library
     *            Library to be added
     * @throws AlgebricksException
     *             for example, if the library is already added.
     */
    void addLibrary(MetadataTransactionContext ctx, Library library) throws AlgebricksException;

    /**
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the library that is to be retrieved.
     * @param libraryName
     *            name of the library that is to be retrieved
     * @return Library
     * @throws AlgebricksException
     * @throws RemoteException
     */
    Library getLibrary(MetadataTransactionContext ctx, String dataverseName, String libraryName)
            throws AlgebricksException, RemoteException;

    /**
     * Retireve libraries installed in a given dataverse.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataverseName
     *            dataverse asociated with the library that is to be retrieved.
     * @return Library
     * @throws AlgebricksException
     */
    List<Library> getDataverseLibraries(MetadataTransactionContext ctx, String dataverseName)
            throws AlgebricksException;

    /**
     * @param mdTxnCtx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param externalFile
     *            An instance of type ExternalFile that represents the external file
     *            being added
     * @throws AlgebricksException
     */
    void addExternalFile(MetadataTransactionContext mdTxnCtx, ExternalFile externalFile) throws AlgebricksException;

    /**
     * @param mdTxnCtx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataset
     *            An instance of type Dataset that represents the "external" dataset
     * @return A list of external files belonging to the dataset
     * @throws AlgebricksException
     */
    List<ExternalFile> getDatasetExternalFiles(MetadataTransactionContext mdTxnCtx, Dataset dataset)
            throws AlgebricksException;

    /**
     * @param mdTxnCtx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param externalFile
     *            An instance of type ExternalFile that represents the external file
     *            being dropped
     * @throws AlgebricksException
     */
    void dropExternalFile(MetadataTransactionContext mdTxnCtx, ExternalFile externalFile) throws AlgebricksException;

    /**
     * @param mdTxnCtx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataset
     *            An instance of type dataset that owns the external files being
     *            dropped
     * @throws AlgebricksException
     */
    void dropDatasetExternalFiles(MetadataTransactionContext mdTxnCtx, Dataset dataset) throws AlgebricksException;

    /**
     * Get en external file
     *
     * @param mdTxnCtx
     * @param dataverseName
     * @param datasetName
     * @param fileNumber
     * @return
     * @throws AlgebricksException
     */
    ExternalFile getExternalFile(MetadataTransactionContext mdTxnCtx, String dataverseName, String datasetName,
            Integer fileNumber) throws AlgebricksException;

    /**
     * update an existing dataset in metadata.
     *
     * @param ctx
     *            MetadataTransactionContext of an active metadata transaction.
     * @param dataset
     *            Existing Dataset.
     * @throws AlgebricksException
     *             For example, if the dataset already exists.
     */
    void updateDataset(MetadataTransactionContext ctx, Dataset dataset) throws AlgebricksException;

    /**
     * Add an extension entity to its extension dataset under the ongoing metadata
     * transaction
     *
     * @param mdTxnCtx
     * @param entity
     * @throws AlgebricksException
     */
    <T extends IExtensionMetadataEntity> void addEntity(MetadataTransactionContext mdTxnCtx, T entity)
            throws AlgebricksException;

    /**
     * Upsert an extension entity to its extension dataset under the ongoing
     * metadata transaction
     *
     * @param mdTxnCtx
     * @param entity
     * @throws AlgebricksException
     */
    <T extends IExtensionMetadataEntity> void upsertEntity(MetadataTransactionContext mdTxnCtx, T entity)
            throws AlgebricksException;

    /**
     * Deletes an extension entity from its extension dataset under the ongoing
     * metadata transaction
     *
     * @param mdTxnCtx
     * @param entity
     * @throws AlgebricksException
     */
    <T extends IExtensionMetadataEntity> void deleteEntity(MetadataTransactionContext mdTxnCtx, T entity)
            throws AlgebricksException;

    /**
     * Gets a list of extension entities matching a search key under the ongoing
     * metadata transaction
     *
     * @param mdTxnCtx
     * @param searchKey
     * @return
     * @throws AlgebricksException
     */
    <T extends IExtensionMetadataEntity> List<T> getEntities(MetadataTransactionContext mdTxnCtx,
            IExtensionMetadataSearchKey searchKey) throws AlgebricksException;

    /**
     * Indicate when the metadata node has left or rejoined the cluster, and the
     * MetadataManager should rebind it
     */
    void rebindMetadataNode();

    /**
     * Feed Connection Related Metadata operations
     */
    void addFeedConnection(MetadataTransactionContext ctx, FeedConnection feedConnection) throws AlgebricksException;

    void dropFeedConnection(MetadataTransactionContext ctx, String dataverseName, String feedName, String datasetName)
            throws AlgebricksException;

    FeedConnection getFeedConnection(MetadataTransactionContext ctx, String dataverseName, String feedName,
            String datasetName) throws AlgebricksException;

    List<FeedConnection> getFeedConections(MetadataTransactionContext ctx, String dataverseName, String feedName)
            throws AlgebricksException;

    long getMaxTxnId();
}
