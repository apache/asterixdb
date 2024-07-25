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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;
import org.apache.asterix.metadata.api.IExtensionMetadataSearchKey;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.api.IMetadataManager;
import org.apache.asterix.metadata.api.IMetadataNode;
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
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback.Operation;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;

/**
 * Provides access to Asterix metadata via remote methods to the metadata node.
 * This metadata manager maintains a local cache of metadata Java objects
 * received from the metadata node, to avoid contacting the metadata node
 * repeatedly. We assume that this metadata manager is the only metadata manager
 * in an Asterix cluster. Therefore, no separate cache-invalidation mechanism is
 * needed at this point. Assumptions/Limitations: The metadata subsystem is
 * started during NC Bootstrap start, i.e., when Asterix is deployed. The
 * metadata subsystem is destroyed in NC Bootstrap end, i.e., when Asterix is
 * undeployed. The metadata subsystem consists of the MetadataManager and the
 * MatadataNode. The MetadataManager provides users access to the metadata. The
 * MetadataNode implements direct access to the storage layer on behalf of the
 * MetadataManager, and translates the binary representation of ADM into Java
 * objects for consumption by the MetadataManager's users. There is exactly one
 * instance of the MetadataManager and of the MetadataNode in the cluster, which
 * may or may not be co-located on the same machine (or in the same JVM). The
 * MetadataManager exists in the same JVM as its user's (e.g., the query
 * compiler). The MetadataNode exists in the same JVM as it's transactional
 * components (LockManager, LogManager, etc.) Users shall access the metadata
 * only through the MetadataManager, and never via the MetadataNode directly.
 * Multiple threads may issue requests to the MetadataManager concurrently. For
 * the sake of accessing metadata, we assume a transaction consists of one
 * thread. Users are responsible for locking the metadata (using the
 * MetadataManager API) before issuing requests. The MetadataNode is responsible
 * for acquiring finer-grained locks on behalf of requests from the
 * MetadataManager. Currently, locks are acquired per BTree, since the BTree
 * does not acquire even finer-grained locks yet internally. The metadata can be
 * queried with AQL DML like any other dataset, but can only be changed with AQL
 * DDL. The transaction ids for metadata transactions must be unique across the
 * cluster, i.e., metadata transaction ids shall never "accidentally" overlap
 * with transaction ids of regular jobs or other metadata transactions.
 */
public abstract class MetadataManager implements IMetadataManager {
    private static final Logger LOGGER = LogManager.getLogger();
    private final MetadataCache cache = new MetadataCache();
    protected final Collection<IAsterixStateProxy> proxies;
    protected IMetadataNode metadataNode;
    protected boolean rebindMetadataNode = false;

    // TODO(mblow): replace references of this (non-constant) field with a method,
    // update field name accordingly
    public static IMetadataManager INSTANCE;

    private MetadataManager(Collection<IAsterixStateProxy> proxies, MetadataNode metadataNode) {
        this(proxies);
        if (metadataNode == null) {
            throw new IllegalArgumentException("Null metadataNode given to MetadataManager");
        }
        this.metadataNode = metadataNode;
    }

    private MetadataManager(Collection<IAsterixStateProxy> proxies) {
        if (proxies == null || proxies.isEmpty()) {
            throw new IllegalArgumentException("Null / empty list of proxies given to MetadataManager");
        }
        this.proxies = proxies;
    }

    protected abstract TxnId createTxnId();

    @Override
    public void init() throws HyracksDataException {
        // no op
    }

    @Override
    public MetadataTransactionContext beginTransaction() throws RemoteException {
        try {
            INSTANCE.init();
        } catch (HyracksDataException e) {
            throw new ACIDException(e);
        }
        TxnId txnId = createTxnId();
        metadataNode.beginTransaction(txnId);
        return new MetadataTransactionContext(txnId);
    }

    @SuppressWarnings("squid:S1181")
    @Override
    public void commitTransaction(MetadataTransactionContext ctx) {
        try {
            metadataNode.commitTransaction(ctx.getTxnId());
            cache.commit(ctx);
        } catch (Throwable th) {
            // Metadata node should abort all transactions on re-joining the new CC
            LOGGER.fatal("Failure committing a metadata transaction", th);
            ExitUtil.halt(ExitUtil.EC_FAILED_TO_COMMIT_METADATA_TXN);
        }
    }

    @SuppressWarnings("squid:S1181")
    @Override
    public void abortTransaction(MetadataTransactionContext ctx) {
        try {
            metadataNode.abortTransaction(ctx.getTxnId());
        } catch (Throwable th) {
            // Metadata node should abort all transactions on re-joining the new CC
            LOGGER.fatal("Failure aborting a metadata transaction", th);
            ExitUtil.halt(ExitUtil.EC_FAILED_TO_ABORT_METADATA_TXN);
        }
    }

    @Override
    public Database getDatabase(MetadataTransactionContext ctx, String databaseName) throws AlgebricksException {
        Objects.requireNonNull(databaseName);
        // first look in the context to see if this transaction created the
        // requested database itself (but the database is still uncommitted)
        Database database = ctx.getDatabase(databaseName);
        if (database != null) {
            // don't add this database to the cache, since it is still uncommitted
            return database;
        }
        if (ctx.databaseIsDropped(databaseName)) {
            // database has been dropped by this transaction but could still be in the cache
            return null;
        }
        database = cache.getDatabase(databaseName);
        if (database != null) {
            // database is already in the cache, don't add it again
            return database;
        }
        try {
            database = metadataNode.getDatabase(ctx.getTxnId(), databaseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // we fetched the database from the MetadataNode. add it to the cache when this transaction commits
        if (database != null) {
            ctx.addDatabase(database);
        }
        return database;
    }

    @Override
    public void addDatabase(MetadataTransactionContext ctx, Database database) throws AlgebricksException {
        try {
            metadataNode.addDatabase(ctx.getTxnId(), database);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addDatabase(database);
    }

    @Override
    public void dropDatabase(MetadataTransactionContext ctx, String databaseName) throws AlgebricksException {
        try {
            Objects.requireNonNull(databaseName);
            metadataNode.dropDatabase(ctx.getTxnId(), databaseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropDatabase(databaseName);
    }

    @Override
    public void addDataverse(MetadataTransactionContext ctx, Dataverse dataverse) throws AlgebricksException {
        try {
            metadataNode.addDataverse(ctx.getTxnId(), dataverse);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addDataverse(dataverse);
    }

    @Override
    public void dropDataverse(MetadataTransactionContext ctx, String database, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            metadataNode.dropDataverse(ctx.getTxnId(), database, dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropDataverse(database, dataverseName);
    }

    @Override
    public boolean isDataverseNotEmpty(MetadataTransactionContext ctx, String database, DataverseName dataverseName)
            throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            return metadataNode.isDataverseNotEmpty(ctx.getTxnId(), database, dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public List<Database> getDatabases(MetadataTransactionContext ctx) throws AlgebricksException {
        try {
            return metadataNode.getDatabases(ctx.getTxnId());
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public List<Dataverse> getDataverses(MetadataTransactionContext ctx) throws AlgebricksException {
        try {
            return metadataNode.getDataverses(ctx.getTxnId());
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public Dataverse getDataverse(MetadataTransactionContext ctx, String database, DataverseName dataverseName)
            throws AlgebricksException {
        //TODO(DB): change cache to consider database
        Objects.requireNonNull(database);
        // First look in the context to see if this transaction created the
        // requested dataverse itself (but the dataverse is still uncommitted).
        Dataverse dataverse = ctx.getDataverse(database, dataverseName);
        if (dataverse != null) {
            // Don't add this dataverse to the cache, since it is still
            // uncommitted.
            return dataverse;
        }
        if (ctx.dataverseIsDropped(database, dataverseName)) {
            // Dataverse has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }
        dataverse = cache.getDataverse(database, dataverseName);
        if (dataverse != null) {
            // Dataverse is already in the cache, don't add it again.
            return dataverse;
        }
        try {
            dataverse = metadataNode.getDataverse(ctx.getTxnId(), database, dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the dataverse from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (dataverse != null) {
            ctx.addDataverse(dataverse);
        }
        return dataverse;
    }

    @Override
    public List<Dataset> getDatabaseDatasets(MetadataTransactionContext ctx, String database)
            throws AlgebricksException {
        List<Dataset> databaseDatasets;
        try {
            Objects.requireNonNull(database);
            // assuming that the transaction can read its own writes on the metadata node
            databaseDatasets = metadataNode.getDatabaseDatasets(ctx.getTxnId(), database);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // don't update the cache to avoid checking against the transaction's uncommitted datasets
        return databaseDatasets;
    }

    @Override
    public List<Dataset> getDataverseDatasets(MetadataTransactionContext ctx, String database,
            DataverseName dataverseName) throws AlgebricksException {
        List<Dataset> dataverseDatasets;
        try {
            Objects.requireNonNull(database);
            // Assuming that the transaction can read its own writes on the metadata node.
            dataverseDatasets = metadataNode.getDataverseDatasets(ctx.getTxnId(), database, dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // Don't update the cache to avoid checking against the transaction's
        // uncommitted datasets.
        return dataverseDatasets;
    }

    @Override
    public void addDataset(MetadataTransactionContext ctx, Dataset dataset) throws AlgebricksException {
        // add dataset into metadataNode
        try {
            metadataNode.addDataset(ctx.getTxnId(), dataset);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // reflect the dataset into the cache
        ctx.addDataset(dataset);
    }

    @Override
    public void dropDataset(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String datasetName, boolean force) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            metadataNode.dropDataset(ctx.getTxnId(), database, dataverseName, datasetName, force);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // Drops the dataset from cache
        ctx.dropDataset(database, dataverseName, datasetName);
    }

    @Override
    public Dataset getDataset(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        Objects.requireNonNull(database);
        // First look in the context to see if this transaction created the
        // requested dataset itself (but the dataset is still uncommitted).
        Dataset dataset = ctx.getDataset(database, dataverseName, datasetName);
        if (dataset != null) {
            // Don't add this dataverse to the cache, since it is still
            // uncommitted.
            return dataset;
        }
        if (ctx.datasetIsDropped(database, dataverseName, datasetName)) {
            // Dataset has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }

        dataset = cache.getDataset(database, dataverseName, datasetName);
        if (dataset != null) {
            // Dataset is already in the cache, don't add it again.
            return dataset;
        }
        try {
            dataset = metadataNode.getDataset(ctx.getTxnId(), database, dataverseName, datasetName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the dataset from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (dataset != null) {
            ctx.addDataset(dataset);
        }
        return dataset;
    }

    @Override
    public List<Index> getDatasetIndexes(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        Objects.requireNonNull(database);
        Dataset dataset = getDataset(ctx, database, dataverseName, datasetName);
        if (dataset == null) {
            return Collections.emptyList();
        }
        List<Index> datasetIndexes;
        try {
            datasetIndexes = metadataNode.getDatasetIndexes(ctx.getTxnId(), database, dataverseName, datasetName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return datasetIndexes;
    }

    @Override
    public void addCompactionPolicy(MetadataTransactionContext mdTxnCtx, CompactionPolicy compactionPolicy)
            throws AlgebricksException {
        try {
            metadataNode.addCompactionPolicy(mdTxnCtx.getTxnId(), compactionPolicy);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.addCompactionPolicy(compactionPolicy);
    }

    @Override
    public CompactionPolicy getCompactionPolicy(MetadataTransactionContext ctx, String database,
            DataverseName dataverse, String policyName) throws AlgebricksException {
        CompactionPolicy compactionPolicy;
        try {
            Objects.requireNonNull(database);
            compactionPolicy = metadataNode.getCompactionPolicy(ctx.getTxnId(), database, dataverse, policyName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return compactionPolicy;
    }

    @Override
    public void addDatatype(MetadataTransactionContext ctx, Datatype datatype) throws AlgebricksException {
        try {
            metadataNode.addDatatype(ctx.getTxnId(), datatype);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        try {
            ctx.addDatatype(metadataNode.getDatatype(ctx.getTxnId(), datatype.getDatabaseName(),
                    datatype.getDataverseName(), datatype.getDatatypeName()));
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void dropDatatype(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String datatypeName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            metadataNode.dropDatatype(ctx.getTxnId(), database, dataverseName, datatypeName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropDataDatatype(database, dataverseName, datatypeName);
    }

    @Override
    public Datatype getDatatype(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String datatypeName) throws AlgebricksException {
        Objects.requireNonNull(database);
        // First look in the context to see if this transaction created the
        // requested datatype itself (but the datatype is still uncommitted).
        Datatype datatype = ctx.getDatatype(database, dataverseName, datatypeName);
        if (datatype != null) {
            // Don't add this dataverse to the cache, since it is still
            // uncommitted.
            return datatype;
        }
        if (ctx.datatypeIsDropped(database, dataverseName, datatypeName)) {
            // Datatype has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }

        datatype = cache.getDatatype(database, dataverseName, datatypeName);
        if (datatype != null) {
            // Datatype is already in the cache, don't add it again.
            return datatype;
        }
        try {
            datatype = metadataNode.getDatatype(ctx.getTxnId(), database, dataverseName, datatypeName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the datatype from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (datatype != null) {
            ctx.addDatatype(datatype);
        }
        return datatype;
    }

    @Override
    public void addIndex(MetadataTransactionContext ctx, Index index) throws AlgebricksException {
        try {
            metadataNode.addIndex(ctx.getTxnId(), index);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addIndex(index);
    }

    @Override
    public void addAdapter(MetadataTransactionContext mdTxnCtx, DatasourceAdapter adapter) throws AlgebricksException {
        try {
            metadataNode.addAdapter(mdTxnCtx.getTxnId(), adapter);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.addAdapter(adapter);
    }

    @Override
    public void dropIndex(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String datasetName, String indexName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            metadataNode.dropIndex(ctx.getTxnId(), database, dataverseName, datasetName, indexName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropIndex(database, dataverseName, datasetName, indexName);
    }

    @Override
    public Index getIndex(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String datasetName, String indexName) throws AlgebricksException {
        Objects.requireNonNull(database);
        // First look in the context to see if this transaction created the
        // requested index itself (but the index is still uncommitted).
        Index index = ctx.getIndex(database, dataverseName, datasetName, indexName);
        if (index != null) {
            // Don't add this index to the cache, since it is still
            // uncommitted.
            return index;
        }

        if (ctx.indexIsDropped(database, dataverseName, datasetName, indexName)) {
            // Index has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }

        index = cache.getIndex(database, dataverseName, datasetName, indexName);
        if (index != null) {
            // Index is already in the cache, don't add it again.
            return index;
        }
        try {
            index = metadataNode.getIndex(ctx.getTxnId(), database, dataverseName, datasetName, indexName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the index from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (index != null) {
            ctx.addIndex(index);
        }
        return index;
    }

    @Override
    public void addNode(MetadataTransactionContext ctx, Node node) throws AlgebricksException {
        try {
            metadataNode.addNode(ctx.getTxnId(), node);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void addNodegroup(MetadataTransactionContext ctx, NodeGroup nodeGroup) throws AlgebricksException {
        modifyNodegroup(ctx, nodeGroup, Operation.INSERT);
    }

    @Override
    public void upsertNodegroup(MetadataTransactionContext ctx, NodeGroup nodeGroup) throws AlgebricksException {
        modifyNodegroup(ctx, nodeGroup, Operation.UPSERT);
    }

    private void modifyNodegroup(MetadataTransactionContext ctx, NodeGroup nodeGroup, Operation op)
            throws AlgebricksException {
        try {
            metadataNode.modifyNodeGroup(ctx.getTxnId(), nodeGroup, op);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addNodeGroup(nodeGroup);
    }

    @Override
    public void dropNodegroup(MetadataTransactionContext ctx, String nodeGroupName, boolean failSilently)
            throws AlgebricksException {
        boolean dropped;
        try {
            dropped = metadataNode.dropNodegroup(ctx.getTxnId(), nodeGroupName, failSilently);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        if (dropped) {
            ctx.dropNodeGroup(nodeGroupName);
        }
    }

    @Override
    public NodeGroup getNodegroup(MetadataTransactionContext ctx, String nodeGroupName) throws AlgebricksException {
        // First look in the context to see if this transaction created the
        // requested dataverse itself (but the dataverse is still uncommitted).
        NodeGroup nodeGroup = ctx.getNodeGroup(nodeGroupName);
        if (nodeGroup != null) {
            // Don't add this dataverse to the cache, since it is still
            // uncommitted.
            return nodeGroup;
        }
        if (ctx.nodeGroupIsDropped(nodeGroupName)) {
            // NodeGroup has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }
        nodeGroup = cache.getNodeGroup(nodeGroupName);
        if (nodeGroup != null) {
            // NodeGroup is already in the cache, don't add it again.
            return nodeGroup;
        }
        try {
            nodeGroup = metadataNode.getNodeGroup(ctx.getTxnId(), nodeGroupName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the nodeGroup from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (nodeGroup != null) {
            ctx.addNodeGroup(nodeGroup);
        }
        return nodeGroup;
    }

    @Override
    public void addFunction(MetadataTransactionContext mdTxnCtx, Function function) throws AlgebricksException {
        try {
            metadataNode.addFunction(mdTxnCtx.getTxnId(), function);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.addFunction(function);
    }

    @Override
    public void dropFunction(MetadataTransactionContext ctx, FunctionSignature functionSignature)
            throws AlgebricksException {
        try {
            metadataNode.dropFunction(ctx.getTxnId(), functionSignature);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropFunction(functionSignature);
    }

    @Override
    public Function getFunction(MetadataTransactionContext ctx, FunctionSignature functionSignature)
            throws AlgebricksException {
        // First look in the context to see if this transaction created the
        // requested function itself (but the function is still uncommitted).
        Function function = ctx.getFunction(functionSignature);
        if (function != null) {
            // Don't add this function to the cache, since it is still
            // uncommitted.
            return function;
        }
        if (ctx.functionIsDropped(functionSignature)) {
            // Function has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }
        //TODO(DB): review this and other similar ones
        if (ctx.getDataverse(functionSignature.getDatabaseName(), functionSignature.getDataverseName()) != null) {
            // This transaction has dropped and subsequently created the same
            // dataverse.
            return null;
        }
        function = cache.getFunction(functionSignature);
        if (function != null) {
            // Function is already in the cache, don't add it again.
            return function;
        }
        try {
            function = metadataNode.getFunction(ctx.getTxnId(), functionSignature);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the function from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (function != null) {
            ctx.addFunction(function);
        }
        return function;
    }

    @Override
    public List<Function> getDataverseFunctions(MetadataTransactionContext ctx, String database,
            DataverseName dataverseName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            return metadataNode.getDataverseFunctions(ctx.getTxnId(), database, dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void addFullTextFilter(MetadataTransactionContext mdTxnCtx, FullTextFilterMetadataEntity filter)
            throws AlgebricksException {
        try {
            metadataNode.addFullTextFilter(mdTxnCtx.getTxnId(), filter);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.addFullTextFilter(filter);
    }

    @Override
    public void dropFullTextFilter(MetadataTransactionContext mdTxnCtx, String database, DataverseName dataverseName,
            String filterName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            metadataNode.dropFullTextFilter(mdTxnCtx.getTxnId(), database, dataverseName, filterName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.dropFullTextFilter(database, dataverseName, filterName);
    }

    @Override
    public FullTextFilterMetadataEntity getFullTextFilter(MetadataTransactionContext ctx, String database,
            DataverseName dataverseName, String filterName) throws AlgebricksException {
        Objects.requireNonNull(database);
        // First look in the context to see if this transaction created the
        // requested full-text filter itself (but the full-text filter is still uncommitted).
        FullTextFilterMetadataEntity filter = ctx.getFullTextFilter(database, dataverseName, filterName);
        if (filter != null) {
            // Don't add this filter to the cache, since it is still
            // uncommitted.
            return filter;
        }

        if (ctx.fullTextFilterIsDropped(database, dataverseName, filterName)) {
            // Filter has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }

        if (ctx.getDataverse(database, dataverseName) != null) {
            // This transaction has dropped and subsequently created the same
            // dataverse.
            return null;
        }

        filter = cache.getFullTextFilter(database, dataverseName, filterName);
        if (filter != null) {
            // filter is already in the cache, don't add it again.
            return filter;
        }

        try {
            filter = metadataNode.getFullTextFilter(ctx.getTxnId(), database, dataverseName, filterName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // We fetched the filter from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (filter != null) {
            ctx.addFullTextFilter(filter);
        }
        return filter;
    }

    @Override
    public void addFullTextConfig(MetadataTransactionContext mdTxnCtx,
            FullTextConfigMetadataEntity configMetadataEntity) throws AlgebricksException {
        if (Strings.isNullOrEmpty(configMetadataEntity.getFullTextConfig().getName())) {
            throw new MetadataException(ErrorCode.FULL_TEXT_CONFIG_ALREADY_EXISTS);
        }

        try {
            metadataNode.addFullTextConfig(mdTxnCtx.getTxnId(), configMetadataEntity);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.addFullTextConfig(configMetadataEntity);
    }

    @Override
    public FullTextConfigMetadataEntity getFullTextConfig(MetadataTransactionContext ctx, String database,
            DataverseName dataverseName, String configName) throws AlgebricksException {
        Objects.requireNonNull(database);
        // First look in the context to see if this transaction created the
        // requested full-text config itself (but the full-text config is still uncommitted).
        FullTextConfigMetadataEntity configMetadataEntity = ctx.getFullTextConfig(database, dataverseName, configName);
        if (configMetadataEntity != null) {
            // Don't add this config to the cache, since it is still
            // uncommitted.
            return configMetadataEntity;
        }

        if (ctx.fullTextConfigIsDropped(database, dataverseName, configName)) {
            // config has been dropped by this transaction but could still be
            // in the cache.
            return null;
        }

        if (ctx.getDataverse(database, dataverseName) != null) {
            // This transaction has dropped and subsequently created the same
            // dataverse.
            return null;
        }

        configMetadataEntity = cache.getFullTextConfig(database, dataverseName, configName);
        if (configMetadataEntity != null) {
            // config is already in the cache, don't add it again.
            return configMetadataEntity;
        }

        try {
            configMetadataEntity = metadataNode.getFullTextConfig(ctx.getTxnId(), database, dataverseName, configName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }

        // We fetched the config from the MetadataNode. Add it to the cache
        // when this transaction commits.
        if (configMetadataEntity != null) {
            ctx.addFullTextConfig(configMetadataEntity);
        }
        return configMetadataEntity;
    }

    @Override
    public void dropFullTextConfig(MetadataTransactionContext mdTxnCtx, String database, DataverseName dataverseName,
            String configName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            metadataNode.dropFullTextConfig(mdTxnCtx.getTxnId(), database, dataverseName, configName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.dropFullTextConfig(database, dataverseName, configName);
    }

    @Override
    public void addFeedPolicy(MetadataTransactionContext mdTxnCtx, FeedPolicyEntity feedPolicy)
            throws AlgebricksException {
        try {
            metadataNode.addFeedPolicy(mdTxnCtx.getTxnId(), feedPolicy);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.addFeedPolicy(feedPolicy);
    }

    @Override
    public void initializeDatasetIdFactory(MetadataTransactionContext ctx) throws AlgebricksException {
        try {
            metadataNode.initializeDatasetIdFactory(ctx.getTxnId());
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public int getMostRecentDatasetId() throws AlgebricksException {
        try {
            return metadataNode.getMostRecentDatasetId();
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void dropAdapter(MetadataTransactionContext ctx, String database, DataverseName dataverseName, String name)
            throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            metadataNode.dropAdapter(ctx.getTxnId(), database, dataverseName, name);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropAdapter(database, dataverseName, name);
    }

    @Override
    public DatasourceAdapter getAdapter(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String name) throws AlgebricksException {
        DatasourceAdapter adapter;
        try {
            Objects.requireNonNull(database);
            adapter = metadataNode.getAdapter(ctx.getTxnId(), database, dataverseName, name);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return adapter;
    }

    @Override
    public void dropLibrary(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String libraryName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            metadataNode.dropLibrary(ctx.getTxnId(), database, dataverseName, libraryName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropLibrary(database, dataverseName, libraryName);
    }

    @Override
    public List<Library> getDatabaseLibraries(MetadataTransactionContext ctx, String database)
            throws AlgebricksException {
        List<Library> databaseLibraries;
        try {
            // assuming that the transaction can read its own writes on the metadata node
            Objects.requireNonNull(database);
            databaseLibraries = metadataNode.getDatabaseLibraries(ctx.getTxnId(), database);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // don't update the cache to avoid checking against the transaction's uncommitted functions
        return databaseLibraries;
    }

    @Override
    public List<Library> getDataverseLibraries(MetadataTransactionContext ctx, String database,
            DataverseName dataverseName) throws AlgebricksException {
        List<Library> dataverseLibaries;
        try {
            // Assuming that the transaction can read its own writes on the
            // metadata node.
            Objects.requireNonNull(database);
            dataverseLibaries = metadataNode.getDataverseLibraries(ctx.getTxnId(), database, dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // Don't update the cache to avoid checking against the transaction's
        // uncommitted functions.
        return dataverseLibaries;
    }

    @Override
    public void addLibrary(MetadataTransactionContext ctx, Library library) throws AlgebricksException {
        try {
            metadataNode.addLibrary(ctx.getTxnId(), library);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addLibrary(library);
    }

    @Override
    public Library getLibrary(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String libraryName) throws AlgebricksException {
        Library library;
        try {
            Objects.requireNonNull(database);
            library = metadataNode.getLibrary(ctx.getTxnId(), database, dataverseName, libraryName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return library;
    }

    @Override
    public FeedPolicyEntity getFeedPolicy(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String policyName) throws AlgebricksException {
        FeedPolicyEntity feedPolicy;
        try {
            Objects.requireNonNull(database);
            feedPolicy = metadataNode.getFeedPolicy(ctx.getTxnId(), database, dataverseName, policyName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return feedPolicy;
    }

    @Override
    public Feed getFeed(MetadataTransactionContext ctx, String database, DataverseName dataverseName, String feedName)
            throws AlgebricksException {
        Feed feed;
        try {
            Objects.requireNonNull(database);
            feed = metadataNode.getFeed(ctx.getTxnId(), database, dataverseName, feedName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return feed;
    }

    @Override
    public List<Feed> getFeeds(MetadataTransactionContext ctx, String database, DataverseName dataverseName)
            throws AlgebricksException {
        List<Feed> feeds;
        try {
            Objects.requireNonNull(database);
            feeds = metadataNode.getFeeds(ctx.getTxnId(), database, dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return feeds;
    }

    @Override
    public void dropFeed(MetadataTransactionContext ctx, String database, DataverseName dataverseName, String feedName)
            throws AlgebricksException {
        Feed feed;
        List<FeedConnection> feedConnections;
        try {
            Objects.requireNonNull(database);
            feed = metadataNode.getFeed(ctx.getTxnId(), database, dataverseName, feedName);
            feedConnections = metadataNode.getFeedConnections(ctx.getTxnId(), database, dataverseName, feedName);
            metadataNode.dropFeed(ctx.getTxnId(), database, dataverseName, feedName);
            for (FeedConnection feedConnection : feedConnections) {
                metadataNode.dropFeedConnection(ctx.getTxnId(), database, dataverseName, feedName,
                        feedConnection.getDatasetName());
                ctx.dropFeedConnection(database, dataverseName, feedName, feedConnection.getDatasetName());
            }
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropFeed(feed);
    }

    @Override
    public void addFeed(MetadataTransactionContext ctx, Feed feed) throws AlgebricksException {
        try {
            metadataNode.addFeed(ctx.getTxnId(), feed);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addFeed(feed);
    }

    @Override
    public void addFeedConnection(MetadataTransactionContext ctx, FeedConnection feedConnection)
            throws AlgebricksException {
        try {
            metadataNode.addFeedConnection(ctx.getTxnId(), feedConnection);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.addFeedConnection(feedConnection);
    }

    @Override
    public void dropFeedConnection(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String feedName, String datasetName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            metadataNode.dropFeedConnection(ctx.getTxnId(), database, dataverseName, feedName, datasetName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        ctx.dropFeedConnection(database, dataverseName, feedName, datasetName);
    }

    @Override
    public FeedConnection getFeedConnection(MetadataTransactionContext ctx, String database,
            DataverseName dataverseName, String feedName, String datasetName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            return metadataNode.getFeedConnection(ctx.getTxnId(), database, dataverseName, feedName, datasetName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public List<FeedConnection> getFeedConections(MetadataTransactionContext ctx, String database,
            DataverseName dataverseName, String feedName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            return metadataNode.getFeedConnections(ctx.getTxnId(), database, dataverseName, feedName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public List<DatasourceAdapter> getDataverseAdapters(MetadataTransactionContext mdTxnCtx, String database,
            DataverseName dataverseName) throws AlgebricksException {
        List<DatasourceAdapter> dataverseAdapters;
        try {
            Objects.requireNonNull(database);
            dataverseAdapters = metadataNode.getDataverseAdapters(mdTxnCtx.getTxnId(), database, dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return dataverseAdapters;
    }

    @Override
    public void dropFeedPolicy(MetadataTransactionContext mdTxnCtx, String database, DataverseName dataverseName,
            String policyName) throws AlgebricksException {
        FeedPolicyEntity feedPolicy;
        try {
            Objects.requireNonNull(database);
            feedPolicy = metadataNode.getFeedPolicy(mdTxnCtx.getTxnId(), database, dataverseName, policyName);
            metadataNode.dropFeedPolicy(mdTxnCtx.getTxnId(), database, dataverseName, policyName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        mdTxnCtx.dropFeedPolicy(feedPolicy);
    }

    @Override
    public List<FeedPolicyEntity> getDataverseFeedPolicies(MetadataTransactionContext mdTxnCtx, String database,
            DataverseName dataverseName) throws AlgebricksException {
        List<FeedPolicyEntity> dataverseFeedPolicies;
        try {
            Objects.requireNonNull(database);
            dataverseFeedPolicies = metadataNode.getDataverseFeedPolicies(mdTxnCtx.getTxnId(), database, dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return dataverseFeedPolicies;
    }

    @Override
    public List<ExternalFile> getDatasetExternalFiles(MetadataTransactionContext mdTxnCtx, Dataset dataset)
            throws AlgebricksException {
        List<ExternalFile> externalFiles;
        try {
            externalFiles = metadataNode.getExternalFiles(mdTxnCtx.getTxnId(), dataset);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return externalFiles;
    }

    @Override
    public void addExternalFile(MetadataTransactionContext ctx, ExternalFile externalFile) throws AlgebricksException {
        try {
            metadataNode.addExternalFile(ctx.getTxnId(), externalFile);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void dropExternalFile(MetadataTransactionContext ctx, ExternalFile externalFile) throws AlgebricksException {
        try {
            metadataNode.dropExternalFile(ctx.getTxnId(), externalFile.getDatabaseName(),
                    externalFile.getDataverseName(), externalFile.getDatasetName(), externalFile.getFileNumber());
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public ExternalFile getExternalFile(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String datasetName, Integer fileNumber) throws AlgebricksException {
        ExternalFile file;
        try {
            Objects.requireNonNull(database);
            file = metadataNode.getExternalFile(ctx.getTxnId(), database, dataverseName, datasetName, fileNumber);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        return file;
    }

    @Override
    public void addSynonym(MetadataTransactionContext ctx, Synonym synonym) throws AlgebricksException {
        try {
            metadataNode.addSynonym(ctx.getTxnId(), synonym);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void dropSynonym(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            metadataNode.dropSynonym(ctx.getTxnId(), database, dataverseName, synonymName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public Synonym getSynonym(MetadataTransactionContext ctx, String database, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            return metadataNode.getSynonym(ctx.getTxnId(), database, dataverseName, synonymName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public List<Synonym> getDataverseSynonyms(MetadataTransactionContext ctx, String database,
            DataverseName dataverseName) throws AlgebricksException {
        try {
            Objects.requireNonNull(database);
            return metadataNode.getDataverseSynonyms(ctx.getTxnId(), database, dataverseName);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    // TODO: Optimize <-- use keys instead of object -->
    @Override
    public void dropDatasetExternalFiles(MetadataTransactionContext mdTxnCtx, Dataset dataset)
            throws AlgebricksException {
        try {
            metadataNode.dropExternalFiles(mdTxnCtx.getTxnId(), dataset);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void updateDataset(MetadataTransactionContext ctx, Dataset dataset) throws AlgebricksException {
        try {
            metadataNode.updateDataset(ctx.getTxnId(), dataset);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // reflect the dataset into the cache
        ctx.dropDataset(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName());
        ctx.addDataset(dataset);
    }

    @Override
    public void updateLibrary(MetadataTransactionContext ctx, Library library) throws AlgebricksException {
        try {
            metadataNode.updateLibrary(ctx.getTxnId(), library);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // reflect the library into the cache
        ctx.dropLibrary(library.getDatabaseName(), library.getDataverseName(), library.getName());
        ctx.addLibrary(library);
    }

    @Override
    public void updateFunction(MetadataTransactionContext ctx, Function function) throws AlgebricksException {
        try {
            metadataNode.updateFunction(ctx.getTxnId(), function);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // reflect the function into the cache
        ctx.dropFunction(function.getSignature());
        ctx.addFunction(function);
    }

    @Override
    public void updateDatatype(MetadataTransactionContext ctx, Datatype datatype) throws AlgebricksException {
        try {
            metadataNode.updateDatatype(ctx.getTxnId(), datatype);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
        // reflect the datatype into the cache
        ctx.dropDataDatatype(datatype.getDatabaseName(), datatype.getDataverseName(), datatype.getDatatypeName());
        ctx.addDatatype(datatype);
    }

    @Override
    public <T extends IExtensionMetadataEntity> void addEntity(MetadataTransactionContext mdTxnCtx, T entity)
            throws AlgebricksException {
        try {
            metadataNode.addEntity(mdTxnCtx.getTxnId(), entity);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public <T extends IExtensionMetadataEntity> void upsertEntity(MetadataTransactionContext mdTxnCtx, T entity)
            throws AlgebricksException {
        try {
            metadataNode.upsertEntity(mdTxnCtx.getTxnId(), entity);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public <T extends IExtensionMetadataEntity> void deleteEntity(MetadataTransactionContext mdTxnCtx, T entity)
            throws AlgebricksException {
        try {
            metadataNode.deleteEntity(mdTxnCtx.getTxnId(), entity);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public <T extends IExtensionMetadataEntity> List<T> getEntities(MetadataTransactionContext mdTxnCtx,
            IExtensionMetadataSearchKey searchKey) throws AlgebricksException {
        try {
            return metadataNode.getEntities(mdTxnCtx.getTxnId(), searchKey);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public JsonNode getEntitiesAsJson(MetadataTransactionContext mdTxnCtx, IMetadataIndex metadataIndex,
            int payloadPosition) throws AlgebricksException {
        try {
            return metadataNode.getEntitiesAsJson(mdTxnCtx.getTxnId(), metadataIndex, payloadPosition);
        } catch (RemoteException e) {
            throw new MetadataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
        }
    }

    @Override
    public void rebindMetadataNode() {
        rebindMetadataNode = true;
    }

    public static void initialize(IAsterixStateProxy proxy, MetadataProperties metadataProperties,
            ICcApplicationContext appCtx) {
        INSTANCE = new CCMetadataManagerImpl(proxy, metadataProperties, appCtx);
    }

    public static void initialize(Collection<IAsterixStateProxy> proxies, MetadataNode metadataNode) {
        INSTANCE = new NCMetadataManagerImpl(proxies, metadataNode);
    }

    private static class CCMetadataManagerImpl extends MetadataManager {
        private final MetadataProperties metadataProperties;
        private final ICcApplicationContext appCtx;

        CCMetadataManagerImpl(IAsterixStateProxy proxy, MetadataProperties metadataProperties,
                ICcApplicationContext appCtx) {
            super(Collections.singleton(proxy));
            this.metadataProperties = metadataProperties;
            this.appCtx = appCtx;
        }

        @Override
        protected TxnId createTxnId() {
            TxnId txnId;
            try {
                txnId = appCtx.getTxnIdFactory().create();
            } catch (AlgebricksException e) {
                throw new ACIDException(e);
            }
            return txnId;
        }

        @Override
        public long getMaxTxnId() {
            return appCtx.getTxnIdFactory().getMaxTxnId();
        }

        @Override
        public synchronized void init() throws HyracksDataException {
            if (metadataNode != null && !rebindMetadataNode) {
                return;
            }
            try {
                metadataNode = proxies.iterator().next()
                        .waitForMetadataNode(metadataProperties.getRegistrationTimeoutSecs(), TimeUnit.SECONDS);
                if (metadataNode != null) {
                    rebindMetadataNode = false;
                } else {
                    throw new HyracksDataException("The MetadataNode failed to bind before the configured timeout ("
                            + metadataProperties.getRegistrationTimeoutSecs() + " seconds); the MetadataNode was "
                            + "configured to run on NC: " + metadataProperties.getMetadataNodeName());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            } catch (RemoteException e) {
                throw new RuntimeDataException(ErrorCode.REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE, e);
            }
            super.init();
        }
    }

    private static class NCMetadataManagerImpl extends MetadataManager {
        private final ITxnIdFactory txnIdFactory;

        NCMetadataManagerImpl(Collection<IAsterixStateProxy> proxies, MetadataNode metadataNode) {
            super(proxies, metadataNode);
            txnIdFactory = metadataNode.getTxnIdFactory();
        }

        @Override
        protected TxnId createTxnId() {
            try {
                return txnIdFactory.create();
            } catch (AlgebricksException e) {
                throw new ACIDException(e);
            }
        }

        @Override
        public long getMaxTxnId() {
            return txnIdFactory.getMaxTxnId();
        }
    }
}
