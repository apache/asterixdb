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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.external.dataset.adapter.AdapterIdentifier;
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
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.runtime.fulltext.AbstractFullTextFilterDescriptor;
import org.apache.asterix.runtime.fulltext.FullTextConfigDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextFilterType;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextFilterEvaluatorFactory;

/**
 * Used to implement serializable transactions against the MetadataCache.
 * Assumes that the MetadataNode also provides serializable transactions. If the
 * MetadataNode provides weaker guarantees than serializable, then you can
 * expect update anomalies in the MetadataManager, e.g., because the
 * MetadataCache could get polluted by a transaction reading an uncommitted
 * write of another transaction, and putting the uncommitted entity in the
 * MetadataCache. Logs the logical operations of a single transaction against
 * the metadata. Once the transaction decides to commit, its log will be forward
 * rolled against the MetadataCache. If it decides to abort, then its logs are
 * simply discarded without changing the MetadataCache. Also provides a
 * transaction-local "view" of its own uncommitted changes to the MetadataCache,
 * to allow metadata transactions to read their uncommitted writes, without
 * changing the MetadataCache. For example, think of what would happen if a
 * transactions read its own uncommitted addDataset(), causing an update to the
 * MetadataCache (the MetadataCache is immediately updated on reads, following
 * the assumption that the MetadataNode won't allow reading uncommitted values).
 * Another transaction might see this uncommitted dataset in the MetadataCache.
 * This class is not thread safe. We assume that a metadata transaction consists
 * of only one thread.
 */
public class MetadataTransactionContext extends MetadataCache {

    // Keeps track of deleted metadata entities.
    // An entity can either be in the droppedCache or in the inherited members
    // of MetadataCache (the "added" entities).
    // The APIs in this class make sure that these two caches are kept in sync.
    protected MetadataCache droppedCache = new MetadataCache();

    protected ArrayList<MetadataLogicalOperation> opLog = new ArrayList<>();
    private final TxnId txnId;

    public MetadataTransactionContext(TxnId txnId) {
        this.txnId = txnId;
    }

    public TxnId getTxnId() {
        return txnId;
    }

    public void addDatabase(Database database) {
        droppedCache.dropDatabase(database);
        logAndApply(new MetadataLogicalOperation(database, true));
    }

    public void addDataverse(Dataverse dataverse) {
        droppedCache.dropDataverse(dataverse);
        logAndApply(new MetadataLogicalOperation(dataverse, true));
    }

    public void addDataset(Dataset dataset) {
        droppedCache.dropDataset(dataset);
        logAndApply(new MetadataLogicalOperation(dataset, true));
    }

    public void addIndex(Index index) {
        droppedCache.dropIndex(index);
        logAndApply(new MetadataLogicalOperation(index, true));
    }

    public void addDatatype(Datatype datatype) {
        droppedCache.dropDatatype(datatype);
        logAndApply(new MetadataLogicalOperation(datatype, true));
    }

    public void addNodeGroup(NodeGroup nodeGroup) {
        droppedCache.dropNodeGroup(nodeGroup);
        logAndApply(new MetadataLogicalOperation(nodeGroup, true));
    }

    public void addFunction(Function function) {
        droppedCache.dropFunction(function);
        logAndApply(new MetadataLogicalOperation(function, true));
    }

    public void addFullTextFilter(FullTextFilterMetadataEntity filterMetadataEntity) {
        droppedCache.dropFullTextFilter(filterMetadataEntity);
        logAndApply(new MetadataLogicalOperation(filterMetadataEntity, true));
    }

    public void addFullTextConfig(FullTextConfigMetadataEntity configMetadataEntity) {
        droppedCache.dropFullTextConfig(configMetadataEntity);
        logAndApply(new MetadataLogicalOperation(configMetadataEntity, true));
    }

    public void addAdapter(DatasourceAdapter adapter) {
        droppedCache.dropAdapterIfExists(adapter);
        logAndApply(new MetadataLogicalOperation(adapter, true));
    }

    public void addCompactionPolicy(CompactionPolicy compactionPolicy) {
        droppedCache.dropCompactionPolicy(compactionPolicy);
        logAndApply(new MetadataLogicalOperation(compactionPolicy, true));
    }

    public void addLibrary(Library library) {
        droppedCache.dropLibrary(library);
        logAndApply(new MetadataLogicalOperation(library, true));
    }

    public void addFeedPolicy(FeedPolicyEntity feedPolicy) {
        droppedCache.dropFeedPolicy(feedPolicy);
        logAndApply(new MetadataLogicalOperation(feedPolicy, true));

    }

    public void addFeed(Feed feed) {
        droppedCache.dropFeedIfExists(feed);
        logAndApply(new MetadataLogicalOperation(feed, true));
    }

    public void addFeedConnection(FeedConnection feedConnection) {
        droppedCache.dropFeedConnection(feedConnection);
        logAndApply(new MetadataLogicalOperation(feedConnection, true));
    }

    public void dropDataset(String database, DataverseName dataverseName, String datasetName) {
        Dataset dataset = new Dataset(database, dataverseName, datasetName, null, null, null, null, null, null, null,
                null, null, -1, MetadataUtil.PENDING_NO_OP);
        droppedCache.addDatasetIfNotExists(dataset);
        logAndApply(new MetadataLogicalOperation(dataset, false));
    }

    public void dropIndex(String database, DataverseName dataverseName, String datasetName, String indexName) {
        Index index = new Index(database, dataverseName, datasetName, indexName, null, null, false, false,
                MetadataUtil.PENDING_NO_OP);
        droppedCache.addIndexIfNotExists(index);
        logAndApply(new MetadataLogicalOperation(index, false));
    }

    public void dropDatabase(String databaseName) {
        Database database = new Database(databaseName, false, MetadataUtil.PENDING_NO_OP);
        droppedCache.addDatabaseIfNotExists(database);
        logAndApply(new MetadataLogicalOperation(database, false));
    }

    public void dropDataverse(String database, DataverseName dataverseName) {
        Dataverse dataverse = new Dataverse(database, dataverseName, null, MetadataUtil.PENDING_NO_OP);
        droppedCache.addDataverseIfNotExists(dataverse);
        logAndApply(new MetadataLogicalOperation(dataverse, false));
    }

    public void dropDataDatatype(String database, DataverseName dataverseName, String datatypeName) {
        Datatype datatype = new Datatype(database, dataverseName, datatypeName, null, false);
        droppedCache.addDatatypeIfNotExists(datatype);
        logAndApply(new MetadataLogicalOperation(datatype, false));
    }

    public void dropNodeGroup(String nodeGroupName) {
        NodeGroup nodeGroup = new NodeGroup(nodeGroupName, null);
        droppedCache.addOrUpdateNodeGroup(nodeGroup);
        logAndApply(new MetadataLogicalOperation(nodeGroup, false));
    }

    public void dropFunction(FunctionSignature signature) {
        Function function = new Function(signature, null, null, null, null, null, null, null, null, null, null, false,
                false, null, null);
        droppedCache.addFunctionIfNotExists(function);
        logAndApply(new MetadataLogicalOperation(function, false));
    }

    public void dropFullTextConfig(String database, DataverseName dataverseName, String configName) {
        FullTextConfigDescriptor config = new FullTextConfigDescriptor(database, dataverseName, configName, null, null);
        FullTextConfigMetadataEntity configMetadataEntity = new FullTextConfigMetadataEntity(config);

        droppedCache.addFullTextConfigIfNotExists(configMetadataEntity);
        logAndApply(new MetadataLogicalOperation(configMetadataEntity, false));
    }

    public void dropFullTextFilter(String database, DataverseName dataverseName, String filterName) {
        AbstractFullTextFilterDescriptor filter =
                new AbstractFullTextFilterDescriptor(database, dataverseName, filterName) {
                    private static final long serialVersionUID = -8222222581298765902L;

                    @Override
                    public FullTextFilterType getFilterType() {
                        return null;
                    }

                    @Override
                    public IFullTextFilterEvaluatorFactory createEvaluatorFactory() {
                        return null;
                    }
                };
        FullTextFilterMetadataEntity filterMetadataEntity = new FullTextFilterMetadataEntity(filter);
        droppedCache.addFullTextFilterIfNotExists(filterMetadataEntity);
        logAndApply(new MetadataLogicalOperation(filterMetadataEntity, false));
    }

    public void dropAdapter(String database, DataverseName dataverseName, String adapterName) {
        AdapterIdentifier adapterIdentifier = new AdapterIdentifier(database, dataverseName, adapterName);
        DatasourceAdapter adapter = new DatasourceAdapter(adapterIdentifier, null, null, null, null, null);
        droppedCache.addAdapterIfNotExists(adapter);
        logAndApply(new MetadataLogicalOperation(adapter, false));
    }

    public void dropLibrary(String database, DataverseName dataverseName, String libraryName) {
        Library library = new Library(database, dataverseName, libraryName, null, null, MetadataUtil.PENDING_NO_OP);
        droppedCache.addLibraryIfNotExists(library);
        logAndApply(new MetadataLogicalOperation(library, false));
    }

    public void dropFeed(Feed feed) {
        droppedCache.addFeedIfNotExists(feed);
        logAndApply(new MetadataLogicalOperation(feed, false));
    }

    public void dropFeedConnection(String database, DataverseName dataverseName, String feedName, String datasetName) {
        FeedConnection feedConnection =
                new FeedConnection(database, dataverseName, feedName, datasetName, null, null, null, null);
        droppedCache.addFeedConnectionIfNotExists(feedConnection);
        logAndApply(new MetadataLogicalOperation(feedConnection, false));
    }

    public void logAndApply(MetadataLogicalOperation op) {
        opLog.add(op);
        doOperation(op);
    }

    public boolean databaseIsDropped(String databaseName) {
        return droppedCache.getDatabase(databaseName) != null;
    }

    public boolean dataverseIsDropped(String databaseName, DataverseName dataverseName) {
        if (droppedCache.getDatabase(databaseName) != null) {
            return true;
        }
        return droppedCache.getDataverse(databaseName, dataverseName) != null;
    }

    public boolean datasetIsDropped(String databaseName, DataverseName dataverseName, String datasetName) {
        if (droppedCache.getDatabase(databaseName) != null) {
            return true;
        }
        if (droppedCache.getDataverse(databaseName, dataverseName) != null) {
            return true;
        }
        return droppedCache.getDataset(databaseName, dataverseName, datasetName) != null;
    }

    public boolean indexIsDropped(String databaseName, DataverseName dataverseName, String datasetName,
            String indexName) {
        if (droppedCache.getDatabase(databaseName) != null) {
            return true;
        }
        if (droppedCache.getDataverse(databaseName, dataverseName) != null) {
            return true;
        }
        if (droppedCache.getDataset(databaseName, dataverseName, datasetName) != null) {
            return true;
        }
        return droppedCache.getIndex(databaseName, dataverseName, datasetName, indexName) != null;
    }

    public boolean datatypeIsDropped(String databaseName, DataverseName dataverseName, String datatypeName) {
        if (droppedCache.getDatabase(databaseName) != null) {
            return true;
        }
        if (droppedCache.getDataverse(databaseName, dataverseName) != null) {
            return true;
        }
        return droppedCache.getDatatype(databaseName, dataverseName, datatypeName) != null;
    }

    public boolean nodeGroupIsDropped(String nodeGroup) {
        return droppedCache.getNodeGroup(nodeGroup) != null;
    }

    public boolean functionIsDropped(FunctionSignature functionSignature) {
        //TODO(DB): check database and dataverse first?
        return droppedCache.getFunction(functionSignature) != null;
    }

    public boolean fullTextConfigIsDropped(String databaseName, DataverseName dataverseName, String configName) {
        //TODO(DB): check database and dataverse first?
        return droppedCache.getFullTextConfig(databaseName, dataverseName, configName) != null;
    }

    public boolean fullTextFilterIsDropped(String databaseName, DataverseName dataverseName, String filterName) {
        //TODO(DB): check database and dataverse first?
        return droppedCache.getFullTextFilter(databaseName, dataverseName, filterName) != null;
    }

    public List<MetadataLogicalOperation> getOpLog() {
        return opLog;
    }

    @Override
    public void clear() {
        super.clear();
        droppedCache.clear();
        opLog.clear();
    }
}
