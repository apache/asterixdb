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

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.Feed.FeedType;
import org.apache.asterix.metadata.entities.FeedPolicy;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entities.PrimaryFeed;
import org.apache.asterix.metadata.entities.SecondaryFeed;
import org.apache.asterix.metadata.feeds.AdapterIdentifier;

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

    protected ArrayList<MetadataLogicalOperation> opLog = new ArrayList<MetadataLogicalOperation>();
    private final JobId jobId;

    public MetadataTransactionContext(JobId jobId) {
        this.jobId = jobId;
    }

    public JobId getJobId() {
        return jobId;
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

    public void addNogeGroup(NodeGroup nodeGroup) {
        droppedCache.dropNodeGroup(nodeGroup);
        logAndApply(new MetadataLogicalOperation(nodeGroup, true));
    }

    public void addFunction(Function function) {
        droppedCache.dropFunction(function);
        logAndApply(new MetadataLogicalOperation(function, true));
    }

    public void addAdapter(DatasourceAdapter adapter) {
        droppedCache.dropAdapter(adapter);
        logAndApply(new MetadataLogicalOperation(adapter, true));
    }

    public void addCompactionPolicy(CompactionPolicy compactionPolicy) {
        droppedCache.dropCompactionPolicy(compactionPolicy);
        logAndApply(new MetadataLogicalOperation(compactionPolicy, true));

    }

    public void dropDataset(String dataverseName, String datasetName) {
        Dataset dataset = new Dataset(dataverseName, datasetName, null, null, null, null, null, null, null, null, -1,
                IMetadataEntity.PENDING_NO_OP);
        droppedCache.addDatasetIfNotExists(dataset);
        logAndApply(new MetadataLogicalOperation(dataset, false));
    }

    public void dropIndex(String dataverseName, String datasetName, String indexName) {
        Index index = new Index(dataverseName, datasetName, indexName, null, null, null, false, false,
                IMetadataEntity.PENDING_NO_OP);
        droppedCache.addIndexIfNotExists(index);
        logAndApply(new MetadataLogicalOperation(index, false));
    }

    public void dropDataverse(String dataverseName) {
        Dataverse dataverse = new Dataverse(dataverseName, null, IMetadataEntity.PENDING_NO_OP);
        droppedCache.addDataverseIfNotExists(dataverse);
        logAndApply(new MetadataLogicalOperation(dataverse, false));
    }

    public void addLibrary(Library library) {
        droppedCache.dropLibrary(library);
        logAndApply(new MetadataLogicalOperation(library, true));
    }

    public void dropDataDatatype(String dataverseName, String datatypeName) {
        Datatype datatype = new Datatype(dataverseName, datatypeName, null, false);
        droppedCache.addDatatypeIfNotExists(datatype);
        logAndApply(new MetadataLogicalOperation(datatype, false));
    }

    public void dropNodeGroup(String nodeGroupName) {
        NodeGroup nodeGroup = new NodeGroup(nodeGroupName, null);
        droppedCache.addNodeGroupIfNotExists(nodeGroup);
        logAndApply(new MetadataLogicalOperation(nodeGroup, false));
    }

    public void dropFunction(FunctionSignature signature) {
        Function function = new Function(signature.getNamespace(), signature.getName(), signature.getArity(), null,
                null, null, null, null);
        droppedCache.addFunctionIfNotExists(function);
        logAndApply(new MetadataLogicalOperation(function, false));
    }

    public void dropAdapter(String dataverseName, String adapterName) {
        AdapterIdentifier adapterIdentifier = new AdapterIdentifier(dataverseName, adapterName);
        DatasourceAdapter adapter = new DatasourceAdapter(adapterIdentifier, null, null);
        droppedCache.addAdapterIfNotExists(adapter);
        logAndApply(new MetadataLogicalOperation(adapter, false));
    }

    public void dropLibrary(String dataverseName, String libraryName) {
        Library library = new Library(dataverseName, libraryName);
        droppedCache.addLibraryIfNotExists(library);
        logAndApply(new MetadataLogicalOperation(library, false));
    }

    public void logAndApply(MetadataLogicalOperation op) {
        opLog.add(op);
        doOperation(op);
    }

    public boolean dataverseIsDropped(String dataverseName) {
        return droppedCache.getDataverse(dataverseName) != null;
    }

    public boolean datasetIsDropped(String dataverseName, String datasetName) {
        if (droppedCache.getDataverse(dataverseName) != null) {
            return true;
        }
        return droppedCache.getDataset(dataverseName, datasetName) != null;
    }

    public boolean indexIsDropped(String dataverseName, String datasetName, String indexName) {
        if (droppedCache.getDataverse(dataverseName) != null) {
            return true;
        }
        if (droppedCache.getDataset(dataverseName, datasetName) != null) {
            return true;
        }
        return droppedCache.getIndex(dataverseName, datasetName, indexName) != null;
    }

    public boolean datatypeIsDropped(String dataverseName, String datatypeName) {
        if (droppedCache.getDataverse(dataverseName) != null) {
            return true;
        }
        return droppedCache.getDatatype(dataverseName, datatypeName) != null;
    }

    public boolean nodeGroupIsDropped(String nodeGroup) {
        return droppedCache.getNodeGroup(nodeGroup) != null;
    }

    public boolean functionIsDropped(FunctionSignature functionSignature) {
        return droppedCache.getFunction(functionSignature) != null;
    }

    public ArrayList<MetadataLogicalOperation> getOpLog() {
        return opLog;
    }

    public void addFeedPolicy(FeedPolicy feedPolicy) {
        droppedCache.dropFeedPolicy(feedPolicy);
        logAndApply(new MetadataLogicalOperation(feedPolicy, true));

    }

    public void addFeed(Feed feed) {
        droppedCache.dropFeed(feed);
        logAndApply(new MetadataLogicalOperation(feed, true));

    }

    public void dropFeed(String dataverseName, String feedName, FeedType feedType) {
        Feed feed = null;
        switch (feedType) {
            case PRIMARY:
                feed = new PrimaryFeed(dataverseName, feedName, null, null, null);
                break;
            case SECONDARY:
                feed = new SecondaryFeed(dataverseName, feedName, null, null);
                break;
        }
        droppedCache.addFeedIfNotExists(feed);
        logAndApply(new MetadataLogicalOperation(feed, false));
    }

    @Override
    public void clear() {
        super.clear();
        droppedCache.clear();
        opLog.clear();
    }

}
