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

package edu.uci.ics.asterix.metadata;

import java.util.ArrayList;

import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.external.dataset.adapter.AdapterIdentifier;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;
import edu.uci.ics.asterix.metadata.entities.CompactionPolicy;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;

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
        Dataset dataset = new Dataset(dataverseName, datasetName, null, null, null, null, -1,
                IMetadataEntity.PENDING_NO_OP);
        droppedCache.addDatasetIfNotExists(dataset);
        logAndApply(new MetadataLogicalOperation(dataset, false));
    }

    public void dropIndex(String dataverseName, String datasetName, String indexName) {
        Index index = new Index(dataverseName, datasetName, indexName, null, null, false, IMetadataEntity.PENDING_NO_OP);
        droppedCache.addIndexIfNotExists(index);
        logAndApply(new MetadataLogicalOperation(index, false));
    }

    public void dropDataverse(String dataverseName) {
        Dataverse dataverse = new Dataverse(dataverseName, null, IMetadataEntity.PENDING_NO_OP);
        droppedCache.addDataverseIfNotExists(dataverse);
        logAndApply(new MetadataLogicalOperation(dataverse, false));
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

    @Override
    public void clear() {
        super.clear();
        droppedCache.clear();
        opLog.clear();
    }
}
