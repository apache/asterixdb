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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.api.IMetadataEntity;
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
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.runtime.fulltext.FullTextConfigDescriptor;

/**
 * Caches metadata entities such that the MetadataManager does not have to
 * contact the MetadataNode. The cache is updated transactionally via logical
 * logging in the MetadataTransactionContext. Note that transaction abort is
 * simply ignored, i.e., updates are not applied to the cache.
 */
public class MetadataCache {

    protected final Map<String, Database> databases = new HashMap<>();
    // Key is dataverse name.
    protected final Map<String, Map<DataverseName, Dataverse>> dataverses = new HashMap<>();
    // Key is dataverse name. Key of value map is dataset name.
    protected final Map<String, Map<DataverseName, Map<String, Dataset>>> datasets = new HashMap<>();
    // Key is dataverse name. Key of value map is dataset name. Key of value map of value map is index name.
    protected final Map<String, Map<DataverseName, Map<String, Map<String, Index>>>> indexes = new HashMap<>();
    // Key is dataverse name. Key of value map is datatype name.
    protected final Map<String, Map<DataverseName, Map<String, Datatype>>> datatypes = new HashMap<>();
    // Key is node group name.
    protected final Map<String, NodeGroup> nodeGroups = new HashMap<>();
    // Key is function Identifier . Key of value map is function name.
    protected final Map<FunctionSignature, Function> functions = new HashMap<>();
    // Key is adapter dataverse name. Key of value map is the adapter name
    protected final Map<String, Map<DataverseName, Map<String, DatasourceAdapter>>> adapters = new HashMap<>();

    // Key is DataverseName, Key of the value map is the Policy name
    protected final Map<String, Map<DataverseName, Map<String, FeedPolicyEntity>>> feedPolicies = new HashMap<>();
    // Key is library dataverse. Key of value map is the library name
    protected final Map<String, Map<DataverseName, Map<String, Library>>> libraries = new HashMap<>();
    // Key is library dataverse. Key of value map is the feed name
    protected final Map<String, Map<DataverseName, Map<String, Feed>>> feeds = new HashMap<>();
    // Key is DataverseName, Key of the value map is the Policy name
    protected final Map<String, Map<DataverseName, Map<String, CompactionPolicy>>> compactionPolicies = new HashMap<>();
    // Key is DataverseName, Key of value map is feedConnectionId
    protected final Map<String, Map<DataverseName, Map<String, FeedConnection>>> feedConnections = new HashMap<>();
    // Key is synonym dataverse. Key of value map is the synonym name
    protected final Map<String, Map<DataverseName, Map<String, Synonym>>> synonyms = new HashMap<>();
    // Key is DataverseName. Key of value map is the full-text filter name
    protected final Map<String, Map<DataverseName, Map<String, FullTextFilterMetadataEntity>>> fullTextFilters =
            new HashMap<>();
    // Key is DataverseName. Key of value map is the full-text config name
    protected final Map<String, Map<DataverseName, Map<String, FullTextConfigMetadataEntity>>> fullTextConfigs =
            new HashMap<>();

    // Atomically executes all metadata operations in ctx's log.
    public void commit(MetadataTransactionContext ctx) {
        // Forward roll the operations written in ctx's log.
        int logIx = 0;
        List<MetadataLogicalOperation> opLog = ctx.getOpLog();
        try {
            for (logIx = 0; logIx < opLog.size(); logIx++) {
                doOperation(opLog.get(logIx));
            }
        } catch (Exception e) {
            // Undo operations.
            try {
                for (int i = logIx - 1; i >= 0; i--) {
                    undoOperation(opLog.get(i));
                }
            } catch (Exception e2) {
                // We encountered an error in undo. This case should never
                // happen. Our only remedy to ensure cache consistency
                // is to clear everything.
                clear();
            }
        } finally {
            ctx.clear();
        }
    }

    public void clear() {
        synchronized (databases) {
            synchronized (dataverses) {
                synchronized (nodeGroups) {
                    synchronized (datasets) {
                        synchronized (indexes) {
                            synchronized (datatypes) {
                                synchronized (functions) {
                                    synchronized (fullTextConfigs) {
                                        synchronized (fullTextFilters) {
                                            synchronized (adapters) {
                                                synchronized (libraries) {
                                                    synchronized (compactionPolicies) {
                                                        synchronized (synonyms) {
                                                            databases.clear();
                                                            dataverses.clear();
                                                            nodeGroups.clear();
                                                            datasets.clear();
                                                            indexes.clear();
                                                            datatypes.clear();
                                                            functions.clear();
                                                            fullTextConfigs.clear();
                                                            fullTextFilters.clear();
                                                            adapters.clear();
                                                            libraries.clear();
                                                            compactionPolicies.clear();
                                                            synonyms.clear();
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public Database addDatabaseIfNotExists(Database database) {
        synchronized (databases) {
            synchronized (dataverses) {
                synchronized (datasets) {
                    synchronized (datatypes) {
                        String databaseName = database.getDatabaseName();
                        if (!databases.containsKey(databaseName)) {
                            dataverses.put(databaseName, new HashMap<>());
                            datasets.put(databaseName, new HashMap<>());
                            datatypes.put(databaseName, new HashMap<>());
                            adapters.put(databaseName, new HashMap<>());
                            return databases.put(databaseName, database);
                        }
                        return null;
                    }
                }
            }
        }
    }

    public Dataverse addDataverseIfNotExists(Dataverse dataverse) {
        synchronized (dataverses) {
            synchronized (datasets) {
                synchronized (datatypes) {
                    String databaseName = dataverse.getDatabaseName();
                    Map<DataverseName, Dataverse> databaseDataverses =
                            dataverses.computeIfAbsent(databaseName, k -> new HashMap<>());
                    DataverseName dataverseName = dataverse.getDataverseName();
                    if (!databaseDataverses.containsKey(dataverseName)) {
                        //TODO(DB): why are we clearing 'datasets', 'datatypes' and 'adapters'? should it be done for db
                        Map<DataverseName, Map<String, Dataset>> dataverseDatasets =
                                datasets.computeIfAbsent(databaseName, k -> new HashMap<>());
                        dataverseDatasets.put(dataverseName, new HashMap<>());

                        Map<DataverseName, Map<String, Datatype>> dataverseDatatypes =
                                datatypes.computeIfAbsent(databaseName, k -> new HashMap<>());
                        dataverseDatatypes.put(dataverseName, new HashMap<>());

                        Map<DataverseName, Map<String, DatasourceAdapter>> dataverseAdapters =
                                adapters.computeIfAbsent(databaseName, k -> new HashMap<>());
                        dataverseAdapters.put(dataverseName, new HashMap<>());

                        return databaseDataverses.put(dataverseName, dataverse);
                    }
                    return null;
                }
            }
        }
    }

    public Dataset addDatasetIfNotExists(Dataset dataset) {
        synchronized (datasets) {
            synchronized (indexes) {
                // Add the primary index associated with the dataset, if the dataset is an
                // internal dataset.
                if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                    Index index = IndexUtil.getPrimaryIndex(dataset);
                    addIndexIfNotExistsInternal(index);
                }

                Map<DataverseName, Map<String, Dataset>> databaseDataverses =
                        datasets.computeIfAbsent(dataset.getDatabaseName(), k -> new HashMap<>());
                Map<String, Dataset> dataverseDatasets =
                        databaseDataverses.computeIfAbsent(dataset.getDataverseName(), k -> new HashMap<>());
                if (!dataverseDatasets.containsKey(dataset.getDatasetName())) {
                    return dataverseDatasets.put(dataset.getDatasetName(), dataset);
                }
                return null;
            }
        }
    }

    public Index addIndexIfNotExists(Index index) {
        synchronized (indexes) {
            return addIndexIfNotExistsInternal(index);
        }
    }

    public Datatype addDatatypeIfNotExists(Datatype datatype) {
        synchronized (datatypes) {
            Map<DataverseName, Map<String, Datatype>> databaseDataverses =
                    datatypes.computeIfAbsent(datatype.getDatabaseName(), k -> new HashMap<>());
            Map<String, Datatype> dataverseDatatypes =
                    databaseDataverses.computeIfAbsent(datatype.getDataverseName(), k -> new HashMap<>());
            if (!dataverseDatatypes.containsKey(datatype.getDatatypeName())) {
                return dataverseDatatypes.put(datatype.getDatatypeName(), datatype);
            }
            return null;
        }
    }

    public NodeGroup addOrUpdateNodeGroup(NodeGroup nodeGroup) {
        synchronized (nodeGroups) {
            return nodeGroups.put(nodeGroup.getNodeGroupName(), nodeGroup);
        }
    }

    public CompactionPolicy addCompactionPolicyIfNotExists(CompactionPolicy compactionPolicy) {
        synchronized (compactionPolicies) {
            Map<DataverseName, Map<String, CompactionPolicy>> databaseDataverses =
                    compactionPolicies.computeIfAbsent(compactionPolicy.getDatabaseName(), k -> new HashMap<>());
            Map<String, CompactionPolicy> dataverseCompactionPolicies =
                    databaseDataverses.computeIfAbsent(compactionPolicy.getDataverseName(), k -> new HashMap<>());
            if (!dataverseCompactionPolicies.containsKey(compactionPolicy.getPolicyName())) {
                return dataverseCompactionPolicies.put(compactionPolicy.getPolicyName(), compactionPolicy);
            }
            return null;
        }
    }

    public Database dropDatabase(Database database) {
        synchronized (databases) {
            synchronized (dataverses) {
                synchronized (datasets) {
                    synchronized (indexes) {
                        synchronized (datatypes) {
                            synchronized (functions) {
                                synchronized (fullTextConfigs) {
                                    synchronized (fullTextFilters) {
                                        synchronized (adapters) {
                                            synchronized (libraries) {
                                                synchronized (feeds) {
                                                    synchronized (compactionPolicies) {
                                                        synchronized (synonyms) {
                                                            String databaseName = database.getDatabaseName();
                                                            synonyms.remove(databaseName);
                                                            compactionPolicies.remove(databaseName);
                                                            //TODO(DB): how about feedConnections, feedPolicies?
                                                            feeds.remove(databaseName);
                                                            libraries.remove(databaseName);
                                                            adapters.remove(databaseName);
                                                            fullTextFilters.remove(databaseName);
                                                            fullTextConfigs.remove(databaseName);
                                                            datatypes.remove(databaseName);
                                                            indexes.remove(databaseName);
                                                            datasets.remove(databaseName);
                                                            dataverses.remove(databaseName);

                                                            List<FunctionSignature> markedFunctionsForRemoval =
                                                                    new ArrayList<>();
                                                            for (FunctionSignature signature : functions.keySet()) {
                                                                if (signature.getDatabaseName().equals(databaseName)) {
                                                                    markedFunctionsForRemoval.add(signature);
                                                                }
                                                            }
                                                            for (FunctionSignature signature : markedFunctionsForRemoval) {
                                                                functions.remove(signature);
                                                            }
                                                            return databases.remove(databaseName);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public Dataverse dropDataverse(Dataverse dataverse) {
        synchronized (dataverses) {
            synchronized (datasets) {
                synchronized (indexes) {
                    synchronized (datatypes) {
                        synchronized (functions) {
                            synchronized (fullTextConfigs) {
                                synchronized (fullTextFilters) {
                                    synchronized (adapters) {
                                        synchronized (libraries) {
                                            synchronized (feeds) {
                                                synchronized (compactionPolicies) {
                                                    synchronized (synonyms) {
                                                        String databaseName = dataverse.getDatabaseName();
                                                        DataverseName dataverseName = dataverse.getDataverseName();
                                                        Map<DataverseName, Map<String, Dataset>> ds =
                                                                datasets.get(databaseName);
                                                        if (ds != null) {
                                                            ds.remove(dataverseName);
                                                        }
                                                        Map<DataverseName, Map<String, Map<String, Index>>> idx =
                                                                indexes.get(databaseName);
                                                        if (idx != null) {
                                                            idx.remove(dataverseName);
                                                        }
                                                        Map<DataverseName, Map<String, Datatype>> dt =
                                                                datatypes.get(databaseName);
                                                        if (dt != null) {
                                                            dt.remove(dataverseName);
                                                        }
                                                        Map<DataverseName, Map<String, DatasourceAdapter>> ad =
                                                                adapters.get(databaseName);
                                                        if (ad != null) {
                                                            ad.remove(dataverseName);
                                                        }
                                                        Map<DataverseName, Map<String, CompactionPolicy>> cp =
                                                                compactionPolicies.get(databaseName);
                                                        if (cp != null) {
                                                            cp.remove(dataverseName);
                                                        }

                                                        List<FunctionSignature> markedFunctionsForRemoval =
                                                                new ArrayList<>();
                                                        for (FunctionSignature signature : functions.keySet()) {
                                                            if (signature.getDatabaseName().equals(databaseName)
                                                                    && signature.getDataverseName()
                                                                            .equals(dataverseName)) {
                                                                markedFunctionsForRemoval.add(signature);
                                                            }
                                                        }
                                                        for (FunctionSignature signature : markedFunctionsForRemoval) {
                                                            functions.remove(signature);
                                                        }
                                                        Map<DataverseName, Map<String, FullTextConfigMetadataEntity>> ftc =
                                                                fullTextConfigs.get(databaseName);
                                                        if (ftc != null) {
                                                            ftc.remove(dataverseName);
                                                        }
                                                        Map<DataverseName, Map<String, FullTextFilterMetadataEntity>> ftf =
                                                                fullTextFilters.get(databaseName);
                                                        if (ftf != null) {
                                                            ftf.remove(dataverseName);
                                                        }
                                                        Map<DataverseName, Map<String, Library>> lib =
                                                                libraries.get(databaseName);
                                                        if (lib != null) {
                                                            lib.remove(dataverseName);
                                                        }
                                                        //TODO(DB): how about feedConnections, feedPolicies?
                                                        Map<DataverseName, Map<String, Feed>> fd =
                                                                feeds.get(databaseName);
                                                        if (fd != null) {
                                                            fd.remove(dataverseName);
                                                        }
                                                        Map<DataverseName, Map<String, Synonym>> syn =
                                                                synonyms.get(databaseName);
                                                        if (syn != null) {
                                                            syn.remove(dataverseName);
                                                        }
                                                        Map<DataverseName, Dataverse> dv = dataverses.get(databaseName);
                                                        return dv == null ? null : dv.remove(dataverseName);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public Dataset dropDataset(Dataset dataset) {
        synchronized (datasets) {
            synchronized (indexes) {

                //remove the indexes of the dataset from indexes' cache
                Map<DataverseName, Map<String, Map<String, Index>>> idxDb = indexes.get(dataset.getDatabaseName());
                if (idxDb != null) {
                    Map<String, Map<String, Index>> datasetMap = idxDb.get(dataset.getDataverseName());
                    if (datasetMap != null) {
                        datasetMap.remove(dataset.getDatasetName());
                    }
                }

                //remove the dataset from datasets' cache
                Map<DataverseName, Map<String, Dataset>> dsDb = datasets.get(dataset.getDatabaseName());
                if (dsDb == null) {
                    return null;
                }
                Map<String, Dataset> m = dsDb.get(dataset.getDataverseName());
                if (m == null) {
                    return null;
                }
                return m.remove(dataset.getDatasetName());
            }
        }
    }

    public Index dropIndex(Index index) {
        synchronized (indexes) {
            Map<DataverseName, Map<String, Map<String, Index>>> databaseDataverses =
                    indexes.get(index.getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, Map<String, Index>> datasetMap = databaseDataverses.get(index.getDataverseName());
            if (datasetMap == null) {
                return null;
            }

            Map<String, Index> indexMap = datasetMap.get(index.getDatasetName());
            if (indexMap == null) {
                return null;
            }
            return indexMap.remove(index.getIndexName());
        }
    }

    public Datatype dropDatatype(Datatype datatype) {
        synchronized (datatypes) {
            Map<DataverseName, Map<String, Datatype>> databaseDataverses = datatypes.get(datatype.getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, Datatype> m = databaseDataverses.get(datatype.getDataverseName());
            if (m == null) {
                return null;
            }
            return m.remove(datatype.getDatatypeName());
        }
    }

    public CompactionPolicy dropCompactionPolicy(CompactionPolicy compactionPolicy) {
        synchronized (compactionPolicies) {
            Map<DataverseName, Map<String, CompactionPolicy>> databaseDataverses =
                    compactionPolicies.get(compactionPolicy.getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, CompactionPolicy> p = databaseDataverses.get(compactionPolicy.getDataverseName());
            if (p != null && p.get(compactionPolicy.getPolicyName()) != null) {
                return p.remove(compactionPolicy.getPolicyName());
            }
            return null;
        }
    }

    public NodeGroup dropNodeGroup(NodeGroup nodeGroup) {
        synchronized (nodeGroups) {
            return nodeGroups.remove(nodeGroup.getNodeGroupName());
        }
    }

    public Database getDatabase(String databaseName) {
        synchronized (databases) {
            return databases.get(databaseName);
        }
    }

    public Dataverse getDataverse(String databaseName, DataverseName dataverseName) {
        synchronized (dataverses) {
            Map<DataverseName, Dataverse> db = dataverses.get(databaseName);
            return db == null ? null : db.get(dataverseName);
        }
    }

    public Dataset getDataset(String databaseName, DataverseName dataverseName, String datasetName) {
        synchronized (datasets) {
            Map<DataverseName, Map<String, Dataset>> db = datasets.get(databaseName);
            if (db == null) {
                return null;
            }
            Map<String, Dataset> dv = db.get(dataverseName);
            return dv == null ? null : dv.get(datasetName);
        }
    }

    public Index getIndex(String databaseName, DataverseName dataverseName, String datasetName, String indexName) {
        synchronized (indexes) {
            Map<DataverseName, Map<String, Map<String, Index>>> db = indexes.get(databaseName);
            if (db == null) {
                return null;
            }
            Map<String, Map<String, Index>> datasetMap = db.get(dataverseName);
            if (datasetMap == null) {
                return null;
            }
            Map<String, Index> indexMap = datasetMap.get(datasetName);
            if (indexMap == null) {
                return null;
            }
            return indexMap.get(indexName);
        }
    }

    public Datatype getDatatype(String databaseName, DataverseName dataverseName, String datatypeName) {
        synchronized (datatypes) {
            Map<DataverseName, Map<String, Datatype>> db = datatypes.get(databaseName);
            if (db == null) {
                return null;
            }
            Map<String, Datatype> m = db.get(dataverseName);
            if (m == null) {
                return null;
            }
            return m.get(datatypeName);
        }
    }

    public NodeGroup getNodeGroup(String nodeGroupName) {
        synchronized (nodeGroups) {
            return nodeGroups.get(nodeGroupName);
        }
    }

    public Function getFunction(FunctionSignature functionSignature) {
        synchronized (functions) {
            return functions.get(functionSignature);
        }
    }

    public FullTextConfigMetadataEntity getFullTextConfig(String databaseName, DataverseName dataverseName,
            String configName) {
        synchronized (fullTextConfigs) {
            Map<DataverseName, Map<String, FullTextConfigMetadataEntity>> db = fullTextConfigs.get(databaseName);
            if (db == null) {
                return null;
            }
            Map<String, FullTextConfigMetadataEntity> m = db.get(dataverseName);
            if (m == null) {
                return null;
            }
            return m.get(configName);
        }
    }

    public FullTextFilterMetadataEntity getFullTextFilter(String databaseName, DataverseName dataverseName,
            String filterName) {
        synchronized (fullTextFilters) {
            Map<DataverseName, Map<String, FullTextFilterMetadataEntity>> db = fullTextFilters.get(databaseName);
            if (db == null) {
                return null;
            }
            Map<String, FullTextFilterMetadataEntity> m = db.get(dataverseName);
            if (m == null) {
                return null;
            }
            return m.get(filterName);
        }
    }

    public List<Dataset> getDataverseDatasets(String databaseName, DataverseName dataverseName) {
        synchronized (datasets) {
            Map<DataverseName, Map<String, Dataset>> db = datasets.get(databaseName);
            if (db == null) {
                return Collections.emptyList();
            }
            Map<String, Dataset> m = db.get(dataverseName);
            if (m == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(m.values());
        }
    }

    public List<Index> getDatasetIndexes(String databaseName, DataverseName dataverseName, String datasetName) {
        synchronized (indexes) {
            Map<DataverseName, Map<String, Map<String, Index>>> db = indexes.get(databaseName);
            if (db == null) {
                return Collections.emptyList();
            }
            Map<String, Map<String, Index>> dv = db.get(dataverseName);
            if (dv == null) {
                return Collections.emptyList();
            }
            Map<String, Index> map = dv.get(datasetName);
            if (map == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(map.values());
        }
    }

    public Function addFunctionIfNotExists(Function function) {
        synchronized (functions) {
            FunctionSignature signature = new FunctionSignature(function.getDatabaseName(), function.getDataverseName(),
                    function.getName(), function.getArity());
            Function fun = functions.get(signature);
            if (fun == null) {
                return functions.put(signature, function);
            }
            return null;
        }
    }

    public Function dropFunction(Function function) {
        synchronized (functions) {
            FunctionSignature signature = new FunctionSignature(function.getDatabaseName(), function.getDataverseName(),
                    function.getName(), function.getArity());
            Function fun = functions.get(signature);
            if (fun == null) {
                return null;
            }
            return functions.remove(signature);
        }
    }

    public FullTextFilterMetadataEntity addFullTextFilterIfNotExists(FullTextFilterMetadataEntity filter) {
        String databaseName = filter.getFullTextFilter().getDatabaseName();
        DataverseName dataverseName = filter.getFullTextFilter().getDataverseName();
        String filterName = filter.getFullTextFilter().getName();
        synchronized (fullTextFilters) {
            Map<DataverseName, Map<String, FullTextFilterMetadataEntity>> databaseDataverses =
                    fullTextFilters.computeIfAbsent(databaseName, k -> new HashMap<>());
            Map<String, FullTextFilterMetadataEntity> m = databaseDataverses.get(dataverseName);
            if (m == null) {
                m = new HashMap<>();
                databaseDataverses.put(dataverseName, m);
            }
            if (!m.containsKey(filterName)) {
                return m.put(filterName, filter);
            }
            return null;
        }
    }

    public FullTextFilterMetadataEntity dropFullTextFilter(FullTextFilterMetadataEntity filterMetadataEntity) {
        DataverseName dataverseName = filterMetadataEntity.getFullTextFilter().getDataverseName();
        String filterName = filterMetadataEntity.getFullTextFilter().getName();
        synchronized (fullTextFilters) {
            Map<DataverseName, Map<String, FullTextFilterMetadataEntity>> databaseDataverses =
                    fullTextFilters.get(filterMetadataEntity.getFullTextFilter().getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, FullTextFilterMetadataEntity> m = databaseDataverses.get(dataverseName);
            if (m == null) {
                return null;
            }
            return m.remove(filterName);
        }
    }

    public FullTextConfigMetadataEntity addFullTextConfigIfNotExists(
            FullTextConfigMetadataEntity configMetadataEntity) {
        FullTextConfigDescriptor config = configMetadataEntity.getFullTextConfig();
        String databaseName = config.getDatabaseName();
        DataverseName dataverseName = config.getDataverseName();
        String configName = config.getName();
        synchronized (fullTextConfigs) {
            Map<DataverseName, Map<String, FullTextConfigMetadataEntity>> databaseDataverses =
                    fullTextConfigs.computeIfAbsent(databaseName, k -> new HashMap<>());
            Map<String, FullTextConfigMetadataEntity> m = databaseDataverses.get(dataverseName);
            if (m == null) {
                m = new HashMap<>();
                databaseDataverses.put(dataverseName, m);
            }
            if (!m.containsKey(configName)) {
                return m.put(configName, configMetadataEntity);
            }
            return null;
        }
    }

    public FullTextConfigMetadataEntity dropFullTextConfig(FullTextConfigMetadataEntity configMetadataEntity) {
        FullTextConfigDescriptor config = configMetadataEntity.getFullTextConfig();
        DataverseName dataverseName = config.getDataverseName();
        String configName = config.getName();
        synchronized (fullTextConfigs) {
            Map<DataverseName, Map<String, FullTextConfigMetadataEntity>> databaseDataverses =
                    fullTextConfigs.get(config.getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, FullTextConfigMetadataEntity> m = databaseDataverses.get(dataverseName);
            if (m == null) {
                return null;
            }
            return m.remove(configName);
        }
    }

    public Object addFeedPolicyIfNotExists(FeedPolicyEntity feedPolicy) {
        synchronized (feedPolicies) {
            Map<DataverseName, Map<String, FeedPolicyEntity>> databaseDataverses =
                    feedPolicies.computeIfAbsent(feedPolicy.getDatabaseName(), k -> new HashMap<>());
            Map<String, FeedPolicyEntity> p = databaseDataverses.get(feedPolicy.getDataverseName());
            if (p == null) {
                p = new HashMap<>();
                p.put(feedPolicy.getPolicyName(), feedPolicy);
                databaseDataverses.put(feedPolicy.getDataverseName(), p);
            } else {
                if (p.get(feedPolicy.getPolicyName()) == null) {
                    p.put(feedPolicy.getPolicyName(), feedPolicy);
                }
            }
            return null;
        }
    }

    public Object dropFeedPolicy(FeedPolicyEntity feedPolicy) {
        synchronized (feedPolicies) {
            Map<DataverseName, Map<String, FeedPolicyEntity>> databaseDataverses =
                    feedPolicies.get(feedPolicy.getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, FeedPolicyEntity> p = databaseDataverses.get(feedPolicy.getDataverseName());
            if (p != null && p.get(feedPolicy.getPolicyName()) != null) {
                return p.remove(feedPolicy.getPolicyName()).getPolicyName();
            }
            return null;
        }
    }

    public DatasourceAdapter addAdapterIfNotExists(DatasourceAdapter adapter) {
        synchronized (adapters) {
            Map<DataverseName, Map<String, DatasourceAdapter>> databaseDataverses =
                    adapters.computeIfAbsent(adapter.getAdapterIdentifier().getDatabaseName(), k -> new HashMap<>());
            Map<String, DatasourceAdapter> adaptersInDataverse =
                    databaseDataverses.get(adapter.getAdapterIdentifier().getDataverseName());
            if (adaptersInDataverse == null) {
                adaptersInDataverse = new HashMap<>();
                databaseDataverses.put(adapter.getAdapterIdentifier().getDataverseName(), adaptersInDataverse);
            }
            DatasourceAdapter adapterObject = adaptersInDataverse.get(adapter.getAdapterIdentifier().getName());
            if (adapterObject == null) {
                return adaptersInDataverse.put(adapter.getAdapterIdentifier().getName(), adapter);
            }
            return null;
        }
    }

    public DatasourceAdapter dropAdapterIfExists(DatasourceAdapter adapter) {
        synchronized (adapters) {
            Map<DataverseName, Map<String, DatasourceAdapter>> databaseDataverses =
                    adapters.get(adapter.getAdapterIdentifier().getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, DatasourceAdapter> adaptersInDataverse =
                    databaseDataverses.get(adapter.getAdapterIdentifier().getDataverseName());
            if (adaptersInDataverse != null) {
                return adaptersInDataverse.remove(adapter.getAdapterIdentifier().getName());
            }
            return null;
        }
    }

    public Library addLibraryIfNotExists(Library library) {
        synchronized (libraries) {
            Map<DataverseName, Map<String, Library>> databaseDataverses =
                    libraries.computeIfAbsent(library.getDatabaseName(), k -> new HashMap<>());
            Map<String, Library> libsInDataverse = databaseDataverses.get(library.getDataverseName());
            boolean needToAdd = (libsInDataverse == null || libsInDataverse.get(library.getName()) != null);
            if (needToAdd) {
                if (libsInDataverse == null) {
                    libsInDataverse = new HashMap<>();
                    databaseDataverses.put(library.getDataverseName(), libsInDataverse);
                }
                return libsInDataverse.put(library.getName(), library);
            }
            return null;
        }
    }

    public Library dropLibrary(Library library) {
        synchronized (libraries) {
            Map<DataverseName, Map<String, Library>> databaseDataverses = libraries.get(library.getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, Library> librariesInDataverse = databaseDataverses.get(library.getDataverseName());
            if (librariesInDataverse != null) {
                return librariesInDataverse.remove(library.getName());
            }
            return null;
        }
    }

    public FeedConnection addFeedConnectionIfNotExists(FeedConnection feedConnection) {
        synchronized (feedConnections) {
            Map<DataverseName, Map<String, FeedConnection>> databaseDataverses =
                    feedConnections.computeIfAbsent(feedConnection.getDatabaseName(), k -> new HashMap<>());
            Map<String, FeedConnection> feedConnsInDataverse =
                    databaseDataverses.get(feedConnection.getDataverseName());
            if (feedConnsInDataverse == null) {
                feedConnsInDataverse = new HashMap<>();
                databaseDataverses.put(feedConnection.getDataverseName(), feedConnsInDataverse);
            }
            return feedConnsInDataverse.put(feedConnection.getConnectionId(), feedConnection);
        }
    }

    public FeedConnection dropFeedConnection(FeedConnection feedConnection) {
        synchronized (feedConnections) {
            Map<DataverseName, Map<String, FeedConnection>> databaseDataverses =
                    feedConnections.get(feedConnection.getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, FeedConnection> feedConnsInDataverse =
                    databaseDataverses.get(feedConnection.getDataverseName());
            if (feedConnsInDataverse != null) {
                return feedConnsInDataverse.remove(feedConnection.getConnectionId());
            } else {
                return null;
            }
        }
    }

    public Feed addFeedIfNotExists(Feed feed) {
        synchronized (feeds) {
            Map<DataverseName, Map<String, Feed>> databaseDataverses =
                    feeds.computeIfAbsent(feed.getDatabaseName(), k -> new HashMap<>());
            Map<String, Feed> feedsInDataverse = databaseDataverses.get(feed.getDataverseName());
            if (feedsInDataverse == null) {
                feedsInDataverse = new HashMap<>();
                databaseDataverses.put(feed.getDataverseName(), feedsInDataverse);
            }
            return feedsInDataverse.put(feed.getFeedName(), feed);
        }
    }

    public Feed dropFeedIfExists(Feed feed) {
        synchronized (feeds) {
            Map<DataverseName, Map<String, Feed>> databaseDataverses = feeds.get(feed.getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, Feed> feedsInDataverse = databaseDataverses.get(feed.getDataverseName());
            if (feedsInDataverse != null) {
                return feedsInDataverse.remove(feed.getFeedName());
            }
            return null;
        }
    }

    public Synonym addSynonymIfNotExists(Synonym synonym) {
        synchronized (synonyms) {
            Map<DataverseName, Map<String, Synonym>> databaseDataverses =
                    synonyms.computeIfAbsent(synonym.getDatabaseName(), k -> new HashMap<>());
            Map<String, Synonym> synonymsInDataverse = databaseDataverses.get(synonym.getDataverseName());
            if (synonymsInDataverse == null) {
                synonymsInDataverse = new HashMap<>();
                databaseDataverses.put(synonym.getDataverseName(), synonymsInDataverse);
            }
            //TODO(DB): this actually overwrites if existing
            return synonymsInDataverse.put(synonym.getSynonymName(), synonym);
        }
    }

    public Synonym dropSynonym(Synonym synonym) {
        synchronized (synonyms) {
            Map<DataverseName, Map<String, Synonym>> databaseDataverses = synonyms.get(synonym.getDatabaseName());
            if (databaseDataverses == null) {
                return null;
            }
            Map<String, Synonym> synonymsInDataverse = databaseDataverses.get(synonym.getDataverseName());
            if (synonymsInDataverse != null) {
                return synonymsInDataverse.remove(synonym.getSynonymName());
            }
            return null;
        }
    }

    private Index addIndexIfNotExistsInternal(Index index) {
        Map<DataverseName, Map<String, Map<String, Index>>> databaseDataverses =
                indexes.computeIfAbsent(index.getDatabaseName(), k -> new HashMap<>());
        Map<String, Map<String, Index>> datasetMap = databaseDataverses.get(index.getDataverseName());
        if (datasetMap == null) {
            datasetMap = new HashMap<>();
            databaseDataverses.put(index.getDataverseName(), datasetMap);
        }
        Map<String, Index> indexMap = datasetMap.get(index.getDatasetName());
        if (indexMap == null) {
            indexMap = new HashMap<>();
            datasetMap.put(index.getDatasetName(), indexMap);
        }
        if (!indexMap.containsKey(index.getIndexName())) {
            return indexMap.put(index.getIndexName(), index);
        }
        return null;
    }

    protected void doOperation(MetadataLogicalOperation op) {
        if (op.isAdd) {
            op.entity.addToCache(this);
        } else {
            op.entity.dropFromCache(this);
        }
    }

    protected void undoOperation(MetadataLogicalOperation op) {
        if (!op.isAdd) {
            op.entity.addToCache(this);
        } else {
            op.entity.dropFromCache(this);
        }
    }

    /**
     * Represents a logical operation against the metadata.
     */
    protected static class MetadataLogicalOperation {
        // Entity to be added/dropped.
        public final IMetadataEntity<?> entity;
        // True for add, false for drop.
        public final boolean isAdd;

        public MetadataLogicalOperation(IMetadataEntity<?> entity, boolean isAdd) {
            this.entity = entity;
            this.isAdd = isAdd;
        }
    }
}
