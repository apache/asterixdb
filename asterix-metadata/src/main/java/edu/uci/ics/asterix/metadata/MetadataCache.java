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

package edu.uci.ics.asterix.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.metadata.api.IMetadataEntity;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * Caches metadata entities such that the MetadataManager does not have to
 * contact the MetadataNode. The cache is updated transactionally via logical
 * logging in the MetadataTransactionContext. Note that transaction abort is
 * simply ignored, i.e., updates are not not applied to the cache.
 */
public class MetadataCache {
    // Key is dataverse name.
    protected final Map<String, Dataverse> dataverses = new HashMap<String, Dataverse>();
    // Key is dataverse name. Key of value map is dataset name.
    protected final Map<String, Map<String, Dataset>> datasets = new HashMap<String, Map<String, Dataset>>();
    // Key is dataverse name. Key of value map is datatype name.
    protected final Map<String, Map<String, Datatype>> datatypes = new HashMap<String, Map<String, Datatype>>();
    // Key is dataverse name.
    protected final Map<String, NodeGroup> nodeGroups = new HashMap<String, NodeGroup>();
    // Key is function Identifier . Key of value map is function name.
    protected final Map<FunctionIdentifier, Function> functions = new HashMap<FunctionIdentifier, Function>();

    // Atomically executes all metadata operations in ctx's log.
    public void commit(MetadataTransactionContext ctx) {
        // Forward roll the operations written in ctx's log.
        int logIx = 0;
        ArrayList<MetadataLogicalOperation> opLog = ctx.getOpLog();
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
        synchronized (dataverses) {
            synchronized (nodeGroups) {
                synchronized (datasets) {
                    synchronized (datatypes) {
                        synchronized (functions) {
                            dataverses.clear();
                            nodeGroups.clear();
                            datasets.clear();
                            datatypes.clear();
                        }
                    }
                }
            }
        }
    }

    public Object addDataverseIfNotExists(Dataverse dataverse) {
        synchronized (dataverses) {
            synchronized (datasets) {
                synchronized (datatypes) {
                    synchronized (functions) {
                        if (!dataverses.containsKey(dataverse)) {
                            datasets.put(dataverse.getDataverseName(), new HashMap<String, Dataset>());
                            datatypes.put(dataverse.getDataverseName(), new HashMap<String, Datatype>());
                            return dataverses.put(dataverse.getDataverseName(), dataverse);
                        }
                    }
                    return null;
                }
            }
        }
    }

    public Object addDatasetIfNotExists(Dataset dataset) {
        synchronized (datasets) {
            Map<String, Dataset> m = datasets.get(dataset.getDataverseName());
            if (m == null) {
                m = new HashMap<String, Dataset>();
                datasets.put(dataset.getDataverseName(), m);
            }
            if (!m.containsKey(dataset.getDatasetName())) {
                return m.put(dataset.getDatasetName(), dataset);
            }
            return null;
        }
    }

    public Object addDatatypeIfNotExists(Datatype datatype) {
        synchronized (datatypes) {
            Map<String, Datatype> m = datatypes.get(datatype.getDataverseName());
            if (m == null) {
                m = new HashMap<String, Datatype>();
                datatypes.put(datatype.getDataverseName(), m);
            }
            if (!m.containsKey(datatype.getDatatypeName())) {
                return m.put(datatype.getDatatypeName(), datatype);
            }
            return null;
        }
    }

    public Object addNodeGroupIfNotExists(NodeGroup nodeGroup) {
        synchronized (nodeGroups) {
            if (!nodeGroups.containsKey(nodeGroup.getNodeGroupName())) {
                return nodeGroups.put(nodeGroup.getNodeGroupName(), nodeGroup);
            }
            return null;
        }
    }

    public Object dropDataverse(Dataverse dataverse) {
        synchronized (dataverses) {
            synchronized (datasets) {
                synchronized (datatypes) {
                    synchronized (functions) {
                        datasets.remove(dataverse.getDataverseName());
                        datatypes.remove(dataverse.getDataverseName());
                        return dataverses.remove(dataverse.getDataverseName());
                    }
                }
            }
        }
    }

    public Object dropDataset(Dataset dataset) {
        synchronized (datasets) {
            Map<String, Dataset> m = datasets.get(dataset.getDataverseName());
            if (m == null) {
                return null;
            }
            return m.remove(dataset.getDatasetName());
        }
    }

    public Object dropDatatype(Datatype datatype) {
        synchronized (datatypes) {
            Map<String, Datatype> m = datatypes.get(datatype.getDataverseName());
            if (m == null) {
                return null;
            }
            return m.remove(datatype.getDatatypeName());
        }
    }

    public Object dropNodeGroup(NodeGroup nodeGroup) {
        synchronized (nodeGroups) {
            return nodeGroups.remove(nodeGroup.getNodeGroupName());
        }
    }

    public Dataverse getDataverse(String dataverseName) {
        synchronized (dataverses) {
            return dataverses.get(dataverseName);
        }
    }

    public Dataset getDataset(String dataverseName, String datasetName) {
        synchronized (datasets) {
            Map<String, Dataset> m = datasets.get(dataverseName);
            if (m == null) {
                return null;
            }
            return m.get(datasetName);
        }
    }

    public Datatype getDatatype(String dataverseName, String datatypeName) {
        synchronized (datatypes) {
            Map<String, Datatype> m = datatypes.get(dataverseName);
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

    public Function getFunction(String dataverse, String functionName, int arity) {
        synchronized (functions) {
            return functions.get(new FunctionIdentifier(dataverse, functionName, arity));
        }
    }

    public List<Dataset> getDataverseDatasets(String dataverseName) {
        List<Dataset> retDatasets = new ArrayList<Dataset>();
        synchronized (datasets) {
            Map<String, Dataset> m = datasets.get(dataverseName);
            if (m == null) {
                return retDatasets;
            }
            for (Map.Entry<String, Dataset> entry : m.entrySet()) {
                retDatasets.add(entry.getValue());
            }
            return retDatasets;
        }
    }

    /**
     * Represents a logical operation against the metadata.
     */
    protected class MetadataLogicalOperation {
        // Entity to be added/dropped.
        public final IMetadataEntity entity;
        // True for add, false for drop.
        public final boolean isAdd;

        public MetadataLogicalOperation(IMetadataEntity entity, boolean isAdd) {
            this.entity = entity;
            this.isAdd = isAdd;
        }
    };

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

    public Object addFunctionIfNotExists(Function function) {
        synchronized (functions) {
            FunctionIdentifier fId = new FunctionIdentifier(function.getDataverseName(), function.getFunctionName(),
                    function.getFunctionArity());

            Function fun = functions.get(fId);
            if (fun == null) {
                return functions.put(fId, function);
            }
            return null;
        }
    }

    public Object dropFunction(Function function) {
        synchronized (functions) {
            FunctionIdentifier fId = new FunctionIdentifier(function.getDataverseName(), function.getFunctionName(),
                    function.getFunctionArity());
            Function fun = functions.get(fId);
            if (fun == null) {
                return null;
            }
            return functions.remove(fId);
        }
    }
}
