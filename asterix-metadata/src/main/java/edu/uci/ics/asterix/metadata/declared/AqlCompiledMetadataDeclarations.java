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

package edu.uci.ics.asterix.metadata.declared;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.annotations.TypeDataGen;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.api.IMetadataManager;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixProperties;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl.IndexKind;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;

public class AqlCompiledMetadataDeclarations {
    private static Logger LOGGER = Logger.getLogger(AqlCompiledMetadataDeclarations.class.getName());

    // We are assuming that there is a one AqlCompiledMetadataDeclarations per
    // transaction.
    private final MetadataTransactionContext mdTxnCtx;
    private String dataverseName = null;
    private FileSplit outputFile;
    private Map<String, String[]> stores;
    private IDataFormat format;
    private Map<String, String> config;

    private final Map<String, IAType> types;
    private final Map<String, TypeDataGen> typeDataGenMap;
    private final IAWriterFactory writerFactory;

    private IMetadataManager metadataManager = MetadataManager.INSTANCE;
    private boolean isConnected = false;

    public AqlCompiledMetadataDeclarations(MetadataTransactionContext mdTxnCtx, String dataverseName,
            FileSplit outputFile, Map<String, String> config, Map<String, String[]> stores, Map<String, IAType> types,
            Map<String, TypeDataGen> typeDataGenMap, IAWriterFactory writerFactory, boolean online) {
        this.mdTxnCtx = mdTxnCtx;
        this.dataverseName = dataverseName;
        this.outputFile = outputFile;
        this.config = config;
        if (stores == null && online)
            this.stores = AsterixProperties.INSTANCE.getStores();
        else
            this.stores = stores;
        this.types = types;
        this.typeDataGenMap = typeDataGenMap;
        this.writerFactory = writerFactory;
    }

    public void connectToDataverse(String dvName) throws AlgebricksException, AsterixException {
        if (isConnected) {
            throw new AlgebricksException("You are already connected to " + dataverseName + " dataverse");
        }
        Dataverse dv;
        try {
            dv = metadataManager.getDataverse(mdTxnCtx, dvName);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        if (dv == null) {
            throw new AlgebricksException("There is no dataverse with this name " + dvName + " to connect to.");
        }
        dataverseName = dvName;
        isConnected = true;
        try {
            format = (IDataFormat) Class.forName(dv.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
    }

    public void disconnectFromDataverse() throws AlgebricksException {
        if (!isConnected)
            throw new AlgebricksException("You are not connected to any dataverse");
        else {
            dataverseName = null;
            format = null;
            isConnected = false;
        }
    }

    public boolean isConnectedToDataverse() {
        return this.isConnected;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public FileSplit getOutputFile() {
        return outputFile;
    }

    public IDataFormat getFormat() throws AlgebricksException {
        if (!isConnected)
            throw new AlgebricksException("You need first to connect to a dataverse.");
        return format;
    }

    public String getPropertyValue(String propertyName) {
        return config.get(propertyName);
    }

    public IAType findType(String typeName) {
        Datatype type;
        try {
            type = metadataManager.getDatatype(mdTxnCtx, dataverseName, typeName);
        } catch (Exception e) {
            throw new IllegalStateException();
        }
        if (type == null) {
            throw new IllegalStateException();
        }
        return type.getDatatype();
    }

    public List<String> findNodeGroupNodeNames(String nodeGroupName) throws AlgebricksException, MetadataException {
        NodeGroup ng = metadataManager.getNodegroup(mdTxnCtx, nodeGroupName);
        if (ng == null) {
            throw new AlgebricksException("No node group with this name " + nodeGroupName);
        }
        return ng.getNodeNames();
    }

    public String[] getStores(String nodeName) {
        return stores.get(nodeName);
    }

    public Map<String, String[]> getAllStores() {
        return stores;
    }

    public AqlCompiledDatasetDecl findDataset(String datasetName) {
        try {
            Dataset datasetRecord = this.metadataManager.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (datasetRecord == null) {
                return null;
            }

            IAqlCompiledDatasetDetails acdd = null;
            switch (datasetRecord.getType()) {
                case FEED:
                case INTERNAL: {
                    String typeName = datasetRecord.getDatatypeName();
                    InternalDatasetDetails id = (InternalDatasetDetails) datasetRecord.getDatasetDetails();
                    ARecordType recType = (ARecordType) findType(typeName);
                    List<Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningEvalFactories = computePartitioningEvaluatorFactories(
                            id.getPartitioningKey(), recType);
                    List<Index> indexRecord = this.metadataManager.getDatasetIndexes(mdTxnCtx, dataverseName,
                            datasetName);
                    AqlCompiledIndexDecl primaryIndex = null;
                    List<AqlCompiledIndexDecl> secondaryIndexes = new ArrayList<AqlCompiledIndexDecl>();
                    for (int i = 0; i < indexRecord.size(); i++) {
                        Index rec = indexRecord.get(i);
                        if (rec.isPrimaryIndex()) {
                            primaryIndex = new AqlCompiledIndexDecl(rec.getIndexName(), IndexKind.BTREE,
                                    rec.getKeyFieldNames());
                        } else {
                            secondaryIndexes.add(new AqlCompiledIndexDecl(rec.getIndexName(),
                                    rec.getIndexType() == IndexType.BTREE ? IndexKind.BTREE : IndexKind.RTREE, rec
                                            .getKeyFieldNames()));
                        }
                    }

                    if (datasetRecord.getType() == DatasetType.INTERNAL) {
                        acdd = new AqlCompiledInternalDatasetDetails(id.getPartitioningKey(),
                                partitioningEvalFactories, id.getNodeGroupName(), primaryIndex, secondaryIndexes);
                    } else {
                        acdd = new AqlCompiledFeedDatasetDetails(id.getPartitioningKey(), partitioningEvalFactories,
                                id.getNodeGroupName(), primaryIndex, secondaryIndexes,
                                ((FeedDatasetDetails) id).getAdapter(), ((FeedDatasetDetails) id).getProperties(),
                                ((FeedDatasetDetails) id).getFunctionIdentifier(), ((FeedDatasetDetails) id)
                                        .getFeedState().toString());
                    }
                    break;
                }

                case EXTERNAL: {
                    acdd = new AqlCompiledExternalDatasetDetails(
                            ((ExternalDatasetDetails) datasetRecord.getDatasetDetails()).getAdapter(),
                            ((ExternalDatasetDetails) datasetRecord.getDatasetDetails()).getProperties());
                    break;
                }

            }
            AqlCompiledDatasetDecl dataset = new AqlCompiledDatasetDecl(datasetRecord.getDatasetName(),
                    datasetRecord.getDatatypeName(), datasetRecord.getType(), acdd);
            return dataset;

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void setOutputFile(FileSplit outputFile) {
        this.outputFile = outputFile;
    }

    public List<Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>> computePartitioningEvaluatorFactories(
            List<String> partitioningExprs, ARecordType recType) {
        List<Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>> evalFactories = new ArrayList<Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType>>(
                partitioningExprs.size());
        for (String expr : partitioningExprs) {
            Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFact = null;
            try {
                evalFact = format.partitioningEvaluatorFactory(recType, expr);
            } catch (AlgebricksException e) {
                throw new IllegalStateException(e);
            }
            evalFactories.add(evalFact);
        }
        return evalFactories;
    }

	public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
            String datasetName, String targetIdxName) throws AlgebricksException, MetadataException {
        FileSplit[] splits = splitsForInternalOrFeedDataset(datasetName, targetIdxName);
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(splits);
        String[] loc = new String[splits.length];
        for (int p = 0; p < splits.length; p++) {
            loc[p] = splits[p].getNodeName();
        }
        AlgebricksPartitionConstraint pc = new AlgebricksAbsolutePartitionConstraint(loc);
        return new Pair<IFileSplitProvider, AlgebricksPartitionConstraint>(splitProvider, pc);
    }

    private FileSplit[] splitsForInternalOrFeedDataset(String datasetName, String targetIdxName)
            throws AlgebricksException, MetadataException {

        File relPathFile = new File(getRelativePath(datasetName + "_idx_" + targetIdxName));
        AqlCompiledDatasetDecl adecl = findDataset(datasetName);
        if (adecl.getDatasetType() != DatasetType.INTERNAL & adecl.getDatasetType() != DatasetType.FEED) {
            throw new AlgebricksException("Not an internal or feed dataset");
        }
        AqlCompiledInternalDatasetDetails compiledDatasetDetails = (AqlCompiledInternalDatasetDetails) adecl
                .getAqlCompiledDatasetDetails();
        List<String> nodeGroup = findNodeGroupNodeNames(compiledDatasetDetails.getNodegroupName());

        if (nodeGroup == null) {
            throw new AlgebricksException("Couldn't find node group " + compiledDatasetDetails.getNodegroupName());
        }

        List<FileSplit> splitArray = new ArrayList<FileSplit>();

        for (String nd : nodeGroup) {
            String[] nodeStores = stores.get(nd);
            if (nodeStores == null) {
                LOGGER.warning("Node " + nd + " has no stores.");
                throw new AlgebricksException("Node " + nd + " has no stores.");
            } else {
                for (int j = 0; j < nodeStores.length; j++) {
                    File f = new File(nodeStores[j] + File.separator + relPathFile);
                    splitArray.add(new FileSplit(nd, new FileReference(f)));
                }
            }
        }
        FileSplit[] splits = new FileSplit[splitArray.size()];
        int i = 0;
        for (FileSplit fs : splitArray) {
            splits[i++] = fs;
        }
        return splits;
    }

    public String getRelativePath(String fileName) {
        return dataverseName + File.separator + fileName;
    }

    public Map<String, TypeDataGen> getTypeDataGenMap() {
        return typeDataGenMap;
    }

    public Map<String, IAType> getTypeDeclarations() {
        return types;
    }

    public IAWriterFactory getWriterFactory() {
        return writerFactory;
    }

    public MetadataTransactionContext getMetadataTransactionContext() {
        return mdTxnCtx;
    }
}