/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.aql.translator;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.base.Statement.Kind;
import edu.uci.ics.asterix.aql.expression.CreateDataverseStatement;
import edu.uci.ics.asterix.aql.expression.CreateFunctionStatement;
import edu.uci.ics.asterix.aql.expression.CreateIndexStatement;
import edu.uci.ics.asterix.aql.expression.DatasetDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDropStatement;
import edu.uci.ics.asterix.aql.expression.DropStatement;
import edu.uci.ics.asterix.aql.expression.ExternalDetailsDecl;
import edu.uci.ics.asterix.aql.expression.FeedDetailsDecl;
import edu.uci.ics.asterix.aql.expression.FunctionDropStatement;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aql.expression.IndexDropStatement;
import edu.uci.ics.asterix.aql.expression.InternalDetailsDecl;
import edu.uci.ics.asterix.aql.expression.NodeGroupDropStatement;
import edu.uci.ics.asterix.aql.expression.NodegroupDecl;
import edu.uci.ics.asterix.aql.expression.OrderedListTypeDefinition;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition.RecordKind;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.TypeDropStatement;
import edu.uci.ics.asterix.aql.expression.TypeExpression;
import edu.uci.ics.asterix.aql.expression.TypeReferenceExpression;
import edu.uci.ics.asterix.aql.expression.UnorderedListTypeDefinition;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.common.parse.IParseFileSplitsDecl;
import edu.uci.ics.asterix.file.DatasetOperations;
import edu.uci.ics.asterix.file.IndexOperations;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinArtifactMap;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinArtifactMap.ARTIFACT_KIND;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinTypeMap;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.translator.AbstractAqlTranslator;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledCreateIndexStatement;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class DdlTranslator extends AbstractAqlTranslator {

    private final MetadataTransactionContext mdTxnCtx;
    private final List<Statement> aqlStatements;
    private final PrintWriter out;
    private final SessionConfig pc;
    private final DisplayFormat pdf;
    private AqlCompiledMetadataDeclarations compiledDeclarations;

    private static Map<String, BuiltinType> builtinTypeMap;

    public DdlTranslator(MetadataTransactionContext mdTxnCtx, List<Statement> aqlStatements, PrintWriter out,
            SessionConfig pc, DisplayFormat pdf) {
        this.mdTxnCtx = mdTxnCtx;
        this.aqlStatements = aqlStatements;
        this.out = out;
        this.pc = pc;
        this.pdf = pdf;
        builtinTypeMap = AsterixBuiltinTypeMap.getBuiltinTypes();
    }

    public void translate(IHyracksClientConnection hcc, boolean disconnectFromDataverse) throws AlgebricksException {
        try {
            compiledDeclarations = compileMetadata(mdTxnCtx, aqlStatements, true);
            compileAndExecuteDDLstatements(hcc, mdTxnCtx, disconnectFromDataverse);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    private void compileAndExecuteDDLstatements(IHyracksClientConnection hcc, MetadataTransactionContext mdTxnCtx,
            boolean disconnectFromDataverse) throws Exception {
        for (Statement stmt : aqlStatements) {
            validateOperation(compiledDeclarations, stmt);
            switch (stmt.getKind()) {
                case DATAVERSE_DECL: {
                    checkForDataverseConnection(false);
                    DataverseDecl dvd = (DataverseDecl) stmt;
                    String dataverseName = dvd.getDataverseName().getValue();
                    compiledDeclarations.connectToDataverse(dataverseName);
                    break;
                }

                case CREATE_DATAVERSE: {
                    checkForDataverseConnection(false);
                    CreateDataverseStatement stmtCreateDataverse = (CreateDataverseStatement) stmt;
                    String dvName = stmtCreateDataverse.getDataverseName().getValue();
                    Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dvName);
                    if (dv != null && !stmtCreateDataverse.getIfNotExists()) {
                        throw new AlgebricksException("A dataverse with this name " + dvName + " already exists.");
                    }
                    MetadataManager.INSTANCE.addDataverse(mdTxnCtx,
                            new Dataverse(dvName, stmtCreateDataverse.getFormat()));
                    break;
                }

                case DATASET_DECL: {
                    checkForDataverseConnection(true);
                    DatasetDecl dd = (DatasetDecl) stmt;
                    String datasetName = dd.getName().getValue();
                    DatasetType dsType = dd.getDatasetType();
                    String itemTypeName = null;
                    IDatasetDetails datasetDetails = null;
                    Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, compiledDeclarations.getDataverseName(),
                            datasetName);
                    if (ds != null) {
                        if (dd.getIfNotExists()) {
                            continue;
                        } else {
                            throw new AlgebricksException("A dataset with this name " + datasetName
                                    + " already exists.");
                        }
                    }
                    itemTypeName = dd.getItemTypeName().getValue();
                    Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx,
                            compiledDeclarations.getDataverseName(), itemTypeName);
                    if (dt == null) {
                        throw new AlgebricksException(": type " + itemTypeName + " could not be found.");
                    }
                    switch (dd.getDatasetType()) {
                        case INTERNAL: {
                            IAType itemType = dt.getDatatype();
                            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                                throw new AlgebricksException("Can only partition ARecord's.");
                            }
                            List<String> partitioningExprs = ((InternalDetailsDecl) dd.getDatasetDetailsDecl())
                                    .getPartitioningExprs();
                            String ngName = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getNodegroupName()
                                    .getValue();
                            datasetDetails = new InternalDatasetDetails(InternalDatasetDetails.FileStructure.BTREE,
                                    InternalDatasetDetails.PartitioningStrategy.HASH, partitioningExprs,
                                    partitioningExprs, ngName);
                            break;
                        }
                        case EXTERNAL: {
                            String adapter = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getAdapter();
                            Map<String, String> properties = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl())
                                    .getProperties();
                            datasetDetails = new ExternalDatasetDetails(adapter, properties);
                            break;
                        }
                        case FEED: {
                            IAType itemType = dt.getDatatype();
                            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                                throw new AlgebricksException("Can only partition ARecord's.");
                            }
                            List<String> partitioningExprs = ((FeedDetailsDecl) dd.getDatasetDetailsDecl())
                                    .getPartitioningExprs();
                            String ngName = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getNodegroupName()
                                    .getValue();
                            String adapter = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getAdapterClassname();
                            Map<String, String> properties = ((FeedDetailsDecl) dd.getDatasetDetailsDecl())
                                    .getProperties();
                            String functionIdentifier = ((FeedDetailsDecl) dd.getDatasetDetailsDecl())
                                    .getFunctionIdentifier();
                            datasetDetails = new FeedDatasetDetails(InternalDatasetDetails.FileStructure.BTREE,
                                    InternalDatasetDetails.PartitioningStrategy.HASH, partitioningExprs,
                                    partitioningExprs, ngName, adapter, properties, functionIdentifier,
                                    FeedDatasetDetails.FeedState.INACTIVE.toString());
                            break;
                        }
                    }
                    MetadataManager.INSTANCE.addDataset(mdTxnCtx, new Dataset(compiledDeclarations.getDataverseName(),
                            datasetName, itemTypeName, datasetDetails, dsType));
                    if (dd.getDatasetType() == DatasetType.INTERNAL || dd.getDatasetType() == DatasetType.FEED) {
                        runCreateDatasetJob(hcc, datasetName);
                    }
                    break;
                }

                case CREATE_INDEX: {
                    checkForDataverseConnection(true);
                    CreateIndexStatement stmtCreateIndex = (CreateIndexStatement) stmt;
                    String datasetName = stmtCreateIndex.getDatasetName().getValue();
                    Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, compiledDeclarations.getDataverseName(),
                            datasetName);
                    if (ds == null) {
                        throw new AlgebricksException("There is no dataset with this name " + datasetName);
                    }
                    String indexName = stmtCreateIndex.getIndexName().getValue();
                    Index idx = MetadataManager.INSTANCE.getIndex(mdTxnCtx, compiledDeclarations.getDataverseName(),
                            datasetName, indexName);
                    if (idx != null) {
                        if (!stmtCreateIndex.getIfNotExists()) {
                            throw new AlgebricksException("An index with this name " + indexName + " already exists.");
                        } else {
                            stmtCreateIndex.setNeedToCreate(false);
                        }
                    } else {
                        MetadataManager.INSTANCE.addIndex(mdTxnCtx, new Index(compiledDeclarations.getDataverseName(),
                                datasetName, indexName, stmtCreateIndex.getIndexType(),
                                stmtCreateIndex.getFieldExprs(), stmtCreateIndex.getGramLength(), false));
                        runCreateIndexJob(hcc, stmtCreateIndex);
                    }
                    break;
                }
                case TYPE_DECL: {
                    checkForDataverseConnection(true);
                    TypeDecl stmtCreateType = (TypeDecl) stmt;
                    String typeName = stmtCreateType.getIdent().getValue();
                    Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx,
                            compiledDeclarations.getDataverseName(), typeName);
                    if (dt != null) {
                        if (!stmtCreateType.getIfNotExists())
                            throw new AlgebricksException("A datatype with this name " + typeName + " already exists.");
                    } else {
                        if (builtinTypeMap.get(typeName) != null) {
                            throw new AlgebricksException("Cannot redefine builtin type " + typeName + ".");
                        } else {
                            Map<String, IAType> typeMap = computeTypes(mdTxnCtx, (TypeDecl) stmt);
                            IAType type = typeMap.get(typeName);
                            MetadataManager.INSTANCE.addDatatype(mdTxnCtx,
                                    new Datatype(compiledDeclarations.getDataverseName(), typeName, type, false));
                        }
                    }
                    break;
                }
                case NODEGROUP_DECL: {
                    NodegroupDecl stmtCreateNodegroup = (NodegroupDecl) stmt;
                    String ngName = stmtCreateNodegroup.getNodegroupName().getValue();
                    NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, ngName);
                    if (ng != null) {
                        if (!stmtCreateNodegroup.getIfNotExists())
                            throw new AlgebricksException("A nodegroup with this name " + ngName + " already exists.");
                    } else {
                        List<Identifier> ncIdentifiers = stmtCreateNodegroup.getNodeControllerNames();
                        List<String> ncNames = new ArrayList<String>(ncIdentifiers.size());
                        for (Identifier id : ncIdentifiers) {
                            ncNames.add(id.getValue());
                        }
                        MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, new NodeGroup(ngName, ncNames));
                    }
                    break;
                }
                // drop statements
                case DATAVERSE_DROP: {
                    DataverseDropStatement stmtDelete = (DataverseDropStatement) stmt;
                    String dvName = stmtDelete.getDataverseName().getValue();
                    if (AsterixBuiltinArtifactMap.isSystemProtectedArtifact(ARTIFACT_KIND.DATAVERSE, dvName)) {
                        throw new AsterixException("Invalid Operation cannot drop dataverse " + dvName
                                + " (protected by system)");
                    }

                    if (compiledDeclarations.isConnectedToDataverse())
                        compiledDeclarations.disconnectFromDataverse();
                    checkForDataverseConnection(false);

                    Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dvName);
                    if (dv == null) {
                        if (!stmtDelete.getIfExists()) {
                            throw new AlgebricksException("There is no dataverse with this name " + dvName + ".");
                        }
                    } else {
                        compiledDeclarations.connectToDataverse(dvName);
                        List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dvName);
                        for (int j = 0; j < datasets.size(); j++) {
                            String datasetName = datasets.get(j).getDatasetName();
                            DatasetType dsType = datasets.get(j).getDatasetType();
                            if (dsType == DatasetType.INTERNAL || dsType == DatasetType.FEED) {
                                List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dvName,
                                        datasetName);
                                for (int k = 0; k < indexes.size(); k++) {
                                    if (indexes.get(k).isSecondaryIndex()) {
                                        compileIndexDropStatement(hcc, mdTxnCtx, datasetName, indexes.get(k)
                                                .getIndexName());
                                    }
                                }
                            }
                            compileDatasetDropStatement(hcc, mdTxnCtx, datasetName);
                        }
                        MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dvName);
                        if (compiledDeclarations.isConnectedToDataverse())
                            compiledDeclarations.disconnectFromDataverse();
                    }
                    break;
                }
                case DATASET_DROP: {
                    checkForDataverseConnection(true);
                    DropStatement stmtDelete = (DropStatement) stmt;
                    String datasetName = stmtDelete.getDatasetName().getValue();
                    if (AsterixBuiltinArtifactMap.isSystemProtectedArtifact(ARTIFACT_KIND.DATASET, datasetName)) {
                        throw new AsterixException("Invalid Operation cannot drop dataset " + datasetName
                                + " (protected by system)");
                    }
                    Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, compiledDeclarations.getDataverseName(),
                            datasetName);
                    if (ds == null) {
                        if (!stmtDelete.getIfExists())
                            throw new AlgebricksException("There is no dataset with this name " + datasetName + ".");
                    } else {
                        if (ds.getDatasetType() == DatasetType.INTERNAL || ds.getDatasetType() == DatasetType.FEED) {
                            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx,
                                    compiledDeclarations.getDataverseName(), datasetName);
                            for (int j = 0; j < indexes.size(); j++) {
                                if (indexes.get(j).isPrimaryIndex()) {
                                    compileIndexDropStatement(hcc, mdTxnCtx, datasetName, indexes.get(j).getIndexName());
                                }
                            }
                        }
                        compileDatasetDropStatement(hcc, mdTxnCtx, datasetName);
                    }
                    break;
                }
                case INDEX_DROP: {
                    checkForDataverseConnection(true);
                    IndexDropStatement stmtDelete = (IndexDropStatement) stmt;
                    String datasetName = stmtDelete.getDatasetName().getValue();
                    Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, compiledDeclarations.getDataverseName(),
                            datasetName);
                    if (ds == null)
                        throw new AlgebricksException("There is no dataset with this name " + datasetName + ".");
                    if (ds.getDatasetType() == DatasetType.INTERNAL || ds.getDatasetType() == DatasetType.FEED) {
                        String indexName = stmtDelete.getIndexName().getValue();
                        Index idx = MetadataManager.INSTANCE.getIndex(mdTxnCtx,
                                compiledDeclarations.getDataverseName(), datasetName, indexName);
                        if (idx == null) {
                            if (!stmtDelete.getIfExists())
                                throw new AlgebricksException("There is no index with this name " + indexName + ".");
                        } else
                            compileIndexDropStatement(hcc, mdTxnCtx, datasetName, indexName);
                    } else {
                        throw new AlgebricksException(datasetName
                                + " is an external dataset. Indexes are not maintained for external datasets.");
                    }
                    break;
                }
                case TYPE_DROP: {
                    checkForDataverseConnection(true);
                    TypeDropStatement stmtDelete = (TypeDropStatement) stmt;
                    String typeName = stmtDelete.getTypeName().getValue();
                    Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx,
                            compiledDeclarations.getDataverseName(), typeName);
                    if (dt == null) {
                        if (!stmtDelete.getIfExists())
                            throw new AlgebricksException("There is no datatype with this name " + typeName + ".");
                    } else
                        MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, compiledDeclarations.getDataverseName(),
                                typeName);
                    break;
                }
                case NODEGROUP_DROP: {
                    NodeGroupDropStatement stmtDelete = (NodeGroupDropStatement) stmt;
                    String nodegroupName = stmtDelete.getNodeGroupName().getValue();
                    if (AsterixBuiltinArtifactMap.isSystemProtectedArtifact(ARTIFACT_KIND.NODEGROUP, nodegroupName)) {
                        throw new AsterixException("Invalid Operation cannot drop nodegroup " + nodegroupName
                                + " (protected by system)");
                    }
                    NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodegroupName);
                    if (ng == null) {
                        if (!stmtDelete.getIfExists())
                            throw new AlgebricksException("There is no nodegroup with this name " + nodegroupName + ".");
                    } else
                        MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, nodegroupName);
                    break;
                }

                case CREATE_FUNCTION: {
                    CreateFunctionStatement cfs = (CreateFunctionStatement) stmt;
                    Function function = new Function(compiledDeclarations.getDataverseName(), cfs
                            .getFunctionIdentifier().getFunctionName(), cfs.getFunctionIdentifier().getArity(),
                            cfs.getParamList(), cfs.getFunctionBody());
                    try {
                        FunctionUtils.getFunctionDecl(function);
                    } catch (Exception e) {
                        throw new AsterixException("Unable to compile function definition", e);
                    }
                    MetadataManager.INSTANCE.addFunction(mdTxnCtx, new Function(
                            compiledDeclarations.getDataverseName(), cfs.getFunctionIdentifier().getFunctionName(), cfs
                                    .getFunctionIdentifier().getArity(), cfs.getParamList(), cfs.getFunctionBody()));
                    break;
                }

                case FUNCTION_DROP: {
                    checkForDataverseConnection(true);
                    FunctionDropStatement stmtDropFunction = (FunctionDropStatement) stmt;
                    String functionName = stmtDropFunction.getFunctionName().getValue();
                    FunctionIdentifier fId = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, functionName,
                            stmtDropFunction.getArity(), false);
                    if (AsterixBuiltinArtifactMap.isSystemProtectedArtifact(ARTIFACT_KIND.FUNCTION, fId)) {
                        throw new AsterixException("Invalid Operation cannot drop function " + functionName
                                + " (protected by system)");
                    }
                    Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx,
                            compiledDeclarations.getDataverseName(), functionName, stmtDropFunction.getArity());
                    if (function == null) {
                        if (!stmtDropFunction.getIfExists())
                            throw new AlgebricksException("There is no function with this name " + functionName + ".");
                    } else {
                        MetadataManager.INSTANCE.dropFunction(mdTxnCtx, compiledDeclarations.getDataverseName(),
                                functionName, stmtDropFunction.getArity());
                    }
                    break;
                }
            }
        }

        if (disconnectFromDataverse) {
            if (compiledDeclarations.isConnectedToDataverse()) {
                compiledDeclarations.disconnectFromDataverse();
            }
        }
    }

    private void checkForDataverseConnection(boolean needConnection) throws AlgebricksException {
        if (compiledDeclarations.isConnectedToDataverse() != needConnection) {
            if (needConnection)
                throw new AlgebricksException("You need first to connect to a dataverse.");
            else
                throw new AlgebricksException("You need first to disconnect from the dataverse.");
        }
    }

    private void runJob(IHyracksClientConnection hcc, JobSpecification jobSpec) throws Exception {
        System.out.println(jobSpec.toString());
        executeJobArray(hcc, new JobSpecification[] { jobSpec }, out, pdf);
    }

    public void executeJobArray(IHyracksClientConnection hcc, JobSpecification[] specs, PrintWriter out,
            DisplayFormat pdf) throws Exception {
        for (int i = 0; i < specs.length; i++) {
            specs[i].setMaxReattempts(0);
            JobId jobId = hcc.startJob(GlobalConfig.HYRACKS_APP_NAME, specs[i]);
            hcc.waitForCompletion(jobId);
        }
    }

    private void runCreateDatasetJob(IHyracksClientConnection hcc, String datasetName) throws AsterixException,
            AlgebricksException, Exception {
        runJob(hcc, DatasetOperations.createDatasetJobSpec(datasetName, compiledDeclarations));
    }

    private void runCreateIndexJob(IHyracksClientConnection hcc, CreateIndexStatement stmtCreateIndex) throws Exception {
        // TODO: Eventually CreateIndexStatement and CompiledCreateIndexStatement should be replaced by the corresponding metadata entity.
        // For now we must still convert to a CompiledCreateIndexStatement here.
        CompiledCreateIndexStatement createIndexStmt = new CompiledCreateIndexStatement(stmtCreateIndex.getIndexName()
                .getValue(), stmtCreateIndex.getDatasetName().getValue(), stmtCreateIndex.getFieldExprs(),
                stmtCreateIndex.getGramLength(), stmtCreateIndex.getIndexType());
        JobSpecification spec = IndexOperations.buildSecondaryIndexCreationJobSpec(createIndexStmt,
                compiledDeclarations);
        if (spec == null) {
            throw new AsterixException("Failed to create job spec for creating index '"
                    + stmtCreateIndex.getDatasetName() + "." + stmtCreateIndex.getIndexName() + "'");
        }
        runJob(hcc, spec);
    }

    private void compileDatasetDropStatement(IHyracksClientConnection hcc, MetadataTransactionContext mdTxnCtx,
            String datasetName) throws Exception {
        CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(datasetName);
        Dataset ds = MetadataManager.INSTANCE
                .getDataset(mdTxnCtx, compiledDeclarations.getDataverseName(), datasetName);
        if (ds.getDatasetType() == DatasetType.INTERNAL || ds.getDatasetType() == DatasetType.FEED) {
            JobSpecification[] jobs = DatasetOperations.createDropDatasetJobSpec(cds, compiledDeclarations);
            for (JobSpecification job : jobs)
                runJob(hcc, job);
        }
        MetadataManager.INSTANCE.dropDataset(mdTxnCtx, compiledDeclarations.getDataverseName(), datasetName);
    }

    public AqlCompiledMetadataDeclarations getCompiledDeclarations() {
        return compiledDeclarations;
    }

    private void compileIndexDropStatement(IHyracksClientConnection hcc, MetadataTransactionContext mdTxnCtx,
            String datasetName, String indexName) throws Exception {
        CompiledIndexDropStatement cds = new CompiledIndexDropStatement(datasetName, indexName);
        runJob(hcc, IndexOperations.buildDropSecondaryIndexJobSpec(cds, compiledDeclarations));
        MetadataManager.INSTANCE.dropIndex(mdTxnCtx, compiledDeclarations.getDataverseName(), datasetName, indexName);
    }

    private Map<String, IAType> computeTypes(MetadataTransactionContext mdTxnCtx, TypeDecl tDec)
            throws AlgebricksException, MetadataException {
        Map<String, IAType> typeMap = new HashMap<String, IAType>();
        Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes = new HashMap<String, Map<ARecordType, List<Integer>>>();
        Map<String, List<AbstractCollectionType>> incompleteItemTypes = new HashMap<String, List<AbstractCollectionType>>();
        Map<String, List<String>> incompleteTopLevelTypeReferences = new HashMap<String, List<String>>();

        firstPass(tDec, typeMap, incompleteFieldTypes, incompleteItemTypes, incompleteTopLevelTypeReferences);
        secondPass(mdTxnCtx, typeMap, incompleteFieldTypes, incompleteItemTypes, incompleteTopLevelTypeReferences);

        return typeMap;
    }

    private void secondPass(MetadataTransactionContext mdTxnCtx, Map<String, IAType> typeMap,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<String, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, List<String>> incompleteTopLevelTypeReferences) throws AlgebricksException, MetadataException {
        // solve remaining top level references
        for (String trefName : incompleteTopLevelTypeReferences.keySet()) {
            IAType t;// = typeMap.get(trefName);
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, compiledDeclarations.getDataverseName(),
                    trefName);
            if (dt == null) {
                throw new AlgebricksException("Could not resolve type " + trefName);
            } else
                t = dt.getDatatype();
            for (String tname : incompleteTopLevelTypeReferences.get(trefName)) {
                typeMap.put(tname, t);
            }
        }
        // solve remaining field type references
        for (String trefName : incompleteFieldTypes.keySet()) {
            IAType t;// = typeMap.get(trefName);
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, compiledDeclarations.getDataverseName(),
                    trefName);
            if (dt == null) {
                throw new AlgebricksException("Could not resolve type " + trefName);
            } else
                t = dt.getDatatype();
            Map<ARecordType, List<Integer>> fieldsToFix = incompleteFieldTypes.get(trefName);
            for (ARecordType recType : fieldsToFix.keySet()) {
                List<Integer> positions = fieldsToFix.get(recType);
                IAType[] fldTypes = recType.getFieldTypes();
                for (Integer pos : positions) {
                    if (fldTypes[pos] == null) {
                        fldTypes[pos] = t;
                    } else { // nullable
                        AUnionType nullableUnion = (AUnionType) fldTypes[pos];
                        nullableUnion.setTypeAtIndex(t, 1);
                    }
                }
            }
        }
        // solve remaining item type references
        for (String trefName : incompleteItemTypes.keySet()) {
            IAType t;// = typeMap.get(trefName);
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, compiledDeclarations.getDataverseName(),
                    trefName);
            if (dt == null) {
                throw new AlgebricksException("Could not resolve type " + trefName);
            } else
                t = dt.getDatatype();
            for (AbstractCollectionType act : incompleteItemTypes.get(trefName)) {
                act.setItemType(t);
            }
        }
    }

    private void firstPass(TypeDecl td, Map<String, IAType> typeMap,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<String, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, List<String>> incompleteTopLevelTypeReferences) throws AlgebricksException {

        TypeExpression texpr = td.getTypeDef();
        String tdname = td.getIdent().getValue();
        if (builtinTypeMap.get(tdname) != null) {
            throw new AlgebricksException("Cannot redefine builtin type " + tdname + " .");
        }
        switch (texpr.getTypeKind()) {
            case TYPEREFERENCE: {
                TypeReferenceExpression tre = (TypeReferenceExpression) texpr;
                IAType t = solveTypeReference(tre, typeMap);
                if (t != null) {
                    typeMap.put(tdname, t);
                } else {
                    addIncompleteTopLevelTypeReference(tdname, tre, incompleteTopLevelTypeReferences);
                }
                break;
            }
            case RECORD: {
                RecordTypeDefinition rtd = (RecordTypeDefinition) texpr;
                ARecordType recType = computeRecordType(tdname, rtd, typeMap, incompleteFieldTypes, incompleteItemTypes);
                typeMap.put(tdname, recType);
                break;
            }
            case ORDEREDLIST: {
                OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) texpr;
                AOrderedListType olType = computeOrderedListType(tdname, oltd, typeMap, incompleteItemTypes,
                        incompleteFieldTypes);
                typeMap.put(tdname, olType);
                break;
            }
            case UNORDEREDLIST: {
                UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) texpr;
                AUnorderedListType ulType = computeUnorderedListType(tdname, ultd, typeMap, incompleteItemTypes,
                        incompleteFieldTypes);
                typeMap.put(tdname, ulType);
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    private AOrderedListType computeOrderedListType(String typeName, OrderedListTypeDefinition oltd,
            Map<String, IAType> typeMap, Map<String, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes) {
        TypeExpression tExpr = oltd.getItemTypeExpression();
        AOrderedListType aolt = new AOrderedListType(null, typeName);
        setCollectionItemType(tExpr, typeMap, incompleteItemTypes, incompleteFieldTypes, aolt);
        return aolt;
    }

    private AUnorderedListType computeUnorderedListType(String typeName, UnorderedListTypeDefinition ultd,
            Map<String, IAType> typeMap, Map<String, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes) {
        TypeExpression tExpr = ultd.getItemTypeExpression();
        AUnorderedListType ault = new AUnorderedListType(null, typeName);
        setCollectionItemType(tExpr, typeMap, incompleteItemTypes, incompleteFieldTypes, ault);
        return ault;
    }

    private void setCollectionItemType(TypeExpression tExpr, Map<String, IAType> typeMap,
            Map<String, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes, AbstractCollectionType act) {
        switch (tExpr.getTypeKind()) {
            case ORDEREDLIST: {
                OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) tExpr;
                IAType t = computeOrderedListType(null, oltd, typeMap, incompleteItemTypes, incompleteFieldTypes);
                act.setItemType(t);
                break;
            }
            case UNORDEREDLIST: {
                UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) tExpr;
                IAType t = computeUnorderedListType(null, ultd, typeMap, incompleteItemTypes, incompleteFieldTypes);
                act.setItemType(t);
                break;
            }
            case RECORD: {
                RecordTypeDefinition rtd = (RecordTypeDefinition) tExpr;
                IAType t = computeRecordType(null, rtd, typeMap, incompleteFieldTypes, incompleteItemTypes);
                act.setItemType(t);
                break;
            }
            case TYPEREFERENCE: {
                TypeReferenceExpression tre = (TypeReferenceExpression) tExpr;
                IAType tref = solveTypeReference(tre, typeMap);
                if (tref != null) {
                    act.setItemType(tref);
                } else {
                    addIncompleteCollectionTypeReference(act, tre, incompleteItemTypes);
                }
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    private ARecordType computeRecordType(String typeName, RecordTypeDefinition rtd, Map<String, IAType> typeMap,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<String, List<AbstractCollectionType>> incompleteItemTypes) {
        List<String> names = rtd.getFieldNames();
        int n = names.size();
        String[] fldNames = new String[n];
        IAType[] fldTypes = new IAType[n];
        int i = 0;
        for (String s : names) {
            fldNames[i++] = s;
        }
        boolean isOpen = rtd.getRecordKind() == RecordKind.OPEN;
        ARecordType recType = new ARecordType(typeName, fldNames, fldTypes, isOpen);
        for (int j = 0; j < n; j++) {
            TypeExpression texpr = rtd.getFieldTypes().get(j);
            switch (texpr.getTypeKind()) {
                case TYPEREFERENCE: {
                    TypeReferenceExpression tre = (TypeReferenceExpression) texpr;
                    IAType tref = solveTypeReference(tre, typeMap);
                    if (tref != null) {
                        if (!rtd.getNullableFields().get(j)) { // not nullable
                            fldTypes[j] = tref;
                        } else { // nullable
                            fldTypes[j] = makeUnionWithNull(null, tref);
                        }
                    } else {
                        addIncompleteFieldTypeReference(recType, j, tre, incompleteFieldTypes);
                        if (rtd.getNullableFields().get(j)) {
                            fldTypes[j] = makeUnionWithNull(null, null);
                        }
                    }
                    break;
                }
                case RECORD: {
                    RecordTypeDefinition recTypeDef2 = (RecordTypeDefinition) texpr;
                    IAType t2 = computeRecordType(null, recTypeDef2, typeMap, incompleteFieldTypes, incompleteItemTypes);
                    if (!rtd.getNullableFields().get(j)) { // not nullable
                        fldTypes[j] = t2;
                    } else { // nullable
                        fldTypes[j] = makeUnionWithNull(null, t2);
                    }
                    break;
                }
                case ORDEREDLIST: {
                    OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) texpr;
                    IAType t2 = computeOrderedListType(null, oltd, typeMap, incompleteItemTypes, incompleteFieldTypes);
                    fldTypes[j] = (rtd.getNullableFields().get(j)) ? makeUnionWithNull(null, t2) : t2;
                    break;
                }
                case UNORDEREDLIST: {
                    UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) texpr;
                    IAType t2 = computeUnorderedListType(null, ultd, typeMap, incompleteItemTypes, incompleteFieldTypes);
                    fldTypes[j] = (rtd.getNullableFields().get(j)) ? makeUnionWithNull(null, t2) : t2;
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }

        }

        return recType;
    }

    private AUnionType makeUnionWithNull(String unionTypeName, IAType type) {
        ArrayList<IAType> unionList = new ArrayList<IAType>(2);
        unionList.add(BuiltinType.ANULL);
        unionList.add(type);
        return new AUnionType(unionList, unionTypeName);
    }

    private void addIncompleteCollectionTypeReference(AbstractCollectionType collType, TypeReferenceExpression tre,
            Map<String, List<AbstractCollectionType>> incompleteItemTypes) {
        String typeName = tre.getIdent().getValue();
        List<AbstractCollectionType> typeList = incompleteItemTypes.get(typeName);
        if (typeList == null) {
            typeList = new LinkedList<AbstractCollectionType>();
            incompleteItemTypes.put(typeName, typeList);
        }
        typeList.add(collType);
    }

    private void addIncompleteFieldTypeReference(ARecordType recType, int fldPosition, TypeReferenceExpression tre,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes) {
        String typeName = tre.getIdent().getValue();
        Map<ARecordType, List<Integer>> refMap = incompleteFieldTypes.get(typeName);
        if (refMap == null) {
            refMap = new HashMap<ARecordType, List<Integer>>();
            incompleteFieldTypes.put(typeName, refMap);
        }
        List<Integer> typeList = refMap.get(recType);
        if (typeList == null) {
            typeList = new ArrayList<Integer>();
            refMap.put(recType, typeList);
        }
        typeList.add(fldPosition);
    }

    private void addIncompleteTopLevelTypeReference(String tdeclName, TypeReferenceExpression tre,
            Map<String, List<String>> incompleteTopLevelTypeReferences) {
        String name = tre.getIdent().getValue();
        List<String> refList = incompleteTopLevelTypeReferences.get(name);
        if (refList == null) {
            refList = new LinkedList<String>();
            incompleteTopLevelTypeReferences.put(name, refList);
        }
        refList.add(tdeclName);
    }

    private IAType solveTypeReference(TypeReferenceExpression tre, Map<String, IAType> typeMap) {
        String name = tre.getIdent().getValue();
        IAType builtin = builtinTypeMap.get(name);
        if (builtin != null) {
            return builtin;
        } else {
            return typeMap.get(name);
        }
    }

    public static interface ICompiledStatement {

        public abstract Kind getKind();
    }

    public static class CompiledLoadFromFileStatement implements ICompiledStatement, IParseFileSplitsDecl {
        private String datasetName;
        private FileSplit[] splits;
        private boolean alreadySorted;
        private Character delimChar;

        public CompiledLoadFromFileStatement(String datasetName, FileSplit[] splits, Character delimChar,
                boolean alreadySorted) {
            this.datasetName = datasetName;
            this.splits = splits;
            this.delimChar = delimChar;
            this.alreadySorted = alreadySorted;
        }

        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public FileSplit[] getSplits() {
            return splits;
        }

        @Override
        public Character getDelimChar() {
            return delimChar;
        }

        public boolean alreadySorted() {
            return alreadySorted;
        }

        @Override
        public boolean isDelimitedFileFormat() {
            return delimChar != null;
        }

        @Override
        public Kind getKind() {
            return Kind.LOAD_FROM_FILE;
        }
    }

    public static class CompiledWriteFromQueryResultStatement implements ICompiledStatement {

        private String datasetName;
        private Query query;
        private int varCounter;

        public CompiledWriteFromQueryResultStatement(String datasetName, Query query, int varCounter) {
            this.datasetName = datasetName;
            this.query = query;
            this.varCounter = varCounter;
        }

        public String getDatasetName() {
            return datasetName;
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Query getQuery() {
            return query;
        }

        @Override
        public Kind getKind() {
            return Kind.WRITE_FROM_QUERY_RESULT;
        }

    }

    public static class CompiledDatasetDropStatement implements ICompiledStatement {
        private String datasetName;

        public CompiledDatasetDropStatement(String datasetName) {
            this.datasetName = datasetName;
        }

        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public Kind getKind() {
            return Kind.DATASET_DROP;
        }
    }

    // added by yasser
    public static class CompiledCreateDataverseStatement implements ICompiledStatement {
        private String dataverseName;
        private String format;

        public CompiledCreateDataverseStatement(String dataverseName, String format) {
            this.dataverseName = dataverseName;
            this.format = format;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getFormat() {
            return format;
        }

        @Override
        public Kind getKind() {
            return Kind.CREATE_DATAVERSE;
        }
    }

    public static class CompiledNodeGroupDropStatement implements ICompiledStatement {
        private String nodeGroupName;

        public CompiledNodeGroupDropStatement(String nodeGroupName) {
            this.nodeGroupName = nodeGroupName;
        }

        public String getNodeGroupName() {
            return nodeGroupName;
        }

        @Override
        public Kind getKind() {
            return Kind.NODEGROUP_DROP;
        }
    }

    public static class CompiledIndexDropStatement implements ICompiledStatement {
        private String datasetName;
        private String indexName;

        public CompiledIndexDropStatement(String datasetName, String indexName) {
            this.datasetName = datasetName;
            this.indexName = indexName;
        }

        public String getDatasetName() {
            return datasetName;
        }

        public String getIndexName() {
            return indexName;
        }

        @Override
        public Kind getKind() {
            return Kind.INDEX_DROP;
        }
    }

    public static class CompiledDataverseDropStatement implements ICompiledStatement {
        private String dataverseName;
        private boolean ifExists;

        public CompiledDataverseDropStatement(String dataverseName, boolean ifExists) {
            this.dataverseName = dataverseName;
            this.ifExists = ifExists;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public boolean getIfExists() {
            return ifExists;
        }

        @Override
        public Kind getKind() {
            return Kind.DATAVERSE_DROP;
        }
    }

    public static class CompiledTypeDropStatement implements ICompiledStatement {
        private String typeName;

        public CompiledTypeDropStatement(String nodeGroupName) {
            this.typeName = nodeGroupName;
        }

        public String getTypeName() {
            return typeName;
        }

        @Override
        public Kind getKind() {
            return Kind.TYPE_DROP;
        }
    }
}
