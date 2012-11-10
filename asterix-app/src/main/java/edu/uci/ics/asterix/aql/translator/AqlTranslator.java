/*
 * Copyright 2009-2012 by The Regents of the University of California
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

import java.io.File;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.api.common.Job.SubmissionMode;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.BeginFeedStatement;
import edu.uci.ics.asterix.aql.expression.ControlFeedStatement;
import edu.uci.ics.asterix.aql.expression.CreateDataverseStatement;
import edu.uci.ics.asterix.aql.expression.CreateFunctionStatement;
import edu.uci.ics.asterix.aql.expression.CreateIndexStatement;
import edu.uci.ics.asterix.aql.expression.DatasetDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDropStatement;
import edu.uci.ics.asterix.aql.expression.DeleteStatement;
import edu.uci.ics.asterix.aql.expression.DropStatement;
import edu.uci.ics.asterix.aql.expression.ExternalDetailsDecl;
import edu.uci.ics.asterix.aql.expression.FeedDetailsDecl;
import edu.uci.ics.asterix.aql.expression.FunctionDecl;
import edu.uci.ics.asterix.aql.expression.FunctionDropStatement;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aql.expression.IndexDropStatement;
import edu.uci.ics.asterix.aql.expression.InsertStatement;
import edu.uci.ics.asterix.aql.expression.InternalDetailsDecl;
import edu.uci.ics.asterix.aql.expression.LoadFromFileStatement;
import edu.uci.ics.asterix.aql.expression.NodeGroupDropStatement;
import edu.uci.ics.asterix.aql.expression.NodegroupDecl;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.SetStatement;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.TypeDropStatement;
import edu.uci.ics.asterix.aql.expression.WriteFromQueryResultStatement;
import edu.uci.ics.asterix.aql.expression.WriteStatement;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.file.DatasetOperations;
import edu.uci.ics.asterix.file.FeedOperations;
import edu.uci.ics.asterix.file.IndexOperations;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeSignature;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.translator.AbstractAqlTranslator;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledBeginFeedStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledControlFeedStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledCreateIndexStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledDatasetDropStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledDeleteStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledIndexDropStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledInsertStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledLoadFromFileStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledWriteFromQueryResultStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import edu.uci.ics.asterix.translator.TypeTranslator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class AqlTranslator extends AbstractAqlTranslator {

    private final List<Statement> aqlStatements;
    private final PrintWriter out;
    private final SessionConfig sessionConfig;
    private final DisplayFormat pdf;
    private Dataverse activeDefaultDataverse;
    private List<FunctionDecl> declaredFunctions;

    public AqlTranslator(List<Statement> aqlStatements, PrintWriter out, SessionConfig pc, DisplayFormat pdf)
            throws MetadataException, AsterixException {
        this.aqlStatements = aqlStatements;
        this.out = out;
        this.sessionConfig = pc;
        this.pdf = pdf;
        declaredFunctions = getDeclaredFunctions(aqlStatements);
    }

    private List<FunctionDecl> getDeclaredFunctions(List<Statement> statements) {
        List<FunctionDecl> functionDecls = new ArrayList<FunctionDecl>();
        for (Statement st : statements) {
            if (st.getKind().equals(Statement.Kind.FUNCTION_DECL)) {
                functionDecls.add((FunctionDecl) st);
            }
        }
        return functionDecls;
    }

    public List<QueryResult> compileAndExecute(IHyracksClientConnection hcc) throws AlgebricksException,
            RemoteException, ACIDException, AsterixException {
        List<QueryResult> executionResult = new ArrayList<QueryResult>();
        FileSplit outputFile = null;
        IAWriterFactory writerFactory = PrinterBasedWriterFactory.INSTANCE;
        Map<String, String> config = new HashMap<String, String>();

        for (Statement stmt : aqlStatements) {
            validateOperation(activeDefaultDataverse, stmt);
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            AqlMetadataProvider metadataProvider = new AqlMetadataProvider(mdTxnCtx, activeDefaultDataverse);
            metadataProvider.setWriterFactory(writerFactory);
            metadataProvider.setOutputFile(outputFile);
            metadataProvider.setConfig(config);
            try {
                switch (stmt.getKind()) {
                    case SET: {
                        SetStatement ss = (SetStatement) stmt;
                        String pname = ss.getPropName();
                        String pvalue = ss.getPropValue();
                        config.put(pname, pvalue);
                        break;
                    }
                    case DATAVERSE_DECL: {
                        activeDefaultDataverse = handleUseDataverseStatement(metadataProvider, stmt);
                        break;
                    }
                    case CREATE_DATAVERSE: {
                        handleCreateDataverseStatement(metadataProvider, stmt);
                        break;
                    }
                    case DATASET_DECL: {
                        handleCreateDatasetStatement(metadataProvider, stmt, hcc);
                        break;
                    }
                    case CREATE_INDEX: {
                        handleCreateIndexStatement(metadataProvider, stmt, hcc);
                        break;
                    }
                    case TYPE_DECL: {
                        handleCreateTypeStatement(metadataProvider, stmt);
                        break;
                    }
                    case NODEGROUP_DECL: {
                        handleCreateNodeGroupStatement(metadataProvider, stmt);
                        break;
                    }
                    case DATAVERSE_DROP: {
                        handleDataverseDropStatement(metadataProvider, stmt, hcc);
                        break;
                    }
                    case DATASET_DROP: {
                        handleDatasetDropStatement(metadataProvider, stmt, hcc);
                        break;
                    }
                    case INDEX_DROP: {
                        handleIndexDropStatement(metadataProvider, stmt, hcc);
                        break;
                    }
                    case TYPE_DROP: {
                        handleTypeDropStatement(metadataProvider, stmt);
                        break;
                    }
                    case NODEGROUP_DROP: {
                        handleNodegroupDropStatement(metadataProvider, stmt);
                        break;
                    }

                    case CREATE_FUNCTION: {
                        handleCreateFunctionStatement(metadataProvider, stmt);
                        break;
                    }

                    case FUNCTION_DROP: {
                        handleFunctionDropStatement(metadataProvider, stmt);
                        break;
                    }

                    case LOAD_FROM_FILE: {
                        handleLoadFromFileStatement(metadataProvider, stmt, hcc);
                        break;
                    }
                    case WRITE_FROM_QUERY_RESULT: {
                        handleWriteFromQueryResultStatement(metadataProvider, stmt, hcc);
                        break;
                    }
                    case INSERT: {
                        handleInsertStatement(metadataProvider, stmt, hcc);
                        break;
                    }
                    case DELETE: {
                        handleDeleteStatement(metadataProvider, stmt, hcc);
                        break;
                    }

                    case BEGIN_FEED: {
                        handleBeginFeedStatement(metadataProvider, stmt, hcc);
                        break;
                    }

                    case CONTROL_FEED: {
                        handleControlFeedStatement(metadataProvider, stmt, hcc);
                        break;
                    }

                    case QUERY: {
                        executionResult.add(handleQuery(metadataProvider, (Query) stmt, hcc));
                        metadataProvider.setWriteTransaction(false);
                        break;
                    }

                    case WRITE: {
                        WriteStatement ws = (WriteStatement) stmt;
                        File f = new File(ws.getFileName());
                        outputFile = new FileSplit(ws.getNcName().getValue(), new FileReference(f));
                        if (ws.getWriterClassName() != null) {
                            try {
                                writerFactory = (IAWriterFactory) Class.forName(ws.getWriterClassName()).newInstance();
                                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                            } catch (Exception e) {
                                throw new AsterixException(e);
                            }
                        }
                        break;
                    }

                }

            } catch (Exception e) {
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                throw new AlgebricksException(e);
            }
        }
        return executionResult;
    }

    private Dataverse handleUseDataverseStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws MetadataException, RemoteException, ACIDException {
        DataverseDecl dvd = (DataverseDecl) stmt;
        String dvName = dvd.getDataverseName().getValue();
        Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
        if (dv == null) {
            throw new MetadataException(" Unknown dataverse " + dvName);
        }
        MetadataManager.INSTANCE.commitTransaction(metadataProvider.getMetadataTxnContext());
        return dv;

    }

    private void handleCreateDataverseStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws MetadataException, AlgebricksException, RemoteException, ACIDException {
        CreateDataverseStatement stmtCreateDataverse = (CreateDataverseStatement) stmt;
        String dvName = stmtCreateDataverse.getDataverseName().getValue();
        Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
        if (dv != null && !stmtCreateDataverse.getIfNotExists()) {
            throw new AlgebricksException("A dataverse with this name " + dvName + " already exists.");
        }
        MetadataManager.INSTANCE.addDataverse(metadataProvider.getMetadataTxnContext(), new Dataverse(dvName,
                stmtCreateDataverse.getFormat()));
        MetadataManager.INSTANCE.commitTransaction(metadataProvider.getMetadataTxnContext());
    }

    private void handleCreateDatasetStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws AsterixException, Exception {
        DatasetDecl dd = (DatasetDecl) stmt;
        String dataverseName = dd.getDataverse() != null ? dd.getDataverse().getValue()
                : activeDefaultDataverse != null ? activeDefaultDataverse.getDataverseName() : null;
        if (dataverseName == null) {
            throw new AlgebricksException(" dataverse not specified ");
        }
        String datasetName = dd.getName().getValue();
        DatasetType dsType = dd.getDatasetType();
        String itemTypeName = dd.getItemTypeName().getValue();

        IDatasetDetails datasetDetails = null;
        Dataset ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                datasetName);
        if (ds != null) {
            if (dd.getIfNotExists()) {
                return;
            } else {
                throw new AlgebricksException("A dataset with this name " + datasetName + " already exists.");
            }
        }
        Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(), dataverseName,
                itemTypeName);
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
                String ngName = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getNodegroupName().getValue();
                datasetDetails = new InternalDatasetDetails(InternalDatasetDetails.FileStructure.BTREE,
                        InternalDatasetDetails.PartitioningStrategy.HASH, partitioningExprs, partitioningExprs, ngName);
                break;
            }
            case EXTERNAL: {
                String adapter = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getAdapter();
                Map<String, String> properties = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getProperties();
                datasetDetails = new ExternalDatasetDetails(adapter, properties);
                break;
            }
            case FEED: {
                IAType itemType = dt.getDatatype();
                if (itemType.getTypeTag() != ATypeTag.RECORD) {
                    throw new AlgebricksException("Can only partition ARecord's.");
                }
                List<String> partitioningExprs = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getPartitioningExprs();
                String ngName = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getNodegroupName().getValue();
                String adapter = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getAdapterFactoryClassname();
                Map<String, String> configuration = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getConfiguration();
                FunctionSignature signature = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getFunctionSignature();
                datasetDetails = new FeedDatasetDetails(InternalDatasetDetails.FileStructure.BTREE,
                        InternalDatasetDetails.PartitioningStrategy.HASH, partitioningExprs, partitioningExprs, ngName,
                        adapter, configuration, signature, FeedDatasetDetails.FeedState.INACTIVE.toString());
                break;
            }
        }
        MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), new Dataset(dataverseName,
                datasetName, itemTypeName, datasetDetails, dsType));
        if (dd.getDatasetType() == DatasetType.INTERNAL || dd.getDatasetType() == DatasetType.FEED) {
            Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(),
                    dataverseName);
            runCreateDatasetJob(hcc, dataverse, datasetName, metadataProvider);
        }
        MetadataManager.INSTANCE.commitTransaction(metadataProvider.getMetadataTxnContext());

    }

    private void handleCreateIndexStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        CreateIndexStatement stmtCreateIndex = (CreateIndexStatement) stmt;
        String dataverseName = stmtCreateIndex.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : stmtCreateIndex.getDataverseName().getValue();
        if (dataverseName == null) {
            throw new AlgebricksException(" dataverse not specified ");
        }
        String datasetName = stmtCreateIndex.getDatasetName().getValue();
        Dataset ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                datasetName);
        if (ds == null) {
            throw new AlgebricksException("There is no dataset with this name " + datasetName + " in dataverse "
                    + dataverseName);
        }
        String indexName = stmtCreateIndex.getIndexName().getValue();
        Index idx = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                datasetName, indexName);
        if (idx != null) {
            if (!stmtCreateIndex.getIfNotExists()) {
                throw new AlgebricksException("An index with this name " + indexName + " already exists.");
            } else {
                stmtCreateIndex.setNeedToCreate(false);
            }
        } else {
            Index index = new Index(dataverseName, datasetName, indexName, stmtCreateIndex.getIndexType(),
                    stmtCreateIndex.getFieldExprs(), stmtCreateIndex.getGramLength(), false);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);
            runCreateIndexJob(hcc, stmtCreateIndex, metadataProvider);

            CompiledCreateIndexStatement cis = new CompiledCreateIndexStatement(index.getIndexName(), dataverseName,
                    index.getDatasetName(), index.getKeyFieldNames(), index.getGramLength(), index.getIndexType());
            JobSpecification loadIndexJobSpec = IndexOperations
                    .buildSecondaryIndexLoadingJobSpec(cis, metadataProvider);
            runJob(hcc, loadIndexJobSpec);
        }
        MetadataManager.INSTANCE.commitTransaction(metadataProvider.getMetadataTxnContext());
    }

    private void handleCreateTypeStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws AlgebricksException, RemoteException, ACIDException, MetadataException {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        TypeDecl stmtCreateType = (TypeDecl) stmt;
        String dataverseName = stmtCreateType.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : stmtCreateType.getDataverseName().getValue();
        if (dataverseName == null) {
            throw new AlgebricksException(" dataverse not specified ");
        }
        String typeName = stmtCreateType.getIdent().getValue();
        Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
        if (dv == null) {
            throw new AlgebricksException("Unknonw dataverse " + dataverseName);
        }
        Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
        if (dt != null) {
            if (!stmtCreateType.getIfNotExists())
                throw new AlgebricksException("A datatype with this name " + typeName + " already exists.");
        } else {
            if (builtinTypeMap.get(typeName) != null) {
                throw new AlgebricksException("Cannot redefine builtin type " + typeName + ".");
            } else {
                Map<TypeSignature, IAType> typeMap = TypeTranslator.computeTypes(mdTxnCtx, (TypeDecl) stmt,
                        dataverseName);
                TypeSignature typeSignature = new TypeSignature(dataverseName, typeName);
                IAType type = typeMap.get(typeSignature);
                MetadataManager.INSTANCE.addDatatype(mdTxnCtx, new Datatype(dataverseName, typeName, type, false));
            }
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }

    private void handleDataverseDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        DataverseDropStatement stmtDelete = (DataverseDropStatement) stmt;
        String dvName = stmtDelete.getDataverseName().getValue();

        Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dvName);
        if (dv == null) {
            if (!stmtDelete.getIfExists()) {
                throw new AlgebricksException("There is no dataverse with this name " + dvName + ".");
            }
        } else {
            List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dvName);
            for (int j = 0; j < datasets.size(); j++) {
                String datasetName = datasets.get(j).getDatasetName();
                DatasetType dsType = datasets.get(j).getDatasetType();
                if (dsType == DatasetType.INTERNAL || dsType == DatasetType.FEED) {
                    List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dvName, datasetName);
                    for (int k = 0; k < indexes.size(); k++) {
                        if (indexes.get(k).isSecondaryIndex()) {
                            compileIndexDropStatement(hcc, dvName, datasetName, indexes.get(k).getIndexName(),
                                    metadataProvider);
                        }
                    }
                }
                compileDatasetDropStatement(hcc, dvName, datasetName, metadataProvider);
            }

            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dvName);
            if (activeDefaultDataverse != null && activeDefaultDataverse.getDataverseName() == dvName) {
                activeDefaultDataverse = null;
            }
        }
        MetadataManager.INSTANCE.commitTransaction(metadataProvider.getMetadataTxnContext());
    }

    private void handleDatasetDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        DropStatement stmtDelete = (DropStatement) stmt;
        String dataverseName = stmtDelete.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : stmtDelete.getDataverseName().getValue();
        if (dataverseName == null) {
            throw new AlgebricksException(" dataverse not specified ");
        }
        String datasetName = stmtDelete.getDatasetName().getValue();
        Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
        if (ds == null) {
            if (!stmtDelete.getIfExists())
                throw new AlgebricksException("There is no dataset with this name " + datasetName + " in dataverse "
                        + dataverseName + ".");
        } else {
            if (ds.getDatasetType() == DatasetType.INTERNAL || ds.getDatasetType() == DatasetType.FEED) {
                List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
                for (int j = 0; j < indexes.size(); j++) {
                    if (indexes.get(j).isPrimaryIndex()) {
                        compileIndexDropStatement(hcc, dataverseName, datasetName, indexes.get(j).getIndexName(),
                                metadataProvider);
                    }
                }
            }
            compileDatasetDropStatement(hcc, dataverseName, datasetName, metadataProvider);
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }

    private void handleIndexDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        IndexDropStatement stmtIndexDrop = (IndexDropStatement) stmt;
        String datasetName = stmtIndexDrop.getDatasetName().getValue();
        String dataverseName = stmtIndexDrop.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : stmtIndexDrop.getDataverseName().getValue();
        if (dataverseName == null) {
            throw new AlgebricksException(" dataverse not specified ");
        }
        Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
        if (ds == null)
            throw new AlgebricksException("There is no dataset with this name " + datasetName + " in dataverse "
                    + dataverseName);
        if (ds.getDatasetType() == DatasetType.INTERNAL || ds.getDatasetType() == DatasetType.FEED) {
            String indexName = stmtIndexDrop.getIndexName().getValue();
            Index idx = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, indexName);
            if (idx == null) {
                if (!stmtIndexDrop.getIfExists())
                    throw new AlgebricksException("There is no index with this name " + indexName + ".");
            } else
                compileIndexDropStatement(hcc, dataverseName, datasetName, indexName, metadataProvider);
        } else {
            throw new AlgebricksException(datasetName
                    + " is an external dataset. Indexes are not maintained for external datasets.");
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }

    private void handleTypeDropStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws AlgebricksException, MetadataException, RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        TypeDropStatement stmtTypeDrop = (TypeDropStatement) stmt;
        String dataverseName = stmtTypeDrop.getDataverseName() == null ? (activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName()) : stmtTypeDrop.getDataverseName().getValue();
        if (dataverseName == null) {
            throw new AlgebricksException(" dataverse not specified ");
        }
        String typeName = stmtTypeDrop.getTypeName().getValue();
        Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
        if (dt == null) {
            if (!stmtTypeDrop.getIfExists())
                throw new AlgebricksException("There is no datatype with this name " + typeName + ".");
        } else {
            MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, dataverseName, typeName);
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

    }

    private void handleNodegroupDropStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws MetadataException, AlgebricksException, RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        NodeGroupDropStatement stmtDelete = (NodeGroupDropStatement) stmt;
        String nodegroupName = stmtDelete.getNodeGroupName().getValue();
        NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodegroupName);
        if (ng == null) {
            if (!stmtDelete.getIfExists())
                throw new AlgebricksException("There is no nodegroup with this name " + nodegroupName + ".");
        } else {
            MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, nodegroupName);
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }

    private void handleCreateFunctionStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws AlgebricksException, MetadataException, RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        CreateFunctionStatement cfs = (CreateFunctionStatement) stmt;
        String dataverse = cfs.getSignature().getNamespace() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : cfs.getSignature().getNamespace();
        if (dataverse == null) {
            throw new AlgebricksException(" dataverse not specified ");
        }
        Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
        if (dv == null) {
            throw new AlgebricksException("There is no dataverse with this name " + dataverse + ".");
        }
        Function function = new Function(dataverse, cfs.getaAterixFunction().getName(), cfs.getaAterixFunction()
                .getArity(), cfs.getParamList(), Function.RETURNTYPE_VOID, cfs.getFunctionBody(),
                Function.LANGUAGE_AQL, FunctionKind.SCALAR.toString());
        MetadataManager.INSTANCE.addFunction(mdTxnCtx, function);
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }

    private void handleFunctionDropStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws MetadataException, RemoteException, ACIDException, AlgebricksException {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        FunctionDropStatement stmtDropFunction = (FunctionDropStatement) stmt;
        FunctionSignature signature = stmtDropFunction.getFunctionSignature();
        Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
        if (function == null) {
            if (!stmtDropFunction.getIfExists())
                throw new AlgebricksException("Unknonw function " + signature);
        } else {
            MetadataManager.INSTANCE.dropFunction(mdTxnCtx, signature);
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }

    private void handleLoadFromFileStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        List<Job> jobs = new ArrayList<Job>();
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        LoadFromFileStatement loadStmt = (LoadFromFileStatement) stmt;
        String dataverseName = loadStmt.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : loadStmt.getDataverseName().getValue();
        CompiledLoadFromFileStatement cls = new CompiledLoadFromFileStatement(dataverseName, loadStmt.getDatasetName()
                .getValue(), loadStmt.getAdapter(), loadStmt.getProperties(), loadStmt.dataIsAlreadySorted());

        IDataFormat format = getDataFormat(metadataProvider.getMetadataTxnContext(), dataverseName);
        Job job = DatasetOperations.createLoadDatasetJobSpec(metadataProvider, cls, format);
        jobs.add(job);
        // Also load the dataset's secondary indexes.
        List<Index> datasetIndexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, loadStmt
                .getDatasetName().getValue());
        for (Index index : datasetIndexes) {
            if (!index.isSecondaryIndex()) {
                continue;
            }
            // Create CompiledCreateIndexStatement from metadata entity 'index'.
            CompiledCreateIndexStatement cis = new CompiledCreateIndexStatement(index.getIndexName(), dataverseName,
                    index.getDatasetName(), index.getKeyFieldNames(), index.getGramLength(), index.getIndexType());
            JobSpecification jobSpec = IndexOperations.buildSecondaryIndexLoadingJobSpec(cis, metadataProvider);
            jobs.add(new Job(jobSpec));
        }
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        for (Job j : jobs) {
            runJob(hcc, j.getJobSpec());
        }
    }

    private void handleWriteFromQueryResultStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        WriteFromQueryResultStatement st1 = (WriteFromQueryResultStatement) stmt;
        String dataverseName = st1.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : st1.getDataverseName().getValue();
        CompiledWriteFromQueryResultStatement clfrqs = new CompiledWriteFromQueryResultStatement(dataverseName, st1
                .getDatasetName().getValue(), st1.getQuery(), st1.getVarCounter());

        Pair<JobSpecification, FileSplit> compiled = rewriteCompileQuery(metadataProvider, clfrqs.getQuery(), clfrqs);
        runJob(hcc, compiled.first);
    }

    private void handleInsertStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        metadataProvider.setWriteTransaction(true);
        InsertStatement stmtInsert = (InsertStatement) stmt;
        String dataverseName = stmtInsert.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : stmtInsert.getDataverseName().getValue();
        CompiledInsertStatement clfrqs = new CompiledInsertStatement(dataverseName, stmtInsert.getDatasetName()
                .getValue(), stmtInsert.getQuery(), stmtInsert.getVarCounter());
        Pair<JobSpecification, FileSplit> compiled = rewriteCompileQuery(metadataProvider, clfrqs.getQuery(), clfrqs);
        runJob(hcc, compiled.first);
    }

    private void handleDeleteStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        metadataProvider.setWriteTransaction(true);
        DeleteStatement stmtDelete = (DeleteStatement) stmt;
        String dataverseName = stmtDelete.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : stmtDelete.getDataverseName().getValue();
        CompiledDeleteStatement clfrqs = new CompiledDeleteStatement(stmtDelete.getVariableExpr(), dataverseName,
                stmtDelete.getDatasetName().getValue(), stmtDelete.getCondition(), stmtDelete.getDieClause(),
                stmtDelete.getVarCounter(), metadataProvider);
        Pair<JobSpecification, FileSplit> compiled = rewriteCompileQuery(metadataProvider, clfrqs.getQuery(), clfrqs);
        runJob(hcc, compiled.first);
    }

    private Pair<JobSpecification, FileSplit> rewriteCompileQuery(AqlMetadataProvider metadataProvider, Query query,
            ICompiledDmlStatement stmt) throws AsterixException, RemoteException, AlgebricksException, JSONException,
            ACIDException {
        Pair<Query, Integer> reWrittenQuery = APIFramework.reWriteQuery(declaredFunctions, metadataProvider, query,
                sessionConfig, out, pdf);
        MetadataManager.INSTANCE.commitTransaction(metadataProvider.getMetadataTxnContext());
        Pair<JobSpecification, FileSplit> compiled = compileQuery(sessionConfig, reWrittenQuery.first,
                metadataProvider, reWrittenQuery.second, stmt);
        return compiled;

    }

    private Pair<JobSpecification, FileSplit> compileQuery(SessionConfig sessionConfig, Query query,
            AqlMetadataProvider metadataProvider, int varCounter, ICompiledDmlStatement statement)
            throws RemoteException, AsterixException, AlgebricksException, JSONException, ACIDException {
        sessionConfig.setGenerateJobSpec(true);
        MetadataTransactionContext mdTxnCtxQuery = MetadataManager.INSTANCE.beginTransaction();
        AqlMetadataProvider metadataProviderInsert = new AqlMetadataProvider(mdTxnCtxQuery, activeDefaultDataverse);
        metadataProviderInsert.setWriterFactory(metadataProvider.getWriterFactory());
        metadataProviderInsert.setOutputFile(metadataProvider.getOutputFile());
        metadataProviderInsert.setConfig(metadataProvider.getConfig());
        JobSpecification spec = APIFramework.compileQuery(declaredFunctions, metadataProvider, query, varCounter,
                statement == null ? null : statement.getDatasetName(), sessionConfig, out, pdf, statement);
        sessionConfig.setGenerateJobSpec(false);
        return new Pair<JobSpecification, FileSplit>(spec, metadataProvider.getOutputFile());
    }

    private void handleBeginFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        BeginFeedStatement bfs = (BeginFeedStatement) stmt;
        String dataverseName = bfs.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : bfs.getDatasetName().getValue();

        CompiledBeginFeedStatement cbfs = new CompiledBeginFeedStatement(dataverseName,
                bfs.getDatasetName().getValue(), bfs.getQuery(), bfs.getVarCounter());

        Dataset dataset;
        dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, bfs.getDatasetName().getValue());
        IDatasetDetails datasetDetails = dataset.getDatasetDetails();
        if (datasetDetails.getDatasetType() != DatasetType.FEED) {
            throw new IllegalArgumentException("Dataset " + bfs.getDatasetName().getValue() + " is not a feed dataset");
        }
        bfs.initialize(mdTxnCtx, dataset);
        cbfs.setQuery(bfs.getQuery());

        Pair<Query, Integer> reWrittenQuery = APIFramework.reWriteQuery(declaredFunctions, metadataProvider,
                bfs.getQuery(), sessionConfig, out, pdf);
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        Pair<JobSpecification, FileSplit> compiled = compileQuery(sessionConfig, reWrittenQuery.first,
                metadataProvider, reWrittenQuery.second, cbfs);
        runJob(hcc, compiled.first);
    }

    private void handleControlFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        ControlFeedStatement cfs = (ControlFeedStatement) stmt;
        String dataverseName = cfs.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : cfs.getDatasetName().getValue();
        CompiledControlFeedStatement clcfs = new CompiledControlFeedStatement(cfs.getOperationType(), dataverseName,
                cfs.getDatasetName().getValue(), cfs.getAlterAdapterConfParams());
        Job job = new Job(FeedOperations.buildControlFeedJobSpec(clcfs, metadataProvider), SubmissionMode.ASYNCHRONOUS);
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        runJob(hcc, job.getJobSpec());
    }

    private QueryResult handleQuery(AqlMetadataProvider metadataProvider, Query query, IHyracksClientConnection hcc)
            throws Exception {
        Pair<JobSpecification, FileSplit> compiled = rewriteCompileQuery(metadataProvider, query, null);
        runJob(hcc, compiled.first);
        GlobalConfig.ASTERIX_LOGGER.info(compiled.first.toJSON().toString(1));
        return new QueryResult(query, compiled.second.getLocalFile().getFile().getAbsolutePath());
    }

    private void runCreateDatasetJob(IHyracksClientConnection hcc, Dataverse dataverse, String datasetName,
            AqlMetadataProvider metadataProvider) throws AsterixException, AlgebricksException, Exception {
        runJob(hcc, DatasetOperations.createDatasetJobSpec(dataverse, datasetName, metadataProvider));
    }

    private void runCreateIndexJob(IHyracksClientConnection hcc, CreateIndexStatement stmtCreateIndex,
            AqlMetadataProvider metadataProvider) throws Exception {
        // TODO: Eventually CreateIndexStatement and
        // CompiledCreateIndexStatement should be replaced by the corresponding
        // metadata entity.
        // For now we must still convert to a CompiledCreateIndexStatement here.
        String dataverseName = stmtCreateIndex.getDataverseName() == null ? activeDefaultDataverse == null ? null
                : activeDefaultDataverse.getDataverseName() : stmtCreateIndex.getDataverseName().getValue();
        CompiledCreateIndexStatement createIndexStmt = new CompiledCreateIndexStatement(stmtCreateIndex.getIndexName()
                .getValue(), dataverseName, stmtCreateIndex.getDatasetName().getValue(),
                stmtCreateIndex.getFieldExprs(), stmtCreateIndex.getGramLength(), stmtCreateIndex.getIndexType());
        JobSpecification spec = IndexOperations.buildSecondaryIndexCreationJobSpec(createIndexStmt, metadataProvider);
        if (spec == null) {
            throw new AsterixException("Failed to create job spec for creating index '"
                    + stmtCreateIndex.getDatasetName() + "." + stmtCreateIndex.getIndexName() + "'");
        }
        runJob(hcc, new Job(spec));
    }

    private void handleCreateNodeGroupStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws MetadataException, AlgebricksException, RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
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
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
    }

    private void runJob(IHyracksClientConnection hcc, Job job) throws Exception {
        executeJobArray(hcc, new Job[] { job }, out, pdf);
    }

    private void runJob(IHyracksClientConnection hcc, JobSpecification spec) throws Exception {
        executeJobArray(hcc, new Job[] { new Job(spec) }, out, pdf);
    }

    private void compileIndexDropStatement(IHyracksClientConnection hcc, String dataverseName, String datasetName,
            String indexName, AqlMetadataProvider metadataProvider) throws Exception {
        CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName, indexName);
        runJob(hcc, new Job(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider)));
        MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName,
                indexName);
    }

    private void compileDatasetDropStatement(IHyracksClientConnection hcc, String dataverseName, String datasetName,
            AqlMetadataProvider metadataProvider) throws Exception {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(dataverseName, datasetName);
        Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
        if (ds.getDatasetType() == DatasetType.INTERNAL || ds.getDatasetType() == DatasetType.FEED) {
            JobSpecification[] jobSpecs = DatasetOperations.createDropDatasetJobSpec(cds, metadataProvider);
            for (JobSpecification spec : jobSpecs)
                runJob(hcc, new Job(spec));
        }
        MetadataManager.INSTANCE.dropDataset(mdTxnCtx, dataverseName, datasetName);
    }

    public void executeJobArray(IHyracksClientConnection hcc, Job[] jobs, PrintWriter out, DisplayFormat pdf)
            throws Exception {
        for (int i = 0; i < jobs.length; i++) {
            JobSpecification spec = jobs[i].getJobSpec();
            spec.setMaxReattempts(0);
            JobId jobId = hcc.startJob(GlobalConfig.HYRACKS_APP_NAME, spec);
            hcc.waitForCompletion(jobId);
        }
    }

    private static IDataFormat getDataFormat(MetadataTransactionContext mdTxnCtx, String dataverseName)
            throws AsterixException {
        Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return format;
    }

    public List<Statement> getAqlStatements() {
        return aqlStatements;
    }

    public PrintWriter getOut() {
        return out;
    }

    public SessionConfig getPc() {
        return sessionConfig;
    }

    public DisplayFormat getPdf() {
        return pdf;
    }

}
