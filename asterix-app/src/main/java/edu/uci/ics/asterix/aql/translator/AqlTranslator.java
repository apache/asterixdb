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
package edu.uci.ics.asterix.aql.translator;

import java.io.File;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.Job;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.BeginFeedStatement;
import edu.uci.ics.asterix.aql.expression.CompactStatement;
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
import edu.uci.ics.asterix.aql.expression.WriteStatement;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.file.DatasetOperations;
import edu.uci.ics.asterix.file.DataverseOperations;
import edu.uci.ics.asterix.file.FeedOperations;
import edu.uci.ics.asterix.file.IndexOperations;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.CompactionPolicy;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeSignature;
import edu.uci.ics.asterix.result.ResultReader;
import edu.uci.ics.asterix.result.ResultUtils;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetIdFactory;
import edu.uci.ics.asterix.translator.AbstractAqlTranslator;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledBeginFeedStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledControlFeedStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledCreateIndexStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledDatasetDropStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledDeleteStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledIndexCompactStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledIndexDropStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledInsertStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.CompiledLoadFromFileStatement;
import edu.uci.ics.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import edu.uci.ics.asterix.translator.TypeTranslator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.serializer.ResultSerializerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

/*
 * Provides functionality for executing a batch of AQL statements (queries included)
 * sequentially.
 */
public class AqlTranslator extends AbstractAqlTranslator {

    private enum ProgressState {
        NO_PROGRESS,
        ADDED_PENDINGOP_RECORD_TO_METADATA
    }

    public static final boolean IS_DEBUG_MODE = false;//true
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

    /**
     * Compiles and submits for execution a list of AQL statements.
     * 
     * @param hcc
     *            A Hyracks client connection that is used to submit a jobspec to Hyracks.
     * @param hdc
     *            A Hyracks dataset client object that is used to read the results.
     * @param asyncResults
     *            True if the results should be read asynchronously or false if we should wait for results to be read.
     * @return A List<QueryResult> containing a QueryResult instance corresponding to each submitted query.
     * @throws Exception
     */
    public List<QueryResult> compileAndExecute(IHyracksClientConnection hcc, IHyracksDataset hdc, boolean asyncResults)
            throws Exception {
        int resultSetIdCounter = 0;
        List<QueryResult> executionResult = new ArrayList<QueryResult>();
        FileSplit outputFile = null;
        IAWriterFactory writerFactory = PrinterBasedWriterFactory.INSTANCE;
        IResultSerializerFactoryProvider resultSerializerFactoryProvider = ResultSerializerFactoryProvider.INSTANCE;
        Map<String, String> config = new HashMap<String, String>();

        for (Statement stmt : aqlStatements) {
            validateOperation(activeDefaultDataverse, stmt);
            AqlMetadataProvider metadataProvider = new AqlMetadataProvider(activeDefaultDataverse);
            metadataProvider.setWriterFactory(writerFactory);
            metadataProvider.setResultSerializerFactoryProvider(resultSerializerFactoryProvider);
            metadataProvider.setOutputFile(outputFile);
            metadataProvider.setConfig(config);
            switch (stmt.getKind()) {
                case SET: {
                    handleSetStatement(metadataProvider, stmt, config);
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
                    metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));
                    metadataProvider.setResultAsyncMode(asyncResults);
                    executionResult.add(handleQuery(metadataProvider, (Query) stmt, hcc, hdc, asyncResults));
                    break;
                }

                case COMPACT: {
                    handleCompactStatement(metadataProvider, stmt, hcc);
                    break;
                }

                case WRITE: {
                    Pair<IAWriterFactory, FileSplit> result = handleWriteStatement(metadataProvider, stmt);
                    if (result.first != null) {
                        writerFactory = result.first;
                    }
                    outputFile = result.second;
                    break;
                }
            }
        }
        return executionResult;
    }

    private void handleSetStatement(AqlMetadataProvider metadataProvider, Statement stmt, Map<String, String> config)
            throws RemoteException, ACIDException {
        SetStatement ss = (SetStatement) stmt;
        String pname = ss.getPropName();
        String pvalue = ss.getPropValue();
        config.put(pname, pvalue);
    }

    private Pair<IAWriterFactory, FileSplit> handleWriteStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        WriteStatement ws = (WriteStatement) stmt;
        File f = new File(ws.getFileName());
        FileSplit outputFile = new FileSplit(ws.getNcName().getValue(), new FileReference(f));
        IAWriterFactory writerFactory = null;
        if (ws.getWriterClassName() != null) {
            writerFactory = (IAWriterFactory) Class.forName(ws.getWriterClassName()).newInstance();
        }
        return new Pair<IAWriterFactory, FileSplit>(writerFactory, outputFile);
    }

    private Dataverse handleUseDataverseStatement(AqlMetadataProvider metadataProvider, Statement stmt)
            throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            DataverseDecl dvd = (DataverseDecl) stmt;
            String dvName = dvd.getDataverseName().getValue();
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
            if (dv == null) {
                throw new MetadataException("Unknown dataverse " + dvName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return dv;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw new MetadataException(e);
        } finally {
            releaseReadLatch();
        }
    }

    private void handleCreateDataverseStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            CreateDataverseStatement stmtCreateDataverse = (CreateDataverseStatement) stmt;
            String dvName = stmtCreateDataverse.getDataverseName().getValue();
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
            if (dv != null) {
                if (stmtCreateDataverse.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A dataverse with this name " + dvName + " already exists.");
                }
            }
            MetadataManager.INSTANCE.addDataverse(metadataProvider.getMetadataTxnContext(), new Dataverse(dvName,
                    stmtCreateDataverse.getFormat(), IMetadataEntity.PENDING_NO_OP));
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void validateCompactionPolicy(String compactionPolicy, Map<String, String> compactionPolicyProperties,
            MetadataTransactionContext mdTxnCtx) throws AsterixException, Exception {
        CompactionPolicy compactionPolicyEntity = MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx,
                MetadataConstants.METADATA_DATAVERSE_NAME, compactionPolicy);
        if (compactionPolicyEntity == null) {
            throw new AsterixException("Unknown compaction policy :" + compactionPolicy);
        }
        String compactionPolicyFactoryClassName = compactionPolicyEntity.getClassName();
        ILSMMergePolicyFactory mergePolicyFactory = (ILSMMergePolicyFactory) Class.forName(
                compactionPolicyFactoryClassName).newInstance();
        for (Map.Entry<String, String> entry : compactionPolicyProperties.entrySet()) {
            if (!mergePolicyFactory.getPropertiesNames().contains(entry.getKey())) {
                throw new AsterixException("Invalid compaction policy property :" + entry.getKey());
            }
        }
    }

    private void handleCreateDatasetStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws AsterixException, Exception {

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        String datasetName = null;
        Dataset dataset = null;
        try {
            DatasetDecl dd = (DatasetDecl) stmt;
            dataverseName = getActiveDataverseName(dd.getDataverse());
            datasetName = dd.getName().getValue();

            DatasetType dsType = dd.getDatasetType();
            String itemTypeName = dd.getItemTypeName().getValue();

            IDatasetDetails datasetDetails = null;
            Dataset ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName);
            if (ds != null) {
                if (dd.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
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
                    ARecordType aRecordType = (ARecordType) itemType;
                    aRecordType.validatePartitioningExpressions(partitioningExprs);
                    String ngName = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getNodegroupName().getValue();
                    String compactionPolicy = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getCompactionPolicy();
                    Map<String, String> compactionPolicyProperties = ((InternalDetailsDecl) dd.getDatasetDetailsDecl())
                            .getCompactionPolicyProperties();
                    if (compactionPolicy == null) {
                        compactionPolicy = GlobalConfig.DEFAULT_COMPACTION_POLICY_NAME;
                        compactionPolicyProperties = GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES;
                    } else {
                        validateCompactionPolicy(compactionPolicy, compactionPolicyProperties, mdTxnCtx);
                    }
                    datasetDetails = new InternalDatasetDetails(InternalDatasetDetails.FileStructure.BTREE,
                            InternalDatasetDetails.PartitioningStrategy.HASH, partitioningExprs, partitioningExprs,
                            ngName, compactionPolicy, compactionPolicyProperties);
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
                    List<String> partitioningExprs = ((FeedDetailsDecl) dd.getDatasetDetailsDecl())
                            .getPartitioningExprs();
                    ARecordType aRecordType = (ARecordType) itemType;
                    aRecordType.validatePartitioningExpressions(partitioningExprs);
                    String ngName = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getNodegroupName().getValue();
                    String adapter = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getAdapterFactoryClassname();
                    Map<String, String> configuration = ((FeedDetailsDecl) dd.getDatasetDetailsDecl())
                            .getConfiguration();
                    FunctionSignature signature = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getFunctionSignature();
                    String compactionPolicy = ((FeedDetailsDecl) dd.getDatasetDetailsDecl()).getCompactionPolicy();
                    Map<String, String> compactionPolicyProperties = ((FeedDetailsDecl) dd.getDatasetDetailsDecl())
                            .getCompactionPolicyProperties();
                    if (compactionPolicy == null) {
                        compactionPolicy = GlobalConfig.DEFAULT_COMPACTION_POLICY_NAME;
                        compactionPolicyProperties = GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES;
                    } else {
                        validateCompactionPolicy(compactionPolicy, compactionPolicyProperties, mdTxnCtx);
                    }
                    datasetDetails = new FeedDatasetDetails(InternalDatasetDetails.FileStructure.BTREE,
                            InternalDatasetDetails.PartitioningStrategy.HASH, partitioningExprs, partitioningExprs,
                            ngName, adapter, configuration, signature,
                            FeedDatasetDetails.FeedState.INACTIVE.toString(), compactionPolicy,
                            compactionPolicyProperties);
                    break;
                }
            }

            //#. initialize DatasetIdFactory if it is not initialized.
            if (!DatasetIdFactory.isInitialized()) {
                DatasetIdFactory.initialize(MetadataManager.INSTANCE.getMostRecentDatasetId());
            }

            //#. add a new dataset with PendingAddOp
            dataset = new Dataset(dataverseName, datasetName, itemTypeName, datasetDetails, dd.getHints(), dsType,
                    DatasetIdFactory.generateDatasetId(), IMetadataEntity.PENDING_ADD_OP);
            MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), dataset);

            if (dd.getDatasetType() == DatasetType.INTERNAL || dd.getDatasetType() == DatasetType.FEED) {
                Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(),
                        dataverseName);
                JobSpecification jobSpec = DatasetOperations.createDatasetJobSpec(dataverse, datasetName,
                        metadataProvider);

                //#. make metadataTxn commit before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                //#. runJob
                runJob(hcc, jobSpec, true);

                //#. begin new metadataTxn
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
            }

            //#. add a new dataset with PendingNoOp after deleting the dataset with PendingAddOp
            MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName);
            dataset.setPendingOp(IMetadataEntity.PENDING_NO_OP);
            MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), dataset);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {

                //#. execute compensation operations
                //   remove the index in NC
                //   [Notice]
                //   As long as we updated(and committed) metadata, we should remove any effect of the job 
                //   because an exception occurs during runJob.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(dataverseName, datasetName);
                try {
                    JobSpecification jobSpec = DatasetOperations.createDropDatasetJobSpec(cds, metadataProvider);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;

                    runJob(hcc, jobSpec, true);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }

                //   remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending dataset(" + dataverseName
                            + "." + datasetName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleCreateIndexStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        String datasetName = null;
        String indexName = null;
        JobSpecification spec = null;
        Dataset ds = null;
        try {
            CreateIndexStatement stmtCreateIndex = (CreateIndexStatement) stmt;
            dataverseName = getActiveDataverseName(stmtCreateIndex.getDataverseName());
            datasetName = stmtCreateIndex.getDatasetName().getValue();

            ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName);
            if (ds == null) {
                throw new AlgebricksException("There is no dataset with this name " + datasetName + " in dataverse "
                        + dataverseName);
            }

            indexName = stmtCreateIndex.getIndexName().getValue();
            Index idx = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName, indexName);

            String itemTypeName = ds.getItemTypeName();
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(), dataverseName,
                    itemTypeName);
            IAType itemType = dt.getDatatype();
            ARecordType aRecordType = (ARecordType) itemType;
            aRecordType.validateKeyFields(stmtCreateIndex.getFieldExprs(), stmtCreateIndex.getIndexType());

            if (idx != null) {
                if (stmtCreateIndex.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("An index with this name " + indexName + " already exists.");
                }
            }

            //#. add a new index with PendingAddOp
            Index index = new Index(dataverseName, datasetName, indexName, stmtCreateIndex.getIndexType(),
                    stmtCreateIndex.getFieldExprs(), stmtCreateIndex.getGramLength(), false,
                    IMetadataEntity.PENDING_ADD_OP);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);

            //#. prepare to create the index artifact in NC.
            CompiledCreateIndexStatement cis = new CompiledCreateIndexStatement(index.getIndexName(), dataverseName,
                    index.getDatasetName(), index.getKeyFieldNames(), index.getGramLength(), index.getIndexType());
            spec = IndexOperations.buildSecondaryIndexCreationJobSpec(cis, metadataProvider);
            if (spec == null) {
                throw new AsterixException("Failed to create job spec for creating index '"
                        + stmtCreateIndex.getDatasetName() + "." + stmtCreateIndex.getIndexName() + "'");
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            //#. create the index artifact in NC.
            runJob(hcc, spec, true);

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            //#. load data into the index in NC.
            cis = new CompiledCreateIndexStatement(index.getIndexName(), dataverseName, index.getDatasetName(),
                    index.getKeyFieldNames(), index.getGramLength(), index.getIndexType());
            spec = IndexOperations.buildSecondaryIndexLoadingJobSpec(cis, metadataProvider);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            runJob(hcc, spec, true);

            //#. begin new metadataTxn
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            //#. add another new index with PendingNoOp after deleting the index with PendingAddOp
            MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName,
                    indexName);
            index.setPendingOp(IMetadataEntity.PENDING_NO_OP);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                //#. execute compensation operations
                //   remove the index in NC
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName, indexName);
                try {
                    JobSpecification jobSpec = IndexOperations
                            .buildDropSecondaryIndexJobSpec(cds, metadataProvider, ds);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;

                    runJob(hcc, jobSpec, true);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }

                //   remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName, indexName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending index(" + dataverseName
                            + "." + datasetName + "." + indexName + ") couldn't be removed from the metadata", e);
                }
            }
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleCreateTypeStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            TypeDecl stmtCreateType = (TypeDecl) stmt;
            String dataverseName = getActiveDataverseName(stmtCreateType.getDataverseName());
            String typeName = stmtCreateType.getIdent().getValue();
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
            if (dv == null) {
                throw new AlgebricksException("Unknown dataverse " + dataverseName);
            }
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
            if (dt != null) {
                if (!stmtCreateType.getIfNotExists()) {
                    throw new AlgebricksException("A datatype with this name " + typeName + " already exists.");
                }
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
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleDataverseDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        List<JobSpecification> jobsToExecute = new ArrayList<JobSpecification>();
        try {
            DataverseDropStatement stmtDelete = (DataverseDropStatement) stmt;
            dataverseName = stmtDelete.getDataverseName().getValue();

            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
            if (dv == null) {
                if (stmtDelete.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("There is no dataverse with this name " + dataverseName + ".");
                }
            }

            //#. prepare jobs which will drop corresponding datasets with indexes. 
            List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dataverseName);
            for (int j = 0; j < datasets.size(); j++) {
                String datasetName = datasets.get(j).getDatasetName();
                DatasetType dsType = datasets.get(j).getDatasetType();
                if (dsType == DatasetType.INTERNAL || dsType == DatasetType.FEED) {

                    List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName,
                            datasetName);
                    for (int k = 0; k < indexes.size(); k++) {
                        if (indexes.get(k).isSecondaryIndex()) {
                            CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                                    indexes.get(k).getIndexName());
                            jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider,
                                    datasets.get(j)));
                        }
                    }

                    CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(dataverseName, datasetName);
                    jobsToExecute.add(DatasetOperations.createDropDatasetJobSpec(cds, metadataProvider));
                }
            }
            jobsToExecute.add(DataverseOperations.createDropDataverseJobSpec(dv, metadataProvider));

            //#. mark PendingDropOp on the dataverse record by 
            //   first, deleting the dataverse record from the DATAVERSE_DATASET
            //   second, inserting the dataverse record with the PendingDropOp value into the DATAVERSE_DATASET
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
            MetadataManager.INSTANCE.addDataverse(mdTxnCtx, new Dataverse(dataverseName, dv.getDataFormat(),
                    IMetadataEntity.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            for (JobSpecification jobSpec : jobsToExecute) {
                runJob(hcc, jobSpec, true);
            }

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            //#. finally, delete the dataverse.
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
            if (activeDefaultDataverse != null && activeDefaultDataverse.getDataverseName() == dataverseName) {
                activeDefaultDataverse = null;
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                if (activeDefaultDataverse != null && activeDefaultDataverse.getDataverseName() == dataverseName) {
                    activeDefaultDataverse = null;
                }

                //#. execute compensation operations
                //   remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        runJob(hcc, jobSpec, true);
                    }
                } catch (Exception e2) {
                    //do no throw exception since still the metadata needs to be compensated. 
                    e.addSuppressed(e2);
                }

                //   remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                try {
                    MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending dataverse(" + dataverseName
                            + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleDatasetDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        String datasetName = null;
        List<JobSpecification> jobsToExecute = new ArrayList<JobSpecification>();
        try {
            DropStatement stmtDelete = (DropStatement) stmt;
            dataverseName = getActiveDataverseName(stmtDelete.getDataverseName());
            datasetName = stmtDelete.getDatasetName().getValue();

            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (ds == null) {
                if (stmtDelete.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("There is no dataset with this name " + datasetName
                            + " in dataverse " + dataverseName + ".");
                }
            }

            if (ds.getDatasetType() == DatasetType.INTERNAL || ds.getDatasetType() == DatasetType.FEED) {

                //#. prepare jobs to drop the datatset and the indexes in NC
                List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
                for (int j = 0; j < indexes.size(); j++) {
                    if (indexes.get(j).isSecondaryIndex()) {
                        CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName,
                                indexes.get(j).getIndexName());
                        jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider, ds));
                    }
                }
                CompiledDatasetDropStatement cds = new CompiledDatasetDropStatement(dataverseName, datasetName);
                jobsToExecute.add(DatasetOperations.createDropDatasetJobSpec(cds, metadataProvider));

                //#. mark the existing dataset as PendingDropOp
                MetadataManager.INSTANCE.dropDataset(mdTxnCtx, dataverseName, datasetName);
                MetadataManager.INSTANCE.addDataset(
                        mdTxnCtx,
                        new Dataset(dataverseName, datasetName, ds.getItemTypeName(), ds.getDatasetDetails(), ds
                                .getHints(), ds.getDatasetType(), ds.getDatasetId(), IMetadataEntity.PENDING_DROP_OP));

                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                //#. run the jobs
                for (JobSpecification jobSpec : jobsToExecute) {
                    runJob(hcc, jobSpec, true);
                }

                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
            }

            //#. finally, delete the dataset.
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx, dataverseName, datasetName);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                //#. execute compensation operations
                //   remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        runJob(hcc, jobSpec, true);
                    }
                } catch (Exception e2) {
                    //do no throw exception since still the metadata needs to be compensated. 
                    e.addSuppressed(e2);
                }

                //   remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending dataset(" + dataverseName
                            + "." + datasetName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleIndexDropStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        String dataverseName = null;
        String datasetName = null;
        String indexName = null;
        List<JobSpecification> jobsToExecute = new ArrayList<JobSpecification>();
        try {
            IndexDropStatement stmtIndexDrop = (IndexDropStatement) stmt;
            datasetName = stmtIndexDrop.getDatasetName().getValue();
            dataverseName = getActiveDataverseName(stmtIndexDrop.getDataverseName());

            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException("There is no dataset with this name " + datasetName + " in dataverse "
                        + dataverseName);
            }

            if (ds.getDatasetType() == DatasetType.INTERNAL || ds.getDatasetType() == DatasetType.FEED) {
                indexName = stmtIndexDrop.getIndexName().getValue();
                Index index = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                if (index == null) {
                    if (stmtIndexDrop.getIfExists()) {
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                        return;
                    } else {
                        throw new AlgebricksException("There is no index with this name " + indexName + ".");
                    }
                }
                //#. prepare a job to drop the index in NC.
                CompiledIndexDropStatement cds = new CompiledIndexDropStatement(dataverseName, datasetName, indexName);
                jobsToExecute.add(IndexOperations.buildDropSecondaryIndexJobSpec(cds, metadataProvider, ds));

                //#. mark PendingDropOp on the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                        new Index(dataverseName, datasetName, indexName, index.getIndexType(),
                                index.getKeyFieldNames(), index.isPrimaryIndex(), IMetadataEntity.PENDING_DROP_OP));

                //#. commit the existing transaction before calling runJob. 
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                for (JobSpecification jobSpec : jobsToExecute) {
                    runJob(hcc, jobSpec, true);
                }

                //#. begin a new transaction
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);

                //#. finally, delete the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
            } else {
                throw new AlgebricksException(datasetName
                        + " is an external dataset. Indexes are not maintained for external datasets.");
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                //#. execute compensation operations
                //   remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        runJob(hcc, jobSpec, true);
                    }
                } catch (Exception e2) {
                    //do no throw exception since still the metadata needs to be compensated.
                    e.addSuppressed(e2);
                }

                //   remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName, indexName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending index(" + dataverseName
                            + "." + datasetName + "." + indexName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;

        } finally {
            releaseWriteLatch();
        }
    }

    private void handleTypeDropStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            TypeDropStatement stmtTypeDrop = (TypeDropStatement) stmt;
            String dataverseName = getActiveDataverseName(stmtTypeDrop.getDataverseName());
            String typeName = stmtTypeDrop.getTypeName().getValue();
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
            if (dt == null) {
                if (!stmtTypeDrop.getIfExists())
                    throw new AlgebricksException("There is no datatype with this name " + typeName + ".");
            } else {
                MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, dataverseName, typeName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleNodegroupDropStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
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
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleCreateFunctionStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
            CreateFunctionStatement cfs = (CreateFunctionStatement) stmt;
            String dataverse = getActiveDataverseName(cfs.getSignature().getNamespace());
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                throw new AlgebricksException("There is no dataverse with this name " + dataverse + ".");
            }
            Function function = new Function(dataverse, cfs.getaAterixFunction().getName(), cfs.getaAterixFunction()
                    .getArity(), cfs.getParamList(), Function.RETURNTYPE_VOID, cfs.getFunctionBody(),
                    Function.LANGUAGE_AQL, FunctionKind.SCALAR.toString());
            MetadataManager.INSTANCE.addFunction(mdTxnCtx, function);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleFunctionDropStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
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
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private void handleLoadFromFileStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();
        List<JobSpecification> jobsToExecute = new ArrayList<JobSpecification>();
        try {
            LoadFromFileStatement loadStmt = (LoadFromFileStatement) stmt;
            String dataverseName = getActiveDataverseName(loadStmt.getDataverseName());
            CompiledLoadFromFileStatement cls = new CompiledLoadFromFileStatement(dataverseName, loadStmt
                    .getDatasetName().getValue(), loadStmt.getAdapter(), loadStmt.getProperties(),
                    loadStmt.dataIsAlreadySorted());

            IDataFormat format = getDataFormat(metadataProvider.getMetadataTxnContext(), dataverseName);
            Job job = DatasetOperations.createLoadDatasetJobSpec(metadataProvider, cls, format);
            jobsToExecute.add(job.getJobSpec());
            // Also load the dataset's secondary indexes.
            List<Index> datasetIndexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, loadStmt
                    .getDatasetName().getValue());
            for (Index index : datasetIndexes) {
                if (!index.isSecondaryIndex()) {
                    continue;
                }
                // Create CompiledCreateIndexStatement from metadata entity 'index'.
                CompiledCreateIndexStatement cis = new CompiledCreateIndexStatement(index.getIndexName(),
                        dataverseName, index.getDatasetName(), index.getKeyFieldNames(), index.getGramLength(),
                        index.getIndexType());
                jobsToExecute.add(IndexOperations.buildSecondaryIndexLoadingJobSpec(cis, metadataProvider));
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            for (JobSpecification jobspec : jobsToExecute) {
                runJob(hcc, jobspec, true);
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private void handleInsertStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            metadataProvider.setWriteTransaction(true);
            InsertStatement stmtInsert = (InsertStatement) stmt;
            String dataverseName = getActiveDataverseName(stmtInsert.getDataverseName());
            CompiledInsertStatement clfrqs = new CompiledInsertStatement(dataverseName, stmtInsert.getDatasetName()
                    .getValue(), stmtInsert.getQuery(), stmtInsert.getVarCounter());
            JobSpecification compiled = rewriteCompileQuery(metadataProvider, clfrqs.getQuery(), clfrqs);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (compiled != null) {
                runJob(hcc, compiled, true);
            }

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private void handleDeleteStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            metadataProvider.setWriteTransaction(true);
            DeleteStatement stmtDelete = (DeleteStatement) stmt;
            String dataverseName = getActiveDataverseName(stmtDelete.getDataverseName());
            CompiledDeleteStatement clfrqs = new CompiledDeleteStatement(stmtDelete.getVariableExpr(), dataverseName,
                    stmtDelete.getDatasetName().getValue(), stmtDelete.getCondition(), stmtDelete.getVarCounter(),
                    metadataProvider);
            JobSpecification compiled = rewriteCompileQuery(metadataProvider, clfrqs.getQuery(), clfrqs);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (compiled != null) {
                runJob(hcc, compiled, true);
            }

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private JobSpecification rewriteCompileQuery(AqlMetadataProvider metadataProvider, Query query,
            ICompiledDmlStatement stmt) throws AsterixException, RemoteException, AlgebricksException, JSONException,
            ACIDException {

        // Query Rewriting (happens under the same ongoing metadata transaction)
        Pair<Query, Integer> reWrittenQuery = APIFramework.reWriteQuery(declaredFunctions, metadataProvider, query,
                sessionConfig, out, pdf);

        // Query Compilation (happens under the same ongoing metadata
        // transaction)
        JobSpecification spec = APIFramework.compileQuery(declaredFunctions, metadataProvider, query,
                reWrittenQuery.second, stmt == null ? null : stmt.getDatasetName(), sessionConfig, out, pdf, stmt);

        return spec;

    }

    private void handleBeginFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            metadataProvider.setWriteTransaction(true);
            BeginFeedStatement bfs = (BeginFeedStatement) stmt;
            String dataverseName = getActiveDataverseName(bfs.getDataverseName());

            CompiledBeginFeedStatement cbfs = new CompiledBeginFeedStatement(dataverseName, bfs.getDatasetName()
                    .getValue(), bfs.getQuery(), bfs.getVarCounter());

            Dataset dataset;
            dataset = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName, bfs
                    .getDatasetName().getValue());
            if (dataset == null) {
                throw new AsterixException("Unknown dataset :" + bfs.getDatasetName().getValue());
            }
            IDatasetDetails datasetDetails = dataset.getDatasetDetails();
            if (datasetDetails.getDatasetType() != DatasetType.FEED) {
                throw new IllegalArgumentException("Dataset " + bfs.getDatasetName().getValue()
                        + " is not a feed dataset");
            }
            bfs.initialize(metadataProvider.getMetadataTxnContext(), dataset);
            cbfs.setQuery(bfs.getQuery());
            metadataProvider.getConfig().put(FunctionUtils.IMPORT_PRIVATE_FUNCTIONS, "" + Boolean.TRUE);

            JobSpecification compiled = rewriteCompileQuery(metadataProvider, bfs.getQuery(), cbfs);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (compiled != null) {
                runJob(hcc, compiled, true);
            }

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private void handleControlFeedStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            ControlFeedStatement cfs = (ControlFeedStatement) stmt;
            String dataverseName = getActiveDataverseName(cfs.getDataverseName());
            CompiledControlFeedStatement clcfs = new CompiledControlFeedStatement(cfs.getOperationType(),
                    dataverseName, cfs.getDatasetName().getValue(), cfs.getAlterAdapterConfParams());
            JobSpecification jobSpec = FeedOperations.buildControlFeedJobSpec(clcfs, metadataProvider);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            runJob(hcc, jobSpec, true);

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private void handleCompactStatement(AqlMetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        String dataverseName = null;
        String datasetName = null;
        List<JobSpecification> jobsToExecute = new ArrayList<JobSpecification>();
        try {
            CompactStatement compactStatement = (CompactStatement) stmt;
            dataverseName = getActiveDataverseName(compactStatement.getDataverseName());
            datasetName = compactStatement.getDatasetName().getValue();

            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException("There is no dataset with this name " + datasetName + " in dataverse "
                        + dataverseName + ".");
            } else if (ds.getDatasetType() != DatasetType.INTERNAL && ds.getDatasetType() != DatasetType.FEED) {
                throw new AlgebricksException("Cannot compact the extrenal dataset " + datasetName + ".");
            }

            // Prepare jobs to compact the datatset and its indexes
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
            for (int j = 0; j < indexes.size(); j++) {
                if (indexes.get(j).isSecondaryIndex()) {
                    CompiledIndexCompactStatement cics = new CompiledIndexCompactStatement(dataverseName, datasetName,
                            indexes.get(j).getIndexName(), indexes.get(j).getKeyFieldNames(), indexes.get(j)
                                    .getGramLength(), indexes.get(j).getIndexType());
                    jobsToExecute.add(IndexOperations.buildSecondaryIndexCompactJobSpec(cics, metadataProvider, ds));
                }
            }
            Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(),
                    dataverseName);
            jobsToExecute.add(DatasetOperations.compactDatasetJobSpec(dataverse, datasetName, metadataProvider));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            //#. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                runJob(hcc, jobSpec, true);
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private QueryResult handleQuery(AqlMetadataProvider metadataProvider, Query query, IHyracksClientConnection hcc,
            IHyracksDataset hdc, boolean asyncResults) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireReadLatch();

        try {
            JobSpecification compiled = rewriteCompileQuery(metadataProvider, query, null);

            QueryResult queryResult = new QueryResult(query, metadataProvider.getResultSetId());
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (sessionConfig.isExecuteQuery() && compiled != null) {
                GlobalConfig.ASTERIX_LOGGER.info(compiled.toJSON().toString(1));
                JobId jobId = runJob(hcc, compiled, false);

                JSONObject response = new JSONObject();
                if (asyncResults) {
                    JSONArray handle = new JSONArray();
                    handle.put(jobId.getId());
                    handle.put(metadataProvider.getResultSetId().getId());
                    response.put("handle", handle);
                    out.print(response);
                    out.flush();
                } else {
                    if (pdf == DisplayFormat.HTML) {
                        out.println("<h4>Results:</h4>");
                        out.println("<pre>");
                    }

                    ByteBuffer buffer = ByteBuffer.allocate(ResultReader.FRAME_SIZE);
                    ResultReader resultReader = new ResultReader(hcc, hdc);
                    resultReader.open(jobId, metadataProvider.getResultSetId());
                    buffer.clear();

                    while (resultReader.read(buffer) > 0) {
                        response.put("results",
                                ResultUtils.getJSONFromBuffer(buffer, resultReader.getFrameTupleAccessor()));
                        buffer.clear();
                        switch (pdf) {
                            case HTML:
                                ResultUtils.prettyPrintHTML(out, response);
                                break;
                            case TEXT:
                            case JSON:
                                out.print(response);
                                break;
                        }
                        out.flush();
                    }
                    if (pdf == DisplayFormat.HTML) {
                        out.println("</pre>");
                    }

                }
                hcc.waitForCompletion(jobId);
            }

            return queryResult;
        } catch (Exception e) {
            e.printStackTrace();
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            releaseReadLatch();
        }
    }

    private void handleCreateNodeGroupStatement(AqlMetadataProvider metadataProvider, Statement stmt) throws Exception {

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        acquireWriteLatch();

        try {
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
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            releaseWriteLatch();
        }
    }

    private JobId runJob(IHyracksClientConnection hcc, JobSpecification spec, boolean waitForCompletion)
            throws Exception {
        JobId[] jobIds = executeJobArray(hcc, new Job[] { new Job(spec) }, out, pdf, waitForCompletion);
        return jobIds[0];
    }

    public JobId[] executeJobArray(IHyracksClientConnection hcc, Job[] jobs, PrintWriter out, DisplayFormat pdf,
            boolean waitForCompletion) throws Exception {
        JobId[] startedJobIds = new JobId[jobs.length];
        for (int i = 0; i < jobs.length; i++) {
            JobSpecification spec = jobs[i].getJobSpec();
            spec.setMaxReattempts(0);
            JobId jobId = hcc.startJob(spec);
            startedJobIds[i] = jobId;
            if (waitForCompletion) {
                hcc.waitForCompletion(jobId);
            }
        }
        return startedJobIds;
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

    private String getActiveDataverseName(String dataverse) throws AlgebricksException {
        if (dataverse != null) {
            return dataverse;
        }
        if (activeDefaultDataverse != null) {
            return activeDefaultDataverse.getDataverseName();
        }
        throw new AlgebricksException("dataverse not specified");
    }

    private String getActiveDataverseName(Identifier dataverse) throws AlgebricksException {
        return getActiveDataverseName(dataverse != null ? dataverse.getValue() : null);
    }

    private void acquireWriteLatch() {
        MetadataManager.INSTANCE.acquireWriteLatch();
    }

    private void releaseWriteLatch() {
        MetadataManager.INSTANCE.releaseWriteLatch();
    }

    private void acquireReadLatch() {
        MetadataManager.INSTANCE.acquireReadLatch();
    }

    private void releaseReadLatch() {
        MetadataManager.INSTANCE.releaseReadLatch();
    }

    private void abort(Exception rootE, Exception parentE, MetadataTransactionContext mdTxnCtx) {
        try {
            if (IS_DEBUG_MODE) {
                rootE.printStackTrace();
            }
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
        } catch (Exception e2) {
            parentE.addSuppressed(e2);
            throw new IllegalStateException(rootE);
        }
    }
}
