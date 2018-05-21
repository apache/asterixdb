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
package org.apache.asterix.app.translator;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.active.NoRetryPolicyFactory;
import org.apache.asterix.algebra.extension.ExtensionStatement;
import org.apache.asterix.api.common.APIFramework;
import org.apache.asterix.api.http.server.AbstractQueryApiServlet;
import org.apache.asterix.api.http.server.ApiServlet;
import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.active.ActiveEntityEventsListener;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.active.FeedEventsListener;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.DatasetConfig.TransactionState;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.common.utils.JobUtils.ProgressState;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.external.operators.FeedIntakeOperatorNodePushable;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.base.IStatementRewriter;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.IndexedTypeExpression;
import org.apache.asterix.lang.common.statement.CompactStatement;
import org.apache.asterix.lang.common.statement.ConnectFeedStatement;
import org.apache.asterix.lang.common.statement.CreateDataverseStatement;
import org.apache.asterix.lang.common.statement.CreateFeedPolicyStatement;
import org.apache.asterix.lang.common.statement.CreateFeedStatement;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.DisconnectFeedStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.ExternalDetailsDecl;
import org.apache.asterix.lang.common.statement.FeedDropStatement;
import org.apache.asterix.lang.common.statement.FeedPolicyDropStatement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.InternalDetailsDecl;
import org.apache.asterix.lang.common.statement.LoadStatement;
import org.apache.asterix.lang.common.statement.NodeGroupDropStatement;
import org.apache.asterix.lang.common.statement.NodegroupDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.RefreshExternalDatasetStatement;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.common.statement.StartFeedStatement;
import org.apache.asterix.lang.common.statement.StopFeedStatement;
import org.apache.asterix.lang.common.statement.TypeDecl;
import org.apache.asterix.lang.common.statement.TypeDropStatement;
import org.apache.asterix.lang.common.statement.WriteStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppRewriterFactory;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.dataset.hints.DatasetHints;
import org.apache.asterix.metadata.dataset.hints.DatasetHints.DatasetNodegroupCardinalityHint;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.lock.ExternalDatasetsRegistry;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.ExternalIndexingOperations;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.metadata.utils.MetadataLockUtil;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.asterix.translator.AbstractLangTranslator;
import org.apache.asterix.translator.CompiledStatements.CompiledDeleteStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledInsertStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledLoadFromFileStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledUpsertStatement;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.ExecutionPlansHtmlPrintUtil;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutorContext;
import org.apache.asterix.translator.NoOpStatementExecutorContext;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.asterix.translator.TypeTranslator;
import org.apache.asterix.translator.util.ValidateUtil;
import org.apache.asterix.utils.DataverseUtil;
import org.apache.asterix.utils.FeedOperations;
import org.apache.asterix.utils.FlushDatasetUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.runtime.serializer.ResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import org.apache.hyracks.api.client.IClusterInfoCollector;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.UnmanagedFileSplit;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;
import org.apache.hyracks.control.common.job.profiling.om.JobletProfile;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
 * Provides functionality for executing a batch of Query statements (queries included)
 * sequentially.
 */
public class QueryTranslator extends AbstractLangTranslator implements IStatementExecutor {

    private static final Logger LOGGER = LogManager.getLogger();

    public static final boolean IS_DEBUG_MODE = false;// true
    protected final List<Statement> statements;
    protected final ICcApplicationContext appCtx;
    protected final SessionOutput sessionOutput;
    protected final SessionConfig sessionConfig;
    protected Dataverse activeDataverse;
    protected final List<FunctionDecl> declaredFunctions;
    protected final APIFramework apiFramework;
    protected final IRewriterFactory rewriterFactory;
    protected final ExecutorService executorService;
    protected final EnumSet<JobFlag> jobFlags = EnumSet.noneOf(JobFlag.class);
    protected final IMetadataLockManager lockManager;

    public QueryTranslator(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compliationProvider, ExecutorService executorService) {
        this.appCtx = appCtx;
        this.lockManager = appCtx.getMetadataLockManager();
        this.statements = statements;
        this.sessionOutput = output;
        this.sessionConfig = output.config();
        declaredFunctions = getDeclaredFunctions(statements);
        apiFramework = new APIFramework(compliationProvider);
        rewriterFactory = compliationProvider.getRewriterFactory();
        activeDataverse = MetadataBuiltinEntities.DEFAULT_DATAVERSE;
        this.executorService = executorService;
        if (appCtx.getServiceContext().getAppConfig().getBoolean(CCConfig.Option.ENFORCE_FRAME_WRITER_PROTOCOL)) {
            this.jobFlags.add(JobFlag.ENFORCE_CONTRACT);
        }
    }

    public SessionOutput getSessionOutput() {
        return sessionOutput;
    }

    protected List<FunctionDecl> getDeclaredFunctions(List<Statement> statements) {
        List<FunctionDecl> functionDecls = new ArrayList<>();
        for (Statement st : statements) {
            if (st.getKind() == Statement.Kind.FUNCTION_DECL) {
                functionDecls.add((FunctionDecl) st);
            }
        }
        return functionDecls;
    }

    @Override
    public void compileAndExecute(IHyracksClientConnection hcc, IStatementExecutorContext ctx,
            IRequestParameters requestParameters) throws Exception {
        int resultSetIdCounter = 0;
        FileSplit outputFile = null;
        IAWriterFactory writerFactory = PrinterBasedWriterFactory.INSTANCE;
        IResultSerializerFactoryProvider resultSerializerFactoryProvider = ResultSerializerFactoryProvider.INSTANCE;
        /*
         * Since the system runs a large number of threads, when HTTP requests don't
         * return, it becomes difficult to find the thread running the request to
         * determine where it has stopped. Setting the thread name helps make that
         * easier
         */
        String threadName = Thread.currentThread().getName();
        Thread.currentThread().setName(QueryTranslator.class.getSimpleName());
        Map<String, String> config = new HashMap<>();
        final IHyracksDataset hdc = requestParameters.getHyracksDataset();
        final ResultDelivery resultDelivery = requestParameters.getResultProperties().getDelivery();
        final long maxResultReads = requestParameters.getResultProperties().getMaxReads();
        final Stats stats = requestParameters.getStats();
        final ResultMetadata outMetadata = requestParameters.getOutMetadata();
        final String clientContextId = requestParameters.getClientContextId();
        try {
            for (Statement stmt : statements) {
                if (sessionConfig.is(SessionConfig.FORMAT_HTML)) {
                    sessionOutput.out().println(ApiServlet.HTML_STATEMENT_SEPARATOR);
                }
                validateOperation(appCtx, activeDataverse, stmt);
                rewriteStatement(stmt); // Rewrite the statement's AST.
                MetadataProvider metadataProvider = new MetadataProvider(appCtx, activeDataverse);
                metadataProvider.getConfig().putAll(config);
                metadataProvider.setWriterFactory(writerFactory);
                metadataProvider.setResultSerializerFactoryProvider(resultSerializerFactoryProvider);
                metadataProvider.setOutputFile(outputFile);
                switch (stmt.getKind()) {
                    case SET:
                        handleSetStatement(stmt, config);
                        break;
                    case DATAVERSE_DECL:
                        activeDataverse = handleUseDataverseStatement(metadataProvider, stmt);
                        break;
                    case CREATE_DATAVERSE:
                        handleCreateDataverseStatement(metadataProvider, stmt);
                        break;
                    case DATASET_DECL:
                        handleCreateDatasetStatement(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case CREATE_INDEX:
                        handleCreateIndexStatement(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case TYPE_DECL:
                        handleCreateTypeStatement(metadataProvider, stmt);
                        break;
                    case NODEGROUP_DECL:
                        handleCreateNodeGroupStatement(metadataProvider, stmt);
                        break;
                    case DATAVERSE_DROP:
                        handleDataverseDropStatement(metadataProvider, stmt, hcc);
                        break;
                    case DATASET_DROP:
                        handleDatasetDropStatement(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case INDEX_DROP:
                        handleIndexDropStatement(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case TYPE_DROP:
                        handleTypeDropStatement(metadataProvider, stmt);
                        break;
                    case NODEGROUP_DROP:
                        handleNodegroupDropStatement(metadataProvider, stmt);
                        break;
                    case CREATE_FUNCTION:
                        handleCreateFunctionStatement(metadataProvider, stmt);
                        break;
                    case FUNCTION_DROP:
                        handleFunctionDropStatement(metadataProvider, stmt);
                        break;
                    case LOAD:
                        handleLoadStatement(metadataProvider, stmt, hcc);
                        break;
                    case INSERT:
                    case UPSERT:
                        if (((InsertStatement) stmt).getReturnExpression() != null) {
                            metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));
                            metadataProvider.setResultAsyncMode(resultDelivery == ResultDelivery.ASYNC
                                    || resultDelivery == ResultDelivery.DEFERRED);
                            metadataProvider.setMaxResultReads(maxResultReads);
                        }
                        handleInsertUpsertStatement(metadataProvider, stmt, hcc, hdc, resultDelivery, outMetadata,
                                stats, false, clientContextId);
                        break;
                    case DELETE:
                        handleDeleteStatement(metadataProvider, stmt, hcc, false);
                        break;
                    case CREATE_FEED:
                        handleCreateFeedStatement(metadataProvider, stmt);
                        break;
                    case DROP_FEED:
                        handleDropFeedStatement(metadataProvider, stmt, hcc);
                        break;
                    case DROP_FEED_POLICY:
                        handleDropFeedPolicyStatement(metadataProvider, stmt);
                        break;
                    case CONNECT_FEED:
                        handleConnectFeedStatement(metadataProvider, stmt);
                        break;
                    case DISCONNECT_FEED:
                        handleDisconnectFeedStatement(metadataProvider, stmt);
                        break;
                    case START_FEED:
                        handleStartFeedStatement(metadataProvider, stmt, hcc);
                        break;
                    case STOP_FEED:
                        handleStopFeedStatement(metadataProvider, stmt);
                        break;
                    case CREATE_FEED_POLICY:
                        handleCreateFeedPolicyStatement(metadataProvider, stmt);
                        break;
                    case QUERY:
                        metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));
                        metadataProvider.setResultAsyncMode(
                                resultDelivery == ResultDelivery.ASYNC || resultDelivery == ResultDelivery.DEFERRED);
                        metadataProvider.setMaxResultReads(maxResultReads);
                        handleQuery(metadataProvider, (Query) stmt, hcc, hdc, resultDelivery, outMetadata, stats,
                                clientContextId, ctx);
                        break;
                    case COMPACT:
                        handleCompactStatement(metadataProvider, stmt, hcc);
                        break;
                    case EXTERNAL_DATASET_REFRESH:
                        handleExternalDatasetRefreshStatement(metadataProvider, stmt, hcc);
                        break;
                    case WRITE:
                        Pair<IAWriterFactory, FileSplit> result = handleWriteStatement(stmt);
                        writerFactory = (result.first != null) ? result.first : writerFactory;
                        outputFile = result.second;
                        break;
                    case FUNCTION_DECL:
                        // No op
                        break;
                    case EXTENSION:
                        ((ExtensionStatement) stmt).handle(hcc, this, requestParameters, metadataProvider,
                                resultSetIdCounter);
                        break;
                    default:
                        throw new CompilationException("Unknown function");
                }
            }
        } finally {
            Thread.currentThread().setName(threadName);
        }
    }

    protected void handleSetStatement(Statement stmt, Map<String, String> config) {
        SetStatement ss = (SetStatement) stmt;
        String pname = ss.getPropName();
        String pvalue = ss.getPropValue();
        config.put(pname, pvalue);
    }

    protected Pair<IAWriterFactory, FileSplit> handleWriteStatement(Statement stmt)
            throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        WriteStatement ws = (WriteStatement) stmt;
        File f = new File(ws.getFileName());
        FileSplit outputFile = new UnmanagedFileSplit(ws.getNcName().getValue(), f.getPath());
        IAWriterFactory writerFactory = null;
        if (ws.getWriterClassName() != null) {
            writerFactory = (IAWriterFactory) Class.forName(ws.getWriterClassName()).newInstance();
        }
        return new Pair<>(writerFactory, outputFile);
    }

    protected Dataverse handleUseDataverseStatement(MetadataProvider metadataProvider, Statement stmt)
            throws Exception {
        DataverseDecl dvd = (DataverseDecl) stmt;
        String dvName = dvd.getDataverseName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockManager.acquireDataverseReadLock(metadataProvider.getLocks(), dvName);
        try {
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
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleCreateDataverseStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {

        CreateDataverseStatement stmtCreateDataverse = (CreateDataverseStatement) stmt;
        String dvName = stmtCreateDataverse.getDataverseName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        lockManager.acquireDataverseReadLock(metadataProvider.getLocks(), dvName);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
            if (dv != null) {
                if (stmtCreateDataverse.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A dataverse with this name " + dvName + " already exists.");
                }
            }
            MetadataManager.INSTANCE.addDataverse(metadataProvider.getMetadataTxnContext(),
                    new Dataverse(dvName, stmtCreateDataverse.getFormat(), MetadataUtil.PENDING_NO_OP));
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected static void validateCompactionPolicy(String compactionPolicy,
            Map<String, String> compactionPolicyProperties, MetadataTransactionContext mdTxnCtx,
            boolean isExternalDataset) throws CompilationException, Exception {
        CompactionPolicy compactionPolicyEntity = MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx,
                MetadataConstants.METADATA_DATAVERSE_NAME, compactionPolicy);
        if (compactionPolicyEntity == null) {
            throw new CompilationException("Unknown compaction policy: " + compactionPolicy);
        }
        String compactionPolicyFactoryClassName = compactionPolicyEntity.getClassName();
        ILSMMergePolicyFactory mergePolicyFactory =
                (ILSMMergePolicyFactory) Class.forName(compactionPolicyFactoryClassName).newInstance();
        if (isExternalDataset && mergePolicyFactory.getName().compareTo("correlated-prefix") == 0) {
            throw new CompilationException("The correlated-prefix merge policy cannot be used with external dataset.");
        }
        if (compactionPolicyProperties == null) {
            if (mergePolicyFactory.getName().compareTo("no-merge") != 0) {
                throw new CompilationException("Compaction policy properties are missing.");
            }
        } else {
            for (Map.Entry<String, String> entry : compactionPolicyProperties.entrySet()) {
                if (!mergePolicyFactory.getPropertiesNames().contains(entry.getKey())) {
                    throw new CompilationException("Invalid compaction policy property: " + entry.getKey());
                }
            }
            for (String p : mergePolicyFactory.getPropertiesNames()) {
                if (!compactionPolicyProperties.containsKey(p)) {
                    throw new CompilationException("Missing compaction policy property: " + p);
                }
            }
        }
    }

    public void handleCreateDatasetStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws CompilationException, Exception {
        MutableObject<ProgressState> progress = new MutableObject<>(ProgressState.NO_PROGRESS);
        DatasetDecl dd = (DatasetDecl) stmt;
        String dataverseName = getActiveDataverse(dd.getDataverse());
        String datasetName = dd.getName().getValue();
        DatasetType dsType = dd.getDatasetType();
        String itemTypeDataverseName = getActiveDataverse(dd.getItemTypeDataverse());
        String itemTypeName = dd.getItemTypeName().getValue();
        String metaItemTypeDataverseName = getActiveDataverse(dd.getMetaItemTypeDataverse());
        String metaItemTypeName = dd.getMetaItemTypeName().getValue();
        Identifier ngNameId = dd.getNodegroupName();
        String nodegroupName = ngNameId == null ? null : ngNameId.getValue();
        String compactionPolicy = dd.getCompactionPolicy();
        Map<String, String> compactionPolicyProperties = dd.getCompactionPolicyProperties();
        boolean defaultCompactionPolicy = compactionPolicy == null;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.createDatasetBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                itemTypeDataverseName, itemTypeDataverseName + "." + itemTypeName, metaItemTypeDataverseName,
                metaItemTypeDataverseName + "." + metaItemTypeName, nodegroupName, compactionPolicy,
                dataverseName + "." + datasetName, defaultCompactionPolicy);
        Dataset dataset = null;
        try {
            IDatasetDetails datasetDetails = null;
            Dataset ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds != null) {
                if (dd.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A dataset with this name " + datasetName + " already exists.");
                }
            }
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    itemTypeDataverseName, itemTypeName);
            if (dt == null) {
                throw new AlgebricksException(": type " + itemTypeName + " could not be found.");
            }
            String ngName = ngNameId != null ? ngNameId.getValue()
                    : configureNodegroupForDataset(appCtx, dd.getHints(), dataverseName, datasetName, metadataProvider);

            if (compactionPolicy == null) {
                compactionPolicy = GlobalConfig.DEFAULT_COMPACTION_POLICY_NAME;
                compactionPolicyProperties = GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES;
            } else {
                validateCompactionPolicy(compactionPolicy, compactionPolicyProperties, mdTxnCtx, false);
            }
            switch (dd.getDatasetType()) {
                case INTERNAL:
                    IAType itemType = dt.getDatatype();
                    if (itemType.getTypeTag() != ATypeTag.OBJECT) {
                        throw new AlgebricksException("Dataset type has to be a record type.");
                    }

                    IAType metaItemType = null;
                    if (metaItemTypeDataverseName != null && metaItemTypeName != null) {
                        metaItemType = metadataProvider.findType(metaItemTypeDataverseName, metaItemTypeName);
                    }
                    if (metaItemType != null && metaItemType.getTypeTag() != ATypeTag.OBJECT) {
                        throw new AlgebricksException("Dataset meta type has to be a record type.");
                    }
                    ARecordType metaRecType = (ARecordType) metaItemType;

                    List<List<String>> partitioningExprs =
                            ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getPartitioningExprs();
                    List<Integer> keySourceIndicators =
                            ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getKeySourceIndicators();
                    boolean autogenerated = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).isAutogenerated();
                    ARecordType aRecordType = (ARecordType) itemType;
                    List<IAType> partitioningTypes = ValidateUtil.validatePartitioningExpressions(aRecordType,
                            metaRecType, partitioningExprs, keySourceIndicators, autogenerated);

                    List<String> filterField = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getFilterField();
                    if (filterField != null) {
                        ValidateUtil.validateFilterField(aRecordType, filterField);
                    }
                    if (compactionPolicy == null && filterField != null) {
                        // If the dataset has a filter and the user didn't specify a merge
                        // policy, then we will pick the
                        // correlated-prefix as the default merge policy.
                        compactionPolicy = GlobalConfig.DEFAULT_FILTERED_DATASET_COMPACTION_POLICY_NAME;
                        compactionPolicyProperties = GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES;
                    }
                    datasetDetails = new InternalDatasetDetails(InternalDatasetDetails.FileStructure.BTREE,
                            InternalDatasetDetails.PartitioningStrategy.HASH, partitioningExprs, partitioningExprs,
                            keySourceIndicators, partitioningTypes, autogenerated, filterField);
                    break;
                case EXTERNAL:
                    String adapter = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getAdapter();
                    Map<String, String> properties = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getProperties();

                    datasetDetails =
                            new ExternalDatasetDetails(adapter, properties, new Date(), TransactionState.COMMIT);
                    break;
                default:
                    throw new CompilationException("Unknown datatype " + dd.getDatasetType());
            }

            // #. initialize DatasetIdFactory if it is not initialized.
            if (!DatasetIdFactory.isInitialized()) {
                DatasetIdFactory.initialize(MetadataManager.INSTANCE.getMostRecentDatasetId());
            }

            // #. add a new dataset with PendingAddOp
            dataset = new Dataset(dataverseName, datasetName, itemTypeDataverseName, itemTypeName,
                    metaItemTypeDataverseName, metaItemTypeName, ngName, compactionPolicy, compactionPolicyProperties,
                    datasetDetails, dd.getHints(), dsType, DatasetIdFactory.generateDatasetId(),
                    MetadataUtil.PENDING_ADD_OP);
            MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), dataset);
            if (dd.getDatasetType() == DatasetType.INTERNAL) {
                JobSpecification jobSpec = DatasetUtil.createDatasetJobSpec(dataset, metadataProvider);

                // #. make metadataTxn commit before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress.setValue(ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA);

                // #. runJob
                runJob(hcc, jobSpec);

                // #. begin new metadataTxn
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
            }

            // #. add a new dataset with PendingNoOp after deleting the dataset with
            // PendingAddOp
            MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName);
            dataset.setPendingOp(MetadataUtil.PENDING_NO_OP);
            MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), dataset);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress.getValue() == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {

                // #. execute compensation operations
                // remove the index in NC
                // [Notice]
                // As long as we updated(and committed) metadata, we should remove any effect of
                // the job
                // because an exception occurs during runJob.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    JobSpecification jobSpec = DatasetUtil.dropDatasetJobSpec(dataset, metadataProvider);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;
                    runJob(hcc, jobSpec);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }

                // remove the record from the metadata.
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
            metadataProvider.getLocks().unlock();
        }
    }

    protected static void validateIfResourceIsActiveInFeed(ICcApplicationContext appCtx, Dataset dataset)
            throws CompilationException {
        StringBuilder builder = null;
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        IActiveEntityEventsListener[] listeners = activeEventHandler.getEventListeners();
        for (IActiveEntityEventsListener listener : listeners) {
            if (listener.isEntityUsingDataset(dataset) && listener.isActive()) {
                if (builder == null) {
                    builder = new StringBuilder();
                }
                builder.append(listener.getEntityId() + "\n");
            }
        }
        if (builder != null) {
            throw new CompilationException("Dataset " + dataset.getDataverseName() + "." + dataset.getDatasetName()
                    + " is currently being " + "fed into by the following active entities.\n" + builder.toString());
        }
    }

    protected static String configureNodegroupForDataset(ICcApplicationContext appCtx, Map<String, String> hints,
            String dataverseName, String datasetName, MetadataProvider metadataProvider) throws Exception {
        IClusterStateManager csm = appCtx.getClusterStateManager();
        Set<String> allNodes = csm.getParticipantNodes(true);
        Set<String> selectedNodes = new LinkedHashSet<>();
        String hintValue = hints.get(DatasetNodegroupCardinalityHint.NAME);
        if (hintValue == null) {
            selectedNodes.addAll(allNodes);
        } else {
            int nodegroupCardinality;
            final Pair<Boolean, String> validation = DatasetHints.validate(appCtx, DatasetNodegroupCardinalityHint.NAME,
                    hints.get(DatasetNodegroupCardinalityHint.NAME));
            boolean valid = validation.first;
            if (!valid) {
                throw new CompilationException(
                        "Incorrect use of hint '" + DatasetNodegroupCardinalityHint.NAME + "': " + validation.second);
            } else {
                nodegroupCardinality = Integer.parseInt(hints.get(DatasetNodegroupCardinalityHint.NAME));
            }
            List<String> allNodeList = new ArrayList<>(allNodes);
            Collections.shuffle(allNodeList);
            selectedNodes.addAll(allNodeList.subList(0, nodegroupCardinality));
        }
        // Creates the associated node group for the dataset.
        return DatasetUtil.createNodeGroupForNewDataset(dataverseName, datasetName, selectedNodes, metadataProvider);
    }

    public void handleCreateIndexStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        CreateIndexStatement stmtCreateIndex = (CreateIndexStatement) stmt;
        String dataverseName = getActiveDataverse(stmtCreateIndex.getDataverseName());
        String datasetName = stmtCreateIndex.getDatasetName().getValue();
        String indexName = stmtCreateIndex.getIndexName().getValue();
        List<Integer> keySourceIndicators = stmtCreateIndex.getFieldSourceIndicators();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        String datasetFullyQualifiedName = dataverseName + "." + datasetName;
        Dataset ds = null;
        Index index = null;
        MetadataLockUtil.createIndexBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                datasetFullyQualifiedName);
        try {
            ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName);
            }

            index = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName, indexName);
            if (index != null) {
                if (stmtCreateIndex.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("An index with this name " + indexName + " already exists.");
                }
            }
            // can't create secondary primary index on an external dataset
            if (ds.getDatasetType() == DatasetType.EXTERNAL && stmtCreateIndex.getFieldExprs().isEmpty()) {
                throw new AsterixException(ErrorCode.CANNOT_CREATE_SEC_PRIMARY_IDX_ON_EXT_DATASET);
            }
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    ds.getItemTypeDataverseName(), ds.getItemTypeName());
            ARecordType aRecordType = (ARecordType) dt.getDatatype();
            ARecordType metaRecordType = null;
            if (ds.hasMetaPart()) {
                Datatype metaDt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                        ds.getMetaItemTypeDataverseName(), ds.getMetaItemTypeName());
                metaRecordType = (ARecordType) metaDt.getDatatype();
            }

            List<List<String>> indexFields = new ArrayList<>();
            List<IAType> indexFieldTypes = new ArrayList<>();
            int keyIndex = 0;
            boolean overridesFieldTypes = false;

            // this set is used to detect duplicates in the specified keys in the create
            // index statement
            // e.g. CREATE INDEX someIdx on dataset(id,id).
            // checking only the names is not enough. Need also to check the source
            // indicators for cases like:
            // CREATE INDEX someIdx on dataset(meta().id, id)
            Set<Pair<List<String>, Integer>> indexKeysSet = new HashSet<>();

            for (Pair<List<String>, IndexedTypeExpression> fieldExpr : stmtCreateIndex.getFieldExprs()) {
                IAType fieldType = null;
                ARecordType subType =
                        KeyFieldTypeUtil.chooseSource(keySourceIndicators, keyIndex, aRecordType, metaRecordType);
                boolean isOpen = subType.isOpen();
                int i = 0;
                if (fieldExpr.first.size() > 1 && !isOpen) {
                    while (i < fieldExpr.first.size() - 1 && !isOpen) {
                        subType = (ARecordType) subType.getFieldType(fieldExpr.first.get(i));
                        i++;
                        isOpen = subType.isOpen();
                    }
                }
                if (fieldExpr.second == null) {
                    fieldType = subType.getSubFieldType(fieldExpr.first.subList(i, fieldExpr.first.size()));
                } else {
                    if (!stmtCreateIndex.isEnforced() && stmtCreateIndex.getIndexType() != IndexType.BTREE) {
                        throw new AsterixException(ErrorCode.INDEX_ILLEGAL_NON_ENFORCED_TYPED,
                                stmtCreateIndex.getIndexType());
                    }
                    if (stmtCreateIndex.isEnforced() && !fieldExpr.second.isUnknownable()) {
                        throw new AsterixException(ErrorCode.INDEX_ILLEGAL_ENFORCED_NON_OPTIONAL,
                                String.valueOf(fieldExpr.first));
                    }
                    // don't allow creating an enforced index on a closed-type field, fields that
                    // are part of schema.
                    // get the field type, if it's not null, then the field is closed-type
                    if (stmtCreateIndex.isEnforced()
                            && subType.getSubFieldType(fieldExpr.first.subList(i, fieldExpr.first.size())) != null) {
                        throw new AsterixException(ErrorCode.INDEX_ILLEGAL_ENFORCED_ON_CLOSED_FIELD,
                                String.valueOf(fieldExpr.first));
                    }
                    if (!isOpen) {
                        throw new AlgebricksException("Typed index on \"" + fieldExpr.first
                                + "\" field could be created only for open datatype");
                    }
                    if (stmtCreateIndex.hasMetaField()) {
                        throw new AlgebricksException("Typed open index can only be created on the record part");
                    }
                    Map<TypeSignature, IAType> typeMap =
                            TypeTranslator.computeTypes(mdTxnCtx, fieldExpr.second.getType(), indexName, dataverseName);
                    TypeSignature typeSignature = new TypeSignature(dataverseName, indexName);
                    fieldType = typeMap.get(typeSignature);
                    overridesFieldTypes = true;
                }
                if (fieldType == null) {
                    throw new AlgebricksException(
                            "Unknown type " + (fieldExpr.second == null ? fieldExpr.first : fieldExpr.second));
                }

                // try to add the key & its source to the set of keys, if key couldn't be added,
                // there is a duplicate
                if (!indexKeysSet
                        .add(new Pair<>(fieldExpr.first, stmtCreateIndex.getFieldSourceIndicators().get(keyIndex)))) {
                    throw new AsterixException(ErrorCode.INDEX_ILLEGAL_REPETITIVE_FIELD,
                            String.valueOf(fieldExpr.first));
                }

                indexFields.add(fieldExpr.first);
                indexFieldTypes.add(fieldType);
                ++keyIndex;
            }

            validateIndexKeyFields(stmtCreateIndex, keySourceIndicators, aRecordType, metaRecordType, indexFields,
                    indexFieldTypes);
            // Checks whether a user is trying to create an inverted secondary index on a
            // dataset
            // with a variable-length primary key.
            // Currently, we do not support this. Therefore, as a temporary solution, we
            // print an
            // error message and stop.
            if (stmtCreateIndex.getIndexType() == IndexType.SINGLE_PARTITION_WORD_INVIX
                    || stmtCreateIndex.getIndexType() == IndexType.SINGLE_PARTITION_NGRAM_INVIX
                    || stmtCreateIndex.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                    || stmtCreateIndex.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
                List<List<String>> partitioningKeys = ds.getPrimaryKeys();
                for (List<String> partitioningKey : partitioningKeys) {
                    IAType keyType = aRecordType.getSubFieldType(partitioningKey);
                    ITypeTraits typeTrait = TypeTraitProvider.INSTANCE.getTypeTrait(keyType);

                    // If it is not a fixed length
                    if (typeTrait.getFixedLength() < 0) {
                        throw new AlgebricksException("The keyword or ngram index -" + indexName
                                + " cannot be created on the dataset -" + datasetName
                                + " due to its variable-length primary key field - " + partitioningKey);
                    }

                }
            }

            Index newIndex = new Index(dataverseName, datasetName, indexName, stmtCreateIndex.getIndexType(),
                    indexFields, keySourceIndicators, indexFieldTypes, stmtCreateIndex.getGramLength(),
                    overridesFieldTypes, stmtCreateIndex.isEnforced(), false, MetadataUtil.PENDING_ADD_OP);
            doCreateIndex(hcc, metadataProvider, ds, newIndex, jobFlags);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    public static void doCreateIndex(IHyracksClientConnection hcc, MetadataProvider metadataProvider, Dataset ds,
            Index index, EnumSet<JobFlag> jobFlags) throws Exception {
        ProgressState progress = ProgressState.NO_PROGRESS;
        boolean bActiveTxn = true;
        Index filesIndex = null;
        boolean firstExternalDatasetIndex = false;
        boolean datasetLocked = false;
        List<ExternalFile> externalFilesSnapshot;
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        JobSpecification spec;
        boolean filesIndexReplicated = false;
        try {
            index.setPendingOp(MetadataUtil.PENDING_ADD_OP);
            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                validateIfResourceIsActiveInFeed(metadataProvider.getApplicationContext(), ds);
            } else {
                // External dataset
                // Check if the dataset is indexible
                if (!ExternalIndexingOperations.isIndexible((ExternalDatasetDetails) ds.getDatasetDetails())) {
                    throw new AlgebricksException(
                            "dataset using " + ((ExternalDatasetDetails) ds.getDatasetDetails()).getAdapter()
                                    + " Adapter can't be indexed");
                }
                // Check if the name of the index is valid
                if (!ExternalIndexingOperations.isValidIndexName(index.getDatasetName(), index.getIndexName())) {
                    throw new AlgebricksException("external dataset index name is invalid");
                }

                // Check if the files index exist
                filesIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                        index.getDataverseName(), index.getDatasetName(),
                        IndexingConstants.getFilesIndexName(index.getDatasetName()));
                firstExternalDatasetIndex = filesIndex == null;
                // Lock external dataset
                ExternalDatasetsRegistry.INSTANCE.buildIndexBegin(ds, firstExternalDatasetIndex);
                datasetLocked = true;
                if (firstExternalDatasetIndex) {
                    // Verify that no one has created an index before we acquire the lock
                    filesIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                            index.getDataverseName(), index.getDatasetName(),
                            IndexingConstants.getFilesIndexName(index.getDatasetName()));
                    if (filesIndex != null) {
                        ExternalDatasetsRegistry.INSTANCE.buildIndexEnd(ds, firstExternalDatasetIndex);
                        firstExternalDatasetIndex = false;
                        ExternalDatasetsRegistry.INSTANCE.buildIndexBegin(ds, firstExternalDatasetIndex);
                    }
                }
                if (firstExternalDatasetIndex) {
                    // Get snapshot from External File System
                    externalFilesSnapshot = ExternalIndexingOperations.getSnapshotFromExternalFileSystem(ds);
                    // Add an entry for the files index
                    filesIndex = new Index(index.getDataverseName(), index.getDatasetName(),
                            IndexingConstants.getFilesIndexName(index.getDatasetName()), IndexType.BTREE,
                            ExternalIndexingOperations.FILE_INDEX_FIELD_NAMES, null,
                            ExternalIndexingOperations.FILE_INDEX_FIELD_TYPES, false, false, false,
                            MetadataUtil.PENDING_ADD_OP);
                    MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), filesIndex);
                    // Add files to the external files index
                    for (ExternalFile file : externalFilesSnapshot) {
                        MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
                    }
                    // This is the first index for the external dataset, replicate the files index
                    spec = ExternalIndexingOperations.buildFilesIndexCreateJobSpec(ds, externalFilesSnapshot,
                            metadataProvider);
                    if (spec == null) {
                        throw new CompilationException(
                                "Failed to create job spec for replicating Files Index For external dataset");
                    }
                    filesIndexReplicated = true;
                    runJob(hcc, spec, jobFlags);
                }
            }

            // check whether there exists another enforced index on the same field
            if (index.isEnforced()) {
                List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(
                        metadataProvider.getMetadataTxnContext(), index.getDataverseName(), index.getDatasetName());
                for (Index existingIndex : indexes) {
                    if (existingIndex.getKeyFieldNames().equals(index.getKeyFieldNames())
                            && !existingIndex.getKeyFieldTypes().equals(index.getKeyFieldTypes())
                            && existingIndex.isEnforced()) {
                        throw new CompilationException("Cannot create index " + index.getIndexName()
                                + " , enforced index " + existingIndex.getIndexName() + " on field \""
                                + StringUtils.join(index.getKeyFieldNames(), ',') + "\" is already defined with type \""
                                + existingIndex.getKeyFieldTypes() + "\"");
                    }
                }
            }
            // #. add a new index with PendingAddOp
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);
            // #. prepare to create the index artifact in NC.
            spec = IndexUtil.buildSecondaryIndexCreationJobSpec(ds, index, metadataProvider);
            if (spec == null) {
                throw new CompilationException("Failed to create job spec for creating index '" + ds.getDatasetName()
                        + "." + index.getIndexName() + "'");
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;
            // #. create the index artifact in NC.
            runJob(hcc, spec, jobFlags);

            // #. flush the internal dataset
            // We need this to guarantee the correctness of component Id acceleration for
            // secondary-to-primary index.
            // Otherwise, the new secondary index component would corresponding to a partial
            // memory component
            // of the primary index, which is incorrect.
            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                FlushDatasetUtil.flushDataset(hcc, metadataProvider, index.getDataverseName(), index.getDatasetName());
            }

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. load data into the index in NC.
            spec = IndexUtil.buildSecondaryIndexLoadingJobSpec(ds, index, metadataProvider);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            runJob(hcc, spec, jobFlags);

            // #. begin new metadataTxn
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. add another new index with PendingNoOp after deleting the index with
            // PendingAddOp
            MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), index.getDataverseName(),
                    index.getDatasetName(), index.getIndexName());
            index.setPendingOp(MetadataUtil.PENDING_NO_OP);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);
            // add another new files index with PendingNoOp after deleting the index with
            // PendingAddOp
            if (firstExternalDatasetIndex) {
                MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), index.getDataverseName(),
                        index.getDatasetName(), filesIndex.getIndexName());
                filesIndex.setPendingOp(MetadataUtil.PENDING_NO_OP);
                MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), filesIndex);
                // update transaction timestamp
                ((ExternalDatasetDetails) ds.getDatasetDetails()).setRefreshTimestamp(new Date());
                MetadataManager.INSTANCE.updateDataset(mdTxnCtx, ds);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            // If files index was replicated for external dataset, it should be cleaned up
            // on NC side
            if (filesIndexReplicated) {
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                try {
                    JobSpecification jobSpec =
                            ExternalIndexingOperations.buildDropFilesIndexJobSpec(metadataProvider, ds);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;
                    runJob(hcc, jobSpec, jobFlags);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // #. execute compensation operations
                // remove the index in NC
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    JobSpecification jobSpec = IndexUtil.buildDropIndexJobSpec(index, metadataProvider, ds);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;
                    runJob(hcc, jobSpec, jobFlags);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }

                if (firstExternalDatasetIndex) {
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    metadataProvider.setMetadataTxnContext(mdTxnCtx);
                    try {
                        // Drop External Files from metadata
                        MetadataManager.INSTANCE.dropDatasetExternalFiles(mdTxnCtx, ds);
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    } catch (Exception e2) {
                        e.addSuppressed(e2);
                        abort(e, e2, mdTxnCtx);
                        throw new IllegalStateException(
                                "System is inconsistent state: pending files for(" + index.getDataverseName() + "."
                                        + index.getDatasetName() + ") couldn't be removed from the metadata",
                                e);
                    }
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    metadataProvider.setMetadataTxnContext(mdTxnCtx);
                    try {
                        // Drop the files index from metadata
                        MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(),
                                index.getDataverseName(), index.getDatasetName(),
                                IndexingConstants.getFilesIndexName(index.getDatasetName()));
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    } catch (Exception e2) {
                        e.addSuppressed(e2);
                        abort(e, e2, mdTxnCtx);
                        throw new IllegalStateException("System is inconsistent state: pending index("
                                + index.getDataverseName() + "." + index.getDatasetName() + "."
                                + IndexingConstants.getFilesIndexName(index.getDatasetName())
                                + ") couldn't be removed from the metadata", e);
                    }
                }
                // remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(),
                            index.getDataverseName(), index.getDatasetName(), index.getIndexName());
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is in inconsistent state: pending index("
                            + index.getDataverseName() + "." + index.getDatasetName() + "." + index.getIndexName()
                            + ") couldn't be removed from the metadata", e);
                }
            }
            throw e;
        } finally {
            if (datasetLocked) {
                ExternalDatasetsRegistry.INSTANCE.buildIndexEnd(ds, firstExternalDatasetIndex);
            }
        }
    }

    protected void validateIndexKeyFields(CreateIndexStatement stmtCreateIndex, List<Integer> keySourceIndicators,
            ARecordType aRecordType, ARecordType metaRecordType, List<List<String>> indexFields,
            List<IAType> indexFieldTypes) throws AlgebricksException {
        ValidateUtil.validateKeyFields(aRecordType, metaRecordType, indexFields, keySourceIndicators, indexFieldTypes,
                stmtCreateIndex.getIndexType());
    }

    protected void handleCreateTypeStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        TypeDecl stmtCreateType = (TypeDecl) stmt;
        String dataverseName = getActiveDataverse(stmtCreateType.getDataverseName());
        String typeName = stmtCreateType.getIdent().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.createTypeBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + typeName);
        try {
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
                if (BuiltinTypeMap.getBuiltinType(typeName) != null) {
                    throw new AlgebricksException("Cannot redefine builtin type " + typeName + ".");
                } else {
                    Map<TypeSignature, IAType> typeMap = TypeTranslator.computeTypes(mdTxnCtx,
                            stmtCreateType.getTypeDef(), stmtCreateType.getIdent().getValue(), dataverseName);
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
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleDataverseDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        DataverseDropStatement stmtDelete = (DataverseDropStatement) stmt;
        String dataverseName = stmtDelete.getDataverseName().getValue();
        if (dataverseName.equals(MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME)) {
            throw new HyracksDataException(
                    MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME + " dataverse can't be dropped");
        }

        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        lockManager.acquireDataverseWriteLock(metadataProvider.getLocks(), dataverseName);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
            if (dv == null) {
                if (stmtDelete.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("There is no dataverse with this name " + dataverseName + ".");
                }
            }
            // # check whether any function in current dataverse is being used by others
            List<Function> functionsInDataverse =
                    MetadataManager.INSTANCE.getDataverseFunctions(mdTxnCtx, dataverseName);
            for (Function function : functionsInDataverse) {
                if (isFunctionUsed(mdTxnCtx, function.getSignature(), dataverseName)) {
                    throw new MetadataException(ErrorCode.METADATA_DROP_FUCTION_IN_USE,
                            function.getDataverseName() + "." + function.getName() + "@" + function.getArity());
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            // # disconnect all feeds from any datasets in the dataverse.
            ActiveNotificationHandler activeEventHandler =
                    (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
            IActiveEntityEventsListener[] activeListeners = activeEventHandler.getEventListeners();
            for (IActiveEntityEventsListener listener : activeListeners) {
                EntityId activeEntityId = listener.getEntityId();
                if (activeEntityId.getExtensionName().equals(Feed.EXTENSION_NAME)
                        && activeEntityId.getDataverse().equals(dataverseName)) {
                    if (listener.getState() != ActivityState.STOPPED) {
                        ((ActiveEntityEventsListener) listener).stop(metadataProvider);
                    }
                    FeedEventsListener feedListener = (FeedEventsListener) listener;
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    bActiveTxn = true;
                    metadataProvider.setMetadataTxnContext(mdTxnCtx);
                    doDropFeed(hcc, metadataProvider, feedListener.getFeed());
                    MetadataManager.INSTANCE.commitTransaction(metadataProvider.getMetadataTxnContext());
                    bActiveTxn = false;
                }
            }
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. prepare jobs which will drop corresponding datasets with indexes.
            List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dataverseName);
            for (Dataset dataset : datasets) {
                String datasetName = dataset.getDatasetName();
                DatasetType dsType = dataset.getDatasetType();
                if (dsType == DatasetType.INTERNAL) {
                    List<Index> indexes =
                            MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
                    for (Index index : indexes) {
                        jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, dataset));
                    }
                } else {
                    // External dataset
                    List<Index> indexes =
                            MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
                    for (int k = 0; k < indexes.size(); k++) {
                        if (ExternalIndexingOperations.isFileIndex(indexes.get(k))) {
                            jobsToExecute.add(
                                    ExternalIndexingOperations.buildDropFilesIndexJobSpec(metadataProvider, dataset));
                        } else {
                            jobsToExecute
                                    .add(IndexUtil.buildDropIndexJobSpec(indexes.get(k), metadataProvider, dataset));
                        }
                    }
                    ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(dataset);
                }
            }
            jobsToExecute.add(DataverseUtil.dropDataverseJobSpec(dv, metadataProvider));
            // #. mark PendingDropOp on the dataverse record by
            // first, deleting the dataverse record from the DATAVERSE_DATASET
            // second, inserting the dataverse record with the PendingDropOp value into the
            // DATAVERSE_DATASET
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
            MetadataManager.INSTANCE.addDataverse(mdTxnCtx,
                    new Dataverse(dataverseName, dv.getDataFormat(), MetadataUtil.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            for (JobSpecification jobSpec : jobsToExecute) {
                runJob(hcc, jobSpec);
            }

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. finally, delete the dataverse.
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);

            // Drops all node groups that no longer needed
            for (Dataset dataset : datasets) {
                String nodeGroup = dataset.getNodeGroupName();
                lockManager.acquireNodeGroupWriteLock(metadataProvider.getLocks(), nodeGroup);
                if (MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodeGroup) != null) {
                    MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, nodeGroup, true);
                }
            }

            if (activeDataverse != null && activeDataverse.getDataverseName() == dataverseName) {
                activeDataverse = null;
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                if (activeDataverse != null && activeDataverse.getDataverseName() == dataverseName) {
                    activeDataverse = null;
                }

                // #. execute compensation operations
                // remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        runJob(hcc, jobSpec);
                    }
                } catch (Exception e2) {
                    // do no throw exception since still the metadata needs to be compensated.
                    e.addSuppressed(e2);
                }

                // remove the record from the metadata.
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
            metadataProvider.getLocks().unlock();
            ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
        }
    }

    public void handleDatasetDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        DropDatasetStatement stmtDelete = (DropDatasetStatement) stmt;
        String dataverseName = getActiveDataverse(stmtDelete.getDataverseName());
        String datasetName = stmtDelete.getDatasetName().getValue();
        MetadataLockUtil.dropDatasetBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + datasetName);
        try {
            doDropDataset(dataverseName, datasetName, metadataProvider, stmtDelete.getIfExists(), hcc, true);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    public static void doDropDataset(String dataverseName, String datasetName, MetadataProvider metadataProvider,
            boolean ifExists, IHyracksClientConnection hcc, boolean dropCorrespondingNodeGroup) throws Exception {
        MutableObject<ProgressState> progress = new MutableObject<>(ProgressState.NO_PROGRESS);
        MutableObject<MetadataTransactionContext> mdTxnCtx =
                new MutableObject<>(MetadataManager.INSTANCE.beginTransaction());
        MutableBoolean bActiveTxn = new MutableBoolean(true);
        metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        try {
            Dataset ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds == null) {
                if (ifExists) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
                    return;
                } else {
                    throw new AsterixException(ErrorCode.NO_DATASET_WITH_NAME, dataverseName, datasetName);
                }
            }
            ds.drop(metadataProvider, mdTxnCtx, jobsToExecute, bActiveTxn, progress, hcc, dropCorrespondingNodeGroup);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
        } catch (Exception e) {
            if (bActiveTxn.booleanValue()) {
                abort(e, e, mdTxnCtx.getValue());
            }

            if (progress.getValue() == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // #. execute compensation operations
                // remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        JobUtils.runJob(hcc, jobSpec, true);
                    }
                } catch (Exception e2) {
                    // do no throw exception since still the metadata needs to be compensated.
                    e.addSuppressed(e2);
                }

                // remove the record from the metadata.
                mdTxnCtx.setValue(MetadataManager.INSTANCE.beginTransaction());
                metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
                try {
                    MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx.getValue());
                    throw new IllegalStateException("System is inconsistent state: pending dataset(" + dataverseName
                            + "." + datasetName + ") couldn't be removed from the metadata", e);
                }
            }
            throw e;
        } finally {
            ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
        }
    }

    protected void handleIndexDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {

        IndexDropStatement stmtIndexDrop = (IndexDropStatement) stmt;
        String datasetName = stmtIndexDrop.getDatasetName().getValue();
        String dataverseName = getActiveDataverse(stmtIndexDrop.getDataverseName());
        String indexName = stmtIndexDrop.getIndexName().getValue();
        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        String dsFullyQualifiedName = dataverseName + "." + datasetName;
        MetadataLockUtil.dropIndexBegin(lockManager, metadataProvider.getLocks(), dataverseName, dsFullyQualifiedName);
        // For external index
        boolean dropFilesIndex = false;
        try {
            Dataset ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName);
            }
            ActiveNotificationHandler activeEventHandler =
                    (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
            IActiveEntityEventsListener[] listeners = activeEventHandler.getEventListeners();
            StringBuilder builder = null;
            for (IActiveEntityEventsListener listener : listeners) {
                if (listener.isEntityUsingDataset(ds)) {
                    if (builder == null) {
                        builder = new StringBuilder();
                    }
                    builder.append(new FeedConnectionId(listener.getEntityId(), datasetName) + "\n");
                }
            }
            if (builder != null) {
                throw new CompilationException("Dataset" + datasetName
                        + " is currently being fed into by the following active entities: " + builder.toString());
            }

            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                Index index = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                if (index == null) {
                    if (stmtIndexDrop.getIfExists()) {
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                        return;
                    } else {
                        throw new AlgebricksException("There is no index with this name " + indexName + ".");
                    }
                }
                ensureNonPrimaryIndexDrop(index);
                // #. prepare a job to drop the index in NC.
                jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, ds));

                // #. mark PendingDropOp on the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                        new Index(dataverseName, datasetName, indexName, index.getIndexType(), index.getKeyFieldNames(),
                                index.getKeyFieldSourceIndicators(), index.getKeyFieldTypes(),
                                index.isOverridingKeyFieldTypes(), index.isEnforced(), index.isPrimaryIndex(),
                                MetadataUtil.PENDING_DROP_OP));

                // #. commit the existing transaction before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                for (JobSpecification jobSpec : jobsToExecute) {
                    runJob(hcc, jobSpec);
                }

                // #. begin a new transaction
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);

                // #. finally, delete the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
            } else {
                // External dataset
                indexName = stmtIndexDrop.getIndexName().getValue();
                Index index = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                if (index == null) {
                    if (stmtIndexDrop.getIfExists()) {
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                        return;
                    } else {
                        throw new AlgebricksException("There is no index with this name " + indexName + ".");
                    }
                } else if (ExternalIndexingOperations.isFileIndex(index)) {
                    throw new AlgebricksException("Dropping a dataset's files index is not allowed.");
                }
                ensureNonPrimaryIndexDrop(index);
                // #. prepare a job to drop the index in NC.
                jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, ds));
                List<Index> datasetIndexes =
                        MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
                if (datasetIndexes.size() == 2) {
                    dropFilesIndex = true;
                    // only one index + the files index, we need to delete both of the indexes
                    for (Index externalIndex : datasetIndexes) {
                        if (ExternalIndexingOperations.isFileIndex(externalIndex)) {
                            jobsToExecute
                                    .add(ExternalIndexingOperations.buildDropFilesIndexJobSpec(metadataProvider, ds));
                            // #. mark PendingDropOp on the existing files index
                            MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName,
                                    externalIndex.getIndexName());
                            MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                                    new Index(dataverseName, datasetName, externalIndex.getIndexName(),
                                            externalIndex.getIndexType(), externalIndex.getKeyFieldNames(),
                                            externalIndex.getKeyFieldSourceIndicators(), index.getKeyFieldTypes(),
                                            index.isOverridingKeyFieldTypes(), index.isEnforced(),
                                            externalIndex.isPrimaryIndex(), MetadataUtil.PENDING_DROP_OP));
                        }
                    }
                }

                // #. mark PendingDropOp on the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                        new Index(dataverseName, datasetName, indexName, index.getIndexType(), index.getKeyFieldNames(),
                                index.getKeyFieldSourceIndicators(), index.getKeyFieldTypes(),
                                index.isOverridingKeyFieldTypes(), index.isEnforced(), index.isPrimaryIndex(),
                                MetadataUtil.PENDING_DROP_OP));

                // #. commit the existing transaction before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                for (JobSpecification jobSpec : jobsToExecute) {
                    runJob(hcc, jobSpec);
                }

                // #. begin a new transaction
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);

                // #. finally, delete the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                if (dropFilesIndex) {
                    // delete the files index too
                    MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName,
                            IndexingConstants.getFilesIndexName(datasetName));
                    MetadataManager.INSTANCE.dropDatasetExternalFiles(mdTxnCtx, ds);
                    ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(ds);
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // #. execute compensation operations
                // remove the all indexes in NC
                try {
                    for (JobSpecification jobSpec : jobsToExecute) {
                        runJob(hcc, jobSpec);
                    }
                } catch (Exception e2) {
                    // do no throw exception since still the metadata needs to be compensated.
                    e.addSuppressed(e2);
                }

                // remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName, indexName);
                    if (dropFilesIndex) {
                        MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                                datasetName, IndexingConstants.getFilesIndexName(datasetName));
                    }
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending index(" + dataverseName + "."
                            + datasetName + "." + indexName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;

        } finally {
            metadataProvider.getLocks().unlock();
            ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
        }
    }

    protected void handleTypeDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {

        TypeDropStatement stmtTypeDrop = (TypeDropStatement) stmt;
        String dataverseName = getActiveDataverse(stmtTypeDrop.getDataverseName());
        String typeName = stmtTypeDrop.getTypeName().getValue();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.dropTypeBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + typeName);
        try {
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
            if (dt == null) {
                if (!stmtTypeDrop.getIfExists()) {
                    throw new AlgebricksException("There is no datatype with this name " + typeName + ".");
                }
            } else {
                MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, dataverseName, typeName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleNodegroupDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        NodeGroupDropStatement stmtDelete = (NodeGroupDropStatement) stmt;
        String nodegroupName = stmtDelete.getNodeGroupName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockManager.acquireNodeGroupWriteLock(metadataProvider.getLocks(), nodegroupName);
        try {
            NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodegroupName);
            if (ng == null) {
                if (!stmtDelete.getIfExists()) {
                    throw new AlgebricksException("There is no nodegroup with this name " + nodegroupName + ".");
                }
            } else {
                MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, nodegroupName, false);
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleCreateFunctionStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        CreateFunctionStatement cfs = (CreateFunctionStatement) stmt;
        FunctionSignature signature = cfs.getFunctionSignature();
        String dataverse = getActiveDataverseName(signature.getNamespace());
        signature.setNamespace(dataverse);

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.functionStatementBegin(lockManager, metadataProvider.getLocks(), dataverse,
                dataverse + "." + signature.getName());
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                throw new AlgebricksException("There is no dataverse with this name " + dataverse + ".");
            }

            //Check whether the function is use-able
            metadataProvider.setDefaultDataverse(dv);
            Query wrappedQuery = new Query(false);
            wrappedQuery.setBody(cfs.getFunctionBodyExpression());
            wrappedQuery.setTopLevel(false);
            List<VarIdentifier> varIds = new ArrayList<>();
            for (String v : cfs.getParamList()) {
                varIds.add(new VarIdentifier(v));
            }
            wrappedQuery.setExternalVars(varIds);
            apiFramework.reWriteQuery(declaredFunctions, metadataProvider, wrappedQuery, sessionOutput, false);

            List<List<List<String>>> dependencies = FunctionUtil.getFunctionDependencies(
                    rewriterFactory.createQueryRewriter(), cfs.getFunctionBodyExpression(), metadataProvider);

            final String language =
                    rewriterFactory instanceof SqlppRewriterFactory ? Function.LANGUAGE_SQLPP : Function.LANGUAGE_AQL;
            Function function = new Function(signature, cfs.getParamList(), Function.RETURNTYPE_VOID,
                    cfs.getFunctionBody(), language, FunctionKind.SCALAR.toString(), dependencies);
            MetadataManager.INSTANCE.addFunction(mdTxnCtx, function);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
            metadataProvider.setDefaultDataverse(activeDataverse);
        }
    }

    protected boolean isFunctionUsed(MetadataTransactionContext ctx, FunctionSignature signature,
            String currentDataverse) throws AlgebricksException {
        List<Dataverse> allDataverses = MetadataManager.INSTANCE.getDataverses(ctx);
        for (Dataverse dataverse : allDataverses) {
            if (currentDataverse != null && dataverse.getDataverseName().equals(currentDataverse)) {
                continue;
            }
            List<Feed> feeds = MetadataManager.INSTANCE.getFeeds(ctx, dataverse.getDataverseName());
            for (Feed feed : feeds) {
                List<FeedConnection> feedConnections = MetadataManager.INSTANCE.getFeedConections(ctx,
                        dataverse.getDataverseName(), feed.getFeedName());
                for (FeedConnection conn : feedConnections) {
                    if (conn.containsFunction(signature)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    protected void handleFunctionDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        FunctionDropStatement stmtDropFunction = (FunctionDropStatement) stmt;
        FunctionSignature signature = stmtDropFunction.getFunctionSignature();
        signature.setNamespace(getActiveDataverseName(signature.getNamespace()));
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.functionStatementBegin(lockManager, metadataProvider.getLocks(), signature.getNamespace(),
                signature.getNamespace() + "." + signature.getName());
        try {
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
            if (function == null && !stmtDropFunction.getIfExists()) {
                throw new AlgebricksException("Unknonw function " + signature);
            } else if (isFunctionUsed(mdTxnCtx, signature, null)) {
                throw new MetadataException(ErrorCode.METADATA_DROP_FUCTION_IN_USE, signature);
            } else {
                MetadataManager.INSTANCE.dropFunction(mdTxnCtx, signature);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleLoadStatement(MetadataProvider metadataProvider, Statement stmt, IHyracksClientConnection hcc)
            throws Exception {
        LoadStatement loadStmt = (LoadStatement) stmt;
        String dataverseName = getActiveDataverse(loadStmt.getDataverseName());
        String datasetName = loadStmt.getDatasetName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.modifyDatasetBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + datasetName);
        try {
            CompiledLoadFromFileStatement cls =
                    new CompiledLoadFromFileStatement(dataverseName, loadStmt.getDatasetName().getValue(),
                            loadStmt.getAdapter(), loadStmt.getProperties(), loadStmt.dataIsAlreadySorted());
            JobSpecification spec = apiFramework.compileQuery(hcc, metadataProvider, null, 0, null, sessionOutput, cls);
            afterCompile();
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            if (spec != null) {
                runJob(hcc, spec);
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    public JobSpecification handleInsertUpsertStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery,
            ResultMetadata outMetadata, Stats stats, boolean compileOnly, String clientContextId) throws Exception {
        InsertStatement stmtInsertUpsert = (InsertStatement) stmt;
        String dataverseName = getActiveDataverse(stmtInsertUpsert.getDataverseName());
        final IMetadataLocker locker = new IMetadataLocker() {
            @Override
            public void lock() throws AlgebricksException {
                MetadataLockUtil.insertDeleteUpsertBegin(lockManager, metadataProvider.getLocks(),
                        dataverseName + "." + stmtInsertUpsert.getDatasetName());
            }

            @Override
            public void unlock() {
                metadataProvider.getLocks().unlock();
            }
        };
        final IStatementCompiler compiler = () -> {
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            boolean bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            try {
                metadataProvider.setWriteTransaction(true);
                final JobSpecification jobSpec = rewriteCompileInsertUpsert(hcc, metadataProvider, stmtInsertUpsert);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                return jobSpec;
            } catch (Exception e) {
                if (bActiveTxn) {
                    abort(e, e, mdTxnCtx);
                }
                throw e;
            }
        };
        if (compileOnly) {
            locker.lock();
            try {
                return compiler.compile();
            } finally {
                locker.unlock();
            }
        }

        if (stmtInsertUpsert.getReturnExpression() != null) {
            deliverResult(hcc, hdc, compiler, metadataProvider, locker, resultDelivery, outMetadata, stats,
                    clientContextId, NoOpStatementExecutorContext.INSTANCE);
        } else {
            locker.lock();
            try {
                final JobSpecification jobSpec = compiler.compile();
                if (jobSpec == null) {
                    return jobSpec;
                }
                runJob(hcc, jobSpec);
            } finally {
                locker.unlock();
            }
        }
        return null;
    }

    public JobSpecification handleDeleteStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, boolean compileOnly) throws Exception {
        DeleteStatement stmtDelete = (DeleteStatement) stmt;
        String dataverseName = getActiveDataverse(stmtDelete.getDataverseName());
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.insertDeleteUpsertBegin(lockManager, metadataProvider.getLocks(),
                dataverseName + "." + stmtDelete.getDatasetName());
        try {
            metadataProvider.setWriteTransaction(true);
            CompiledDeleteStatement clfrqs = new CompiledDeleteStatement(stmtDelete.getVariableExpr(), dataverseName,
                    stmtDelete.getDatasetName().getValue(), stmtDelete.getCondition(), stmtDelete.getVarCounter(),
                    stmtDelete.getQuery());
            JobSpecification jobSpec = rewriteCompileQuery(hcc, metadataProvider, clfrqs.getQuery(), clfrqs);
            afterCompile();

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (jobSpec != null && !compileOnly) {
                runJob(hcc, jobSpec);
            }
            return jobSpec;
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    @Override
    public JobSpecification rewriteCompileQuery(IClusterInfoCollector clusterInfoCollector,
            MetadataProvider metadataProvider, Query query, ICompiledDmlStatement stmt)
            throws RemoteException, AlgebricksException, ACIDException {

        // Query Rewriting (happens under the same ongoing metadata transaction)
        Pair<IReturningStatement, Integer> rewrittenResult =
                apiFramework.reWriteQuery(declaredFunctions, metadataProvider, query, sessionOutput, true);

        // Query Compilation (happens under the same ongoing metadata transaction)
        return apiFramework.compileQuery(clusterInfoCollector, metadataProvider, (Query) rewrittenResult.first,
                rewrittenResult.second, stmt == null ? null : stmt.getDatasetName(), sessionOutput, stmt);
    }

    private JobSpecification rewriteCompileInsertUpsert(IClusterInfoCollector clusterInfoCollector,
            MetadataProvider metadataProvider, InsertStatement insertUpsert)
            throws RemoteException, AlgebricksException, ACIDException {

        // Insert/upsert statement rewriting (happens under the same ongoing metadata
        // transaction)
        Pair<IReturningStatement, Integer> rewrittenResult =
                apiFramework.reWriteQuery(declaredFunctions, metadataProvider, insertUpsert, sessionOutput, true);

        InsertStatement rewrittenInsertUpsert = (InsertStatement) rewrittenResult.first;
        String dataverseName = getActiveDataverse(rewrittenInsertUpsert.getDataverseName());
        String datasetName = rewrittenInsertUpsert.getDatasetName().getValue();
        CompiledInsertStatement clfrqs;
        switch (insertUpsert.getKind()) {
            case INSERT:
                clfrqs = new CompiledInsertStatement(dataverseName, datasetName, rewrittenInsertUpsert.getQuery(),
                        rewrittenInsertUpsert.getVarCounter(), rewrittenInsertUpsert.getVar(),
                        rewrittenInsertUpsert.getReturnExpression());
                break;
            case UPSERT:
                clfrqs = new CompiledUpsertStatement(dataverseName, datasetName, rewrittenInsertUpsert.getQuery(),
                        rewrittenInsertUpsert.getVarCounter(), rewrittenInsertUpsert.getVar(),
                        rewrittenInsertUpsert.getReturnExpression());
                break;
            default:
                throw new AlgebricksException("Unsupported statement type " + rewrittenInsertUpsert.getKind());
        }
        // Insert/upsert statement compilation (happens under the same ongoing metadata
        // transaction)
        return apiFramework.compileQuery(clusterInfoCollector, metadataProvider, rewrittenInsertUpsert.getQuery(),
                rewrittenResult.second, datasetName, sessionOutput, clfrqs);
    }

    protected void handleCreateFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        CreateFeedStatement cfs = (CreateFeedStatement) stmt;
        String dataverseName = getActiveDataverse(cfs.getDataverseName());
        String feedName = cfs.getFeedName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.createFeedBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + feedName);
        try {
            Feed feed =
                    MetadataManager.INSTANCE.getFeed(metadataProvider.getMetadataTxnContext(), dataverseName, feedName);
            if (feed != null) {
                if (cfs.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A feed with this name " + feedName + " already exists.");
                }
            }
            feed = new Feed(dataverseName, feedName, cfs.getConfiguration());
            FeedMetadataUtil.validateFeed(feed, mdTxnCtx, appCtx);
            MetadataManager.INSTANCE.addFeed(metadataProvider.getMetadataTxnContext(), feed);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleCreateFeedPolicyStatement(MetadataProvider metadataProvider, Statement stmt)
            throws AlgebricksException, HyracksDataException {
        String dataverse;
        String policy;
        FeedPolicyEntity newPolicy = null;
        MetadataTransactionContext mdTxnCtx = null;
        CreateFeedPolicyStatement cfps = (CreateFeedPolicyStatement) stmt;
        dataverse = getActiveDataverse(null);
        policy = cfps.getPolicyName();
        MetadataLockUtil.createFeedPolicyBegin(lockManager, metadataProvider.getLocks(), dataverse,
                dataverse + "." + policy);
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            FeedPolicyEntity feedPolicy =
                    MetadataManager.INSTANCE.getFeedPolicy(metadataProvider.getMetadataTxnContext(), dataverse, policy);
            if (feedPolicy != null) {
                if (cfps.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A policy with this name " + policy + " already exists.");
                }
            }
            boolean extendingExisting = cfps.getSourcePolicyName() != null;
            String description = cfps.getDescription() == null ? "" : cfps.getDescription();
            if (extendingExisting) {
                FeedPolicyEntity sourceFeedPolicy = MetadataManager.INSTANCE
                        .getFeedPolicy(metadataProvider.getMetadataTxnContext(), dataverse, cfps.getSourcePolicyName());
                if (sourceFeedPolicy == null) {
                    sourceFeedPolicy = MetadataManager.INSTANCE.getFeedPolicy(metadataProvider.getMetadataTxnContext(),
                            MetadataConstants.METADATA_DATAVERSE_NAME, cfps.getSourcePolicyName());
                    if (sourceFeedPolicy == null) {
                        throw new AlgebricksException("Unknown policy " + cfps.getSourcePolicyName());
                    }
                }
                Map<String, String> policyProperties = sourceFeedPolicy.getProperties();
                policyProperties.putAll(cfps.getProperties());
                newPolicy = new FeedPolicyEntity(dataverse, policy, description, policyProperties);
            } else {
                Properties prop = new Properties();
                try {
                    InputStream stream = new FileInputStream(cfps.getSourcePolicyFile());
                    prop.load(stream);
                } catch (Exception e) {
                    throw new AlgebricksException("Unable to read policy file" + cfps.getSourcePolicyFile(), e);
                }
                Map<String, String> policyProperties = new HashMap<>();
                prop.forEach((key, value) -> policyProperties.put((String) key, (String) value));
                newPolicy = new FeedPolicyEntity(dataverse, policy, description, policyProperties);
            }
            MetadataManager.INSTANCE.addFeedPolicy(mdTxnCtx, newPolicy);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (RemoteException | ACIDException e) {
            abort(e, e, mdTxnCtx);
            throw HyracksDataException.create(e);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleDropFeedStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        FeedDropStatement stmtFeedDrop = (FeedDropStatement) stmt;
        String dataverseName = getActiveDataverse(stmtFeedDrop.getDataverseName());
        String feedName = stmtFeedDrop.getFeedName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.dropFeedBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + feedName);
        try {
            Feed feed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, dataverseName, feedName);
            if (feed == null) {
                if (!stmtFeedDrop.getIfExists()) {
                    throw new AlgebricksException("There is no feed with this name " + feedName + ".");
                }
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;
            }
            doDropFeed(hcc, metadataProvider, feed);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void doDropFeed(IHyracksClientConnection hcc, MetadataProvider metadataProvider, Feed feed)
            throws Exception {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        EntityId feedId = feed.getFeedId();
        ActiveNotificationHandler activeNotificationHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        ActiveEntityEventsListener listener =
                (ActiveEntityEventsListener) activeNotificationHandler.getListener(feedId);
        if (listener != null && listener.getState() != ActivityState.STOPPED) {
            throw new AlgebricksException("Feed " + feedId
                    + " is currently active and connected to the following dataset(s) \n" + listener.toString());
        } else if (listener != null) {
            listener.unregister();
        }
        JobSpecification spec = FeedOperations.buildRemoveFeedStorageJob(metadataProvider,
                MetadataManager.INSTANCE.getFeed(mdTxnCtx, feedId.getDataverse(), feedId.getEntityName()));
        runJob(hcc, spec);
        MetadataManager.INSTANCE.dropFeed(mdTxnCtx, feed.getDataverseName(), feed.getFeedName());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Removed feed " + feedId);
        }
    }

    protected void handleDropFeedPolicyStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        FeedPolicyDropStatement stmtFeedPolicyDrop = (FeedPolicyDropStatement) stmt;
        String dataverseName = getActiveDataverse(stmtFeedPolicyDrop.getDataverseName());
        String policyName = stmtFeedPolicyDrop.getPolicyName().getValue();
        MetadataLockUtil.dropFeedPolicyBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + policyName);
        try {
            FeedPolicyEntity feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(mdTxnCtx, dataverseName, policyName);
            if (feedPolicy == null) {
                if (!stmtFeedPolicyDrop.getIfExists()) {
                    throw new AlgebricksException("Unknown policy " + policyName + " in dataverse " + dataverseName);
                }
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;
            }
            MetadataManager.INSTANCE.dropFeedPolicy(mdTxnCtx, dataverseName, policyName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    private void handleStartFeedStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        StartFeedStatement sfs = (StartFeedStatement) stmt;
        String dataverseName = getActiveDataverse(sfs.getDataverseName());
        String feedName = sfs.getFeedName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean committed = false;
        MetadataLockUtil.startFeedBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + feedName);
        try {
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            // Runtime handler
            EntityId entityId = new EntityId(Feed.EXTENSION_NAME, dataverseName, feedName);
            // Feed & Feed Connections
            Feed feed = FeedMetadataUtil.validateIfFeedExists(dataverseName, feedName,
                    metadataProvider.getMetadataTxnContext());
            List<FeedConnection> feedConnections = MetadataManager.INSTANCE
                    .getFeedConections(metadataProvider.getMetadataTxnContext(), dataverseName, feedName);
            if (feedConnections.isEmpty()) {
                throw new CompilationException(ErrorCode.FEED_START_FEED_WITHOUT_CONNECTION, feedName);
            }
            for (FeedConnection feedConnection : feedConnections) {
                // what if the dataset is in a different dataverse
                String fqName = feedConnection.getDataverseName() + "." + feedConnection.getDatasetName();
                lockManager.acquireDatasetReadLock(metadataProvider.getLocks(), fqName);
            }
            ActiveNotificationHandler activeEventHandler =
                    (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
            ActiveEntityEventsListener listener = (ActiveEntityEventsListener) activeEventHandler.getListener(entityId);
            if (listener == null) {
                // Prepare policy
                List<Dataset> datasets = new ArrayList<>();
                for (FeedConnection connection : feedConnections) {
                    Dataset ds =
                            metadataProvider.findDataset(connection.getDataverseName(), connection.getDatasetName());
                    datasets.add(ds);
                }
                listener = new FeedEventsListener(this, metadataProvider.getApplicationContext(), hcc, entityId,
                        datasets, null, FeedIntakeOperatorNodePushable.class.getSimpleName(),
                        NoRetryPolicyFactory.INSTANCE, feed, feedConnections);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            committed = true;
            listener.start(metadataProvider);
        } catch (Exception e) {
            if (!committed) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    private void handleStopFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        StopFeedStatement sfst = (StopFeedStatement) stmt;
        String dataverseName = getActiveDataverse(sfst.getDataverseName());
        String feedName = sfst.getFeedName().getValue();
        EntityId entityId = new EntityId(Feed.EXTENSION_NAME, dataverseName, feedName);
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        // Obtain runtime info from ActiveListener
        ActiveEntityEventsListener listener = (ActiveEntityEventsListener) activeEventHandler.getListener(entityId);
        if (listener == null) {
            throw new AlgebricksException("Feed " + feedName + " is not started.");
        }
        MetadataLockUtil.stopFeedBegin(lockManager, metadataProvider.getLocks(), entityId.getDataverse(),
                entityId.getEntityName());
        try {
            listener.stop(metadataProvider);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    private void handleConnectFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        FeedConnection fc;
        ConnectFeedStatement cfs = (ConnectFeedStatement) stmt;
        String dataverseName = getActiveDataverse(cfs.getDataverseName());
        String feedName = cfs.getFeedName();
        String datasetName = cfs.getDatasetName().getValue();
        String policyName = cfs.getPolicy();
        String whereClauseBody = cfs.getWhereClauseBody();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        // TODO: Check whether we are connecting a change feed to a non-meta dataset
        // Check whether feed is alive
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        // Transaction handling
        MetadataLockUtil.connectFeedBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + datasetName, dataverseName + "." + feedName);
        try {
            // validation
            Dataset dataset = FeedMetadataUtil.validateIfDatasetExists(metadataProvider, dataverseName, datasetName);
            Feed feed = FeedMetadataUtil.validateIfFeedExists(dataverseName, feedName,
                    metadataProvider.getMetadataTxnContext());
            FeedEventsListener listener = (FeedEventsListener) activeEventHandler.getListener(feed.getFeedId());
            if (listener != null && listener.isActive()) {
                throw new CompilationException(ErrorCode.FEED_CHANGE_FEED_CONNECTIVITY_ON_ALIVE_FEED, feedName);
            }
            ARecordType outputType = FeedMetadataUtil.getOutputType(feed,
                    feed.getConfiguration().get(ExternalDataConstants.KEY_TYPE_NAME));
            List<FunctionSignature> appliedFunctions = cfs.getAppliedFunctions();
            for (FunctionSignature func : appliedFunctions) {
                if (MetadataManager.INSTANCE.getFunction(mdTxnCtx, func) == null) {
                    throw new CompilationException(ErrorCode.FEED_CONNECT_FEED_APPLIED_INVALID_FUNCTION,
                            func.getName());
                }
            }
            fc = MetadataManager.INSTANCE.getFeedConnection(metadataProvider.getMetadataTxnContext(), dataverseName,
                    feedName, datasetName);
            if (fc != null) {
                throw new AlgebricksException("Feed" + feedName + " is already connected dataset " + datasetName);
            }
            fc = new FeedConnection(dataverseName, feedName, datasetName, appliedFunctions, policyName, whereClauseBody,
                    outputType.getTypeName());
            MetadataManager.INSTANCE.addFeedConnection(metadataProvider.getMetadataTxnContext(), fc);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            if (listener != null) {
                listener.add(dataset);
                listener.addFeedConnection(fc);
            }
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleDisconnectFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        DisconnectFeedStatement cfs = (DisconnectFeedStatement) stmt;
        String dataverseName = getActiveDataverse(cfs.getDataverseName());
        String datasetName = cfs.getDatasetName().getValue();
        String feedName = cfs.getFeedName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.disconnectFeedBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + datasetName, dataverseName + "." + cfs.getFeedName());
        try {
            ActiveNotificationHandler activeEventHandler =
                    (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
            // Check whether feed is alive
            ActiveEntityEventsListener listener = (ActiveEntityEventsListener) activeEventHandler
                    .getListener(new EntityId(Feed.EXTENSION_NAME, dataverseName, feedName));
            if (listener != null && listener.isActive()) {
                throw new CompilationException(ErrorCode.FEED_CHANGE_FEED_CONNECTIVITY_ON_ALIVE_FEED, feedName);
            }
            FeedMetadataUtil.validateIfDatasetExists(metadataProvider, dataverseName, cfs.getDatasetName().getValue());
            FeedMetadataUtil.validateIfFeedExists(dataverseName, cfs.getFeedName().getValue(), mdTxnCtx);
            FeedConnection fc = MetadataManager.INSTANCE.getFeedConnection(metadataProvider.getMetadataTxnContext(),
                    dataverseName, feedName, datasetName);
            Dataset ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds == null) {
                throw new CompilationException("Dataset " + dataverseName + "." + datasetName + " doesn't exist");
            }
            if (fc == null) {
                throw new CompilationException("Feed " + feedName + " is currently not connected to "
                        + cfs.getDatasetName().getValue() + ". Invalid operation!");
            }
            MetadataManager.INSTANCE.dropFeedConnection(mdTxnCtx, dataverseName, feedName, datasetName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            if (listener != null) {
                listener.remove(ds);
            }
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleCompactStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        CompactStatement compactStatement = (CompactStatement) stmt;
        String dataverseName = getActiveDataverse(compactStatement.getDataverseName());
        String datasetName = compactStatement.getDatasetName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        MetadataLockUtil.compactBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + datasetName);
        try {
            Dataset ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName + ".");
            }
            // Prepare jobs to compact the datatset and its indexes
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
            if (indexes.isEmpty()) {
                throw new AlgebricksException(
                        "Cannot compact the extrenal dataset " + datasetName + " because it has no indexes");
            }
            Dataverse dataverse =
                    MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dataverseName);
            jobsToExecute.add(DatasetUtil.compactDatasetJobSpec(dataverse, datasetName, metadataProvider));

            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                for (Index index : indexes) {
                    if (index.isSecondaryIndex()) {
                        jobsToExecute.add(IndexUtil.buildSecondaryIndexCompactJobSpec(ds, index, metadataProvider));
                    }
                }
            } else {
                prepareCompactJobsForExternalDataset(indexes, ds, jobsToExecute, metadataProvider);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            // #. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                runJob(hcc, jobSpec);
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
            ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
        }
    }

    protected void prepareCompactJobsForExternalDataset(List<Index> indexes, Dataset ds,
            List<JobSpecification> jobsToExecute, MetadataProvider metadataProvider) throws AlgebricksException {
        for (int j = 0; j < indexes.size(); j++) {
            jobsToExecute.add(IndexUtil.buildSecondaryIndexCompactJobSpec(ds, indexes.get(j), metadataProvider));

        }
    }

    private interface IMetadataLocker {
        void lock() throws AlgebricksException;

        void unlock() throws AlgebricksException;
    }

    private interface IResultPrinter {
        void print(JobId jobId) throws HyracksDataException, AlgebricksException;
    }

    private interface IStatementCompiler {
        JobSpecification compile() throws AlgebricksException, RemoteException, ACIDException;
    }

    protected void handleQuery(MetadataProvider metadataProvider, Query query, IHyracksClientConnection hcc,
            IHyracksDataset hdc, ResultDelivery resultDelivery, ResultMetadata outMetadata, Stats stats,
            String clientContextId, IStatementExecutorContext ctx) throws Exception {
        final IMetadataLocker locker = new IMetadataLocker() {
            @Override
            public void lock() {
            }

            @Override
            public void unlock() {
                metadataProvider.getLocks().unlock();
                // release external datasets' locks acquired during compilation of the query
                ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
            }
        };
        final IStatementCompiler compiler = () -> {
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            boolean bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            try {
                final JobSpecification jobSpec = rewriteCompileQuery(hcc, metadataProvider, query, null);
                afterCompile();
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                return query.isExplain() || !sessionConfig.isExecuteQuery() ? null : jobSpec;
            } catch (Exception e) {
                LOGGER.log(Level.INFO, e.getMessage(), e);
                if (bActiveTxn) {
                    abort(e, e, mdTxnCtx);
                }
                throw e;
            }
        };
        deliverResult(hcc, hdc, compiler, metadataProvider, locker, resultDelivery, outMetadata, stats, clientContextId,
                ctx);
    }

    private void deliverResult(IHyracksClientConnection hcc, IHyracksDataset hdc, IStatementCompiler compiler,
            MetadataProvider metadataProvider, IMetadataLocker locker, ResultDelivery resultDelivery,
            ResultMetadata outMetadata, Stats stats, String clientContextId, IStatementExecutorContext ctx)
            throws Exception {
        final ResultSetId resultSetId = metadataProvider.getResultSetId();
        switch (resultDelivery) {
            case ASYNC:
                MutableBoolean printed = new MutableBoolean(false);
                executorService.submit(() -> asyncCreateAndRunJob(hcc, compiler, locker, resultDelivery,
                        clientContextId, ctx, resultSetId, printed));
                synchronized (printed) {
                    while (!printed.booleanValue()) {
                        printed.wait();
                    }
                }
                break;
            case IMMEDIATE:
                createAndRunJob(hcc, jobFlags, null, compiler, locker, resultDelivery, id -> {
                    final ResultReader resultReader = new ResultReader(hdc, id, resultSetId);
                    updateJobStats(id, stats);
                    // stop buffering and allow for streaming result delivery
                    sessionOutput.release();
                    ResultUtil.printResults(appCtx, resultReader, sessionOutput, stats,
                            metadataProvider.findOutputRecordType());
                }, clientContextId, ctx);
                break;
            case DEFERRED:
                createAndRunJob(hcc, jobFlags, null, compiler, locker, resultDelivery, id -> {
                    updateJobStats(id, stats);
                    ResultUtil.printResultHandle(sessionOutput, new ResultHandle(id, resultSetId));
                    if (outMetadata != null) {
                        outMetadata.getResultSets()
                                .add(Triple.of(id, resultSetId, metadataProvider.findOutputRecordType()));
                    }
                }, clientContextId, ctx);
                break;
            default:
                break;
        }
    }

    private void updateJobStats(JobId jobId, Stats stats) {
        final IJobManager jobManager =
                ((ClusterControllerService) appCtx.getServiceContext().getControllerService()).getJobManager();
        final JobRun run = jobManager.get(jobId);
        if (run == null || run.getStatus() != JobStatus.TERMINATED) {
            return;
        }
        final JobProfile jobProfile = run.getJobProfile();
        final Collection<JobletProfile> jobletProfiles = jobProfile.getJobletProfiles().values();
        long processedObjects = 0;
        for (JobletProfile jp : jobletProfiles) {
            final Collection<TaskProfile> jobletTasksProfile = jp.getTaskProfiles().values();
            for (TaskProfile tp : jobletTasksProfile) {
                processedObjects += tp.getStatsCollector().getAggregatedStats().getTupleCounter().get();
            }
        }
        stats.setProcessedObjects(processedObjects);
    }

    private void asyncCreateAndRunJob(IHyracksClientConnection hcc, IStatementCompiler compiler, IMetadataLocker locker,
            ResultDelivery resultDelivery, String clientContextId, IStatementExecutorContext ctx,
            ResultSetId resultSetId, MutableBoolean printed) {
        Mutable<JobId> jobId = new MutableObject<>(JobId.INVALID);
        try {
            createAndRunJob(hcc, jobFlags, jobId, compiler, locker, resultDelivery, id -> {
                final ResultHandle handle = new ResultHandle(id, resultSetId);
                ResultUtil.printStatus(sessionOutput, AbstractQueryApiServlet.ResultStatus.RUNNING);
                ResultUtil.printResultHandle(sessionOutput, handle);
                synchronized (printed) {
                    printed.setTrue();
                    printed.notify();
                }
            }, clientContextId, ctx);
        } catch (Exception e) {
            if (Objects.equals(JobId.INVALID, jobId.getValue())) {
                // compilation failed
                ResultUtil.printStatus(sessionOutput, AbstractQueryApiServlet.ResultStatus.FAILED);
                ResultUtil.printError(sessionOutput.out(), e);
            } else {
                GlobalConfig.ASTERIX_LOGGER.log(Level.ERROR,
                        resultDelivery.name() + " job with id " + jobId.getValue() + " " + "failed", e);
            }
        } finally {
            synchronized (printed) {
                if (printed.isFalse()) {
                    printed.setTrue();
                    printed.notify();
                }
            }
        }
    }

    private void runJob(IHyracksClientConnection hcc, JobSpecification jobSpec) throws Exception {
        runJob(hcc, jobSpec, jobFlags);
    }

    private static void runJob(IHyracksClientConnection hcc, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags)
            throws Exception {
        JobUtils.runJob(hcc, jobSpec, jobFlags, true);
    }

    private static void createAndRunJob(IHyracksClientConnection hcc, EnumSet<JobFlag> jobFlags, Mutable<JobId> jId,
            IStatementCompiler compiler, IMetadataLocker locker, ResultDelivery resultDelivery, IResultPrinter printer,
            String clientContextId, IStatementExecutorContext ctx) throws Exception {
        locker.lock();
        try {
            final JobSpecification jobSpec = compiler.compile();
            if (jobSpec == null) {
                return;
            }
            final JobId jobId = JobUtils.runJob(hcc, jobSpec, jobFlags, false);
            if (ctx != null && clientContextId != null) {
                ctx.put(clientContextId, jobId); // Adds the running job into the context.
            }
            if (jId != null) {
                jId.setValue(jobId);
            }
            if (ResultDelivery.ASYNC == resultDelivery) {
                printer.print(jobId);
                hcc.waitForCompletion(jobId);
            } else {
                hcc.waitForCompletion(jobId);
                printer.print(jobId);
            }
        } finally {
            locker.unlock();
            // No matter the job succeeds or fails, removes it into the context.
            if (ctx != null && clientContextId != null) {
                ctx.removeJobIdFromClientContextId(clientContextId);
            }
        }
    }

    protected void handleCreateNodeGroupStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        NodegroupDecl stmtCreateNodegroup = (NodegroupDecl) stmt;
        String ngName = stmtCreateNodegroup.getNodegroupName().getValue();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockManager.acquireNodeGroupWriteLock(metadataProvider.getLocks(), ngName);
        try {
            NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, ngName);
            if (ng != null) {
                if (!stmtCreateNodegroup.getIfNotExists()) {
                    throw new AlgebricksException("A nodegroup with this name " + ngName + " already exists.");
                }
            } else {
                List<Identifier> ncIdentifiers = stmtCreateNodegroup.getNodeControllerNames();
                List<String> ncNames = new ArrayList<>(ncIdentifiers.size());
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
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleExternalDatasetRefreshStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        RefreshExternalDatasetStatement stmtRefresh = (RefreshExternalDatasetStatement) stmt;
        String dataverseName = getActiveDataverse(stmtRefresh.getDataverseName());
        String datasetName = stmtRefresh.getDatasetName().getValue();
        TransactionState transactionState = TransactionState.COMMIT;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        JobSpecification spec = null;
        Dataset ds = null;
        List<ExternalFile> metadataFiles = null;
        List<ExternalFile> deletedFiles = null;
        List<ExternalFile> addedFiles = null;
        List<ExternalFile> appendedFiles = null;
        List<Index> indexes = null;
        Dataset transactionDataset = null;
        boolean lockAquired = false;
        boolean success = false;
        MetadataLockUtil.refreshDatasetBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + datasetName);
        try {
            ds = metadataProvider.findDataset(dataverseName, datasetName);
            // Dataset exists ?
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName);
            }
            // Dataset external ?
            if (ds.getDatasetType() != DatasetType.EXTERNAL) {
                throw new AlgebricksException(
                        "dataset " + datasetName + " in dataverse " + dataverseName + " is not an external dataset");
            }
            // Dataset has indexes ?
            indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
            if (indexes.isEmpty()) {
                throw new AlgebricksException("External dataset " + datasetName + " in dataverse " + dataverseName
                        + " doesn't have any index");
            }

            // Record transaction time
            Date txnTime = new Date();

            // refresh lock here
            ExternalDatasetsRegistry.INSTANCE.refreshBegin(ds);
            lockAquired = true;

            // Get internal files
            metadataFiles = MetadataManager.INSTANCE.getDatasetExternalFiles(mdTxnCtx, ds);
            deletedFiles = new ArrayList<>();
            addedFiles = new ArrayList<>();
            appendedFiles = new ArrayList<>();

            // Compute delta
            // Now we compare snapshot with external file system
            if (ExternalIndexingOperations.isDatasetUptodate(ds, metadataFiles, addedFiles, deletedFiles,
                    appendedFiles)) {
                ((ExternalDatasetDetails) ds.getDatasetDetails()).setRefreshTimestamp(txnTime);
                MetadataManager.INSTANCE.updateDataset(mdTxnCtx, ds);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                // latch will be released in the finally clause
                return;
            }

            // At this point, we know data has changed in the external file system, record
            // transaction in metadata and start
            transactionDataset = ExternalIndexingOperations.createTransactionDataset(ds);
            /*
             * Remove old dataset record and replace it with a new one
             */
            MetadataManager.INSTANCE.updateDataset(mdTxnCtx, transactionDataset);

            // Add delta files to the metadata
            for (ExternalFile file : addedFiles) {
                MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
            }
            for (ExternalFile file : appendedFiles) {
                MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
            }
            for (ExternalFile file : deletedFiles) {
                MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
            }

            // Create the files index update job
            spec = ExternalIndexingOperations.buildFilesIndexUpdateOp(ds, metadataFiles, addedFiles, appendedFiles,
                    metadataProvider);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            transactionState = TransactionState.BEGIN;

            // run the files update job
            runJob(hcc, spec);

            for (Index index : indexes) {
                if (!ExternalIndexingOperations.isFileIndex(index)) {
                    spec = ExternalIndexingOperations.buildIndexUpdateOp(ds, index, metadataFiles, addedFiles,
                            appendedFiles, metadataProvider);
                    // run the files update job
                    runJob(hcc, spec);
                }
            }

            // all index updates has completed successfully, record transaction state
            spec = ExternalIndexingOperations.buildCommitJob(ds, indexes, metadataProvider);

            // Aquire write latch again -> start a transaction and record the decision to
            // commit
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            bActiveTxn = true;
            ((ExternalDatasetDetails) transactionDataset.getDatasetDetails())
                    .setState(TransactionState.READY_TO_COMMIT);
            ((ExternalDatasetDetails) transactionDataset.getDatasetDetails()).setRefreshTimestamp(txnTime);
            MetadataManager.INSTANCE.updateDataset(mdTxnCtx, transactionDataset);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            transactionState = TransactionState.READY_TO_COMMIT;
            // We don't release the latch since this job is expected to be quick
            runJob(hcc, spec);
            // Start a new metadata transaction to record the final state of the transaction
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            bActiveTxn = true;

            for (ExternalFile file : metadataFiles) {
                if (file.getPendingOp() == ExternalFilePendingOp.DROP_OP) {
                    MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                } else if (file.getPendingOp() == ExternalFilePendingOp.NO_OP) {
                    Iterator<ExternalFile> iterator = appendedFiles.iterator();
                    while (iterator.hasNext()) {
                        ExternalFile appendedFile = iterator.next();
                        if (file.getFileName().equals(appendedFile.getFileName())) {
                            // delete existing file
                            MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                            // delete existing appended file
                            MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, appendedFile);
                            // add the original file with appended information
                            appendedFile.setFileNumber(file.getFileNumber());
                            appendedFile.setPendingOp(ExternalFilePendingOp.NO_OP);
                            MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, appendedFile);
                            iterator.remove();
                        }
                    }
                }
            }

            // remove the deleted files delta
            for (ExternalFile file : deletedFiles) {
                MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
            }

            // insert new files
            for (ExternalFile file : addedFiles) {
                MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                file.setPendingOp(ExternalFilePendingOp.NO_OP);
                MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
            }

            // mark the transaction as complete
            ((ExternalDatasetDetails) transactionDataset.getDatasetDetails()).setState(TransactionState.COMMIT);
            MetadataManager.INSTANCE.updateDataset(mdTxnCtx, transactionDataset);

            // commit metadata transaction
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            success = true;
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            if (transactionState == TransactionState.READY_TO_COMMIT) {
                throw new IllegalStateException("System is inconsistent state: commit of (" + dataverseName + "."
                        + datasetName + ") refresh couldn't carry out the commit phase", e);
            }
            if (transactionState == TransactionState.COMMIT) {
                // Nothing to do , everything should be clean
                throw e;
            }
            if (transactionState == TransactionState.BEGIN) {
                // transaction failed, need to do the following
                // clean NCs removing transaction components
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                spec = ExternalIndexingOperations.buildAbortOp(ds, indexes, metadataProvider);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                try {
                    runJob(hcc, spec);
                } catch (Exception e2) {
                    // This should never happen -- fix throw illegal
                    e.addSuppressed(e2);
                    throw new IllegalStateException("System is in inconsistent state. Failed to abort refresh", e);
                }
                // remove the delta of files
                // return the state of the dataset to committed
                try {
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    for (ExternalFile file : deletedFiles) {
                        MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                    }
                    for (ExternalFile file : addedFiles) {
                        MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                    }
                    for (ExternalFile file : appendedFiles) {
                        MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                    }
                    MetadataManager.INSTANCE.updateDataset(mdTxnCtx, ds);
                    // commit metadata transaction
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    abort(e, e2, mdTxnCtx);
                    e.addSuppressed(e2);
                    throw new IllegalStateException("System is in inconsistent state. Failed to drop delta files", e);
                }
            }
        } finally {
            if (lockAquired) {
                ExternalDatasetsRegistry.INSTANCE.refreshEnd(ds, success);
            }
            metadataProvider.getLocks().unlock();
        }
    }

    @Override
    public String getActiveDataverseName(String dataverse) {
        return (dataverse != null) ? dataverse : activeDataverse.getDataverseName();
    }

    @Override
    public ExecutionPlans getExecutionPlans() {
        return apiFramework.getExecutionPlans();
    }

    public String getActiveDataverse(Identifier dataverse) {
        return getActiveDataverseName(dataverse != null ? dataverse.getValue() : null);
    }

    /**
     * Abort the ongoing metadata transaction logging the error cause
     *
     * @param rootE
     * @param parentE
     * @param mdTxnCtx
     */
    public static void abort(Exception rootE, Exception parentE, MetadataTransactionContext mdTxnCtx) {
        boolean interrupted = Thread.interrupted();
        try {
            if (IS_DEBUG_MODE) {
                LOGGER.log(Level.ERROR, rootE.getMessage(), rootE);
            }
            if (mdTxnCtx != null) {
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            }
        } catch (Exception e2) {
            parentE.addSuppressed(e2);
            throw new IllegalStateException(rootE);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    protected void rewriteStatement(Statement stmt) throws CompilationException {
        IStatementRewriter rewriter = rewriterFactory.createStatementRewriter();
        rewriter.rewrite(stmt);
    }

    private void ensureNonPrimaryIndexDrop(Index index) throws AlgebricksException {
        if (index.isPrimaryIndex()) {
            throw new MetadataException(ErrorCode.CANNOT_DROP_INDEX, index.getIndexName(), index.getDatasetName());
        }
    }

    protected void afterCompile() {
        if (sessionOutput.config().is(SessionConfig.FORMAT_HTML)) {
            ExecutionPlansHtmlPrintUtil.print(sessionOutput.out(), getExecutionPlans());
        }
    }
}
