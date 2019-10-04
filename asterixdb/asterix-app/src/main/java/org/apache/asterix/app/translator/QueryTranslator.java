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
import org.apache.asterix.app.active.ActiveEntityEventsListener;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.active.FeedEventsListener;
import org.apache.asterix.app.result.ExecutionError;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.fields.ErrorsPrinter;
import org.apache.asterix.app.result.fields.ResultHandlePrinter;
import org.apache.asterix.app.result.fields.ResultsPrinter;
import org.apache.asterix.app.result.fields.StatusPrinter;
import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.api.IRequestTracker;
import org.apache.asterix.common.api.IResponsePrinter;
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
import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.exceptions.WarningCollector;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.common.utils.JobUtils.ProgressState;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
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
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.asterix.translator.AbstractLangTranslator;
import org.apache.asterix.translator.ClientRequest;
import org.apache.asterix.translator.CompiledStatements.CompiledDeleteStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledInsertStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledLoadFromFileStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledUpsertStatement;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.ExecutionPlansHtmlPrintUtil;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SchedulableClientRequest;
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
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.UnmanagedFileSplit;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
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
    protected final ILangCompilationProvider compilationProvider;
    protected final APIFramework apiFramework;
    protected final IRewriterFactory rewriterFactory;
    protected final ExecutorService executorService;
    protected final EnumSet<JobFlag> jobFlags = EnumSet.noneOf(JobFlag.class);
    protected final IMetadataLockManager lockManager;
    protected final IResponsePrinter responsePrinter;
    protected final WarningCollector warningCollector;

    public QueryTranslator(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compilationProvider, ExecutorService executorService,
            IResponsePrinter responsePrinter) {
        this.appCtx = appCtx;
        this.lockManager = appCtx.getMetadataLockManager();
        this.statements = statements;
        this.sessionOutput = output;
        this.sessionConfig = output.config();
        this.compilationProvider = compilationProvider;
        declaredFunctions = getDeclaredFunctions(statements);
        apiFramework = new APIFramework(compilationProvider);
        rewriterFactory = compilationProvider.getRewriterFactory();
        activeDataverse = MetadataBuiltinEntities.DEFAULT_DATAVERSE;
        this.executorService = executorService;
        this.responsePrinter = responsePrinter;
        this.warningCollector = new WarningCollector();
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
    public void compileAndExecute(IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        validateStatements(statements, requestParameters.isMultiStatement(),
                requestParameters.getStatementCategoryRestrictionMask());
        trackRequest(requestParameters);
        int resultSetIdCounter = 0;
        FileSplit outputFile = null;
        IAWriterFactory writerFactory = PrinterBasedWriterFactory.INSTANCE;
        IResultSerializerFactoryProvider resultSerializerFactoryProvider = ResultSerializerFactoryProvider.INSTANCE;
        String threadName = Thread.currentThread().getName();
        Thread.currentThread().setName(
                QueryTranslator.class.getSimpleName() + ":" + requestParameters.getRequestReference().getUuid());
        Map<String, String> config = new HashMap<>();
        final IResultSet resultSet = requestParameters.getResultSet();
        final ResultDelivery resultDelivery = requestParameters.getResultProperties().getDelivery();
        final long maxResultReads = requestParameters.getResultProperties().getMaxReads();
        final Stats stats = requestParameters.getStats();
        final ResultMetadata outMetadata = requestParameters.getOutMetadata();
        final Map<String, IAObject> stmtParams = requestParameters.getStatementParameters();
        warningCollector.setMaxWarnings(sessionConfig.getMaxWarnings());
        try {
            for (Statement stmt : statements) {
                if (sessionConfig.is(SessionConfig.FORMAT_HTML)) {
                    sessionOutput.out().println(ApiServlet.HTML_STATEMENT_SEPARATOR);
                }
                validateOperation(appCtx, activeDataverse, stmt);
                IStatementRewriter stmtRewriter = rewriterFactory.createStatementRewriter();
                rewriteStatement(stmt, stmtRewriter); // Rewrite the statement's AST.
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
                        handleCreateDataverseStatement(metadataProvider, stmt, requestParameters);
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
                        handleDataverseDropStatement(metadataProvider, stmt, hcc, requestParameters);
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
                        handleInsertUpsertStatement(metadataProvider, stmt, hcc, resultSet, resultDelivery, outMetadata,
                                stats, false, requestParameters, stmtParams, stmtRewriter);
                        break;
                    case DELETE:
                        handleDeleteStatement(metadataProvider, stmt, hcc, false, stmtParams, stmtRewriter);
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
                        if (stats.getType() == Stats.ProfileType.FULL) {
                            this.jobFlags.add(JobFlag.PROFILE_RUNTIME);
                        }
                        handleQuery(metadataProvider, (Query) stmt, hcc, resultSet, resultDelivery, outMetadata, stats,
                                requestParameters, stmtParams, stmtRewriter);
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
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, stmt.getSourceLocation(),
                                "Unexpected statement: " + stmt.getKind());
                }
            }
        } finally {
            // async queries are completed after their job completes
            if (ResultDelivery.ASYNC != resultDelivery) {
                appCtx.getRequestTracker().complete(requestParameters.getRequestReference().getUuid());
            }
            Thread.currentThread().setName(threadName);
        }
    }

    protected void handleSetStatement(Statement stmt, Map<String, String> config) throws CompilationException {
        SetStatement ss = (SetStatement) stmt;
        String pname = ss.getPropName();
        String pvalue = ss.getPropValue();
        if (pname.startsWith(APIFramework.PREFIX_INTERNAL_PARAMETERS)) {
            throw new CompilationException(ErrorCode.ILLEGAL_SET_PARAMETER, pname);
        }
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
        SourceLocation sourceLoc = dvd.getSourceLocation();
        String dvName = dvd.getDataverseName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockManager.acquireDataverseReadLock(metadataProvider.getLocks(), dvName);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
            if (dv == null) {
                throw new MetadataException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, dvName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return dv;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw new MetadataException(ErrorCode.METADATA_ERROR, e, sourceLoc, e.toString());
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleCreateDataverseStatement(MetadataProvider metadataProvider, Statement stmt,
            IRequestParameters requestParameters) throws Exception {
        CreateDataverseStatement stmtCreateDataverse = (CreateDataverseStatement) stmt;
        String dvName = stmtCreateDataverse.getDataverseName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockManager.acquireDataverseReadLock(metadataProvider.getLocks(), dvName);
        try {
            doCreateDataverseStatement(mdTxnCtx, metadataProvider, stmtCreateDataverse);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    @SuppressWarnings("squid:S00112")
    protected boolean doCreateDataverseStatement(MetadataTransactionContext mdTxnCtx, MetadataProvider metadataProvider,
            CreateDataverseStatement stmtCreateDataverse) throws Exception {
        String dvName = stmtCreateDataverse.getDataverseName().getValue();
        Dataverse dv = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dvName);
        if (dv != null) {
            if (stmtCreateDataverse.getIfNotExists()) {
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return false;
            } else {
                throw new CompilationException(ErrorCode.DATAVERSE_EXISTS, stmtCreateDataverse.getSourceLocation(),
                        dvName);
            }
        }
        MetadataManager.INSTANCE.addDataverse(metadataProvider.getMetadataTxnContext(),
                new Dataverse(dvName, stmtCreateDataverse.getFormat(), MetadataUtil.PENDING_NO_OP));
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        return true;
    }

    protected static void validateCompactionPolicy(String compactionPolicy,
            Map<String, String> compactionPolicyProperties, MetadataTransactionContext mdTxnCtx,
            boolean isExternalDataset, SourceLocation sourceLoc) throws CompilationException, Exception {
        CompactionPolicy compactionPolicyEntity = MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx,
                MetadataConstants.METADATA_DATAVERSE_NAME, compactionPolicy);
        if (compactionPolicyEntity == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Unknown compaction policy: " + compactionPolicy);
        }
        String compactionPolicyFactoryClassName = compactionPolicyEntity.getClassName();
        ILSMMergePolicyFactory mergePolicyFactory =
                (ILSMMergePolicyFactory) Class.forName(compactionPolicyFactoryClassName).newInstance();
        if (isExternalDataset && mergePolicyFactory.getName().compareTo("correlated-prefix") == 0) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "The correlated-prefix merge policy cannot be used with external dataset.");
        }
        if (compactionPolicyProperties == null) {
            if (mergePolicyFactory.getName().compareTo("no-merge") != 0) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Compaction policy properties are missing.");
            }
        } else {
            for (Map.Entry<String, String> entry : compactionPolicyProperties.entrySet()) {
                if (!mergePolicyFactory.getPropertiesNames().contains(entry.getKey())) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Invalid compaction policy property: " + entry.getKey());
                }
            }
            for (String p : mergePolicyFactory.getPropertiesNames()) {
                if (!compactionPolicyProperties.containsKey(p)) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Missing compaction policy property: " + p);
                }
            }
        }
    }

    public void handleCreateDatasetStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws CompilationException, Exception {
        MutableObject<ProgressState> progress = new MutableObject<>(ProgressState.NO_PROGRESS);
        DatasetDecl dd = (DatasetDecl) stmt;
        SourceLocation sourceLoc = dd.getSourceLocation();
        String dataverseName = getActiveDataverse(dd.getDataverse());
        String datasetName = dd.getName().getValue();
        String datasetFullyQualifiedName = dataverseName + "." + datasetName;
        DatasetType dsType = dd.getDatasetType();
        String itemTypeDataverseName = getActiveDataverse(dd.getItemTypeDataverse());
        String itemTypeName = dd.getItemTypeName().getValue();
        String itemTypeFullyQualifiedName = itemTypeDataverseName + "." + itemTypeName;
        String metaItemTypeDataverseName = null;
        String metaItemTypeName = null;
        String metaItemTypeFullyQualifiedName = null;
        Identifier metaItemTypeId = dd.getMetaItemTypeName();
        if (metaItemTypeId != null) {
            metaItemTypeName = metaItemTypeId.getValue();
            metaItemTypeDataverseName = getActiveDataverse(dd.getMetaItemTypeDataverse());
            metaItemTypeFullyQualifiedName = metaItemTypeDataverseName + "." + metaItemTypeName;
        }
        Identifier ngNameId = dd.getNodegroupName();
        String nodegroupName = ngNameId == null ? null : ngNameId.getValue();
        String compactionPolicy = dd.getCompactionPolicy();
        Map<String, String> compactionPolicyProperties = dd.getCompactionPolicyProperties();
        String compressionScheme = metadataProvider.getCompressionManager()
                .getDdlOrDefaultCompressionScheme(dd.getDatasetCompressionScheme());
        boolean defaultCompactionPolicy = compactionPolicy == null;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.createDatasetBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                itemTypeDataverseName, itemTypeFullyQualifiedName, metaItemTypeDataverseName,
                metaItemTypeFullyQualifiedName, nodegroupName, compactionPolicy, datasetFullyQualifiedName,
                defaultCompactionPolicy);
        Dataset dataset = null;
        try {
            IDatasetDetails datasetDetails;
            Dataset ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds != null) {
                if (dd.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.DATASET_EXISTS, sourceLoc, datasetName, dataverseName);
                }
            }
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    itemTypeDataverseName, itemTypeName);
            if (dt == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_TYPE, sourceLoc, itemTypeName);
            }
            String ngName = ngNameId != null ? ngNameId.getValue()
                    : configureNodegroupForDataset(appCtx, dd.getHints(), dataverseName, datasetName, metadataProvider,
                            sourceLoc);

            if (compactionPolicy == null) {
                compactionPolicy = StorageConstants.DEFAULT_COMPACTION_POLICY_NAME;
                compactionPolicyProperties = StorageConstants.DEFAULT_COMPACTION_POLICY_PROPERTIES;
            } else {
                validateCompactionPolicy(compactionPolicy, compactionPolicyProperties, mdTxnCtx, false, sourceLoc);
            }
            switch (dd.getDatasetType()) {
                case INTERNAL:
                    IAType itemType = dt.getDatatype();
                    if (itemType.getTypeTag() != ATypeTag.OBJECT) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                                "Dataset type has to be a record type.");
                    }

                    IAType metaItemType = null;
                    if (metaItemTypeDataverseName != null && metaItemTypeName != null) {
                        metaItemType = metadataProvider.findType(metaItemTypeDataverseName, metaItemTypeName);
                    }
                    if (metaItemType != null && metaItemType.getTypeTag() != ATypeTag.OBJECT) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                                "Dataset meta type has to be a record type.");
                    }
                    ARecordType metaRecType = (ARecordType) metaItemType;

                    List<List<String>> partitioningExprs =
                            ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getPartitioningExprs();
                    List<Integer> keySourceIndicators =
                            ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getKeySourceIndicators();
                    boolean autogenerated = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).isAutogenerated();
                    ARecordType aRecordType = (ARecordType) itemType;
                    List<IAType> partitioningTypes = ValidateUtil.validatePartitioningExpressions(aRecordType,
                            metaRecType, partitioningExprs, keySourceIndicators, autogenerated, sourceLoc);

                    List<String> filterField = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getFilterField();
                    if (filterField != null) {
                        ValidateUtil.validateFilterField(aRecordType, filterField, sourceLoc);
                    }
                    if (compactionPolicy == null && filterField != null) {
                        // If the dataset has a filter and the user didn't specify a merge
                        // policy, then we will pick the
                        // correlated-prefix as the default merge policy.
                        compactionPolicy = StorageConstants.DEFAULT_FILTERED_DATASET_COMPACTION_POLICY_NAME;
                        compactionPolicyProperties = StorageConstants.DEFAULT_COMPACTION_POLICY_PROPERTIES;
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
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Unknown dataset type " + dd.getDatasetType());
            }

            // #. initialize DatasetIdFactory if it is not initialized.
            if (!DatasetIdFactory.isInitialized()) {
                DatasetIdFactory.initialize(MetadataManager.INSTANCE.getMostRecentDatasetId());
            }

            // #. add a new dataset with PendingAddOp
            dataset = new Dataset(dataverseName, datasetName, itemTypeDataverseName, itemTypeName,
                    metaItemTypeDataverseName, metaItemTypeName, ngName, compactionPolicy, compactionPolicyProperties,
                    datasetDetails, dd.getHints(), dsType, DatasetIdFactory.generateDatasetId(),
                    MetadataUtil.PENDING_ADD_OP, compressionScheme);
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

    protected static void validateIfResourceIsActiveInFeed(ICcApplicationContext appCtx, Dataset dataset,
            SourceLocation sourceLoc) throws CompilationException {
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        IActiveEntityEventsListener[] listeners = activeEventHandler.getEventListeners();
        for (IActiveEntityEventsListener listener : listeners) {
            if (listener.isEntityUsingDataset(dataset) && listener.isActive()) {
                throw new CompilationException(ErrorCode.COMPILATION_CANT_DROP_ACTIVE_DATASET, sourceLoc,
                        dataset.getFullyQualifiedName(), listener.getEntityId().toString());
            }
        }
    }

    protected static String configureNodegroupForDataset(ICcApplicationContext appCtx, Map<String, String> hints,
            String dataverseName, String datasetName, MetadataProvider metadataProvider, SourceLocation sourceLoc)
            throws Exception {
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
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
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
        SourceLocation sourceLoc = stmtCreateIndex.getSourceLocation();
        String dataverseName = getActiveDataverse(stmtCreateIndex.getDataverseName());
        String datasetName = stmtCreateIndex.getDatasetName().getValue();
        String indexName = stmtCreateIndex.getIndexName().getValue();
        List<Integer> keySourceIndicators = stmtCreateIndex.getFieldSourceIndicators();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        String datasetFullyQualifiedName = dataverseName + "." + datasetName;
        boolean isSecondaryPrimary = stmtCreateIndex.getFieldExprs().isEmpty();
        Dataset ds = null;
        Index index = null;
        MetadataLockUtil.createIndexBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                datasetFullyQualifiedName);
        try {
            ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        dataverseName);
            }

            index = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName, indexName);
            if (index != null) {
                if (stmtCreateIndex.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.INDEX_EXISTS, sourceLoc, indexName);
                }
            }
            // find keySourceIndicators for secondary primary index since the parser isn't aware of them
            if (isSecondaryPrimary && ds.getDatasetType() == DatasetType.INTERNAL) {
                keySourceIndicators = ((InternalDatasetDetails) ds.getDatasetDetails()).getKeySourceIndicator();
            }
            // disable creating secondary primary index on an external dataset
            if (isSecondaryPrimary && ds.getDatasetType() == DatasetType.EXTERNAL) {
                throw new AsterixException(ErrorCode.CANNOT_CREATE_SEC_PRIMARY_IDX_ON_EXT_DATASET);
            }
            // disable creating an index on meta fields (fields with source indicator == 1 are meta fields)
            if (keySourceIndicators.stream().anyMatch(fieldSource -> fieldSource == 1) && !isSecondaryPrimary) {
                throw new AsterixException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Cannot create index on meta fields");
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
                        throw new AsterixException(ErrorCode.INDEX_ILLEGAL_NON_ENFORCED_TYPED, sourceLoc,
                                stmtCreateIndex.getIndexType());
                    }
                    if (stmtCreateIndex.isEnforced() && !fieldExpr.second.isUnknownable()) {
                        throw new AsterixException(ErrorCode.INDEX_ILLEGAL_ENFORCED_NON_OPTIONAL, sourceLoc,
                                String.valueOf(fieldExpr.first));
                    }
                    // don't allow creating an enforced index on a closed-type field, fields that
                    // are part of schema.
                    // get the field type, if it's not null, then the field is closed-type
                    if (stmtCreateIndex.isEnforced()
                            && subType.getSubFieldType(fieldExpr.first.subList(i, fieldExpr.first.size())) != null) {
                        throw new AsterixException(ErrorCode.INDEX_ILLEGAL_ENFORCED_ON_CLOSED_FIELD, sourceLoc,
                                String.valueOf(fieldExpr.first));
                    }
                    if (!isOpen) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Typed index on \""
                                + fieldExpr.first + "\" field could be created only for open datatype");
                    }
                    if (stmtCreateIndex.hasMetaField()) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                                "Typed open index can only be created on the record part");
                    }
                    Map<TypeSignature, IAType> typeMap =
                            TypeTranslator.computeTypes(mdTxnCtx, fieldExpr.second.getType(), indexName, dataverseName);
                    TypeSignature typeSignature = new TypeSignature(dataverseName, indexName);
                    fieldType = typeMap.get(typeSignature);
                    overridesFieldTypes = true;
                }
                if (fieldType == null) {
                    throw new CompilationException(ErrorCode.UNKNOWN_TYPE, sourceLoc, fieldExpr.second == null
                            ? String.valueOf(fieldExpr.first) : String.valueOf(fieldExpr.second));
                }

                // try to add the key & its source to the set of keys, if key couldn't be added,
                // there is a duplicate
                if (!indexKeysSet
                        .add(new Pair<>(fieldExpr.first, stmtCreateIndex.getFieldSourceIndicators().get(keyIndex)))) {
                    throw new AsterixException(ErrorCode.INDEX_ILLEGAL_REPETITIVE_FIELD, sourceLoc,
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
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                                "The keyword or ngram index -" + indexName + " cannot be created on the dataset -"
                                        + datasetName + " due to its variable-length primary key field - "
                                        + partitioningKey);
                    }

                }
            }

            Index newIndex = new Index(dataverseName, datasetName, indexName, stmtCreateIndex.getIndexType(),
                    indexFields, keySourceIndicators, indexFieldTypes, stmtCreateIndex.getGramLength(),
                    overridesFieldTypes, stmtCreateIndex.isEnforced(), false, MetadataUtil.PENDING_ADD_OP);
            doCreateIndex(hcc, metadataProvider, ds, newIndex, jobFlags, sourceLoc);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void doCreateIndex(IHyracksClientConnection hcc, MetadataProvider metadataProvider, Dataset ds,
            Index index, EnumSet<JobFlag> jobFlags, SourceLocation sourceLoc) throws Exception {
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
                validateDatasetState(metadataProvider, ds, sourceLoc);
            } else {
                // External dataset
                // Check if the dataset is indexible
                if (!ExternalIndexingOperations.isIndexible((ExternalDatasetDetails) ds.getDatasetDetails())) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "dataset using " + ((ExternalDatasetDetails) ds.getDatasetDetails()).getAdapter()
                                    + " Adapter can't be indexed");
                }
                // Check if the name of the index is valid
                if (!ExternalIndexingOperations.isValidIndexName(index.getDatasetName(), index.getIndexName())) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "external dataset index name is invalid");
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
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
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
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Cannot create index "
                                + index.getIndexName() + " , enforced index " + existingIndex.getIndexName()
                                + " on field \"" + StringUtils.join(index.getKeyFieldNames(), ',')
                                + "\" is already defined with type \"" + existingIndex.getKeyFieldTypes() + "\"");
                    }
                }
            }
            // #. add a new index with PendingAddOp
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);
            // #. prepare to create the index artifact in NC.
            spec = IndexUtil.buildSecondaryIndexCreationJobSpec(ds, index, metadataProvider, sourceLoc);
            if (spec == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Failed to create job spec for creating index '" + ds.getDatasetName() + "."
                                + index.getIndexName() + "'");
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
            spec = IndexUtil.buildSecondaryIndexLoadingJobSpec(ds, index, metadataProvider, sourceLoc);
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
                    JobSpecification jobSpec = IndexUtil.buildDropIndexJobSpec(index, metadataProvider, ds, sourceLoc);
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
        SourceLocation sourceLoc = stmtCreateType.getSourceLocation();
        String dataverseName = getActiveDataverse(stmtCreateType.getDataverseName());
        String typeName = stmtCreateType.getIdent().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.createTypeBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + typeName);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, dataverseName);
            }
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, typeName);
            if (dt != null) {
                if (!stmtCreateType.getIfNotExists()) {
                    throw new CompilationException(ErrorCode.TYPE_EXISTS, sourceLoc, typeName);
                }
            } else {
                if (BuiltinTypeMap.getBuiltinType(typeName) != null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Cannot redefine builtin type " + typeName + ".");
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
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        DataverseDropStatement stmtDelete = (DataverseDropStatement) stmt;
        SourceLocation sourceLoc = stmtDelete.getSourceLocation();
        String dataverseName = stmtDelete.getDataverseName().getValue();
        if (dataverseName.equals(MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME + " dataverse can't be dropped");
        }
        lockManager.acquireDataverseWriteLock(metadataProvider.getLocks(), dataverseName);
        try {
            doDropDataverse(stmtDelete, sourceLoc, metadataProvider, hcc);
        } finally {
            metadataProvider.getLocks().unlock();
            ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
        }
    }

    protected boolean doDropDataverse(DataverseDropStatement stmtDelete, SourceLocation sourceLoc,
            MetadataProvider metadataProvider, IHyracksClientConnection hcc) throws Exception {
        String dataverseName = stmtDelete.getDataverseName().getValue();
        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
            if (dv == null) {
                if (stmtDelete.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, dataverseName);
                }
            }
            // # check whether any function in current dataverse is being used by others
            List<Function> functionsInDataverse =
                    MetadataManager.INSTANCE.getDataverseFunctions(mdTxnCtx, dataverseName);
            for (Function function : functionsInDataverse) {
                if (isFunctionUsed(mdTxnCtx, function.getSignature(), dataverseName)) {
                    throw new MetadataException(ErrorCode.METADATA_DROP_FUCTION_IN_USE, sourceLoc,
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
                    doDropFeed(hcc, metadataProvider, feedListener.getFeed(), sourceLoc);
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
                        jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, dataset, sourceLoc));
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
                            jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(indexes.get(k), metadataProvider, dataset,
                                    sourceLoc));
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

            if (activeDataverse.getDataverseName().equals(dataverseName)) {
                activeDataverse = MetadataBuiltinEntities.DEFAULT_DATAVERSE;
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                if (activeDataverse.getDataverseName().equals(dataverseName)) {
                    activeDataverse = MetadataBuiltinEntities.DEFAULT_DATAVERSE;
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
        }
    }

    public void handleDatasetDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        DropDatasetStatement stmtDelete = (DropDatasetStatement) stmt;
        SourceLocation sourceLoc = stmtDelete.getSourceLocation();
        String dataverseName = getActiveDataverse(stmtDelete.getDataverseName());
        String datasetName = stmtDelete.getDatasetName().getValue();
        MetadataLockUtil.dropDatasetBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + datasetName);
        try {
            doDropDataset(dataverseName, datasetName, metadataProvider, stmtDelete.getIfExists(), hcc, true, sourceLoc);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    public void doDropDataset(String dataverseName, String datasetName, MetadataProvider metadataProvider,
            boolean ifExists, IHyracksClientConnection hcc, boolean dropCorrespondingNodeGroup,
            SourceLocation sourceLoc) throws Exception {
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
                    throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                            dataverseName);
                }
            }
            validateDatasetState(metadataProvider, ds, sourceLoc);
            ds.drop(metadataProvider, mdTxnCtx, jobsToExecute, bActiveTxn, progress, hcc, dropCorrespondingNodeGroup,
                    sourceLoc);
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
        SourceLocation sourceLoc = stmtIndexDrop.getSourceLocation();
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
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        dataverseName);
            }
            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                Index index = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                if (index == null) {
                    if (stmtIndexDrop.getIfExists()) {
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                        return;
                    } else {
                        throw new CompilationException(ErrorCode.UNKNOWN_INDEX, sourceLoc, indexName);
                    }
                }
                ensureNonPrimaryIndexDrop(index, sourceLoc);
                validateDatasetState(metadataProvider, ds, sourceLoc);
                // #. prepare a job to drop the index in NC.
                jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, ds, sourceLoc));

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
                        throw new CompilationException(ErrorCode.UNKNOWN_INDEX, sourceLoc, indexName);
                    }
                } else if (ExternalIndexingOperations.isFileIndex(index)) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Dropping a dataset's files index is not allowed.");
                }
                ensureNonPrimaryIndexDrop(index, sourceLoc);
                // #. prepare a job to drop the index in NC.
                jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, ds, sourceLoc));
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
        SourceLocation sourceLoc = stmtTypeDrop.getSourceLocation();
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
                    throw new CompilationException(ErrorCode.UNKNOWN_TYPE, sourceLoc, typeName);
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
        SourceLocation sourceLoc = stmtDelete.getSourceLocation();
        String nodegroupName = stmtDelete.getNodeGroupName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockManager.acquireNodeGroupWriteLock(metadataProvider.getLocks(), nodegroupName);
        try {
            NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodegroupName);
            if (ng == null) {
                if (!stmtDelete.getIfExists()) {
                    throw new CompilationException(ErrorCode.UNKNOWN_NODEGROUP, sourceLoc, nodegroupName);
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
        SourceLocation sourceLoc = cfs.getSourceLocation();
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
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, dataverse);
            }

            //Check whether the function is use-able
            metadataProvider.setDefaultDataverse(dv);
            Query wrappedQuery = new Query(false);
            wrappedQuery.setSourceLocation(sourceLoc);
            wrappedQuery.setBody(cfs.getFunctionBodyExpression());
            wrappedQuery.setTopLevel(false);
            List<VarIdentifier> paramVars = new ArrayList<>();
            for (String v : cfs.getParamList()) {
                paramVars.add(new VarIdentifier(v));
            }
            apiFramework.reWriteQuery(declaredFunctions, metadataProvider, wrappedQuery, sessionOutput, false,
                    paramVars, warningCollector);

            List<List<List<String>>> dependencies = FunctionUtil.getFunctionDependencies(
                    rewriterFactory.createQueryRewriter(), cfs.getFunctionBodyExpression(), metadataProvider);

            Function function = new Function(signature, cfs.getParamList(), Function.RETURNTYPE_VOID,
                    cfs.getFunctionBody(), getFunctionLanguage(), FunctionKind.SCALAR.toString(), dependencies);
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

    private String getFunctionLanguage() {
        switch (compilationProvider.getLanguage()) {
            case SQLPP:
                return Function.LANGUAGE_SQLPP;
            case AQL:
                return Function.LANGUAGE_AQL;
            default:
                throw new IllegalStateException(String.valueOf(compilationProvider.getLanguage()));
        }
    }

    protected boolean isFunctionUsed(MetadataTransactionContext ctx, FunctionSignature signature,
            String currentDataverse) throws AlgebricksException {
        List<Dataverse> allDataverses = MetadataManager.INSTANCE.getDataverses(ctx);
        for (Dataverse dataverse : allDataverses) {
            if (dataverse.getDataverseName().equals(currentDataverse)) {
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
        SourceLocation sourceLoc = stmtDropFunction.getSourceLocation();
        FunctionSignature signature = stmtDropFunction.getFunctionSignature();
        signature.setNamespace(getActiveDataverseName(signature.getNamespace()));
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockUtil.functionStatementBegin(lockManager, metadataProvider.getLocks(), signature.getNamespace(),
                signature.getNamespace() + "." + signature.getName());
        try {
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
            // If function == null && stmtDropFunction.getIfExists() == true, commit txn directly.
            if (function == null && !stmtDropFunction.getIfExists()) {
                throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, sourceLoc, signature);
            } else if (function != null) {
                if (isFunctionUsed(mdTxnCtx, signature, null)) {
                    throw new MetadataException(ErrorCode.METADATA_DROP_FUCTION_IN_USE, sourceLoc, signature);
                } else {
                    MetadataManager.INSTANCE.dropFunction(mdTxnCtx, signature);
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
            cls.setSourceLocation(stmt.getSourceLocation());
            JobSpecification spec = apiFramework.compileQuery(hcc, metadataProvider, null, 0, null, sessionOutput, cls,
                    null, responsePrinter, warningCollector);
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
            IHyracksClientConnection hcc, IResultSet resultSet, ResultDelivery resultDelivery,
            ResultMetadata outMetadata, Stats stats, boolean compileOnly, IRequestParameters requestParameters,
            Map<String, IAObject> stmtParams, IStatementRewriter stmtRewriter) throws Exception {
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
                final JobSpecification jobSpec =
                        rewriteCompileInsertUpsert(hcc, metadataProvider, stmtInsertUpsert, stmtParams, stmtRewriter);
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
            deliverResult(hcc, resultSet, compiler, metadataProvider, locker, resultDelivery, outMetadata, stats,
                    requestParameters, false);
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
            IHyracksClientConnection hcc, boolean compileOnly, Map<String, IAObject> stmtParams,
            IStatementRewriter stmtRewriter) throws Exception {
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
            clfrqs.setSourceLocation(stmt.getSourceLocation());
            JobSpecification jobSpec =
                    rewriteCompileQuery(hcc, metadataProvider, clfrqs.getQuery(), clfrqs, stmtParams, stmtRewriter);
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
            MetadataProvider metadataProvider, Query query, ICompiledDmlStatement stmt,
            Map<String, IAObject> stmtParams, IStatementRewriter stmtRewriter)
            throws AlgebricksException, ACIDException {

        Map<VarIdentifier, IAObject> externalVars = createExternalVariables(stmtParams, stmtRewriter);

        // Query Rewriting (happens under the same ongoing metadata transaction)
        Pair<IReturningStatement, Integer> rewrittenResult = apiFramework.reWriteQuery(declaredFunctions,
                metadataProvider, query, sessionOutput, true, externalVars.keySet(), warningCollector);

        // Query Compilation (happens under the same ongoing metadata transaction)
        return apiFramework.compileQuery(clusterInfoCollector, metadataProvider, (Query) rewrittenResult.first,
                rewrittenResult.second, stmt == null ? null : stmt.getDatasetName(), sessionOutput, stmt, externalVars,
                responsePrinter, warningCollector);
    }

    private JobSpecification rewriteCompileInsertUpsert(IClusterInfoCollector clusterInfoCollector,
            MetadataProvider metadataProvider, InsertStatement insertUpsert, Map<String, IAObject> stmtParams,
            IStatementRewriter stmtRewriter) throws AlgebricksException, ACIDException {
        SourceLocation sourceLoc = insertUpsert.getSourceLocation();

        Map<VarIdentifier, IAObject> externalVars = createExternalVariables(stmtParams, stmtRewriter);

        // Insert/upsert statement rewriting (happens under the same ongoing metadata transaction)
        Pair<IReturningStatement, Integer> rewrittenResult = apiFramework.reWriteQuery(declaredFunctions,
                metadataProvider, insertUpsert, sessionOutput, true, externalVars.keySet(), warningCollector);

        InsertStatement rewrittenInsertUpsert = (InsertStatement) rewrittenResult.first;
        String dataverseName = getActiveDataverse(rewrittenInsertUpsert.getDataverseName());
        String datasetName = rewrittenInsertUpsert.getDatasetName().getValue();
        CompiledInsertStatement clfrqs;
        switch (insertUpsert.getKind()) {
            case INSERT:
                clfrqs = new CompiledInsertStatement(dataverseName, datasetName, rewrittenInsertUpsert.getQuery(),
                        rewrittenInsertUpsert.getVarCounter(), rewrittenInsertUpsert.getVar(),
                        rewrittenInsertUpsert.getReturnExpression());
                clfrqs.setSourceLocation(insertUpsert.getSourceLocation());
                break;
            case UPSERT:
                clfrqs = new CompiledUpsertStatement(dataverseName, datasetName, rewrittenInsertUpsert.getQuery(),
                        rewrittenInsertUpsert.getVarCounter(), rewrittenInsertUpsert.getVar(),
                        rewrittenInsertUpsert.getReturnExpression());
                clfrqs.setSourceLocation(insertUpsert.getSourceLocation());
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Unsupported statement type " + rewrittenInsertUpsert.getKind());
        }
        // Insert/upsert statement compilation (happens under the same ongoing metadata
        // transaction)
        return apiFramework.compileQuery(clusterInfoCollector, metadataProvider, rewrittenInsertUpsert.getQuery(),
                rewrittenResult.second, datasetName, sessionOutput, clfrqs, externalVars, responsePrinter,
                warningCollector);
    }

    protected void handleCreateFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        CreateFeedStatement cfs = (CreateFeedStatement) stmt;
        SourceLocation sourceLoc = cfs.getSourceLocation();
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
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "A feed with this name " + feedName + " already exists.");
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
        SourceLocation sourceLoc = cfps.getSourceLocation();
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
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "A policy with this name " + policy + " already exists.");
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
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                                "Unknown policy " + cfps.getSourcePolicyName());
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
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Unable to read policy file" + cfps.getSourcePolicyFile(), e);
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
        SourceLocation sourceLoc = stmtFeedDrop.getSourceLocation();
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
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "There is no feed with this name " + feedName + ".");
                }
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;
            }
            doDropFeed(hcc, metadataProvider, feed, sourceLoc);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void doDropFeed(IHyracksClientConnection hcc, MetadataProvider metadataProvider, Feed feed,
            SourceLocation sourceLoc) throws Exception {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        EntityId feedId = feed.getFeedId();
        ActiveNotificationHandler activeNotificationHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        ActiveEntityEventsListener listener =
                (ActiveEntityEventsListener) activeNotificationHandler.getListener(feedId);
        if (listener != null && listener.getState() != ActivityState.STOPPED) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Feed " + feedId
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
        SourceLocation sourceLoc = stmtFeedPolicyDrop.getSourceLocation();
        String dataverseName = getActiveDataverse(stmtFeedPolicyDrop.getDataverseName());
        String policyName = stmtFeedPolicyDrop.getPolicyName().getValue();
        MetadataLockUtil.dropFeedPolicyBegin(lockManager, metadataProvider.getLocks(), dataverseName,
                dataverseName + "." + policyName);
        try {
            FeedPolicyEntity feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(mdTxnCtx, dataverseName, policyName);
            if (feedPolicy == null) {
                if (!stmtFeedPolicyDrop.getIfExists()) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Unknown policy " + policyName + " in dataverse " + dataverseName);
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
        SourceLocation sourceLoc = sfs.getSourceLocation();
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
                throw new CompilationException(ErrorCode.FEED_START_FEED_WITHOUT_CONNECTION, sourceLoc, feedName);
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
        SourceLocation sourceLoc = sfst.getSourceLocation();
        String dataverseName = getActiveDataverse(sfst.getDataverseName());
        String feedName = sfst.getFeedName().getValue();
        EntityId entityId = new EntityId(Feed.EXTENSION_NAME, dataverseName, feedName);
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        // Obtain runtime info from ActiveListener
        ActiveEntityEventsListener listener = (ActiveEntityEventsListener) activeEventHandler.getListener(entityId);
        if (listener == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Feed " + feedName + " is not started.");
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
        SourceLocation sourceLoc = cfs.getSourceLocation();
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
                throw new CompilationException(ErrorCode.FEED_CHANGE_FEED_CONNECTIVITY_ON_ALIVE_FEED, sourceLoc,
                        feedName);
            }
            ARecordType outputType = FeedMetadataUtil.getOutputType(feed,
                    feed.getConfiguration().get(ExternalDataConstants.KEY_TYPE_NAME));
            List<FunctionSignature> appliedFunctions = cfs.getAppliedFunctions();
            for (FunctionSignature func : appliedFunctions) {
                if (MetadataManager.INSTANCE.getFunction(mdTxnCtx, func) == null) {
                    throw new CompilationException(ErrorCode.FEED_CONNECT_FEED_APPLIED_INVALID_FUNCTION, sourceLoc,
                            func.getName());
                }
            }
            fc = MetadataManager.INSTANCE.getFeedConnection(metadataProvider.getMetadataTxnContext(), dataverseName,
                    feedName, datasetName);
            if (fc != null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Feed" + feedName + " is already connected dataset " + datasetName);
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
        SourceLocation sourceLoc = cfs.getSourceLocation();
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
                throw new CompilationException(ErrorCode.FEED_CHANGE_FEED_CONNECTIVITY_ON_ALIVE_FEED, sourceLoc,
                        feedName);
            }
            FeedMetadataUtil.validateIfDatasetExists(metadataProvider, dataverseName, cfs.getDatasetName().getValue());
            FeedMetadataUtil.validateIfFeedExists(dataverseName, cfs.getFeedName().getValue(), mdTxnCtx);
            FeedConnection fc = MetadataManager.INSTANCE.getFeedConnection(metadataProvider.getMetadataTxnContext(),
                    dataverseName, feedName, datasetName);
            Dataset ds = metadataProvider.findDataset(dataverseName, datasetName);
            if (ds == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        dataverseName);
            }
            if (fc == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Feed " + feedName
                        + " is currently not connected to " + cfs.getDatasetName().getValue() + ". Invalid operation!");
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
        SourceLocation sourceLoc = compactStatement.getSourceLocation();
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
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        dataverseName);
            }
            // Prepare jobs to compact the datatset and its indexes
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
            if (indexes.isEmpty()) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Cannot compact the extrenal dataset " + datasetName + " because it has no indexes");
            }
            Dataverse dataverse =
                    MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dataverseName);
            jobsToExecute.add(DatasetUtil.compactDatasetJobSpec(dataverse, datasetName, metadataProvider));

            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                for (Index index : indexes) {
                    if (index.isSecondaryIndex()) {
                        jobsToExecute.add(
                                IndexUtil.buildSecondaryIndexCompactJobSpec(ds, index, metadataProvider, sourceLoc));
                    }
                }
            } else {
                prepareCompactJobsForExternalDataset(indexes, ds, jobsToExecute, metadataProvider, sourceLoc);
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
            List<JobSpecification> jobsToExecute, MetadataProvider metadataProvider, SourceLocation sourceLoc)
            throws AlgebricksException {
        for (int j = 0; j < indexes.size(); j++) {
            jobsToExecute
                    .add(IndexUtil.buildSecondaryIndexCompactJobSpec(ds, indexes.get(j), metadataProvider, sourceLoc));

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
            IResultSet resultSet, ResultDelivery resultDelivery, ResultMetadata outMetadata, Stats stats,
            IRequestParameters requestParameters, Map<String, IAObject> stmtParams, IStatementRewriter stmtRewriter)
            throws Exception {
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
                final JobSpecification jobSpec =
                        rewriteCompileQuery(hcc, metadataProvider, query, null, stmtParams, stmtRewriter);
                // update stats with count of compile-time warnings. needs to be adapted for multi-statement.
                stats.updateTotalWarningsCount(warningCollector.getTotalWarningsCount());
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
        deliverResult(hcc, resultSet, compiler, metadataProvider, locker, resultDelivery, outMetadata, stats,
                requestParameters, true);
    }

    private void deliverResult(IHyracksClientConnection hcc, IResultSet resultSet, IStatementCompiler compiler,
            MetadataProvider metadataProvider, IMetadataLocker locker, ResultDelivery resultDelivery,
            ResultMetadata outMetadata, Stats stats, IRequestParameters requestParameters, boolean cancellable)
            throws Exception {
        final ResultSetId resultSetId = metadataProvider.getResultSetId();
        switch (resultDelivery) {
            case ASYNC:
                MutableBoolean printed = new MutableBoolean(false);
                executorService.submit(() -> asyncCreateAndRunJob(hcc, compiler, locker, resultDelivery,
                        requestParameters, cancellable, resultSetId, printed, metadataProvider));
                synchronized (printed) {
                    while (!printed.booleanValue()) {
                        printed.wait();
                    }
                }
                break;
            case IMMEDIATE:
                createAndRunJob(hcc, jobFlags, null, compiler, locker, resultDelivery, id -> {
                    final ResultReader resultReader = new ResultReader(resultSet, id, resultSetId);
                    updateJobStats(id, stats, metadataProvider.getResultSetId());
                    responsePrinter.addResultPrinter(new ResultsPrinter(appCtx, resultReader,
                            metadataProvider.findOutputRecordType(), stats, sessionOutput));
                    responsePrinter.printResults();
                }, requestParameters, cancellable, appCtx, metadataProvider);
                break;
            case DEFERRED:
                createAndRunJob(hcc, jobFlags, null, compiler, locker, resultDelivery, id -> {
                    updateJobStats(id, stats, metadataProvider.getResultSetId());
                    responsePrinter.addResultPrinter(
                            new ResultHandlePrinter(sessionOutput, new ResultHandle(id, resultSetId)));
                    responsePrinter.printResults();
                    if (outMetadata != null) {
                        outMetadata.getResultSets()
                                .add(Triple.of(id, resultSetId, metadataProvider.findOutputRecordType()));
                    }
                }, requestParameters, cancellable, appCtx, metadataProvider);
                break;
            default:
                break;
        }
    }

    private void updateJobStats(JobId jobId, Stats stats, ResultSetId rsId) throws HyracksDataException {
        final ClusterControllerService controllerService =
                (ClusterControllerService) appCtx.getServiceContext().getControllerService();
        org.apache.asterix.api.common.ResultMetadata resultMetadata =
                (org.apache.asterix.api.common.ResultMetadata) controllerService.getResultDirectoryService()
                        .getResultMetadata(jobId, rsId);
        stats.setProcessedObjects(resultMetadata.getProcessedObjects());
        if (jobFlags.contains(JobFlag.PROFILE_RUNTIME)) {
            stats.setJobProfile(resultMetadata.getJobProfile());
        }
        stats.setDiskIoCount(resultMetadata.getDiskIoCount());
        stats.updateTotalWarningsCount(resultMetadata.getTotalWarningsCount());
        WarningUtil.mergeWarnings(resultMetadata.getWarnings(), warningCollector);
    }

    private void asyncCreateAndRunJob(IHyracksClientConnection hcc, IStatementCompiler compiler, IMetadataLocker locker,
            ResultDelivery resultDelivery, IRequestParameters requestParameters, boolean cancellable,
            ResultSetId resultSetId, MutableBoolean printed, MetadataProvider metadataProvider) {
        Mutable<JobId> jobId = new MutableObject<>(JobId.INVALID);
        try {
            createAndRunJob(hcc, jobFlags, jobId, compiler, locker, resultDelivery, id -> {
                final ResultHandle handle = new ResultHandle(id, resultSetId);
                responsePrinter.addResultPrinter(new StatusPrinter(AbstractQueryApiServlet.ResultStatus.RUNNING));
                responsePrinter.addResultPrinter(new ResultHandlePrinter(sessionOutput, handle));
                responsePrinter.printResults();
                synchronized (printed) {
                    printed.setTrue();
                    printed.notify();
                }
            }, requestParameters, cancellable, appCtx, metadataProvider);
        } catch (Exception e) {
            if (Objects.equals(JobId.INVALID, jobId.getValue())) {
                // compilation failed
                responsePrinter.addResultPrinter(new StatusPrinter(AbstractQueryApiServlet.ResultStatus.FAILED));
                responsePrinter.addResultPrinter(new ErrorsPrinter(Collections.singletonList(ExecutionError.of(e))));
                try {
                    responsePrinter.printResults();
                } catch (HyracksDataException ex) {
                    LOGGER.error("failed to print result", ex);
                }
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
            IRequestParameters requestParameters, boolean cancellable, ICcApplicationContext appCtx,
            MetadataProvider metadataProvider) throws Exception {
        final IRequestTracker requestTracker = appCtx.getRequestTracker();
        final ClientRequest clientRequest =
                (ClientRequest) requestTracker.get(requestParameters.getRequestReference().getUuid());
        locker.lock();
        try {
            final JobSpecification jobSpec = compiler.compile();
            if (jobSpec == null) {
                return;
            }
            if (cancellable) {
                clientRequest.markCancellable();
            }
            final SchedulableClientRequest schedulableRequest =
                    SchedulableClientRequest.of(clientRequest, requestParameters, metadataProvider, jobSpec);
            appCtx.getReceptionist().ensureSchedulable(schedulableRequest);
            final JobId jobId = JobUtils.runJob(hcc, jobSpec, jobFlags, false);
            clientRequest.setJobId(jobId);
            if (jId != null) {
                jId.setValue(jobId);
            }
            if (ResultDelivery.ASYNC == resultDelivery) {
                printer.print(jobId);
                hcc.waitForCompletion(jobId);
            } else {
                hcc.waitForCompletion(jobId);
                ensureNotCancelled(clientRequest);
                printer.print(jobId);
            }
        } catch (Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new RuntimeDataException(ErrorCode.REQUEST_CANCELLED, clientRequest.getId());
            }
            throw e;
        } finally {
            // complete async jobs after their job completes
            if (ResultDelivery.ASYNC == resultDelivery) {
                requestTracker.complete(clientRequest.getId());
            }
            locker.unlock();
        }
    }

    protected void handleCreateNodeGroupStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        NodegroupDecl stmtCreateNodegroup = (NodegroupDecl) stmt;
        SourceLocation sourceLoc = stmtCreateNodegroup.getSourceLocation();
        String ngName = stmtCreateNodegroup.getNodegroupName().getValue();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockManager.acquireNodeGroupWriteLock(metadataProvider.getLocks(), ngName);
        try {
            NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, ngName);
            if (ng != null) {
                if (!stmtCreateNodegroup.getIfNotExists()) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "A nodegroup with this name " + ngName + " already exists.");
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
        SourceLocation sourceLoc = stmtRefresh.getSourceLocation();
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
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        dataverseName);
            }
            // Dataset external ?
            if (ds.getDatasetType() != DatasetType.EXTERNAL) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "dataset " + datasetName + " in dataverse " + dataverseName + " is not an external dataset");
            }
            // Dataset has indexes ?
            indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
            if (indexes.isEmpty()) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "External dataset " + datasetName
                        + " in dataverse " + dataverseName + " doesn't have any index");
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
                            appendedFiles, metadataProvider, sourceLoc);
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
        return (dataverse != null && !dataverse.isEmpty()) ? dataverse : activeDataverse.getDataverseName();
    }

    @Override
    public ExecutionPlans getExecutionPlans() {
        return apiFramework.getExecutionPlans();
    }

    @Override
    public IResponsePrinter getResponsePrinter() {
        return responsePrinter;
    }

    public String getActiveDataverse(Identifier dataverse) {
        return getActiveDataverseName(dataverse != null ? dataverse.getValue() : null);
    }

    @Override
    public void getWarnings(Collection<? super Warning> outWarnings, long maxWarnings) {
        warningCollector.getWarnings(outWarnings, maxWarnings);
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

    protected void rewriteStatement(Statement stmt, IStatementRewriter rewriter) throws CompilationException {
        rewriter.rewrite(stmt);
    }

    private void ensureNonPrimaryIndexDrop(Index index, SourceLocation sourceLoc) throws AlgebricksException {
        if (index.isPrimaryIndex()) {
            throw new MetadataException(ErrorCode.CANNOT_DROP_INDEX, sourceLoc, index.getIndexName(),
                    index.getDatasetName());
        }
    }

    protected void afterCompile() {
        if (sessionOutput.config().is(SessionConfig.FORMAT_HTML)) {
            ExecutionPlansHtmlPrintUtil.print(sessionOutput.out(), getExecutionPlans());
        }
    }

    protected void trackRequest(IRequestParameters requestParameters) throws HyracksDataException {
        final IClientRequest clientRequest = appCtx.getReceptionist().requestReceived(requestParameters);
        appCtx.getRequestTracker().track(clientRequest);
    }

    public static void validateStatements(List<Statement> statements, boolean allowMultiStatement,
            int stmtCategoryRestrictionMask) throws CompilationException {
        if (!allowMultiStatement) {
            if (statements.stream().filter(QueryTranslator::isNotAllowedMultiStatement).count() > 1) {
                throw new CompilationException(ErrorCode.UNSUPPORTED_MULTIPLE_STATEMENTS);
            }
        }
        if (stmtCategoryRestrictionMask != RequestParameters.NO_CATEGORY_RESTRICTION_MASK) {
            for (Statement stmt : statements) {
                if (isNotAllowedStatementCategory(stmt, stmtCategoryRestrictionMask)) {
                    throw new CompilationException(ErrorCode.PROHIBITED_STATEMENT_CATEGORY, stmt.getSourceLocation(),
                            stmt.getKind());
                }
            }
        }
    }

    protected static boolean isNotAllowedMultiStatement(Statement statement) {
        switch (statement.getKind()) {
            case DATAVERSE_DECL:
            case FUNCTION_DECL:
            case SET:
            case WRITE:
                return false;
            default:
                return true;
        }
    }

    private static boolean isNotAllowedStatementCategory(Statement statement, int categoryRestrictionMask) {
        int category = statement.getCategory();
        if (category <= 0) {
            throw new IllegalArgumentException(String.valueOf(category));
        }
        int i = category & categoryRestrictionMask;
        return i == 0;
    }

    private Map<VarIdentifier, IAObject> createExternalVariables(Map<String, IAObject> stmtParams,
            IStatementRewriter stmtRewriter) {
        if (stmtParams == null || stmtParams.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<VarIdentifier, IAObject> m = new HashMap<>();
        for (Map.Entry<String, IAObject> me : stmtParams.entrySet()) {
            String paramName = me.getKey();
            String extVarName = stmtRewriter.toExternalVariableName(paramName);
            if (extVarName != null) {
                m.put(new VarIdentifier(extVarName), me.getValue());
            }
        }
        return m;
    }

    protected void validateDatasetState(MetadataProvider metadataProvider, Dataset dataset, SourceLocation sourceLoc)
            throws Exception {
        validateIfResourceIsActiveInFeed(metadataProvider.getApplicationContext(), dataset, sourceLoc);
    }

    private static void ensureNotCancelled(ClientRequest clientRequest) throws RuntimeDataException {
        if (clientRequest.isCancelled()) {
            throw new RuntimeDataException(ErrorCode.REQUEST_CANCELLED, clientRequest.getId());
        }
    }
}
