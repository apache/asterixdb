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

import static org.apache.asterix.common.api.IIdentifierMapper.Modifier.PLURAL;
import static org.apache.asterix.common.utils.IdentifierUtil.dataset;
import static org.apache.asterix.common.utils.IdentifierUtil.dataverse;
import static org.apache.asterix.lang.common.statement.CreateFullTextFilterStatement.FIELD_TYPE_STOPWORDS;

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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

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
import org.apache.asterix.app.cc.GlobalTxManager;
import org.apache.asterix.app.external.ExternalLibraryJobUtils;
import org.apache.asterix.app.result.ExecutionError;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.fields.ErrorsPrinter;
import org.apache.asterix.app.result.fields.ResultHandlePrinter;
import org.apache.asterix.app.result.fields.ResultsPrinter;
import org.apache.asterix.app.result.fields.StatusPrinter;
import org.apache.asterix.column.validation.ColumnPropertiesValidationUtil;
import org.apache.asterix.column.validation.ColumnSupportedTypesValidator;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.api.IRequestTracker;
import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.cluster.IGlobalTxManager;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.DatasetConfig.TransactionState;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.exceptions.WarningCollector;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.common.external.IDataSourceAdapter;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.asterix.common.metadata.IMetadataLockUtil;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.common.utils.JobUtils.ProgressState;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.dataset.adapter.AdapterIdentifier;
import org.apache.asterix.external.operators.FeedIntakeOperatorNodePushable;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.WriterValidationUtil;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.base.IStatementRewriter;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.IndexedTypeExpression;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.AdapterDropStatement;
import org.apache.asterix.lang.common.statement.AnalyzeDropStatement;
import org.apache.asterix.lang.common.statement.AnalyzeStatement;
import org.apache.asterix.lang.common.statement.CompactStatement;
import org.apache.asterix.lang.common.statement.ConnectFeedStatement;
import org.apache.asterix.lang.common.statement.CopyFromStatement;
import org.apache.asterix.lang.common.statement.CopyToStatement;
import org.apache.asterix.lang.common.statement.CreateAdapterStatement;
import org.apache.asterix.lang.common.statement.CreateDatabaseStatement;
import org.apache.asterix.lang.common.statement.CreateDataverseStatement;
import org.apache.asterix.lang.common.statement.CreateFeedPolicyStatement;
import org.apache.asterix.lang.common.statement.CreateFeedStatement;
import org.apache.asterix.lang.common.statement.CreateFullTextConfigStatement;
import org.apache.asterix.lang.common.statement.CreateFullTextFilterStatement;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.lang.common.statement.CreateLibraryStatement;
import org.apache.asterix.lang.common.statement.CreateSynonymStatement;
import org.apache.asterix.lang.common.statement.CreateViewStatement;
import org.apache.asterix.lang.common.statement.DatabaseDropStatement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.DisconnectFeedStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.ExternalDetailsDecl;
import org.apache.asterix.lang.common.statement.FeedDropStatement;
import org.apache.asterix.lang.common.statement.FeedPolicyDropStatement;
import org.apache.asterix.lang.common.statement.FullTextConfigDropStatement;
import org.apache.asterix.lang.common.statement.FullTextFilterDropStatement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.FunctionDropStatement;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.InternalDetailsDecl;
import org.apache.asterix.lang.common.statement.LibraryDropStatement;
import org.apache.asterix.lang.common.statement.LoadStatement;
import org.apache.asterix.lang.common.statement.NodeGroupDropStatement;
import org.apache.asterix.lang.common.statement.NodegroupDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.common.statement.StartFeedStatement;
import org.apache.asterix.lang.common.statement.StopFeedStatement;
import org.apache.asterix.lang.common.statement.SynonymDropStatement;
import org.apache.asterix.lang.common.statement.TypeDecl;
import org.apache.asterix.lang.common.statement.TypeDropStatement;
import org.apache.asterix.lang.common.statement.UpsertStatement;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.statement.ViewDropStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.util.ViewUtil;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppQueryRewriter;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.dataset.DatasetFormatInfo;
import org.apache.asterix.metadata.dataset.hints.DatasetHints;
import org.apache.asterix.metadata.dataset.hints.DatasetHints.DatasetNodegroupCardinalityHint;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Database;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.FullTextConfigMetadataEntity;
import org.apache.asterix.metadata.entities.FullTextFilterMetadataEntity;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.metadata.entities.ViewDetails;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.functions.ExternalFunctionCompilerUtil;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.metadata.utils.SampleOperationsHelper;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.BuiltinTypeMap;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.runtime.fulltext.AbstractFullTextFilterDescriptor;
import org.apache.asterix.runtime.fulltext.FullTextConfigDescriptor;
import org.apache.asterix.runtime.fulltext.StopwordsFullTextFilterDescriptor;
import org.apache.asterix.runtime.operators.DatasetStreamStats;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.asterix.transaction.management.service.transaction.GlobalTxInfo;
import org.apache.asterix.translator.AbstractLangTranslator;
import org.apache.asterix.translator.ClientRequest;
import org.apache.asterix.translator.CompiledStatements;
import org.apache.asterix.translator.CompiledStatements.CompiledCopyFromFileStatement;
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
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.api.client.IClusterInfoCollector;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor.DropOption;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.TokenizerCategory;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

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
    protected Namespace activeNamespace;
    protected final List<FunctionDecl> declaredFunctions;
    protected final ILangCompilationProvider compilationProvider;
    protected final APIFramework apiFramework;
    protected final IRewriterFactory rewriterFactory;
    protected final ExecutorService executorService;
    protected final EnumSet<JobFlag> jobFlags = EnumSet.noneOf(JobFlag.class);
    protected final IMetadataLockManager lockManager;
    protected final IMetadataLockUtil lockUtil;
    protected final IResponsePrinter responsePrinter;
    protected final WarningCollector warningCollector;
    protected final ReentrantReadWriteLock compilationLock;
    protected final IGlobalTxManager globalTxManager;
    protected final INamespaceResolver namespaceResolver;

    public QueryTranslator(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
            ILangCompilationProvider compilationProvider, ExecutorService executorService,
            IResponsePrinter responsePrinter) {
        this.appCtx = appCtx;
        this.lockManager = appCtx.getMetadataLockManager();
        this.lockUtil = appCtx.getMetadataLockUtil();
        this.statements = statements;
        this.sessionOutput = output;
        this.sessionConfig = output.config();
        this.compilationProvider = compilationProvider;
        declaredFunctions = new ArrayList<>();
        apiFramework = new APIFramework(compilationProvider);
        rewriterFactory = compilationProvider.getRewriterFactory();
        activeNamespace = MetadataBuiltinEntities.DEFAULT_NAMESPACE;
        this.executorService = executorService;
        this.responsePrinter = responsePrinter;
        this.warningCollector = new WarningCollector();
        this.compilationLock = appCtx.getCompilationLock();
        if (appCtx.getServiceContext().getAppConfig().getBoolean(CCConfig.Option.ENFORCE_FRAME_WRITER_PROTOCOL)) {
            this.jobFlags.add(JobFlag.ENFORCE_CONTRACT);
        }
        this.globalTxManager = appCtx.getGlobalTxManager();
        this.namespaceResolver = appCtx.getNamespaceResolver();
    }

    public SessionOutput getSessionOutput() {
        return sessionOutput;
    }

    public IWarningCollector getWarningCollector() {
        return warningCollector;
    }

    @Override
    public void compileAndExecute(IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        validateStatements(requestParameters);
        trackRequest(requestParameters);
        Counter resultSetIdCounter = new Counter(0);
        FileSplit outputFile = null;
        String threadName = Thread.currentThread().getName();
        Thread.currentThread().setName(
                QueryTranslator.class.getSimpleName() + ":" + requestParameters.getRequestReference().getUuid());
        Map<String, String> config = new HashMap<>();
        final IResultSet resultSet = requestParameters.getResultSet();
        final ResultDelivery resultDelivery = requestParameters.getResultProperties().getDelivery();
        final long maxResultReads = requestParameters.getResultProperties().getMaxReads();
        final Stats stats = requestParameters.getStats();
        final StatementProperties statementProperties = requestParameters.getStatementProperties();
        final ResultMetadata outMetadata = requestParameters.getOutMetadata();
        final Map<String, IAObject> stmtParams = requestParameters.getStatementParameters();
        warningCollector.setMaxWarnings(sessionConfig.getMaxWarnings());
        try {
            for (Statement stmt : statements) {
                if (sessionConfig.is(SessionConfig.FORMAT_HTML)) {
                    sessionOutput.out().println(ApiServlet.HTML_STATEMENT_SEPARATOR);
                }
                validateOperation(appCtx, activeNamespace, stmt);
                MetadataProvider metadataProvider = MetadataProvider.create(appCtx, activeNamespace);
                configureMetadataProvider(metadataProvider, config, resultSetIdCounter, outputFile, requestParameters,
                        stmt);
                IStatementRewriter stmtRewriter = rewriterFactory.createStatementRewriter();
                rewriteStatement(stmt, stmtRewriter, metadataProvider); // Rewrite the statement's AST.
                Statement.Kind kind = stmt.getKind();
                statementProperties.setKind(kind);
                switch (kind) {
                    case SET:
                        handleSetStatement(stmt, config);
                        break;
                    case DATAVERSE_DECL:
                        activeNamespace = handleUseDataverseStatement(metadataProvider, stmt);
                        break;
                    case CREATE_DATABASE:
                        handleCreateDatabaseStatement(metadataProvider, stmt, requestParameters);
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
                    case CREATE_FULL_TEXT_FILTER:
                        handleCreateFullTextFilterStatement(metadataProvider, stmt);
                        break;
                    case CREATE_FULL_TEXT_CONFIG:
                        handleCreateFullTextConfigStatement(metadataProvider, stmt);
                        break;
                    case TYPE_DECL:
                        handleCreateTypeStatement(metadataProvider, stmt);
                        break;
                    case NODEGROUP_DECL:
                        handleCreateNodeGroupStatement(metadataProvider, stmt);
                        break;
                    case DATABASE_DROP:
                        handleDatabaseDropStatement(metadataProvider, stmt, hcc, requestParameters);
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
                    case FULL_TEXT_FILTER_DROP:
                        handleFullTextFilterDrop(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case FULL_TEXT_CONFIG_DROP:
                        handleFullTextConfigDrop(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case TYPE_DROP:
                        handleTypeDropStatement(metadataProvider, stmt);
                        break;
                    case NODEGROUP_DROP:
                        handleNodegroupDropStatement(metadataProvider, stmt);
                        break;
                    case CREATE_ADAPTER:
                        handleCreateAdapterStatement(metadataProvider, stmt);
                        break;
                    case ADAPTER_DROP:
                        handleAdapterDropStatement(metadataProvider, stmt);
                        break;
                    case CREATE_FUNCTION:
                        handleCreateFunctionStatement(metadataProvider, stmt, stmtRewriter, requestParameters);
                        break;
                    case FUNCTION_DROP:
                        handleFunctionDropStatement(metadataProvider, stmt, requestParameters);
                        break;
                    case CREATE_LIBRARY:
                        handleCreateLibraryStatement(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case LIBRARY_DROP:
                        handleLibraryDropStatement(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case CREATE_SYNONYM:
                        handleCreateSynonymStatement(metadataProvider, stmt);
                        break;
                    case SYNONYM_DROP:
                        handleDropSynonymStatement(metadataProvider, stmt);
                        break;
                    case CREATE_VIEW:
                        handleCreateViewStatement(metadataProvider, stmt, stmtRewriter, requestParameters);
                        break;
                    case VIEW_DROP:
                        handleViewDropStatement(metadataProvider, stmt);
                        break;
                    case LOAD:
                        if (stats.getProfileType() == Stats.ProfileType.FULL) {
                            this.jobFlags.add(JobFlag.PROFILE_RUNTIME);
                        }
                        handleLoadStatement(metadataProvider, stmt, hcc);
                        break;
                    case COPY_FROM:
                        if (stats.getProfileType() == Stats.ProfileType.FULL) {
                            this.jobFlags.add(JobFlag.PROFILE_RUNTIME);
                        }
                        handleCopyFromStatement(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case COPY_TO:
                        metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter.getAndInc()));
                        // The result should to be read just once
                        metadataProvider.setMaxResultReads(1);
                        if (stats.getProfileType() == Stats.ProfileType.FULL) {
                            this.jobFlags.add(JobFlag.PROFILE_RUNTIME);
                        }
                        handleCopyToStatement(metadataProvider, stmt, hcc, resultSet, resultDelivery, outMetadata,
                                requestParameters, stmtParams, stats);
                        break;
                    case INSERT:
                    case UPSERT:
                        if (((InsertStatement) stmt).getReturnExpression() != null) {
                            metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter.getAndInc()));
                            metadataProvider.setResultAsyncMode(resultDelivery == ResultDelivery.ASYNC
                                    || resultDelivery == ResultDelivery.DEFERRED);
                            metadataProvider.setMaxResultReads(maxResultReads);
                        }
                        if (stats.getProfileType() == Stats.ProfileType.FULL) {
                            this.jobFlags.add(JobFlag.PROFILE_RUNTIME);
                        }
                        handleInsertUpsertStatement(metadataProvider, stmt, hcc, resultSet, resultDelivery, outMetadata,
                                stats, requestParameters, stmtParams, stmtRewriter);
                        break;
                    case DELETE:
                        handleDeleteStatement(metadataProvider, stmt, hcc, stmtParams, stmtRewriter, requestParameters);
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
                        metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter.getAndInc()));
                        metadataProvider.setResultAsyncMode(
                                resultDelivery == ResultDelivery.ASYNC || resultDelivery == ResultDelivery.DEFERRED);
                        metadataProvider.setMaxResultReads(maxResultReads);
                        if (stats.getProfileType() == Stats.ProfileType.FULL) {
                            this.jobFlags.add(JobFlag.PROFILE_RUNTIME);
                        }
                        handleQuery(metadataProvider, (Query) stmt, hcc, resultSet, resultDelivery, outMetadata, stats,
                                requestParameters, stmtParams, stmtRewriter);
                        break;
                    case ANALYZE:
                        handleAnalyzeStatement(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case ANALYZE_DROP:
                        handleAnalyzeDropStatement(metadataProvider, stmt, hcc, requestParameters);
                        break;
                    case COMPACT:
                        handleCompactStatement(metadataProvider, stmt, hcc);
                        break;
                    case FUNCTION_DECL:
                        handleDeclareFunctionStatement(metadataProvider, stmt);
                        break;
                    case EXTENSION:
                        final ExtensionStatement extStmt = (ExtensionStatement) stmt;
                        statementProperties.setName(extStmt.getName());
                        if (!isCompileOnly()) {
                            extStmt.handle(hcc, this, requestParameters, metadataProvider,
                                    resultSetIdCounter.getAndInc());
                        }
                        break;
                    default:
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, stmt.getSourceLocation(),
                                "Unexpected statement: " + kind);
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

    protected void configureMetadataProvider(MetadataProvider metadataProvider, Map<String, String> config,
            Counter resultSetIdCounter, FileSplit outputFile, IRequestParameters requestParameters,
            Statement statement) {
        if (statement.getKind() == Statement.Kind.QUERY && requestParameters.isSQLCompatMode()) {
            metadataProvider.getConfig().put(SqlppQueryRewriter.SQL_COMPAT_OPTION, Boolean.TRUE.toString());
        }
        metadataProvider.getConfig().putAll(config);
        metadataProvider.setResultSetIdCounter(resultSetIdCounter);
        metadataProvider.setOutputFile(outputFile);
    }

    protected DataverseDecl getRequestDataverseDecl(IRequestParameters requestParameters) throws AlgebricksException {
        String requestDataverseName = requestParameters.getDefaultDataverseName();
        if (requestDataverseName == null) {
            return null;
        }
        Namespace requestNamespace = namespaceResolver.resolve(requestDataverseName);
        return new DataverseDecl(requestNamespace, true);
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

    protected LangRewritingContext createLangRewritingContext(MetadataProvider metadataProvider,
            List<FunctionDecl> declaredFunctions, List<ViewDecl> declaredViews, IWarningCollector warningCollector,
            int varCounter) {
        return new LangRewritingContext(metadataProvider, declaredFunctions, declaredViews, warningCollector,
                varCounter);
    }

    protected Namespace handleUseDataverseStatement(MetadataProvider metadataProvider, Statement stmt)
            throws Exception {
        DataverseDecl dvd = (DataverseDecl) stmt;
        DataverseName dvName = dvd.getDataverseName();
        String database = dvd.getDatabaseName();
        metadataProvider.validateNamespaceName(dvd.getNamespace(), dvd.getSourceLocation());
        //TODO(DB): read lock on database
        lockManager.acquireDataverseReadLock(metadataProvider.getLocks(), database, dvName);
        try {
            return doUseDataverseStatement(metadataProvider, dvd);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected Namespace doUseDataverseStatement(MetadataProvider metadataProvider, DataverseDecl stmtUseDataverse)
            throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            DataverseName dvName = stmtUseDataverse.getDataverseName();
            String dbName = stmtUseDataverse.getDatabaseName();
            Dataverse dv =
                    MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dbName, dvName);
            if (dv == null) {
                if (stmtUseDataverse.getIfExists()) {
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(stmtUseDataverse.getSourceLocation(),
                                ErrorCode.UNKNOWN_DATAVERSE,
                                MetadataUtil.dataverseName(dbName, dvName, metadataProvider.isUsingDatabase())));
                    }
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return activeNamespace;
                } else {
                    throw new MetadataException(ErrorCode.UNKNOWN_DATAVERSE, stmtUseDataverse.getSourceLocation(),
                            MetadataUtil.dataverseName(dbName, dvName, metadataProvider.isUsingDatabase()));
                }
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            //TODO(DB): should the stmt namespace be used?
            return new Namespace(dv.getDatabaseName(), dv.getDataverseName());
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected void handleCreateDatabaseStatement(MetadataProvider metadataProvider, Statement stmt,
            IRequestParameters requestParameters) throws Exception {
        CreateDatabaseStatement stmtCreateDatabase = (CreateDatabaseStatement) stmt;
        String database = stmtCreateDatabase.getDatabaseName().getValue();
        metadataProvider.validateDatabaseName(database, stmt.getSourceLocation());
        if (isCompileOnly()) {
            return;
        }
        lockUtil.createDatabaseBegin(lockManager, metadataProvider.getLocks(), database);
        try {
            doCreateDatabaseStatement(metadataProvider, stmtCreateDatabase, requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doCreateDatabaseStatement(MetadataProvider mdProvider, CreateDatabaseStatement stmtCreateDatabase,
            IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        mdProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            String databaseName = stmtCreateDatabase.getDatabaseName().getValue();
            Database database = MetadataManager.INSTANCE.getDatabase(mdTxnCtx, databaseName);
            if (database != null) {
                if (stmtCreateDatabase.ifNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.DATABASE_EXISTS, stmtCreateDatabase.getSourceLocation(),
                            databaseName);
                }
            }
            MetadataManager.INSTANCE.addDatabase(mdTxnCtx,
                    new Database(databaseName, false, MetadataUtil.PENDING_NO_OP));
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected void handleCreateDataverseStatement(MetadataProvider metadataProvider, Statement stmt,
            IRequestParameters requestParameters) throws Exception {
        CreateDataverseStatement stmtCreateDataverse = (CreateDataverseStatement) stmt;
        DataverseName dvName = stmtCreateDataverse.getDataverseName();
        String dbName = stmtCreateDataverse.getDatabaseName();
        Namespace stmtNamespace = stmtCreateDataverse.getNamespace();
        metadataProvider.validateNamespaceName(stmtNamespace, stmtCreateDataverse.getSourceLocation());
        if (isCompileOnly()) {
            return;
        }
        lockUtil.createDataverseBegin(lockManager, metadataProvider.getLocks(), dbName, dvName);
        try {
            doCreateDataverseStatement(metadataProvider, stmtCreateDataverse, requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    @SuppressWarnings("squid:S00112")
    protected boolean doCreateDataverseStatement(MetadataProvider metadataProvider,
            CreateDataverseStatement stmtCreateDataverse, IRequestParameters requestParameters) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            DataverseName dvName = stmtCreateDataverse.getDataverseName();
            String dbName = stmtCreateDataverse.getDatabaseName();
            Database db = MetadataManager.INSTANCE.getDatabase(mdTxnCtx, dbName);
            if (db == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATABASE, stmtCreateDataverse.getSourceLocation(),
                        dbName);
            }

            Dataverse dv =
                    MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dbName, dvName);
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
                    new Dataverse(dbName, dvName, stmtCreateDataverse.getFormat(), MetadataUtil.PENDING_NO_OP));
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected static void validateCompactionPolicy(String compactionPolicy,
            Map<String, String> compactionPolicyProperties, MetadataTransactionContext mdTxnCtx,
            boolean isExternalDataset, SourceLocation sourceLoc) throws Exception {
        CompactionPolicy compactionPolicyEntity = MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx,
                MetadataConstants.SYSTEM_DATABASE, MetadataConstants.METADATA_DATAVERSE_NAME, compactionPolicy);
        if (compactionPolicyEntity == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Unknown compaction policy: " + compactionPolicy);
        }
        String compactionPolicyFactoryClassName = compactionPolicyEntity.getClassName();
        ILSMMergePolicyFactory mergePolicyFactory =
                (ILSMMergePolicyFactory) Class.forName(compactionPolicyFactoryClassName).newInstance();
        if (isExternalDataset && mergePolicyFactory.getName().compareTo("correlated-prefix") == 0) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "The correlated-prefix merge policy cannot be used with external " + dataset(PLURAL));
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
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        DatasetDecl dd = (DatasetDecl) stmt;
        String datasetName = dd.getName().getValue();
        metadataProvider.validateDatabaseObjectName(dd.getNamespace(), datasetName, stmt.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(dd.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        TypeExpression itemTypeExpr = dd.getItemType();
        Triple<Namespace, String, Boolean> itemTypeQualifiedName = extractDatasetItemTypeName(stmtActiveNamespace,
                datasetName, itemTypeExpr, false, stmt.getSourceLocation());
        Namespace itemTypeNamespace = itemTypeQualifiedName.first;
        DataverseName itemTypeDataverseName = itemTypeNamespace.getDataverseName();
        String itemTypeDatabase = itemTypeNamespace.getDatabaseName();

        String itemTypeName = itemTypeQualifiedName.second;
        boolean itemTypeAnonymous = itemTypeQualifiedName.third;

        TypeExpression metaItemTypeExpr = dd.getMetaItemType();
        Namespace metaItemTypeNamespace = null;
        DataverseName metaItemTypeDataverseName = null;
        String metaItemTypeDatabase = null;
        String metaItemTypeName = null;
        boolean metaItemTypeAnonymous;
        if (metaItemTypeExpr != null) {
            Triple<Namespace, String, Boolean> metaItemTypeQualifiedName = extractDatasetItemTypeName(
                    stmtActiveNamespace, datasetName, metaItemTypeExpr, true, stmt.getSourceLocation());
            metaItemTypeNamespace = metaItemTypeQualifiedName.first;
            metaItemTypeDataverseName = metaItemTypeNamespace.getDataverseName();
            metaItemTypeDatabase = metaItemTypeNamespace.getDatabaseName();
            metaItemTypeName = metaItemTypeQualifiedName.second;
            metaItemTypeAnonymous = metaItemTypeQualifiedName.third;
        } else {
            metaItemTypeAnonymous = true; // doesn't matter
        }

        String nodegroupName = dd.getNodegroupName();
        String compactionPolicy = dd.getCompactionPolicy();
        boolean defaultCompactionPolicy = compactionPolicy == null;

        if (isCompileOnly()) {
            return;
        }

        lockUtil.createDatasetBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, datasetName,
                itemTypeDatabase, itemTypeDataverseName, itemTypeName, itemTypeAnonymous, metaItemTypeDatabase,
                metaItemTypeDataverseName, metaItemTypeName, metaItemTypeAnonymous, nodegroupName, compactionPolicy,
                defaultCompactionPolicy, dd.getDatasetType(), dd.getDatasetDetailsDecl());
        try {
            doCreateDatasetStatement(metadataProvider, dd, stmtActiveNamespace, datasetName, itemTypeNamespace,
                    itemTypeExpr, itemTypeName, metaItemTypeExpr, metaItemTypeNamespace, metaItemTypeName, hcc,
                    requestParameters);
            if (dd.getQuery() != null) {
                final IResultSet resultSet = requestParameters.getResultSet();
                final ResultDelivery resultDelivery = requestParameters.getResultProperties().getDelivery();
                final Stats stats = requestParameters.getStats();
                IStatementRewriter stmtRewriter = rewriterFactory.createStatementRewriter();
                final ResultMetadata outMetadata = requestParameters.getOutMetadata();
                final Map<String, IAObject> stmtParams = requestParameters.getStatementParameters();
                UpsertStatement upsertStmt =
                        new UpsertStatement(stmtActiveNamespace, datasetName, dd.getQuery(), -1, null, null);
                handleInsertUpsertStatement(metadataProvider, upsertStmt, hcc, resultSet, resultDelivery, outMetadata,
                        stats, requestParameters, stmtParams, stmtRewriter);
            }
        } finally {
            metadataProvider.getLocks().unlock();
        }

    }

    protected Optional<? extends Dataset> doCreateDatasetStatement(MetadataProvider metadataProvider, DatasetDecl dd,
            Namespace namespace, String datasetName, Namespace itemTypeNamespace, TypeExpression itemTypeExpr,
            String itemTypeName, TypeExpression metaItemTypeExpr, Namespace metaItemTypeNamespace,
            String metaItemTypeName, IHyracksClientConnection hcc, IRequestParameters requestParameters)
            throws Exception {
        DataverseName dataverseName = namespace.getDataverseName();
        String databaseName = namespace.getDatabaseName();

        DataverseName itemTypeDataverseName = null;
        String itemTypeDatabaseName = null;
        if (itemTypeNamespace != null) {
            itemTypeDataverseName = itemTypeNamespace.getDataverseName();
            itemTypeDatabaseName = itemTypeNamespace.getDatabaseName();
        }

        DataverseName metaItemTypeDataverseName = null;
        String metaItemTypeDatabaseName = null;
        if (metaItemTypeNamespace != null) {
            metaItemTypeDataverseName = metaItemTypeNamespace.getDataverseName();
            metaItemTypeDatabaseName = metaItemTypeNamespace.getDatabaseName();
        }

        MutableObject<ProgressState> progress = new MutableObject<>(ProgressState.NO_PROGRESS);
        SourceLocation sourceLoc = dd.getSourceLocation();
        DatasetType dsType = dd.getDatasetType();
        String ngNameId = dd.getNodegroupName();
        String compactionPolicy = dd.getCompactionPolicy();
        Map<String, String> compactionPolicyProperties = dd.getCompactionPolicyProperties();
        String compressionScheme = metadataProvider.getCompressionManager()
                .getDdlOrDefaultCompressionScheme(dd.getDatasetCompressionScheme());
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Dataset dataset = null;
        Datatype itemTypeEntity = null, metaItemTypeEntity = null;
        boolean itemTypeAdded = false, metaItemTypeAdded = false;

        StorageProperties storageProperties = metadataProvider.getStorageProperties();
        DatasetFormatInfo datasetFormatInfo = dd.getDatasetFormatInfo(storageProperties.getStorageFormat(),
                storageProperties.getColumnMaxTupleCount(), storageProperties.getColumnFreeSpaceTolerance(),
                storageProperties.getColumnMaxLeafNodeSize());
        try {
            //TODO(DB): also check for database existence?

            // Check if the dataverse exists
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }

            IDatasetDetails datasetDetails;
            Dataset ds = metadataProvider.findDataset(databaseName, dataverseName, datasetName, true);
            if (ds != null) {
                if (ds.getDatasetType() == DatasetType.VIEW) {
                    throw new CompilationException(ErrorCode.VIEW_EXISTS, sourceLoc,
                            DatasetUtil.getFullyQualifiedDisplayName(ds));
                }
                if (dd.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return Optional.empty();
                } else {
                    throw new CompilationException(ErrorCode.DATASET_EXISTS, sourceLoc, datasetName, dataverseName);
                }
            }
            List<TypeExpression> partitioningExprTypes = null;
            if (dsType == DatasetType.INTERNAL) {
                partitioningExprTypes = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getPartitioningExprTypes();
            }

            Pair<Datatype, Boolean> itemTypePair = fetchDatasetItemType(mdTxnCtx, dsType, datasetFormatInfo.getFormat(),
                    partitioningExprTypes, itemTypeDatabaseName, itemTypeDataverseName, itemTypeName, itemTypeExpr,
                    false, metadataProvider, sourceLoc);
            itemTypeEntity = itemTypePair.first;
            IAType itemType = itemTypeEntity.getDatatype();
            boolean itemTypeIsInline = itemTypePair.second;

            String ngName = ngNameId != null ? ngNameId
                    : configureNodegroupForDataset(appCtx, dd.getHints(), databaseName, dataverseName, datasetName,
                            metadataProvider, sourceLoc);

            if (compactionPolicy == null) {
                compactionPolicy = StorageConstants.DEFAULT_COMPACTION_POLICY_NAME;
                compactionPolicyProperties = StorageConstants.DEFAULT_COMPACTION_POLICY_PROPERTIES;
            } else {
                validateCompactionPolicy(compactionPolicy, compactionPolicyProperties, mdTxnCtx, false, sourceLoc);
            }

            IAType metaItemType = null;
            boolean metaItemTypeIsInline = false;
            switch (dsType) {
                case INTERNAL:
                    if (metaItemTypeExpr != null) {
                        Pair<Datatype, Boolean> metaItemTypePair =
                                fetchDatasetItemType(mdTxnCtx, dsType, datasetFormatInfo.getFormat(),
                                        partitioningExprTypes, metaItemTypeDatabaseName, metaItemTypeDataverseName,
                                        metaItemTypeName, metaItemTypeExpr, true, metadataProvider, sourceLoc);
                        metaItemTypeEntity = metaItemTypePair.first;
                        metaItemType = metaItemTypeEntity.getDatatype();
                        metaItemTypeIsInline = metaItemTypePair.second;
                    }
                    ARecordType metaRecType = (ARecordType) metaItemType;

                    List<List<String>> partitioningExprs =
                            ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getPartitioningExprs();

                    List<Integer> keySourceIndicators =
                            ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getKeySourceIndicators();
                    boolean autogenerated = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).isAutogenerated();
                    ARecordType aRecordType = (ARecordType) itemType;
                    List<IAType> partitioningTypes = validatePartitioningExpressions(aRecordType, metaRecType,
                            partitioningExprs, keySourceIndicators, autogenerated, sourceLoc, partitioningExprTypes);

                    List<String> filterField = ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getFilterField();
                    Integer filterSourceIndicator =
                            ((InternalDetailsDecl) dd.getDatasetDetailsDecl()).getFilterSourceIndicator();

                    if (filterField != null) {
                        ValidateUtil.validateFilterField(aRecordType, metaRecType, filterSourceIndicator, filterField,
                                sourceLoc);
                    }
                    if (compactionPolicy == null && filterField != null) {
                        // If the dataset has a filter and the user didn't specify a merge
                        // policy, then we will pick the
                        // correlated-prefix as the default merge policy.
                        compactionPolicy = StorageConstants.DEFAULT_FILTERED_DATASET_COMPACTION_POLICY_NAME;
                        compactionPolicyProperties = StorageConstants.DEFAULT_COMPACTION_POLICY_PROPERTIES;
                    }
                    boolean isDatasetWithoutTypeSpec = aRecordType.getFieldNames().length == 0 && metaRecType == null;
                    // Validate dataset properties if the format is COLUMN
                    ColumnPropertiesValidationUtil.validate(sourceLoc, datasetFormatInfo.getFormat(), compactionPolicy,
                            filterField);
                    datasetDetails = new InternalDatasetDetails(InternalDatasetDetails.FileStructure.BTREE,
                            InternalDatasetDetails.PartitioningStrategy.HASH, partitioningExprs, partitioningExprs,
                            keySourceIndicators, partitioningTypes, autogenerated, filterSourceIndicator, filterField,
                            isDatasetWithoutTypeSpec);
                    break;
                case EXTERNAL:
                    ExternalDetailsDecl externalDetails = (ExternalDetailsDecl) dd.getDatasetDetailsDecl();
                    Map<String, String> properties = createExternalDatasetProperties(databaseName, dataverseName, dd,
                            itemTypeEntity, metadataProvider, mdTxnCtx);
                    ExternalDataUtils.normalize(properties);
                    ExternalDataUtils.validate(properties);
                    ExternalDataUtils.validateType(properties, (ARecordType) itemType);
                    validateExternalDatasetProperties(externalDetails, properties, dd.getSourceLocation(), mdTxnCtx,
                            appCtx);
                    datasetDetails = new ExternalDatasetDetails(externalDetails.getAdapter(), properties, new Date(),
                            TransactionState.COMMIT);
                    break;
                default:
                    throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_DATASET_TYPE,
                            dataset.getDatasetType().toString());
            }

            // #. initialize DatasetIdFactory if it is not initialized.
            if (!DatasetIdFactory.isInitialized()) {
                DatasetIdFactory.initialize(MetadataManager.INSTANCE.getMostRecentDatasetId());
            }

            // #. add a new dataset with PendingAddOp
            dataset = (Dataset) createDataset(dd, databaseName, dataverseName, datasetName, itemTypeDatabaseName,
                    itemTypeDataverseName, itemTypeName, metaItemTypeDatabaseName, metaItemTypeDataverseName,
                    metaItemTypeName, dsType, compactionPolicy, compactionPolicyProperties, compressionScheme,
                    datasetFormatInfo, datasetDetails, ngName);
            MetadataManager.INSTANCE.addDataset(metadataProvider.getMetadataTxnContext(), dataset);

            if (itemTypeIsInline) {
                MetadataManager.INSTANCE.addDatatype(mdTxnCtx, itemTypeEntity);
                itemTypeAdded = true;
            }
            if (metaItemTypeIsInline) {
                MetadataManager.INSTANCE.addDatatype(mdTxnCtx, metaItemTypeEntity);
                metaItemTypeAdded = true;
            }

            if (dsType == DatasetType.INTERNAL) {
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
            MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), databaseName, dataverseName,
                    datasetName, requestParameters.isForceDropDataset());
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
                    JobSpecification jobSpec =
                            DatasetUtil.dropDatasetJobSpec(dataset, metadataProvider, EnumSet.of(DropOption.IF_EXISTS));
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
                    MetadataManager.INSTANCE.dropDataset(mdTxnCtx, databaseName, dataverseName, datasetName,
                            requestParameters.isForceDropDataset());
                    if (itemTypeAdded) {
                        MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, itemTypeEntity.getDatabaseName(),
                                itemTypeEntity.getDataverseName(), itemTypeEntity.getDatatypeName());
                    }
                    if (metaItemTypeAdded) {
                        MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, metaItemTypeEntity.getDatabaseName(),
                                metaItemTypeEntity.getDataverseName(), metaItemTypeEntity.getDatatypeName());
                    }
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending dataset(" + dataverseName
                            + "." + datasetName + ") couldn't be removed from the metadata", e);
                }
            }
            throw e;
        }
        return Optional.of(dataset);
    }

    protected IDataset createDataset(DatasetDecl dd, String database, DataverseName dataverseName, String datasetName,
            String itemTypeDatabase, DataverseName itemTypeDataverseName, String itemTypeName,
            String metaItemTypeDatabase, DataverseName metaItemTypeDataverseName, String metaItemTypeName,
            DatasetType dsType, String compactionPolicy, Map<String, String> compactionPolicyProperties,
            String compressionScheme, DatasetFormatInfo datasetFormatInfo, IDatasetDetails datasetDetails,
            String ngName) throws AlgebricksException {
        return new Dataset(database, dataverseName, datasetName, itemTypeDatabase, itemTypeDataverseName, itemTypeName,
                metaItemTypeDatabase, metaItemTypeDataverseName, metaItemTypeName, ngName, compactionPolicy,
                compactionPolicyProperties, datasetDetails, dd.getHints(), dsType, DatasetIdFactory.generateDatasetId(),
                MetadataUtil.PENDING_ADD_OP, compressionScheme, datasetFormatInfo);
    }

    protected Triple<Namespace, String, Boolean> extractDatasetItemTypeName(Namespace datasetNamespace,
            String datasetName, TypeExpression itemTypeExpr, boolean isMetaItemType, SourceLocation sourceLoc)
            throws CompilationException {
        switch (itemTypeExpr.getTypeKind()) {
            case TYPEREFERENCE:
                TypeReferenceExpression itemTypeRefExpr = (TypeReferenceExpression) itemTypeExpr;
                Pair<Namespace, Identifier> itemTypeIdent = itemTypeRefExpr.getIdent();
                Namespace typeNamespace = itemTypeIdent.first != null ? itemTypeIdent.first : datasetNamespace;
                String typeName = itemTypeRefExpr.getIdent().second.getValue();
                return new Triple<>(typeNamespace, typeName, false);
            case RECORD:
                String inlineTypeName = TypeUtil.createDatasetInlineTypeName(datasetName, isMetaItemType);
                return new Triple<>(datasetNamespace, inlineTypeName, true);
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc,
                        String.valueOf(itemTypeExpr.getTypeKind()));
        }
    }

    protected Pair<Datatype, Boolean> fetchDatasetItemType(MetadataTransactionContext mdTxnCtx, DatasetType datasetType,
            DatasetConfig.DatasetFormat format, List<TypeExpression> partitioningExprTypes, String itemTypeDatabaseName,
            DataverseName itemTypeDataverseName, String itemTypeName, TypeExpression itemTypeExpr,
            boolean isMetaItemType, MetadataProvider metadataProvider, SourceLocation sourceLoc)
            throws AlgebricksException {
        switch (itemTypeExpr.getTypeKind()) {
            case TYPEREFERENCE:
                Datatype itemTypeEntity =
                        metadataProvider.findTypeEntity(itemTypeDatabaseName, itemTypeDataverseName, itemTypeName);
                if (itemTypeEntity == null || itemTypeEntity.getIsAnonymous()) {
                    // anonymous types cannot be referred from CREATE DATASET/VIEW
                    throw new AsterixException(ErrorCode.UNKNOWN_TYPE, sourceLoc,
                            DatasetUtil.getFullyQualifiedDisplayName(itemTypeDataverseName, itemTypeName));
                }
                IAType itemType = itemTypeEntity.getDatatype();
                validateDatasetItemType(datasetType, format, partitioningExprTypes, itemType, isMetaItemType,
                        sourceLoc);
                return new Pair<>(itemTypeEntity, false);
            case RECORD:
                itemType = translateType(itemTypeDatabaseName, itemTypeDataverseName, itemTypeName, itemTypeExpr,
                        mdTxnCtx);
                validateDatasetItemType(datasetType, format, partitioningExprTypes, itemType, isMetaItemType,
                        sourceLoc);
                itemTypeEntity =
                        new Datatype(itemTypeDatabaseName, itemTypeDataverseName, itemTypeName, itemType, true);
                return new Pair<>(itemTypeEntity, true);
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc,
                        String.valueOf(itemTypeExpr.getTypeKind()));
        }
    }

    protected void validateDatasetItemType(DatasetType datasetType, DatasetConfig.DatasetFormat format,
            List<TypeExpression> partitioningExprTypes, IAType itemType, boolean isMetaItemType,
            SourceLocation sourceLoc) throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.OBJECT) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    String.format("%s %s has to be a record type.",
                            datasetType == DatasetType.VIEW ? "view" : StringUtils.capitalize(dataset()),
                            isMetaItemType ? "meta type" : "type"));
        }
        if (datasetType == DatasetType.VIEW) {
            ViewUtil.validateViewItemType((ARecordType) itemType, sourceLoc);
        }

        // Validate columnar type
        if (datasetType == DatasetType.INTERNAL) {
            ColumnSupportedTypesValidator.validate(format, itemType, sourceLoc);
            if (partitioningExprTypes != null) {
                for (TypeExpression typeExpr : partitioningExprTypes) {
                    String typeName = ((TypeReferenceExpression) typeExpr).getIdent().second.getValue();
                    IAType type = BuiltinTypeMap.getBuiltinType(typeName);
                    if (type != null) {
                        // type will be validated next
                        ColumnSupportedTypesValidator.validate(format, type, sourceLoc);
                    }
                }
            }
        }
    }

    protected Map<String, String> createExternalDatasetProperties(String databaseName, DataverseName dataverseName,
            DatasetDecl dd, Datatype itemType, MetadataProvider metadataProvider, MetadataTransactionContext mdTxnCtx)
            throws AlgebricksException {
        ExternalDetailsDecl externalDetails = (ExternalDetailsDecl) dd.getDatasetDetailsDecl();
        Map<String, String> properties = externalDetails.getProperties();
        ExternalDataUtils.validateParquetTypeAndConfiguration(properties, (ARecordType) itemType.getDatatype());
        return properties;
    }

    protected static void validateIfResourceIsActiveInFeed(ICcApplicationContext appCtx, Dataset dataset,
            SourceLocation sourceLoc) throws CompilationException {
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        IActiveEntityEventsListener[] listeners = activeEventHandler.getEventListeners();
        for (IActiveEntityEventsListener listener : listeners) {
            if (listener.isEntityUsingDataset(dataset) && listener.isActive()) {
                throw new CompilationException(ErrorCode.COMPILATION_CANT_DROP_ACTIVE_DATASET, sourceLoc,
                        DatasetUtil.getFullyQualifiedDisplayName(dataset), listener.getEntityId().toString());
            }
        }
    }

    protected static String configureNodegroupForDataset(ICcApplicationContext appCtx, Map<String, String> hints,
            String databaseName, DataverseName dataverseName, String datasetName, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws Exception {
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
        return DatasetUtil.createNodeGroupForNewDataset(databaseName, dataverseName, datasetName, selectedNodes,
                metadataProvider);
    }

    public void handleCreateIndexStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        CreateIndexStatement stmtCreateIndex = (CreateIndexStatement) stmt;
        String datasetName = stmtCreateIndex.getDatasetName().getValue();
        String indexName = stmtCreateIndex.getIndexName().getValue();
        String fullTextConfigName = stmtCreateIndex.getFullTextConfigName();
        metadataProvider.validateDatabaseObjectName(stmtCreateIndex.getNamespace(), indexName,
                stmt.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(stmtCreateIndex.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.createIndexBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, datasetName,
                fullTextConfigName);
        try {
            doCreateIndex(metadataProvider, stmtCreateIndex, databaseName, dataverseName, datasetName, hcc,
                    requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void doCreateIndex(MetadataProvider metadataProvider, CreateIndexStatement stmtCreateIndex,
            String databaseName, DataverseName dataverseName, String datasetName, IHyracksClientConnection hcc,
            IRequestParameters requestParameters) throws Exception {
        SourceLocation sourceLoc = stmtCreateIndex.getSourceLocation();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            // Check if the dataverse exists
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }

            Dataset ds = metadataProvider.findDataset(databaseName, dataverseName, datasetName);
            if (ds == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }

            DatasetType datasetType = ds.getDatasetType();
            IndexType indexType = stmtCreateIndex.getIndexType();
            List<CreateIndexStatement.IndexedElement> indexedElements = stmtCreateIndex.getIndexedElements();
            int indexedElementsCount = indexedElements.size();
            boolean isSecondaryPrimary = indexedElementsCount == 0;
            validateIndexType(datasetType, indexType, isSecondaryPrimary, sourceLoc);

            String indexName = stmtCreateIndex.getIndexName().getValue();
            Index index = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), databaseName,
                    dataverseName, datasetName, indexName);
            if (index != null) {
                if (stmtCreateIndex.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.INDEX_EXISTS, sourceLoc, indexName);
                }
            }

            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    ds.getItemTypeDatabaseName(), ds.getItemTypeDataverseName(), ds.getItemTypeName());
            ARecordType aRecordType = (ARecordType) dt.getDatatype();
            /* TODO: unused for now becase indexes on meta are disabled -- see below
            ARecordType metaRecordType = null;
            if (ds.hasMetaPart()) {
                Datatype metaDt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                        ds.getMetaItemTypeDataverseName(), ds.getMetaItemTypeName());
                metaRecordType = (ARecordType) metaDt.getDatatype();
            }
            */
            if (!ds.hasMetaPart()) {
                aRecordType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(aRecordType, null, ds);
            }

            List<List<IAType>> indexFieldTypes = new ArrayList<>(indexedElementsCount);
            boolean hadUnnest = false;
            boolean overridesFieldTypes = false;

            // this set is used to detect duplicates in the specified keys in the create
            // index statement
            // e.g. CREATE INDEX someIdx on dataset(id,id).
            // checking only the names is not enough.
            // Need also to check the source indicators for the most general case
            // (even though indexes on meta fields are curently disabled -- see below)
            Set<Triple<Integer, List<List<String>>, List<List<String>>>> indexKeysSet = new HashSet<>();

            for (CreateIndexStatement.IndexedElement indexedElement : indexedElements) {
                // disable creating an index on meta fields (fields with source indicator == 1 are meta fields)
                if (indexedElement.getSourceIndicator() != Index.RECORD_INDICATOR) {
                    throw new AsterixException(ErrorCode.COMPILATION_ERROR, indexedElement.getSourceLocation(),
                            "Cannot create index on meta fields");
                }
                ARecordType sourceRecordType = aRecordType;
                IAType inputTypePrime;
                boolean inputTypeNullable, inputTypeMissable;
                List<Pair<List<String>, IndexedTypeExpression>> projectList = indexedElement.getProjectList();
                int projectCount = projectList.size();
                if (indexedElement.hasUnnest()) {
                    if (indexType != IndexType.ARRAY) {
                        throw new CompilationException(ErrorCode.COMPILATION_INCOMPATIBLE_INDEX_TYPE,
                                indexedElement.getSourceLocation(), String.valueOf(indexType));
                    }
                    // allow only 1 unnesting element in ARRAY index
                    if (hadUnnest) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, indexedElement.getSourceLocation(),
                                "Cannot create composite index with multiple array fields using different arrays");
                    }
                    hadUnnest = true;
                    if (projectCount == 0) {
                        // Note. UNNEST with no SELECT is supposed to have 1 project element with 'null' path
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, indexedElement.getSourceLocation(),
                                "Invalid index element");
                    }
                    Triple<IAType, Boolean, Boolean> unnestTypeResult = KeyFieldTypeUtil.getKeyUnnestType(
                            sourceRecordType, indexedElement.getUnnestList(), indexedElement.getSourceLocation());
                    if (unnestTypeResult == null) {
                        inputTypePrime = null; // = ANY
                        inputTypeNullable = inputTypeMissable = true;
                    } else {
                        inputTypePrime = unnestTypeResult.first;
                        inputTypeNullable = unnestTypeResult.second;
                        inputTypeMissable = unnestTypeResult.third;
                    }
                } else {
                    if (projectCount != 1) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, indexedElement.getSourceLocation(),
                                "Invalid index element");
                    }
                    inputTypePrime = sourceRecordType;
                    inputTypeNullable = inputTypeMissable = false;
                }

                // at this point 'inputTypePrime' is either a record, or if we had unnest then it could also be anything else.
                List<IAType> fieldTypes = new ArrayList<>(projectCount);
                for (int i = 0; i < projectCount; i++) {
                    Pair<List<String>, IndexedTypeExpression> projectPair = projectList.get(i);
                    List<String> projectPath = projectPair.first;
                    IndexedTypeExpression projectTypeExpr = projectPair.second;
                    IAType projectTypePrime;
                    boolean projectTypeNullable, projectTypeMissable;
                    if (projectPath == null) {
                        boolean emptyPathOk = indexedElement.hasUnnest() && i == 0;
                        if (!emptyPathOk) {
                            throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                                    indexedElement.getSourceLocation(), "Invalid index element");
                        }
                        projectTypePrime = inputTypePrime;
                        projectTypeNullable = inputTypeNullable;
                        projectTypeMissable = inputTypeMissable;
                    } else if (inputTypePrime == null) {
                        projectTypePrime = null; // ANY
                        projectTypeNullable = projectTypeMissable = true;
                    } else {
                        if (inputTypePrime.getTypeTag() != ATypeTag.OBJECT) {
                            throw new CompilationException(ErrorCode.TYPE_MISMATCH_GENERIC, sourceLoc, ATypeTag.OBJECT,
                                    inputTypePrime.getTypeTag());
                        }
                        ARecordType inputTypePrimeRecord = (ARecordType) inputTypePrime;
                        Triple<IAType, Boolean, Boolean> projectTypeResult = KeyFieldTypeUtil.getKeyProjectType(
                                inputTypePrimeRecord, projectPath, indexedElement.getSourceLocation());
                        if (projectTypeResult != null) {
                            projectTypePrime = projectTypeResult.first;
                            projectTypeNullable = inputTypeNullable || projectTypeResult.second;
                            projectTypeMissable = inputTypeMissable || projectTypeResult.third;
                        } else {
                            projectTypePrime = null; // ANY
                            projectTypeNullable = projectTypeMissable = true;
                        }
                    }

                    boolean isFieldFromSchema = projectTypePrime != null;
                    IAType fieldTypePrime;
                    boolean fieldTypeNullable, fieldTypeMissable;
                    if (projectTypeExpr == null) {
                        // the type of the indexed field is NOT specified in the DDL
                        if (stmtCreateIndex.hasCastDefaultNull()) {
                            throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                                    stmtCreateIndex.getSourceLocation(),
                                    "CAST modifier is used without specifying the type of the indexed field");
                        }
                        fieldTypePrime = projectTypePrime;
                        fieldTypeNullable = projectTypeNullable;
                        fieldTypeMissable = projectTypeMissable;
                    } else {
                        // the type of the indexed field is explicitly specified in the DDL
                        Map<TypeSignature, IAType> typeMap = TypeTranslator.computeTypes(databaseName, dataverseName,
                                indexName, projectTypeExpr.getType(), databaseName, dataverseName, mdTxnCtx);
                        TypeSignature typeSignature = new TypeSignature(databaseName, dataverseName, indexName);
                        fieldTypePrime = typeMap.get(typeSignature);
                        // BACK-COMPAT: keep prime type only if we're overriding field types
                        fieldTypeNullable = fieldTypeMissable = false;
                        overridesFieldTypes = true;

                        if (stmtCreateIndex.isEnforced()) {
                            if (!projectTypeExpr.isUnknownable()) {
                                throw new CompilationException(ErrorCode.INDEX_ILLEGAL_ENFORCED_NON_OPTIONAL,
                                        indexedElement.getSourceLocation(),
                                        LogRedactionUtil.userData(String.valueOf(projectPath)));
                            }
                            // don't allow creating an enforced index on a closed-type field having field type different
                            // from the closed-type
                            if (isFieldFromSchema) {
                                if (fieldTypePrime == null) {
                                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                            indexedElement.getSourceLocation(), "cannot find type of field");
                                } else if (!projectTypePrime.deepEqual(fieldTypePrime)) {
                                    throw new CompilationException(ErrorCode.TYPE_MISMATCH_GENERIC, sourceLoc,
                                            projectTypePrime.getTypeTag(), fieldTypePrime.getTypeTag());
                                }
                            }
                        } else {
                            if (indexType != IndexType.BTREE && indexType != IndexType.ARRAY) {
                                throw new CompilationException(ErrorCode.INDEX_ILLEGAL_NON_ENFORCED_TYPED,
                                        indexedElement.getSourceLocation(), indexType);
                            }
                            if (isFieldFromSchema) {
                                // allow overriding the type of the closed-field only if CAST modifier is used
                                if (!stmtCreateIndex.hasCastDefaultNull()) {
                                    if (fieldTypePrime == null) {
                                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                                indexedElement.getSourceLocation(), "cannot find type of field");
                                    } else if (!projectTypePrime.deepEqual(fieldTypePrime)) {
                                        throw new CompilationException(ErrorCode.TYPE_MISMATCH_GENERIC, sourceLoc,
                                                projectTypePrime.getTypeTag(), fieldTypePrime.getTypeTag());
                                    }
                                }
                            }
                        }
                    }

                    if (fieldTypePrime == null) {
                        if (projectPath != null) {
                            String fieldName = LogRedactionUtil.userData(RecordUtil.toFullyQualifiedName(projectPath));
                            throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                                    indexedElement.getSourceLocation(),
                                    "cannot find type of field '" + fieldName + "'");
                        }
                        // projectPath == null should only be the case with array index having UNNESTs only
                        if (indexedElement.hasUnnest()) {
                            List<List<String>> unnestList = indexedElement.getUnnestList();
                            List<String> arrayField = unnestList.get(unnestList.size() - 1);
                            String fieldName = LogRedactionUtil.userData(RecordUtil.toFullyQualifiedName(arrayField));
                            throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                                    indexedElement.getSourceLocation(),
                                    "cannot find type of elements of field '" + fieldName + "'");
                        }
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                indexedElement.getSourceLocation(), "cannot find type of field");
                    }
                    validateIndexFieldType(indexType, fieldTypePrime, projectPath, indexedElement.getSourceLocation());

                    IAType fieldType =
                            KeyFieldTypeUtil.makeUnknownableType(fieldTypePrime, fieldTypeNullable, fieldTypeMissable);
                    fieldTypes.add(fieldType);
                }

                // Try to add the key & its source to the set of keys for duplicate detection.
                if (!indexKeysSet.add(indexedElement.toIdentifier())) {
                    throw new AsterixException(ErrorCode.INDEX_ILLEGAL_REPETITIVE_FIELD,
                            indexedElement.getSourceLocation(),
                            LogRedactionUtil.userData(indexedElement.getProjectListDisplayForm()));
                }

                indexFieldTypes.add(fieldTypes);
            }

            boolean unknownKeyOptionAllowed =
                    (indexType == IndexType.BTREE || indexType == IndexType.ARRAY) && !isSecondaryPrimary;
            if (stmtCreateIndex.hasExcludeUnknownKey() && !unknownKeyOptionAllowed) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "can only specify exclude/include unknown key for B-Tree & Array indexes");
            }
            boolean castDefaultNullAllowed = indexType == IndexType.BTREE && !isSecondaryPrimary;
            if (stmtCreateIndex.hasCastDefaultNull() && !castDefaultNullAllowed) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "CAST modifier is only allowed for B-Tree indexes");
            }
            if (stmtCreateIndex.getCastDefaultNull().getOrElse(false)) {
                if (stmtCreateIndex.isEnforced()) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "CAST modifier cannot be specified together with ENFORCED");
                }
            }
            Index.IIndexDetails indexDetails;
            if (Index.IndexCategory.of(indexType) == Index.IndexCategory.ARRAY) {
                if (!stmtCreateIndex.hasExcludeUnknownKey()
                        || !stmtCreateIndex.getExcludeUnknownKey().getOrElse(false)) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Array indexes must specify EXCLUDE UNKNOWN KEY.");
                }
                if (!hadUnnest) {
                    // prohibited by the grammar
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc,
                            String.valueOf(indexType));
                }
                if (stmtCreateIndex.isEnforced()) {
                    // not supported yet.
                    throw new CompilationException(ErrorCode.COMPILATION_INCOMPATIBLE_INDEX_TYPE, sourceLoc,
                            String.valueOf(indexType));
                }

                List<Index.ArrayIndexElement> indexElementList = new ArrayList<>(indexedElementsCount);
                for (int i = 0; i < indexedElementsCount; i++) {
                    CreateIndexStatement.IndexedElement indexedElement = indexedElements.get(i);
                    List<List<String>> projectList =
                            indexedElement.getProjectList().stream().map(Pair::getFirst).collect(Collectors.toList());
                    indexElementList.add(new Index.ArrayIndexElement(indexedElement.getUnnestList(), projectList,
                            indexFieldTypes.get(i), indexedElement.getSourceIndicator()));
                }
                indexDetails = new Index.ArrayIndexDetails(indexElementList, overridesFieldTypes);
            } else {
                List<List<String>> keyFieldNames = new ArrayList<>(indexedElementsCount);
                List<IAType> keyFieldTypes = new ArrayList<>(indexedElementsCount);
                List<Integer> keyFieldSourceIndicators = new ArrayList<>(indexedElementsCount);
                // secondary primary indexes do not have search keys (no SKs), and thus no equivalent indicators
                if (!isSecondaryPrimary) {
                    for (int i = 0; i < indexedElementsCount; i++) {
                        CreateIndexStatement.IndexedElement indexedElement = indexedElements.get(i);
                        keyFieldNames.add(indexedElement.getProjectList().get(0).first);
                        keyFieldTypes.add(indexFieldTypes.get(i).get(0));
                        keyFieldSourceIndicators.add(indexedElement.getSourceIndicator());
                    }
                }
                switch (Index.IndexCategory.of(indexType)) {
                    case VALUE:
                        Map<String, String> castConfig = TypeUtil.validateConfiguration(stmtCreateIndex.getCastConfig(),
                                stmtCreateIndex.getSourceLocation());
                        String datetimeFormat = TypeUtil.getDatetimeFormat(castConfig);
                        String dateFormat = TypeUtil.getDateFormat(castConfig);
                        String timeFormat = TypeUtil.getTimeFormat(castConfig);
                        indexDetails = new Index.ValueIndexDetails(keyFieldNames, keyFieldSourceIndicators,
                                keyFieldTypes, overridesFieldTypes, stmtCreateIndex.getExcludeUnknownKey(),
                                stmtCreateIndex.getCastDefaultNull(), datetimeFormat, dateFormat, timeFormat);
                        break;
                    case TEXT:
                        indexDetails = new Index.TextIndexDetails(keyFieldNames, keyFieldSourceIndicators,
                                keyFieldTypes, overridesFieldTypes, stmtCreateIndex.getGramLength(),
                                stmtCreateIndex.getFullTextConfigName());
                        break;
                    default:
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc,
                                String.valueOf(indexType));
                }
            }

            Index newIndex = new Index(databaseName, dataverseName, datasetName, indexName, indexType, indexDetails,
                    stmtCreateIndex.isEnforced(), false, MetadataUtil.PENDING_ADD_OP);

            bActiveTxn = false; // doCreateIndexImpl() takes over the current transaction
            doCreateIndexImpl(hcc, metadataProvider, ds, newIndex, jobFlags, sourceLoc);

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        }
    }

    public void handleCreateFullTextFilterStatement(MetadataProvider metadataProvider, Statement stmt)
            throws Exception {
        CreateFullTextFilterStatement stmtCreateFilter = (CreateFullTextFilterStatement) stmt;
        String fullTextFilterName = stmtCreateFilter.getFilterName();
        metadataProvider.validateDatabaseObjectName(stmtCreateFilter.getNamespace(), fullTextFilterName,
                stmt.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(stmtCreateFilter.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.createFullTextFilterBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                fullTextFilterName);
        try {
            doCreateFullTextFilter(metadataProvider, stmtCreateFilter, databaseName, dataverseName);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void doCreateFullTextFilter(MetadataProvider metadataProvider,
            CreateFullTextFilterStatement stmtCreateFilter, String databaseName, DataverseName dataverseName)
            throws Exception {
        AbstractFullTextFilterDescriptor filterDescriptor;
        String filterType = stmtCreateFilter.getFilterType();
        if (filterType == null) {
            throw new CompilationException(ErrorCode.PARSE_ERROR, stmtCreateFilter.getSourceLocation(),
                    "full-text filter type is null");
        }

        if (FIELD_TYPE_STOPWORDS.equals(filterType)) {
            filterDescriptor = new StopwordsFullTextFilterDescriptor(databaseName, dataverseName,
                    stmtCreateFilter.getFilterName(), stmtCreateFilter.getStopwordsList());
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, stmtCreateFilter.getSourceLocation(),
                    "Unexpected full-text filter type: " + filterType);
        }

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, stmtCreateFilter.getSourceLocation(),
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }

            String filterName = stmtCreateFilter.getFilterName();
            FullTextFilterMetadataEntity existingFilter =
                    MetadataManager.INSTANCE.getFullTextFilter(mdTxnCtx, databaseName, dataverseName, filterName);
            if (existingFilter != null) {
                if (stmtCreateFilter.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.FULL_TEXT_FILTER_ALREADY_EXISTS,
                            stmtCreateFilter.getSourceLocation(), filterName);
                }
            }

            MetadataManager.INSTANCE.addFullTextFilter(mdTxnCtx, new FullTextFilterMetadataEntity(filterDescriptor));
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    public void handleCreateFullTextConfigStatement(MetadataProvider metadataProvider, Statement stmt)
            throws Exception {
        CreateFullTextConfigStatement stmtCreateConfig = (CreateFullTextConfigStatement) stmt;
        String configName = stmtCreateConfig.getConfigName();
        metadataProvider.validateDatabaseObjectName(stmtCreateConfig.getNamespace(), configName,
                stmt.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(stmtCreateConfig.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        ImmutableList<String> filterNames = stmtCreateConfig.getFilterNames();

        if (isCompileOnly()) {
            return;
        }
        lockUtil.createFullTextConfigBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                configName, filterNames);
        try {
            doCreateFullTextConfig(metadataProvider, stmtCreateConfig, databaseName, dataverseName, configName,
                    filterNames);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void doCreateFullTextConfig(MetadataProvider metadataProvider,
            CreateFullTextConfigStatement stmtCreateConfig, String databaseName, DataverseName dataverseName,
            String configName, ImmutableList<String> filterNames) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        try {
            FullTextConfigMetadataEntity existingConfig =
                    MetadataManager.INSTANCE.getFullTextConfig(mdTxnCtx, databaseName, dataverseName, configName);
            if (existingConfig != null) {
                if (stmtCreateConfig.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.FULL_TEXT_CONFIG_ALREADY_EXISTS,
                            stmtCreateConfig.getSourceLocation(), configName);
                }
            }

            for (String filterName : filterNames) {
                FullTextFilterMetadataEntity filterMetadataEntity =
                        MetadataManager.INSTANCE.getFullTextFilter(mdTxnCtx, databaseName, dataverseName, filterName);
                if (filterMetadataEntity == null) {
                    throw new CompilationException(ErrorCode.FULL_TEXT_FILTER_NOT_FOUND,
                            stmtCreateConfig.getSourceLocation(), filterName);
                }
            }

            TokenizerCategory tokenizerCategory = stmtCreateConfig.getTokenizerCategory();
            FullTextConfigDescriptor configDescriptor = new FullTextConfigDescriptor(databaseName, dataverseName,
                    configName, tokenizerCategory, filterNames);
            FullTextConfigMetadataEntity configMetadataEntity = new FullTextConfigMetadataEntity(configDescriptor);

            MetadataManager.INSTANCE.addFullTextConfig(mdTxnCtx, configMetadataEntity);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    private void doCreateIndexImpl(IHyracksClientConnection hcc, MetadataProvider metadataProvider, Dataset ds,
            Index index, EnumSet<JobFlag> jobFlags, SourceLocation sourceLoc) throws Exception {
        ProgressState progress = ProgressState.NO_PROGRESS;
        boolean bActiveTxn = true;
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        JobSpecification spec;
        try {
            index.setPendingOp(MetadataUtil.PENDING_ADD_OP);
            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                validateDatasetState(metadataProvider, ds, sourceLoc);
            } else if (ds.getDatasetType() == DatasetType.EXTERNAL) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, dataset() + " using "
                        + ((ExternalDatasetDetails) ds.getDatasetDetails()).getAdapter() + " adapter can't be indexed");
            }

            // check whether there exists another enforced index on the same field
            if (index.isEnforced()) {
                List<List<String>> indexKeyFieldNames;
                List<IAType> indexKeyFieldTypes;
                switch (Index.IndexCategory.of(index.getIndexType())) {
                    case VALUE:
                        Index.ValueIndexDetails valueIndexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
                        indexKeyFieldNames = valueIndexDetails.getKeyFieldNames();
                        indexKeyFieldTypes = valueIndexDetails.getKeyFieldTypes();
                        break;
                    case TEXT:
                        Index.TextIndexDetails textIndexDetails = (Index.TextIndexDetails) index.getIndexDetails();
                        indexKeyFieldNames = textIndexDetails.getKeyFieldNames();
                        indexKeyFieldTypes = textIndexDetails.getKeyFieldTypes();
                        break;
                    default:
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc, "");
                }
                List<Index> indexes =
                        MetadataManager.INSTANCE.getDatasetIndexes(metadataProvider.getMetadataTxnContext(),
                                index.getDatabaseName(), index.getDataverseName(), index.getDatasetName());
                for (Index existingIndex : indexes) {
                    if (!existingIndex.isEnforced()) {
                        continue;
                    }
                    List<List<String>> existingIndexKeyFieldNames;
                    List<IAType> existingIndexKeyFieldTypes;
                    switch (Index.IndexCategory.of(existingIndex.getIndexType())) {
                        case VALUE:
                            Index.ValueIndexDetails valueIndexDetails =
                                    (Index.ValueIndexDetails) existingIndex.getIndexDetails();
                            existingIndexKeyFieldNames = valueIndexDetails.getKeyFieldNames();
                            existingIndexKeyFieldTypes = valueIndexDetails.getKeyFieldTypes();
                            break;
                        case TEXT:
                            Index.TextIndexDetails textIndexDetails =
                                    (Index.TextIndexDetails) existingIndex.getIndexDetails();
                            existingIndexKeyFieldNames = textIndexDetails.getKeyFieldNames();
                            existingIndexKeyFieldTypes = textIndexDetails.getKeyFieldTypes();
                            break;
                        default:
                            // ARRAY indexed cannot be enforced yet.
                            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc, "");
                    }
                    if (existingIndexKeyFieldNames.equals(indexKeyFieldNames)
                            && !existingIndexKeyFieldTypes.equals(indexKeyFieldTypes)) {
                        String fieldNames = indexKeyFieldNames.stream().map(RecordUtil::toFullyQualifiedName)
                                .collect(Collectors.joining(","));
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                                "Cannot create index " + index.getIndexName() + " , enforced index "
                                        + existingIndex.getIndexName() + " on field(s) '"
                                        + LogRedactionUtil.userData(fieldNames) + "' is already defined with type(s) '"
                                        + StringUtils.join(existingIndexKeyFieldTypes, ',') + "'");
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
                FlushDatasetUtil.flushDataset(hcc, metadataProvider, index.getDatabaseName(), index.getDataverseName(),
                        index.getDatasetName());
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
            MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), index.getDatabaseName(),
                    index.getDataverseName(), index.getDatasetName(), index.getIndexName());
            index.setPendingOp(MetadataUtil.PENDING_NO_OP);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
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

                // remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(),
                            index.getDatabaseName(), index.getDataverseName(), index.getDatasetName(),
                            index.getIndexName());
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
        }
    }

    protected void validateIndexType(DatasetType datasetType, IndexType indexType, boolean isSecondaryPrimaryIndex,
            SourceLocation sourceLoc) throws AlgebricksException {
        // disable creating secondary primary index on an external dataset
        if (datasetType == DatasetType.EXTERNAL && isSecondaryPrimaryIndex) {
            throw new CompilationException(ErrorCode.CANNOT_CREATE_SEC_PRIMARY_IDX_ON_EXT_DATASET);
        }
        if (indexType != IndexType.BTREE && isSecondaryPrimaryIndex) {
            throw new CompilationException(ErrorCode.COMPILATION_INCOMPATIBLE_INDEX_TYPE, sourceLoc,
                    String.valueOf(indexType));
        }
    }

    protected void validateIndexFieldType(IndexType indexType, IAType fieldType, List<String> displayFieldName,
            SourceLocation sourceLoc) throws AlgebricksException {
        ValidateUtil.validateIndexFieldType(indexType, fieldType, displayFieldName, sourceLoc);
    }

    protected List<IAType> validatePartitioningExpressions(ARecordType recType, ARecordType metaRecType,
            List<List<String>> partitioningExprs, List<Integer> keySourceIndicators, boolean autogenerated,
            SourceLocation sourceLoc, List<TypeExpression> partitioningExprTypes) throws AlgebricksException {
        return ValidateUtil.validatePartitioningExpressions(recType, metaRecType, partitioningExprs,
                keySourceIndicators, autogenerated, sourceLoc, partitioningExprTypes);
    }

    protected void handleCreateTypeStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        TypeDecl stmtCreateType = (TypeDecl) stmt;
        SourceLocation sourceLoc = stmtCreateType.getSourceLocation();
        String typeName = stmtCreateType.getIdent().getValue();
        metadataProvider.validateDatabaseObjectName(stmtCreateType.getNamespace(), typeName, sourceLoc);
        Namespace stmtActiveNamespace = getActiveNamespace(stmtCreateType.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockUtil.createTypeBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, typeName);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, databaseName, dataverseName, typeName);
            if (dt != null) {
                if (!stmtCreateType.getIfNotExists()) {
                    throw new CompilationException(ErrorCode.TYPE_EXISTS, sourceLoc, typeName);
                }
            } else {
                if (BuiltinTypeMap.getBuiltinType(typeName) != null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Cannot redefine builtin type " + typeName + ".");
                } else if (TypeUtil.isReservedInlineTypeName(typeName)) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Reserved type name " + typeName + ".");
                } else {
                    IAType type =
                            translateType(databaseName, dataverseName, typeName, stmtCreateType.getTypeDef(), mdTxnCtx);
                    MetadataManager.INSTANCE.addDatatype(mdTxnCtx,
                            new Datatype(databaseName, dataverseName, typeName, type, false));
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

    private IAType translateType(String databaseName, DataverseName dataverseName, String typeName,
            TypeExpression typeDef, MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        Map<TypeSignature, IAType> typeMap = TypeTranslator.computeTypes(databaseName, dataverseName, typeName, typeDef,
                databaseName, dataverseName, mdTxnCtx);
        TypeSignature typeSignature = new TypeSignature(databaseName, dataverseName, typeName);
        return typeMap.get(typeSignature);
    }

    protected void handleDatabaseDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        DatabaseDropStatement stmtDropDatabase = (DatabaseDropStatement) stmt;
        SourceLocation sourceLoc = stmtDropDatabase.getSourceLocation();
        String databaseName = stmtDropDatabase.getDatabaseName().getValue();
        metadataProvider.validateDatabaseName(databaseName, sourceLoc);

        if (isSystemDatabase(databaseName) || isDefaultDatabase(databaseName)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    databaseName + " database can't be dropped");
        }
        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropDatabaseBegin(lockManager, metadataProvider.getLocks(), databaseName);
        try {
            doDropDatabase(stmtDropDatabase, metadataProvider, hcc, requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doDropDatabase(DatabaseDropStatement stmtDropDatabase, MetadataProvider mdProvider,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        SourceLocation srcLoc = stmtDropDatabase.getSourceLocation();
        String databaseName = stmtDropDatabase.getDatabaseName().getValue();
        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        mdProvider.setMetadataTxnContext(mdTxnCtx);
        List<FeedEventsListener> stopFeeds = new ArrayList<>();
        List<JobSpecification> dropJobs = new ArrayList<>();
        try {
            Database database = MetadataManager.INSTANCE.getDatabase(mdTxnCtx, databaseName);
            if (database == null) {
                if (stmtDropDatabase.ifExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATABASE, srcLoc, databaseName);
                }
            }

            validateDatabaseStateBeforeDrop(mdProvider, database, srcLoc);

            List<Dataset> datasets =
                    prepareDatabaseDropJobs(mdProvider, srcLoc, databaseName, mdTxnCtx, stopFeeds, dropJobs);

            // #. mark PendingDropOp on the database record by
            // first, deleting the database record from the 'Database' collection
            // second, inserting the database record with the PendingDropOp value into the 'Database' collection
            // Note: the delete operation fails if the database cannot be deleted due to metadata dependencies
            MetadataManager.INSTANCE.dropDatabase(mdTxnCtx, databaseName);
            MetadataManager.INSTANCE.addDatabase(mdTxnCtx,
                    new Database(databaseName, database.isSystemDatabase(), MetadataUtil.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            runDropJobs(mdProvider, hcc, stopFeeds, dropJobs);

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            mdProvider.setMetadataTxnContext(mdTxnCtx);

            // #. finally, delete the database
            MetadataManager.INSTANCE.dropDatabase(mdTxnCtx, databaseName);

            // drop all node groups that no longer needed
            for (Dataset dataset : datasets) {
                String nodeGroup = dataset.getNodeGroupName();
                lockManager.acquireNodeGroupWriteLock(mdProvider.getLocks(), nodeGroup);
                if (MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodeGroup) != null) {
                    MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, nodeGroup, true);
                }
            }

            if (activeNamespace.getDatabaseName().equals(databaseName)) {
                activeNamespace = MetadataBuiltinEntities.DEFAULT_NAMESPACE;
            }

            validateDatasetsStateAfterNamespaceDrop(mdProvider, mdTxnCtx, datasets);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                if (activeNamespace.getDatabaseName().equals(databaseName)) {
                    activeNamespace = MetadataBuiltinEntities.DEFAULT_NAMESPACE;
                }

                // #. execute compensation operations
                // remove the all artifacts in NC
                try {
                    for (JobSpecification jobSpec : dropJobs) {
                        runJob(hcc, jobSpec);
                    }
                } catch (Exception e2) {
                    // do no throw exception since still the metadata needs to be compensated.
                    e.addSuppressed(e2);
                }

                // remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                try {
                    MetadataManager.INSTANCE.dropDatabase(mdTxnCtx, databaseName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending database(" + databaseName
                            + ") couldn't be removed from the metadata", e);
                }
            }
            throw e;
        }
    }

    protected void handleDataverseDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        DataverseDropStatement stmtDropDataverse = (DataverseDropStatement) stmt;
        SourceLocation sourceLoc = stmtDropDataverse.getSourceLocation();
        DataverseName dataverseName = stmtDropDataverse.getDataverseName();
        String databaseName = stmtDropDataverse.getDatabaseName();
        metadataProvider.validateNamespaceName(stmtDropDataverse.getNamespace(), sourceLoc);
        if (isDefaultDataverse(dataverseName) || isMetadataDataverse(dataverseName)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    dataverseName + " " + dataverse() + " can't be dropped");
        }
        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropDataverseBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName);
        try {
            doDropDataverse(stmtDropDataverse, metadataProvider, hcc, requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doDropDataverse(DataverseDropStatement stmtDropDataverse, MetadataProvider metadataProvider,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        SourceLocation sourceLoc = stmtDropDataverse.getSourceLocation();
        DataverseName dataverseName = stmtDropDataverse.getDataverseName();
        String databaseName = stmtDropDataverse.getDatabaseName();
        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        List<FeedEventsListener> feedsToStop = new ArrayList<>();
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        try {
            Database db = MetadataManager.INSTANCE.getDatabase(mdTxnCtx, databaseName);
            if (db == null) {
                if (stmtDropDataverse.getIfExists()) {
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(sourceLoc, ErrorCode.UNKNOWN_DATABASE, databaseName));
                    }
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATABASE, sourceLoc, databaseName);
                }
            }
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                if (stmtDropDataverse.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, MetadataUtil
                            .dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
                }
            }

            if (stmtDropDataverse.getIfEmpty() && isDataverseNotEmpty(databaseName, dataverseName, mdTxnCtx)) {
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return false;
            }

            validateDataverseStateBeforeDrop(metadataProvider, dv, sourceLoc);

            // #. prepare jobs which will drop corresponding feed storage
            ActiveNotificationHandler activeEventHandler =
                    (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
            IActiveEntityEventsListener[] activeListeners = activeEventHandler.getEventListeners();
            for (IActiveEntityEventsListener listener : activeListeners) {
                EntityId activeEntityId = listener.getEntityId();
                if (activeEntityId.getExtensionName().equals(Feed.EXTENSION_NAME)
                        && activeEntityId.getDatabaseName().equals(databaseName)
                        && activeEntityId.getDataverseName().equals(dataverseName)) {
                    FeedEventsListener feedListener = (FeedEventsListener) listener;
                    feedsToStop.add(feedListener);
                    jobsToExecute
                            .add(FeedOperations.buildRemoveFeedStorageJob(metadataProvider, feedListener.getFeed()));
                }
            }

            // #. prepare jobs which will drop corresponding datasets with indexes.
            List<Dataset> datasets =
                    MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, databaseName, dataverseName);
            for (Dataset dataset : datasets) {
                String datasetName = dataset.getDatasetName();
                DatasetType dsType = dataset.getDatasetType();
                switch (dsType) {
                    case INTERNAL:
                        List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, databaseName,
                                dataverseName, datasetName);
                        for (Index index : indexes) {
                            jobsToExecute
                                    .add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, dataset, sourceLoc));
                        }
                        break;
                    case EXTERNAL:
                    case VIEW:
                        break;
                }
            }

            // #. prepare jobs which will drop corresponding libraries.
            List<Library> libraries =
                    MetadataManager.INSTANCE.getDataverseLibraries(mdTxnCtx, databaseName, dataverseName);
            for (Library library : libraries) {
                jobsToExecute.add(ExternalLibraryJobUtils.buildDropLibraryJobSpec(stmtDropDataverse.getNamespace(),
                        library.getName(), metadataProvider));
            }

            jobsToExecute.add(DataverseUtil.dropDataverseJobSpec(dv, metadataProvider));

            // #. mark PendingDropOp on the dataverse record by
            // first, deleting the dataverse record from the DATAVERSE_DATASET
            // second, inserting the dataverse record with the PendingDropOp value into the DATAVERSE_DATASET
            // Note: the delete operation fails if the dataverse cannot be deleted due to metadata dependencies
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, databaseName, dataverseName);
            MetadataManager.INSTANCE.addDataverse(mdTxnCtx,
                    new Dataverse(databaseName, dataverseName, dv.getDataFormat(), MetadataUtil.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            runDropJobs(metadataProvider, hcc, feedsToStop, jobsToExecute);

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. finally, delete the dataverse.
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, databaseName, dataverseName);

            // Drops all node groups that no longer needed
            for (Dataset dataset : datasets) {
                String nodeGroup = dataset.getNodeGroupName();
                lockManager.acquireNodeGroupWriteLock(metadataProvider.getLocks(), nodeGroup);
                if (MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodeGroup) != null) {
                    MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, nodeGroup, true);
                }
            }

            if (activeNamespace.getDataverseName().equals(dataverseName)) {
                activeNamespace = MetadataBuiltinEntities.DEFAULT_NAMESPACE;
            }

            validateDatasetsStateAfterNamespaceDrop(metadataProvider, mdTxnCtx, datasets);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                if (activeNamespace.getDataverseName().equals(dataverseName)) {
                    activeNamespace = MetadataBuiltinEntities.DEFAULT_NAMESPACE;
                }

                // #. execute compensation operations
                // remove the all artifacts in NC
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
                    MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, databaseName, dataverseName);
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

    protected boolean isDataverseNotEmpty(String database, DataverseName dataverseName,
            MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        return MetadataManager.INSTANCE.isDataverseNotEmpty(mdTxnCtx, database, dataverseName);
    }

    protected void validateDatabaseStateBeforeDrop(MetadataProvider metadataProvider, Database database,
            SourceLocation sourceLoc) throws AlgebricksException, HyracksDataException {
        // may be overridden by product extensions for additional checks before dropping the database
    }

    protected void validateDataverseStateBeforeDrop(MetadataProvider metadataProvider, Dataverse dataverse,
            SourceLocation sourceLoc) throws AlgebricksException, HyracksDataException {
        // may be overridden by product extensions for additional checks before dropping the dataverse
    }

    protected void validateDatasetsStateAfterNamespaceDrop(MetadataProvider metadataProvider,
            MetadataTransactionContext mdTxnCtx, List<Dataset> datasets) throws AlgebricksException {
        // may be overridden by product extensions for additional checks after dropping a database/dataverse
    }

    public void handleDatasetDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        DropDatasetStatement stmtDelete = (DropDatasetStatement) stmt;
        SourceLocation sourceLoc = stmtDelete.getSourceLocation();
        String datasetName = stmtDelete.getDatasetName().getValue();
        metadataProvider.validateDatabaseObjectName(stmtDelete.getNamespace(), datasetName, sourceLoc);
        Namespace stmtActiveNamespace = getActiveNamespace(stmtDelete.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropDatasetBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, datasetName);
        try {
            doDropDataset(databaseName, dataverseName, datasetName, metadataProvider, stmtDelete.getIfExists(), hcc,
                    requestParameters, true, sourceLoc);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doDropDataset(String databaseName, DataverseName dataverseName, String datasetName,
            MetadataProvider metadataProvider, boolean ifExists, IHyracksClientConnection hcc,
            IRequestParameters requestParameters, boolean dropCorrespondingNodeGroup, SourceLocation sourceLoc)
            throws Exception {
        MutableObject<ProgressState> progress = new MutableObject<>(ProgressState.NO_PROGRESS);
        MutableObject<MetadataTransactionContext> mdTxnCtx =
                new MutableObject<>(MetadataManager.INSTANCE.beginTransaction());
        MutableBoolean bActiveTxn = new MutableBoolean(true);
        metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        Dataset ds = null;
        try {
            //TODO(DB): also check for database existence?

            // Check if the dataverse exists
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx.getValue(), databaseName, dataverseName);
            if (dv == null) {
                if (ifExists) {
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(sourceLoc, ErrorCode.UNKNOWN_DATAVERSE, MetadataUtil
                                .dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase())));
                    }
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, MetadataUtil
                            .dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
                }
            }
            ds = metadataProvider.findDataset(databaseName, dataverseName, datasetName, true);
            if (ds == null) {
                if (ifExists) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                            MetadataUtil.dataverseName(databaseName, dataverseName,
                                    metadataProvider.isUsingDatabase()));
                }
            } else if (ds.getDatasetType() == DatasetType.VIEW) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            validateDatasetState(metadataProvider, ds, sourceLoc);

            ds.drop(metadataProvider, mdTxnCtx, jobsToExecute, bActiveTxn, progress, hcc, dropCorrespondingNodeGroup,
                    sourceLoc, EnumSet.of(DropOption.IF_EXISTS), requestParameters.isForceDropDataset());

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
            return true;
        } catch (Exception e) {
            LOGGER.error("failed to drop dataset; executing compensating operations", e);
            if (bActiveTxn.booleanValue()) {
                abort(e, e, mdTxnCtx.getValue());
            }

            if (progress.getValue() == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // #. execute compensation operations
                // remove the all indexes in NC
                try {
                    if (ds != null) {
                        jobsToExecute.clear();
                        // start another txn for the compensating operations
                        mdTxnCtx.setValue(MetadataManager.INSTANCE.beginTransaction());
                        bActiveTxn.setValue(true);
                        metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
                        ds.drop(metadataProvider, mdTxnCtx, jobsToExecute, bActiveTxn, progress, hcc,
                                dropCorrespondingNodeGroup, sourceLoc, EnumSet.of(DropOption.IF_EXISTS),
                                requestParameters.isForceDropDataset());
                    }
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
                    MetadataManager.INSTANCE.dropDataset(metadataProvider.getMetadataTxnContext(), databaseName,
                            dataverseName, datasetName, requestParameters.isForceDropDataset());
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx.getValue());
                    throw new IllegalStateException("System is inconsistent state: pending dataset(" + dataverseName
                            + "." + datasetName + ") couldn't be removed from the metadata", e);
                }
            }
            throw e;
        }
    }

    protected void handleIndexDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        IndexDropStatement stmtIndexDrop = (IndexDropStatement) stmt;
        metadataProvider.validateDatabaseObjectName(stmtIndexDrop.getNamespace(),
                stmtIndexDrop.getIndexName().getValue(), stmtIndexDrop.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(stmtIndexDrop.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String datasetName = stmtIndexDrop.getDatasetName().getValue();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropIndexBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, datasetName);
        try {
            doDropIndex(metadataProvider, stmtIndexDrop, databaseName, dataverseName, datasetName, hcc,
                    requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doDropIndex(MetadataProvider metadataProvider, IndexDropStatement stmtIndexDrop,
            String databaseName, DataverseName dataverseName, String datasetName, IHyracksClientConnection hcc,
            IRequestParameters requestParameters) throws Exception {
        SourceLocation sourceLoc = stmtIndexDrop.getSourceLocation();
        String indexName = stmtIndexDrop.getIndexName().getValue();
        ProgressState progress = ProgressState.NO_PROGRESS;
        List<JobSpecification> jobsToExecute = new ArrayList<>();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            Dataset ds = metadataProvider.findDataset(databaseName, dataverseName, datasetName);
            if (ds == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                Index index = MetadataManager.INSTANCE.getIndex(mdTxnCtx, databaseName, dataverseName, datasetName,
                        indexName);
                if (index == null) {
                    if (stmtIndexDrop.getIfExists()) {
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                        return false;
                    } else {
                        throw new CompilationException(ErrorCode.UNKNOWN_INDEX, sourceLoc, indexName);
                    }
                }
                ensureNonPrimaryIndexDrop(index, sourceLoc);
                validateDatasetState(metadataProvider, ds, sourceLoc);
                prepareIndexDrop(metadataProvider, databaseName, dataverseName, datasetName, sourceLoc, indexName,
                        jobsToExecute, mdTxnCtx, ds, index);

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
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, databaseName, dataverseName, datasetName, indexName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
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
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), databaseName,
                            dataverseName, datasetName, indexName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending index(" + dataverseName + "."
                            + datasetName + "." + indexName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        }
    }

    protected void handleFullTextFilterDrop(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        FullTextFilterDropStatement stmtFilterDrop = (FullTextFilterDropStatement) stmt;
        Namespace stmtActiveNamespace = getActiveNamespace(stmtFilterDrop.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String fullTextFilterName = stmtFilterDrop.getFilterName();

        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropFullTextFilterBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                fullTextFilterName);
        try {
            doDropFullTextFilter(metadataProvider, stmtFilterDrop, databaseName, dataverseName, fullTextFilterName);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void doDropFullTextFilter(MetadataProvider metadataProvider, FullTextFilterDropStatement stmtFilterDrop,
            String databaseName, DataverseName dataverseName, String fullTextFilterName)
            throws AlgebricksException, RemoteException {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            FullTextFilterMetadataEntity filter = MetadataManager.INSTANCE.getFullTextFilter(mdTxnCtx, databaseName,
                    dataverseName, fullTextFilterName);
            if (filter == null) {
                if (stmtFilterDrop.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.FULL_TEXT_FILTER_NOT_FOUND,
                            stmtFilterDrop.getSourceLocation(), fullTextFilterName);
                }
            }

            MetadataManager.INSTANCE.dropFullTextFilter(mdTxnCtx, databaseName, dataverseName, fullTextFilterName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected void handleFullTextConfigDrop(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters)
            throws AlgebricksException, RemoteException {
        FullTextConfigDropStatement stmtConfigDrop = (FullTextConfigDropStatement) stmt;
        Namespace stmtActiveNamespace = getActiveNamespace(stmtConfigDrop.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String configName = stmtConfigDrop.getConfigName();

        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropFullTextConfigBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                configName);
        try {
            doDropFullTextConfig(metadataProvider, stmtConfigDrop, databaseName, dataverseName, hcc, requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    private void doDropFullTextConfig(MetadataProvider metadataProvider, FullTextConfigDropStatement stmtConfigDrop,
            String databaseName, DataverseName dataverseName, IHyracksClientConnection hcc,
            IRequestParameters requestParameters) throws RemoteException, AlgebricksException {
        // If the config name is null, then it means the default config
        if (Strings.isNullOrEmpty(stmtConfigDrop.getConfigName())) {
            throw new CompilationException(ErrorCode.FULL_TEXT_DEFAULT_CONFIG_CANNOT_BE_DELETED_OR_CREATED,
                    stmtConfigDrop.getSourceLocation());
        }

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        String fullTextConfigName = stmtConfigDrop.getConfigName();

        try {
            FullTextConfigMetadataEntity configMetadataEntity = MetadataManager.INSTANCE.getFullTextConfig(mdTxnCtx,
                    databaseName, dataverseName, fullTextConfigName);
            if (configMetadataEntity == null) {
                if (stmtConfigDrop.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.FULL_TEXT_CONFIG_NOT_FOUND,
                            stmtConfigDrop.getSourceLocation(), fullTextConfigName);
                }
            }

            MetadataManager.INSTANCE.dropFullTextConfig(mdTxnCtx, databaseName, dataverseName, fullTextConfigName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected void handleTypeDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        TypeDropStatement stmtTypeDrop = (TypeDropStatement) stmt;
        SourceLocation sourceLoc = stmtTypeDrop.getSourceLocation();
        String typeName = stmtTypeDrop.getTypeName().getValue();
        metadataProvider.validateDatabaseObjectName(stmtTypeDrop.getNamespace(), typeName, sourceLoc);
        Namespace stmtActiveNamespace = getActiveNamespace(stmtTypeDrop.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockUtil.dropTypeBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, typeName);
        try {
            // Check if the dataverse exists
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                if (stmtTypeDrop.getIfExists()) {
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(sourceLoc, ErrorCode.UNKNOWN_DATAVERSE, MetadataUtil
                                .dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase())));
                    }
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, MetadataUtil
                            .dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
                }
            }

            Datatype dt = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, databaseName, dataverseName, typeName);
            if (dt == null) {
                if (!stmtTypeDrop.getIfExists()) {
                    throw new CompilationException(ErrorCode.UNKNOWN_TYPE, sourceLoc, typeName);
                }
            } else {
                MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, databaseName, dataverseName, typeName);
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
        metadataProvider.validateDatabaseObjectName(null, nodegroupName, sourceLoc);
        if (isCompileOnly()) {
            return;
        }
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

    public void handleCreateViewStatement(MetadataProvider metadataProvider, Statement stmt,
            IStatementRewriter stmtRewriter, IRequestParameters requestParameters) throws Exception {
        CreateViewStatement cvs = (CreateViewStatement) stmt;
        String viewName = cvs.getViewName();
        metadataProvider.validateDatabaseObjectName(cvs.getNamespace(), viewName, stmt.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(cvs.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();

        String itemTypeDatabaseName;
        DataverseName viewItemTypeDataverseName;
        String viewItemTypeName;
        boolean viewItemTypeAnonymous;
        if (cvs.hasItemType()) {
            Triple<Namespace, String, Boolean> viewTypeQualifiedName = extractDatasetItemTypeName(stmtActiveNamespace,
                    viewName, cvs.getItemType(), false, stmt.getSourceLocation());
            itemTypeDatabaseName = viewTypeQualifiedName.first.getDatabaseName();
            viewItemTypeDataverseName = viewTypeQualifiedName.first.getDataverseName();
            viewItemTypeName = viewTypeQualifiedName.second;
            viewItemTypeAnonymous = viewTypeQualifiedName.third;
        } else {
            itemTypeDatabaseName = MetadataBuiltinEntities.ANY_OBJECT_DATATYPE.getDatabaseName();
            viewItemTypeDataverseName = MetadataBuiltinEntities.ANY_OBJECT_DATATYPE.getDataverseName();
            viewItemTypeName = MetadataBuiltinEntities.ANY_OBJECT_DATATYPE.getDatatypeName();
            viewItemTypeAnonymous = false;
        }

        if (isCompileOnly()) {
            return;
        }
        lockUtil.createDatasetBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, viewName,
                itemTypeDatabaseName, viewItemTypeDataverseName, viewItemTypeName, viewItemTypeAnonymous, null, null,
                null, false, null, null, true, DatasetType.VIEW, null);
        try {
            doCreateView(metadataProvider, cvs, databaseName, dataverseName, viewName, itemTypeDatabaseName,
                    viewItemTypeDataverseName, viewItemTypeName, stmtRewriter, requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
            metadataProvider.setDefaultNamespace(activeNamespace);
        }
    }

    protected CreateResult doCreateView(MetadataProvider metadataProvider, CreateViewStatement cvs, String databaseName,
            DataverseName dataverseName, String viewName, String itemTypeDatabaseName,
            DataverseName itemTypeDataverseName, String itemTypeName, IStatementRewriter stmtRewriter,
            IRequestParameters requestParameters) throws Exception {
        SourceLocation sourceLoc = cvs.getSourceLocation();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            Namespace ns = new Namespace(dv.getDatabaseName(), dv.getDataverseName());
            Dataset existingDataset =
                    MetadataManager.INSTANCE.getDataset(mdTxnCtx, databaseName, dataverseName, viewName);
            if (existingDataset != null) {
                if (DatasetUtil.isNotView(existingDataset)) {
                    throw new CompilationException(ErrorCode.DATASET_EXISTS, sourceLoc,
                            existingDataset.getDatasetName(), existingDataset.getDataverseName());
                }
                if (cvs.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return CreateResult.NOOP;
                } else if (!cvs.getReplaceIfExists()) {
                    throw new CompilationException(ErrorCode.VIEW_EXISTS, sourceLoc,
                            existingDataset.getDatasetFullyQualifiedName());
                }
            }

            DatasetFullyQualifiedName viewQualifiedName =
                    new DatasetFullyQualifiedName(databaseName, dataverseName, viewName);

            Datatype itemTypeEntity = null;
            boolean itemTypeIsInline = false;
            CreateViewStatement.KeyDecl primaryKeyDecl = cvs.getPrimaryKeyDecl();
            List<String> primaryKeyFields = null;
            List<CreateViewStatement.ForeignKeyDecl> foreignKeyDecls = cvs.getForeignKeyDecls();
            List<ViewDetails.ForeignKey> foreignKeys = null;
            String datetimeFormat = null, dateFormat = null, timeFormat = null;
            if (cvs.hasItemType()) {
                Pair<Datatype, Boolean> itemTypePair = fetchDatasetItemType(mdTxnCtx, DatasetType.VIEW,
                        DatasetConfig.DatasetFormat.ROW, null, itemTypeDatabaseName, itemTypeDataverseName,
                        itemTypeName, cvs.getItemType(), false, metadataProvider, sourceLoc);
                itemTypeEntity = itemTypePair.first;
                itemTypeIsInline = itemTypePair.second;
                ARecordType itemType = (ARecordType) itemTypeEntity.getDatatype();
                if (primaryKeyDecl != null) {
                    primaryKeyFields = ValidateUtil.validateViewKeyFields(primaryKeyDecl, itemType, false, sourceLoc);
                }
                if (foreignKeyDecls != null) {
                    foreignKeys = new ArrayList<>(foreignKeyDecls.size());
                    for (CreateViewStatement.ForeignKeyDecl foreignKeyDecl : foreignKeyDecls) {
                        List<String> foreignKeyFields =
                                ValidateUtil.validateViewKeyFields(foreignKeyDecl, itemType, true, sourceLoc);
                        DataverseName refDataverseName = foreignKeyDecl.getReferencedDataverseName();
                        String refDatabaseName = foreignKeyDecl.getReferencedDatabaseName();
                        if (refDataverseName == null) {
                            refDataverseName = dataverseName;
                            refDatabaseName = databaseName;
                        } else {
                            Dataverse refDataverse =
                                    MetadataManager.INSTANCE.getDataverse(mdTxnCtx, refDatabaseName, refDataverseName);
                            if (refDataverse == null) {
                                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc,
                                        MetadataUtil.dataverseName(refDatabaseName, refDataverseName,
                                                metadataProvider.isUsingDatabase()));
                            }
                        }
                        String refDatasetName = foreignKeyDecl.getReferencedDatasetName().getValue();
                        boolean isSelfRef = refDatabaseName.equals(databaseName)
                                && refDataverseName.equals(dataverseName) && refDatasetName.equals(viewName);
                        DatasetType refDatasetType;
                        DatasetFullyQualifiedName refQualifiedName;
                        List<String> refPrimaryKeyFields;
                        if (isSelfRef) {
                            refDatasetType = DatasetType.VIEW;
                            refQualifiedName = viewQualifiedName;
                            refPrimaryKeyFields = primaryKeyFields;
                        } else {
                            // findDataset() will acquire lock on referenced dataset (view)
                            Dataset refDataset = metadataProvider.findDataset(refDatabaseName, refDataverseName,
                                    refDatasetName, true);
                            if (refDataset == null || DatasetUtil.isNotView(refDataset)) {
                                throw new CompilationException(ErrorCode.UNKNOWN_VIEW, sourceLoc,
                                        DatasetUtil.getFullyQualifiedDisplayName(refDataverseName, refDatasetName));
                            }
                            ViewDetails refViewDetails = (ViewDetails) refDataset.getDatasetDetails();
                            refDatasetType = refDataset.getDatasetType();
                            refQualifiedName =
                                    new DatasetFullyQualifiedName(refDatabaseName, refDataverseName, refDatasetName);
                            refPrimaryKeyFields = refViewDetails.getPrimaryKeyFields();
                        }

                        if (refPrimaryKeyFields == null) {
                            throw new CompilationException(ErrorCode.INVALID_FOREIGN_KEY_DEFINITION_REF_PK_NOT_FOUND,
                                    sourceLoc, DatasetUtil.getDatasetTypeDisplayName(refDatasetType),
                                    DatasetUtil.getFullyQualifiedDisplayName(refDataverseName, refDatasetName));
                        } else if (refPrimaryKeyFields.size() != foreignKeyFields.size()) {
                            throw new CompilationException(ErrorCode.INVALID_FOREIGN_KEY_DEFINITION_REF_PK_MISMATCH,
                                    sourceLoc, DatasetUtil.getDatasetTypeDisplayName(refDatasetType),
                                    DatasetUtil.getFullyQualifiedDisplayName(refDataverseName, refDatasetName));
                        } else if (isSelfRef
                                && !OperatorPropertiesUtil.disjoint(refPrimaryKeyFields, foreignKeyFields)) {
                            throw new CompilationException(ErrorCode.INVALID_FOREIGN_KEY_DEFINITION, sourceLoc);
                        }

                        foreignKeys.add(new ViewDetails.ForeignKey(foreignKeyFields, refQualifiedName));
                    }
                }

                Map<String, String> viewConfig =
                        TypeUtil.validateConfiguration(cvs.getViewConfiguration(), cvs.getSourceLocation());
                datetimeFormat = TypeUtil.getDatetimeFormat(viewConfig);
                dateFormat = TypeUtil.getDateFormat(viewConfig);
                timeFormat = TypeUtil.getTimeFormat(viewConfig);

            } else {
                if (primaryKeyDecl != null) {
                    throw new CompilationException(ErrorCode.INVALID_PRIMARY_KEY_DEFINITION, cvs.getSourceLocation());
                }
                if (foreignKeyDecls != null) {
                    throw new CompilationException(ErrorCode.INVALID_FOREIGN_KEY_DEFINITION, cvs.getSourceLocation());
                }
                if (cvs.getViewConfiguration() != null) {
                    throw new CompilationException(ErrorCode.ILLEGAL_SET_PARAMETER, cvs.getSourceLocation(),
                            cvs.getViewConfiguration().keySet().iterator().next());
                }
            }

            if (existingDataset != null) {
                ViewDetails existingViewDetails = (ViewDetails) existingDataset.getDatasetDetails();
                List<String> existingPrimaryKeyFields = existingViewDetails.getPrimaryKeyFields();
                // For now don't allow view replacement if existing view has primary keys and they are different
                // from the new view's primary keys, because there could be another view that references
                // these primary keys via its foreign keys declaration.
                // In the future we should relax this check: scan datasets metadata and allow replacement in this case
                // if there's no view that references this view
                boolean allowToReplace =
                        existingPrimaryKeyFields == null || existingPrimaryKeyFields.equals(primaryKeyFields);
                if (!allowToReplace) {
                    throw new CompilationException(ErrorCode.CANNOT_CHANGE_PRIMARY_KEY, cvs.getSourceLocation(),
                            DatasetUtil.getDatasetTypeDisplayName(existingDataset.getDatasetType()),
                            DatasetUtil.getFullyQualifiedDisplayName(existingDataset));
                }
            }

            // Check whether the view is usable:
            // create a view declaration for this function,
            // and a query body that queries this view:
            ViewDecl viewDecl = new ViewDecl(viewQualifiedName, cvs.getViewBodyExpression());
            viewDecl.setSourceLocation(sourceLoc);
            IQueryRewriter queryRewriter = rewriterFactory.createQueryRewriter();
            Query wrappedQuery =
                    queryRewriter.createViewAccessorQuery(viewDecl, metadataProvider.getNamespaceResolver());
            metadataProvider.setDefaultNamespace(ns);
            LangRewritingContext langRewritingContext = createLangRewritingContext(metadataProvider, declaredFunctions,
                    Collections.singletonList(viewDecl), warningCollector, wrappedQuery.getVarCounter());
            apiFramework.reWriteQuery(langRewritingContext, wrappedQuery, sessionOutput, false, false,
                    Collections.emptyList());

            List<List<DependencyFullyQualifiedName>> dependencies =
                    ViewUtil.getViewDependencies(viewDecl, foreignKeys, queryRewriter);

            ViewDetails viewDetails = new ViewDetails(cvs.getViewBody(), dependencies, cvs.getDefaultNull(),
                    primaryKeyFields, foreignKeys, datetimeFormat, dateFormat, timeFormat);

            Dataset view =
                    new Dataset(databaseName, dataverseName, viewName, itemTypeDatabaseName, itemTypeDataverseName,
                            itemTypeName, MetadataConstants.METADATA_NODEGROUP_NAME, "", Collections.emptyMap(),
                            viewDetails, Collections.emptyMap(), DatasetType.VIEW, 0, MetadataUtil.PENDING_NO_OP);
            if (existingDataset == null) {
                if (itemTypeIsInline) {
                    MetadataManager.INSTANCE.addDatatype(mdTxnCtx, itemTypeEntity);
                }
                MetadataManager.INSTANCE.addDataset(mdTxnCtx, view);
            } else {
                if (itemTypeIsInline) {
                    MetadataManager.INSTANCE.updateDatatype(mdTxnCtx, itemTypeEntity);
                }
                MetadataManager.INSTANCE.updateDataset(mdTxnCtx, view);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return existingDataset != null ? CreateResult.REPLACED : CreateResult.CREATED;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    public void handleViewDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        ViewDropStatement stmtDrop = (ViewDropStatement) stmt;
        SourceLocation sourceLoc = stmtDrop.getSourceLocation();
        String viewName = stmtDrop.getViewName().getValue();
        metadataProvider.validateDatabaseObjectName(stmtDrop.getNamespace(), viewName, sourceLoc);
        Namespace stmtActiveNamespace = getActiveNamespace(stmtDrop.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropDatasetBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, viewName);
        try {
            doDropView(metadataProvider, stmtDrop, databaseName, dataverseName, viewName);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doDropView(MetadataProvider metadataProvider, ViewDropStatement stmtViewDrop, String databaseName,
            DataverseName dataverseName, String viewName) throws Exception {
        SourceLocation sourceLoc = stmtViewDrop.getSourceLocation();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            // Check if the dataverse exists
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                if (stmtViewDrop.getIfExists()) {
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(stmtViewDrop.getSourceLocation(), ErrorCode.UNKNOWN_DATAVERSE,
                                MetadataUtil.dataverseName(databaseName, dataverseName,
                                        metadataProvider.isUsingDatabase())));
                    }
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, MetadataUtil
                            .dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
                }
            }
            Dataset dataset = metadataProvider.findDataset(databaseName, dataverseName, viewName, true);
            if (dataset == null) {
                if (stmtViewDrop.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_VIEW, sourceLoc,
                            DatasetUtil.getFullyQualifiedDisplayName(dataverseName, viewName));
                }
            } else if (DatasetUtil.isNotView(dataset)) {
                throw new CompilationException(ErrorCode.UNKNOWN_VIEW, sourceLoc,
                        DatasetUtil.getFullyQualifiedDisplayName(dataverseName, viewName));
            }
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx, databaseName, dataverseName, viewName, false);
            String itemTypeDatabaseName = dataset.getItemTypeDatabaseName();
            if (TypeUtil.isDatasetInlineTypeName(dataset, itemTypeDatabaseName, dataset.getItemTypeDataverseName(),
                    dataset.getItemTypeName())) {
                MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, itemTypeDatabaseName,
                        dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected void handleDeclareFunctionStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        FunctionDecl fds = (FunctionDecl) stmt;
        FunctionSignature signature = fds.getSignature();
        DataverseName funDataverse = signature.getDataverseName();
        Namespace funNamespace = signature.getNamespace();
        metadataProvider.validateDatabaseObjectName(funNamespace, signature.getName(), stmt.getSourceLocation());
        if (funDataverse == null) {
            signature.setDataverseName(activeNamespace.getDatabaseName(), activeNamespace.getDataverseName());
        }
        declaredFunctions.add(fds);
    }

    public void handleCreateFunctionStatement(MetadataProvider metadataProvider, Statement stmt,
            IStatementRewriter stmtRewriter, IRequestParameters requestParameters) throws Exception {
        CreateFunctionStatement cfs = (CreateFunctionStatement) stmt;
        FunctionSignature signature = cfs.getFunctionSignature();
        DataverseName funDataverse = signature.getDataverseName();
        Namespace funNamespace = signature.getNamespace();
        metadataProvider.validateDatabaseObjectName(funNamespace, signature.getName(), stmt.getSourceLocation());
        DataverseName dataverseName;
        String databaseName;
        if (funDataverse == null) {
            dataverseName = activeNamespace.getDataverseName();
            databaseName = activeNamespace.getDatabaseName();
            signature.setDataverseName(databaseName, dataverseName);
        } else {
            dataverseName = funDataverse;
            databaseName = signature.getDatabaseName();
        }
        DataverseName libraryDataverseName = null;
        String libraryDatabaseName = null;
        String libraryName = cfs.getLibraryName();
        if (libraryName != null) {
            libraryDataverseName = cfs.getLibraryDataverseName();
            libraryDatabaseName = cfs.getLibraryDatabaseName();
            if (libraryDataverseName == null) {
                libraryDataverseName = dataverseName;
                libraryDatabaseName = databaseName;
            }
        }
        if (isCompileOnly()) {
            return;
        }
        lockUtil.createFunctionBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                signature.getName(), libraryDatabaseName, libraryDataverseName, libraryName);
        try {
            doCreateFunction(metadataProvider, cfs, signature, stmtRewriter, requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
            metadataProvider.setDefaultNamespace(activeNamespace);
        }
    }

    protected CreateResult doCreateFunction(MetadataProvider metadataProvider, CreateFunctionStatement cfs,
            FunctionSignature functionSignature, IStatementRewriter stmtRewriter, IRequestParameters requestParameters)
            throws Exception {
        DataverseName dataverseName = functionSignature.getDataverseName();
        String databaseName = functionSignature.getDatabaseName();
        SourceLocation sourceLoc = cfs.getSourceLocation();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            Namespace ns = new Namespace(dv.getDatabaseName(), dv.getDataverseName());
            List<TypeSignature> existingInlineTypes;
            Function existingFunction = MetadataManager.INSTANCE.getFunction(mdTxnCtx, functionSignature);
            if (existingFunction != null) {
                if (cfs.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return CreateResult.NOOP;
                } else if (!cfs.getReplaceIfExists()) {
                    throw new CompilationException(ErrorCode.FUNCTION_EXISTS, cfs.getSourceLocation(),
                            functionSignature.toString(false));
                }
                existingInlineTypes = TypeUtil.getFunctionInlineTypes(existingFunction);
            } else {
                existingInlineTypes = Collections.emptyList();
            }

            IQueryRewriter queryRewriter = rewriterFactory.createQueryRewriter();
            Map<TypeSignature, Datatype> newInlineTypes;
            Function function;
            if (cfs.isExternal()) {
                if (functionSignature.getArity() == FunctionIdentifier.VARARGS) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, cfs.getSourceLocation(),
                            "Variable number of parameters is not supported for external functions");
                }
                List<Pair<VarIdentifier, TypeExpression>> paramList = cfs.getParameters();
                int paramCount = paramList.size();
                List<String> paramNames = new ArrayList<>(paramCount);
                List<TypeSignature> paramTypes = new ArrayList<>(paramCount);
                LinkedHashSet<TypeSignature> depTypes = new LinkedHashSet<>();
                newInlineTypes = new HashMap<>();

                for (int i = 0; i < paramCount; i++) {
                    Pair<VarIdentifier, TypeExpression> paramPair = paramList.get(i);
                    TypeSignature paramTypeSignature;
                    TypeSignature paramDepTypeSignature;
                    Datatype paramInlineTypeEntity;
                    TypeExpression paramTypeExpr = paramPair.getSecond();
                    if (paramTypeExpr != null) {
                        Triple<TypeSignature, TypeSignature, Datatype> paramTypeInfo = translateFunctionParameterType(
                                functionSignature, i, paramTypeExpr, sourceLoc, metadataProvider, mdTxnCtx);
                        paramTypeSignature = paramTypeInfo.first;
                        paramDepTypeSignature = paramTypeInfo.second;
                        paramInlineTypeEntity = paramTypeInfo.third;
                    } else {
                        paramTypeSignature = null; // == any
                        paramDepTypeSignature = null;
                        paramInlineTypeEntity = null;
                    }
                    paramTypes.add(paramTypeSignature); // null == any
                    if (paramDepTypeSignature != null) {
                        depTypes.add(paramDepTypeSignature);
                    }
                    if (paramInlineTypeEntity != null) {
                        newInlineTypes.put(paramTypeSignature, paramInlineTypeEntity);
                    }
                    VarIdentifier paramName = paramPair.getFirst();
                    paramNames.add(queryRewriter.toFunctionParameterName(paramName));
                }

                TypeSignature returnTypeSignature;
                TypeSignature returnDepTypeSignature;
                Datatype returnInlineTypeEntity;
                TypeExpression returnTypeExpr = cfs.getReturnType();
                if (returnTypeExpr != null) {
                    Triple<TypeSignature, TypeSignature, Datatype> returnTypeInfo = translateFunctionParameterType(
                            functionSignature, -1, returnTypeExpr, sourceLoc, metadataProvider, mdTxnCtx);
                    returnTypeSignature = returnTypeInfo.first;
                    returnDepTypeSignature = returnTypeInfo.second;
                    returnInlineTypeEntity = returnTypeInfo.third;
                } else {
                    returnTypeSignature = null; // == any
                    returnDepTypeSignature = null;
                    returnInlineTypeEntity = null;
                }
                if (returnDepTypeSignature != null) {
                    depTypes.add(returnDepTypeSignature);
                }
                if (returnInlineTypeEntity != null) {
                    newInlineTypes.put(returnTypeSignature, returnInlineTypeEntity);
                }

                DataverseName libraryDataverseName = cfs.getLibraryDataverseName();
                String libraryDatabaseName = cfs.getLibraryDatabaseName();
                if (libraryDataverseName == null) {
                    libraryDataverseName = dataverseName;
                    libraryDatabaseName = databaseName;
                }
                String libraryName = cfs.getLibraryName();
                Library library = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, libraryDatabaseName,
                        libraryDataverseName, libraryName);
                if (library == null) {
                    throw new CompilationException(ErrorCode.UNKNOWN_LIBRARY, sourceLoc, libraryName);
                }

                ExternalFunctionLanguage language =
                        ExternalFunctionCompilerUtil.getExternalFunctionLanguage(library.getLanguage());
                List<String> externalIdentifier = cfs.getExternalIdentifier();
                ExternalFunctionCompilerUtil.validateExternalIdentifier(externalIdentifier, language,
                        cfs.getSourceLocation());
                List<List<DependencyFullyQualifiedName>> dependencies =
                        FunctionUtil.getExternalFunctionDependencies(depTypes);

                function = new Function(functionSignature, paramNames, paramTypes, returnTypeSignature, null,
                        FunctionKind.SCALAR.toString(), library.getLanguage(), libraryDatabaseName,
                        libraryDataverseName, libraryName, externalIdentifier, cfs.getNullCall(),
                        cfs.getDeterministic(), cfs.getResources(), dependencies);
            } else {
                List<Pair<VarIdentifier, TypeExpression>> paramList = cfs.getParameters();
                int paramCount = paramList.size();
                List<VarIdentifier> paramVars = new ArrayList<>(paramCount);
                List<String> paramNames = new ArrayList<>(paramCount);
                for (Pair<VarIdentifier, TypeExpression> paramPair : paramList) {
                    VarIdentifier paramName = paramPair.getFirst();
                    paramVars.add(paramName);
                    paramNames.add(queryRewriter.toFunctionParameterName(paramName));
                    if (paramPair.getSecond() != null) {
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc,
                                paramName.toString());
                    }
                }

                // Check whether the function is usable:
                // create a function declaration for this function,
                // and a query body calls this function with each argument set to 'missing'
                FunctionDecl fd = new FunctionDecl(functionSignature, paramVars, cfs.getFunctionBodyExpression(), true);
                fd.setSourceLocation(sourceLoc);

                Query wrappedQuery = queryRewriter.createFunctionAccessorQuery(fd);
                List<FunctionDecl> fdList = new ArrayList<>(declaredFunctions.size() + 1);
                fdList.addAll(declaredFunctions);
                fdList.add(fd);
                metadataProvider.setDefaultNamespace(ns);
                LangRewritingContext langRewritingContext = createLangRewritingContext(metadataProvider, fdList, null,
                        warningCollector, wrappedQuery.getVarCounter());
                apiFramework.reWriteQuery(langRewritingContext, wrappedQuery, sessionOutput, false, false,
                        Collections.emptyList());

                List<List<DependencyFullyQualifiedName>> dependencies =
                        FunctionUtil.getFunctionDependencies(fd, queryRewriter);

                newInlineTypes = Collections.emptyMap();
                function = new Function(functionSignature, paramNames, null, null, cfs.getFunctionBody(),
                        FunctionKind.SCALAR.toString(), compilationProvider.getParserFactory().getLanguage(), null,
                        null, null, null, null, null, null, dependencies);
            }

            if (existingFunction == null) {
                // add new function and its inline types
                for (Datatype newInlineType : newInlineTypes.values()) {
                    MetadataManager.INSTANCE.addDatatype(mdTxnCtx, newInlineType);
                }
                MetadataManager.INSTANCE.addFunction(mdTxnCtx, function);
            } else {
                // replace existing function and its inline types
                for (TypeSignature existingInlineType : existingInlineTypes) {
                    Datatype newInlineType =
                            newInlineTypes.isEmpty() ? null : newInlineTypes.remove(existingInlineType);
                    if (newInlineType == null) {
                        MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, existingInlineType.getDatabaseName(),
                                existingInlineType.getDataverseName(), existingInlineType.getName());
                    } else {
                        MetadataManager.INSTANCE.updateDatatype(mdTxnCtx, newInlineType);
                    }
                }
                for (Datatype inlineType : newInlineTypes.values()) {
                    MetadataManager.INSTANCE.addDatatype(mdTxnCtx, inlineType);
                }
                MetadataManager.INSTANCE.updateFunction(mdTxnCtx, function);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Installed function: " + functionSignature);
            }
            return existingFunction != null ? CreateResult.REPLACED : CreateResult.CREATED;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    private Triple<TypeSignature, TypeSignature, Datatype> translateFunctionParameterType(
            FunctionSignature functionSignature, int paramIdx, TypeExpression paramTypeExpr, SourceLocation sourceLoc,
            MetadataProvider metadataProvider, MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        TypeSignature paramTypeSignature, depTypeSignature;
        Datatype paramInlineTypeEntity = null;
        switch (paramTypeExpr.getTypeKind()) {
            case TYPEREFERENCE:
                TypeReferenceExpression paramTypeRefExpr = (TypeReferenceExpression) paramTypeExpr;
                String paramTypeName = paramTypeRefExpr.getIdent().second.getValue();
                BuiltinType builtinType = BuiltinTypeMap.getBuiltinType(paramTypeName);
                if (builtinType != null) {
                    // built-in type
                    paramTypeSignature = new TypeSignature(builtinType);
                    depTypeSignature = null;
                } else {
                    // user-defined type
                    Namespace paramTypeRefNamespace = paramTypeRefExpr.getIdent().first;
                    DataverseName paramTypeDataverseName;
                    String paramTypeDatabaseName;
                    if (paramTypeRefNamespace == null) {
                        paramTypeDataverseName = functionSignature.getDataverseName();
                        paramTypeDatabaseName = functionSignature.getDatabaseName();
                    } else {
                        paramTypeDataverseName = paramTypeRefNamespace.getDataverseName();
                        paramTypeDatabaseName = paramTypeRefNamespace.getDatabaseName();
                    }

                    Datatype paramTypeEntity = metadataProvider.findTypeEntity(paramTypeDatabaseName,
                            paramTypeDataverseName, paramTypeName);
                    if (paramTypeEntity == null || paramTypeEntity.getIsAnonymous()) {
                        throw new CompilationException(ErrorCode.UNKNOWN_TYPE, sourceLoc, paramTypeName);
                    }
                    paramTypeSignature = depTypeSignature =
                            new TypeSignature(paramTypeDatabaseName, paramTypeDataverseName, paramTypeName);
                }
                break;
            case ORDEREDLIST:
            case UNORDEREDLIST:
                DataverseName paramTypeDataverseName = functionSignature.getDataverseName();
                String paramTypeDatabaseName = functionSignature.getDatabaseName();
                paramTypeName = TypeUtil.createFunctionParameterTypeName(functionSignature.getName(),
                        functionSignature.getArity(), paramIdx);
                IAType paramType = translateType(paramTypeDatabaseName, paramTypeDataverseName, paramTypeName,
                        paramTypeExpr, mdTxnCtx);
                paramTypeSignature = new TypeSignature(paramTypeDatabaseName, paramTypeDataverseName, paramTypeName);
                depTypeSignature = FunctionUtil.getTypeDependencyFromFunctionParameter(paramTypeExpr,
                        paramTypeDatabaseName, paramTypeDataverseName);
                paramInlineTypeEntity =
                        new Datatype(paramTypeDatabaseName, paramTypeDataverseName, paramTypeName, paramType, true);
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc);
        }

        return new Triple<>(paramTypeSignature, depTypeSignature, paramInlineTypeEntity);
    }

    protected void handleFunctionDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IRequestParameters requestParameters) throws Exception {
        FunctionDropStatement stmtDropFunction = (FunctionDropStatement) stmt;
        FunctionSignature signature = stmtDropFunction.getFunctionSignature();
        DataverseName funDataverse = signature.getDataverseName();
        Namespace funNamespace = signature.getNamespace();
        metadataProvider.validateDatabaseObjectName(funNamespace, signature.getName(),
                stmtDropFunction.getSourceLocation());
        DataverseName dataverseName;
        String databaseName;
        if (funDataverse == null) {
            dataverseName = activeNamespace.getDataverseName();
            databaseName = activeNamespace.getDatabaseName();
            signature.setDataverseName(databaseName, dataverseName);
        } else {
            dataverseName = funDataverse;
            databaseName = signature.getDatabaseName();
        }
        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropFunctionBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                signature.getName());
        try {
            doDropFunction(metadataProvider, stmtDropFunction, signature, requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doDropFunction(MetadataProvider metadataProvider, FunctionDropStatement stmtDropFunction,
            FunctionSignature signature, IRequestParameters requestParameters) throws Exception {
        DataverseName dataverseName = signature.getDataverseName();
        String databaseName = signature.getDatabaseName();
        SourceLocation sourceLoc = stmtDropFunction.getSourceLocation();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dataverse == null) {
                if (stmtDropFunction.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, MetadataUtil
                            .dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
                }
            }
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
            if (function == null) {
                if (stmtDropFunction.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, sourceLoc, signature.toString());
                }
            }

            List<TypeSignature> inlineTypes = TypeUtil.getFunctionInlineTypes(function);

            MetadataManager.INSTANCE.dropFunction(mdTxnCtx, signature);
            for (TypeSignature inlineType : inlineTypes) {
                MetadataManager.INSTANCE.dropDatatype(mdTxnCtx, inlineType.getDatabaseName(),
                        inlineType.getDataverseName(), inlineType.getName());
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected void handleCreateAdapterStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        CreateAdapterStatement cas = (CreateAdapterStatement) stmt;
        String adapterName = cas.getAdapterName();
        metadataProvider.validateDatabaseObjectName(cas.getNamespace(), adapterName, cas.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(cas.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        DataverseName libraryDataverseName = cas.getLibraryDataverseName();
        String libraryDatabaseName = cas.getLibraryDatabaseName();
        if (libraryDataverseName == null) {
            libraryDataverseName = dataverseName;
            libraryDatabaseName = databaseName;
        }
        String libraryName = cas.getLibraryName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.createAdapterBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, adapterName,
                libraryDatabaseName, libraryDataverseName, libraryName);
        try {
            doCreateAdapter(metadataProvider, databaseName, dataverseName, cas);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void doCreateAdapter(MetadataProvider metadataProvider, String databaseName, DataverseName dataverseName,
            CreateAdapterStatement cas) throws Exception {
        SourceLocation sourceLoc = cas.getSourceLocation();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            String adapterName = cas.getAdapterName();
            DatasourceAdapter adapter =
                    MetadataManager.INSTANCE.getAdapter(mdTxnCtx, databaseName, dataverseName, adapterName);
            if (adapter != null) {
                if (cas.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                }
                throw new CompilationException(ErrorCode.ADAPTER_EXISTS, sourceLoc, adapterName);
            }

            DataverseName libraryDataverseName = cas.getLibraryDataverseName();
            String libraryDatabaseName = cas.getLibraryDatabaseName();
            if (libraryDataverseName == null) {
                libraryDataverseName = dataverseName;
                libraryDatabaseName = databaseName;
            }
            String libraryName = cas.getLibraryName();
            Library library = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, libraryDatabaseName, libraryDataverseName,
                    libraryName);
            if (library == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_LIBRARY, sourceLoc, libraryName);
            }
            // Add adapters
            ExternalFunctionLanguage language =
                    ExternalFunctionCompilerUtil.getExternalFunctionLanguage(library.getLanguage());
            List<String> externalIdentifier = cas.getExternalIdentifier();
            ExternalFunctionCompilerUtil.validateExternalIdentifier(externalIdentifier, language,
                    cas.getSourceLocation());

            if (language != ExternalFunctionLanguage.JAVA) {
                throw new CompilationException(ErrorCode.UNSUPPORTED_ADAPTER_LANGUAGE, cas.getSourceLocation(),
                        language.name());
            }
            String adapterFactoryClass = externalIdentifier.get(0);

            adapter = new DatasourceAdapter(new AdapterIdentifier(databaseName, dataverseName, adapterName),
                    IDataSourceAdapter.AdapterType.EXTERNAL, adapterFactoryClass, libraryDatabaseName,
                    libraryDataverseName, libraryName);
            MetadataManager.INSTANCE.addAdapter(mdTxnCtx, adapter);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Installed adapter: " + adapterName);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected void handleAdapterDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        AdapterDropStatement stmtDropAdapter = (AdapterDropStatement) stmt;
        SourceLocation sourceLoc = stmtDropAdapter.getSourceLocation();
        String adapterName = stmtDropAdapter.getAdapterName();
        metadataProvider.validateDatabaseObjectName(stmtDropAdapter.getNamespace(), adapterName, sourceLoc);
        Namespace stmtActiveNamespace = getActiveNamespace(stmtDropAdapter.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropAdapterBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, adapterName);
        try {
            doDropAdapter(metadataProvider, stmtDropAdapter, databaseName, dataverseName, adapterName);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doDropAdapter(MetadataProvider metadataProvider, AdapterDropStatement stmtDropAdapter,
            String databaseName, DataverseName dataverseName, String adapterName) throws Exception {
        SourceLocation sourceLoc = stmtDropAdapter.getSourceLocation();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dataverse == null) {
                if (stmtDropAdapter.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc, MetadataUtil
                            .dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
                }
            }
            DatasourceAdapter adapter =
                    MetadataManager.INSTANCE.getAdapter(mdTxnCtx, databaseName, dataverseName, adapterName);
            if (adapter == null) {
                if (stmtDropAdapter.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_ADAPTER, sourceLoc, adapterName);
                }
            }

            MetadataManager.INSTANCE.dropAdapter(mdTxnCtx, databaseName, dataverseName, adapterName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected void handleCreateLibraryStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        CreateLibraryStatement cls = (CreateLibraryStatement) stmt;
        metadataProvider.validateDatabaseObjectName(cls.getNamespace(), cls.getLibraryName(), cls.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(cls.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String libraryName = cls.getLibraryName();
        String libraryHash = cls.getHash();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.createLibraryBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, libraryName);
        try {
            doCreateLibrary(metadataProvider, databaseName, dataverseName, libraryName, libraryHash, cls, hcc,
                    requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected CreateResult doCreateLibrary(MetadataProvider metadataProvider, String databaseName,
            DataverseName dataverseName, String libraryName, String libraryHash, CreateLibraryStatement cls,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        JobUtils.ProgressState progress = ProgressState.NO_PROGRESS;
        boolean prepareJobSuccessful = false;
        JobSpecification abortJobSpec = null;
        Library existingLibrary = null;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Namespace stmtActiveNamespace = getActiveNamespace(cls.getNamespace());
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            ExternalFunctionLanguage language = cls.getLang();
            existingLibrary = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, databaseName, dataverseName, libraryName);
            if (existingLibrary != null && !cls.getReplaceIfExists()) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                        "A library with this name " + libraryName + " already exists.");
            }

            // #. add/update library with PendingAddOp
            Library libraryPendingAdd = new Library(databaseName, dataverseName, libraryName, language.name(),
                    libraryHash, MetadataUtil.PENDING_ADD_OP);
            if (existingLibrary == null) {
                MetadataManager.INSTANCE.addLibrary(mdTxnCtx, libraryPendingAdd);
            } else {
                MetadataManager.INSTANCE.updateLibrary(mdTxnCtx, libraryPendingAdd);
            }

            // #. prepare to create library artifacts in NC.
            Triple<JobSpecification, JobSpecification, JobSpecification> jobSpecs =
                    ExternalLibraryJobUtils.buildCreateLibraryJobSpec(stmtActiveNamespace, libraryName, language,
                            cls.getLocation(), cls.getAuthToken(), metadataProvider);
            JobSpecification prepareJobSpec = jobSpecs.first;
            JobSpecification commitJobSpec = jobSpecs.second;
            abortJobSpec = jobSpecs.third;

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            // #. create library artifacts in NCs.
            runJob(hcc, prepareJobSpec, jobFlags);
            prepareJobSuccessful = true;
            runJob(hcc, commitJobSpec, jobFlags);

            // #. begin new metadataTxn
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            Library newLibrary = new Library(databaseName, dataverseName, libraryName, language.name(), libraryHash,
                    MetadataUtil.PENDING_NO_OP);
            MetadataManager.INSTANCE.updateLibrary(mdTxnCtx, newLibrary);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return existingLibrary != null ? CreateResult.REPLACED : CreateResult.CREATED;
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                boolean undoFailure = false;
                if (!prepareJobSuccessful) {
                    // 'prepare' job failed -> try running 'abort' job
                    try {
                        runJob(hcc, abortJobSpec, jobFlags);
                    } catch (Exception e2) {
                        e.addSuppressed(e2);
                        undoFailure = true;
                    }
                } else if (existingLibrary == null) {
                    // 'commit' job failed for a new library -> try removing the library
                    try {
                        JobSpecification dropLibraryJobSpec = ExternalLibraryJobUtils
                                .buildDropLibraryJobSpec(stmtActiveNamespace, libraryName, metadataProvider);
                        runJob(hcc, dropLibraryJobSpec, jobFlags);
                    } catch (Exception e2) {
                        e.addSuppressed(e2);
                        undoFailure = true;
                    }
                } else {
                    // 'commit' job failed for an existing library -> bad state
                    undoFailure = true;
                }

                // revert/remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                try {
                    if (existingLibrary == null) {
                        MetadataManager.INSTANCE.dropLibrary(mdTxnCtx, databaseName, dataverseName, libraryName);
                    } else {
                        MetadataManager.INSTANCE.updateLibrary(mdTxnCtx, existingLibrary);
                    }
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending library(" + libraryName
                            + ") couldn't be reverted/removed from the metadata", e);
                }

                if (undoFailure) {
                    throw new IllegalStateException(
                            "System is inconsistent state: library(" + libraryName + ") couldn't be deployed", e);
                }
            }
            throw e;
        }
    }

    protected void handleLibraryDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        LibraryDropStatement stmtDropLibrary = (LibraryDropStatement) stmt;
        String libraryName = stmtDropLibrary.getLibraryName();
        metadataProvider.validateDatabaseObjectName(stmtDropLibrary.getNamespace(), libraryName,
                stmtDropLibrary.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(stmtDropLibrary.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropLibraryBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, libraryName);
        try {
            doDropLibrary(metadataProvider, stmtDropLibrary, databaseName, dataverseName, libraryName, hcc,
                    requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doDropLibrary(MetadataProvider metadataProvider, LibraryDropStatement stmtDropLibrary,
            String databaseName, DataverseName dataverseName, String libraryName, IHyracksClientConnection hcc,
            IRequestParameters requestParameters) throws Exception {
        JobUtils.ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Namespace stmtActiveNamespace = getActiveNamespace(stmtDropLibrary.getNamespace());
        try {
            Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dataverse == null) {
                if (stmtDropLibrary.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, stmtDropLibrary.getSourceLocation(),
                            MetadataUtil.dataverseName(databaseName, dataverseName,
                                    metadataProvider.isUsingDatabase()));
                }
            }
            Library library = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, databaseName, dataverseName, libraryName);
            if (library == null) {
                if (stmtDropLibrary.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.UNKNOWN_LIBRARY, stmtDropLibrary.getSourceLocation(),
                            libraryName);
                }
            }

            // #. mark the existing library as PendingDropOp
            // do drop instead of update because drop will fail if the library is used by functions/adapters
            MetadataManager.INSTANCE.dropLibrary(mdTxnCtx, databaseName, dataverseName, libraryName);
            MetadataManager.INSTANCE.addLibrary(mdTxnCtx, new Library(databaseName, dataverseName, libraryName,
                    library.getLanguage(), library.getHash(), MetadataUtil.PENDING_DROP_OP));

            // #. drop library artifacts in NCs.
            JobSpecification jobSpec =
                    ExternalLibraryJobUtils.buildDropLibraryJobSpec(stmtActiveNamespace, libraryName, metadataProvider);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            // #. drop library artifacts in NCs.
            runJob(hcc, jobSpec, jobFlags);

            // #. begin new metadataTxn
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. drop library
            MetadataManager.INSTANCE.dropLibrary(mdTxnCtx, databaseName, dataverseName, libraryName);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                try {
                    MetadataManager.INSTANCE.dropLibrary(mdTxnCtx, databaseName, dataverseName, libraryName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending library(" + libraryName
                            + ") couldn't be removed from the metadata", e);
                }
            }
            throw e;
        }
    }

    protected void handleCreateSynonymStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        CreateSynonymStatement css = (CreateSynonymStatement) stmt;
        metadataProvider.validateDatabaseObjectName(css.getNamespace(), css.getSynonymName(), css.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(css.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String synonymName = css.getSynonymName();
        Namespace objectNamespace = css.getObjectNamespace() != null ? css.getObjectNamespace() : stmtActiveNamespace;
        String objectName = css.getObjectName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.createSynonymBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, synonymName);
        try {
            doCreateSynonym(metadataProvider, css, stmtActiveNamespace, synonymName, objectNamespace, objectName);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected CreateResult doCreateSynonym(MetadataProvider metadataProvider, CreateSynonymStatement css,
            Namespace namespace, String synonymName, Namespace objectNamespace, String objectName) throws Exception {
        String databaseName = namespace.getDatabaseName();
        DataverseName dataverseName = namespace.getDataverseName();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, css.getSourceLocation(),
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            Synonym synonym = MetadataManager.INSTANCE.getSynonym(metadataProvider.getMetadataTxnContext(),
                    databaseName, dataverseName, synonymName);
            if (synonym != null) {
                if (css.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    if (warningCollector.shouldWarn()) {
                        warningCollector
                                .warn(Warning.of(css.getSourceLocation(), ErrorCode.SYNONYM_EXISTS, synonymName));
                    }
                    return CreateResult.NOOP;
                }
                throw new CompilationException(ErrorCode.SYNONYM_EXISTS, css.getSourceLocation(), synonymName);
            }
            synonym = new Synonym(databaseName, dataverseName, synonymName, objectNamespace.getDatabaseName(),
                    objectNamespace.getDataverseName(), objectName);
            MetadataManager.INSTANCE.addSynonym(metadataProvider.getMetadataTxnContext(), synonym);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return CreateResult.CREATED;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected void handleDropSynonymStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        SynonymDropStatement stmtSynDrop = (SynonymDropStatement) stmt;
        String synonymName = stmtSynDrop.getSynonymName();
        metadataProvider.validateDatabaseObjectName(stmtSynDrop.getNamespace(), synonymName,
                stmtSynDrop.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(stmtSynDrop.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropSynonymBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, synonymName);
        try {
            doDropSynonym(metadataProvider, stmtSynDrop, databaseName, dataverseName, synonymName);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doDropSynonym(MetadataProvider metadataProvider, SynonymDropStatement stmtSynDrop,
            String databaseName, DataverseName dataverseName, String synonymName) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            Synonym synonym = MetadataManager.INSTANCE.getSynonym(mdTxnCtx, databaseName, dataverseName, synonymName);
            if (synonym == null) {
                if (stmtSynDrop.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                }
                throw new CompilationException(ErrorCode.UNKNOWN_SYNONYM, stmtSynDrop.getSourceLocation(), synonymName);
            }
            MetadataManager.INSTANCE.dropSynonym(mdTxnCtx, databaseName, dataverseName, synonymName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    protected void handleLoadStatement(MetadataProvider metadataProvider, Statement stmt, IHyracksClientConnection hcc)
            throws Exception {
        LoadStatement loadStmt = (LoadStatement) stmt;
        String datasetName = loadStmt.getDatasetName();
        metadataProvider.validateDatabaseObjectName(loadStmt.getNamespace(), datasetName, loadStmt.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(loadStmt.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockUtil.modifyDatasetBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, datasetName);
        try {
            Map<String, String> properties = loadStmt.getProperties();
            ExternalDataUtils.normalize(properties);
            ExternalDataUtils.validate(properties);
            CompiledLoadFromFileStatement cls = new CompiledLoadFromFileStatement(databaseName, dataverseName,
                    loadStmt.getDatasetName(), loadStmt.getAdapter(), properties, loadStmt.dataIsAlreadySorted());
            cls.setSourceLocation(stmt.getSourceLocation());
            JobSpecification spec = apiFramework.compileQuery(hcc, metadataProvider, null, 0, null, sessionOutput, cls,
                    null, responsePrinter, warningCollector, null, jobFlags);
            afterCompile();
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            if (spec != null && !isCompileOnly()) {
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

    protected Map<String, String> createExternalDataPropertiesForCopyFromStmt(String databaseName,
            DataverseName dataverseName, CopyFromStatement copyFromStatement, Datatype itemType,
            MetadataTransactionContext mdTxnCtx, MetadataProvider md) throws AlgebricksException {
        ExternalDetailsDecl edd = copyFromStatement.getExternalDetails();
        Map<String, String> properties = copyFromStatement.getExternalDetails().getProperties();
        String path = copyFromStatement.getPath();
        String pathKey = ExternalDataUtils.getPathKey(edd.getAdapter());
        properties.put(pathKey, path);
        return properties;
    }

    protected void handleCopyFromStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        CopyFromStatement copyStmt = (CopyFromStatement) stmt;
        String datasetName = copyStmt.getDatasetName();
        metadataProvider.validateDatabaseObjectName(copyStmt.getNamespace(), datasetName, copyStmt.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(copyStmt.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockUtil.insertDeleteUpsertBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                datasetName);
        JobId jobId = null;
        boolean atomic = false;
        try {
            metadataProvider.setWriteTransaction(true);
            Dataset dataset = metadataProvider.findDataset(databaseName, dataverseName, copyStmt.getDatasetName());
            if (dataset == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, stmt.getSourceLocation(),
                        datasetName,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            Datatype itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getItemTypeDatabaseName(),
                    dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
            // Copy statement with csv files will have a type expression
            if (copyStmt.getTypeExpr() != null) {
                TypeExpression itemTypeExpr = copyStmt.getTypeExpr();
                Triple<Namespace, String, Boolean> itemTypeQualifiedName = extractDatasetItemTypeName(
                        stmtActiveNamespace, datasetName, itemTypeExpr, false, stmt.getSourceLocation());
                Namespace itemTypeNamespace = itemTypeQualifiedName.first;
                DataverseName itemTypeDataverseName = itemTypeNamespace.getDataverseName();
                String itemTypeName = itemTypeQualifiedName.second;
                String itemTypeDatabaseName = itemTypeNamespace.getDatabaseName();
                IAType itemTypeEntity = translateType(itemTypeDatabaseName, itemTypeDataverseName, itemTypeName,
                        itemTypeExpr, mdTxnCtx);
                itemType =
                        new Datatype(itemTypeDatabaseName, itemTypeDataverseName, itemTypeName, itemTypeEntity, true);
            }
            ExternalDetailsDecl externalDetails = copyStmt.getExternalDetails();
            Map<String, String> properties = createExternalDataPropertiesForCopyFromStmt(databaseName, dataverseName,
                    copyStmt, itemType, mdTxnCtx, metadataProvider);
            ExternalDataUtils.normalize(properties);
            ExternalDataUtils.validate(properties);
            validateExternalDatasetProperties(externalDetails, properties, copyStmt.getSourceLocation(), mdTxnCtx,
                    appCtx);
            CompiledCopyFromFileStatement cls = new CompiledCopyFromFileStatement(databaseName, dataverseName,
                    copyStmt.getDatasetName(), itemType, externalDetails.getAdapter(), properties);
            cls.setSourceLocation(stmt.getSourceLocation());
            JobSpecification spec = apiFramework.compileQuery(hcc, metadataProvider, null, 0, null, sessionOutput, cls,
                    null, responsePrinter, warningCollector, null, jobFlags);
            afterCompile();
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            if (spec != null && !isCompileOnly()) {
                atomic = dataset.isAtomic();
                if (atomic) {
                    int numParticipatingNodes = appCtx.getNodeJobTracker()
                            .getJobParticipatingNodes(spec, LSMTreeIndexInsertUpdateDeleteOperatorDescriptor.class)
                            .size();
                    int numParticipatingPartitions = appCtx.getNodeJobTracker().getNumParticipatingPartitions(spec,
                            LSMTreeIndexInsertUpdateDeleteOperatorDescriptor.class);
                    List<Integer> participatingDatasetIds = new ArrayList<>();
                    participatingDatasetIds.add(dataset.getDatasetId());
                    spec.setProperty(GlobalTxManager.GlOBAL_TX_PROPERTY_NAME, new GlobalTxInfo(participatingDatasetIds,
                            numParticipatingNodes, numParticipatingPartitions));
                }
                jobId = JobUtils.runJob(hcc, spec, jobFlags, false);
                final IRequestTracker requestTracker = appCtx.getRequestTracker();
                final ClientRequest clientRequest =
                        (ClientRequest) requestTracker.get(requestParameters.getRequestReference().getUuid());
                clientRequest.setJobId(jobId);
                String nameBefore = Thread.currentThread().getName();
                try {
                    Thread.currentThread().setName(nameBefore + " : WaitForCompletionForJobId: " + jobId);
                    hcc.waitForCompletion(jobId);
                } finally {
                    Thread.currentThread().setName(nameBefore);
                }
                if (atomic) {
                    globalTxManager.commitTransaction(jobId);
                }
            }
        } catch (Exception e) {
            if (atomic && jobId != null) {
                globalTxManager.abortTransaction(jobId);
            }
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleCopyToStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IResultSet resultSet, ResultDelivery resultDelivery,
            ResultMetadata outMetadata, IRequestParameters requestParameters, Map<String, IAObject> stmtParams,
            Stats stats) throws Exception {
        CopyToStatement copyTo = (CopyToStatement) stmt;
        final IRequestTracker requestTracker = appCtx.getRequestTracker();
        final ClientRequest clientRequest =
                (ClientRequest) requestTracker.get(requestParameters.getRequestReference().getUuid());
        final IMetadataLocker locker = new IMetadataLocker() {
            @Override
            public void lock() throws RuntimeDataException, InterruptedException {
                try {
                    compilationLock.readLock().lockInterruptibly();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ensureNotCancelled(clientRequest);
                    throw e;
                }
            }

            @Override
            public void unlock() {
                metadataProvider.getLocks().unlock();
                compilationLock.readLock().unlock();
            }
        };
        final IStatementCompiler compiler = () -> {
            long compileStart = System.nanoTime();
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            boolean bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            try {
                ExternalDetailsDecl edd = copyTo.getExternalDetailsDecl();
                edd.setProperties(createAndValidateAdapterConfigurationForCopyToStmt(edd,
                        ExternalDataConstants.WRITER_SUPPORTED_ADAPTERS, copyTo.getSourceLocation(), mdTxnCtx,
                        metadataProvider));

                Map<VarIdentifier, IAObject> externalVars = createExternalVariables(copyTo, stmtParams);
                // Query Rewriting (happens under the same ongoing metadata transaction)
                LangRewritingContext langRewritingContext = createLangRewritingContext(metadataProvider,
                        declaredFunctions, null, warningCollector, copyTo.getVarCounter());
                Pair<IReturningStatement, Integer> rewrittenResult = apiFramework.reWriteQuery(langRewritingContext,
                        copyTo, sessionOutput, true, true, externalVars.keySet());

                CompiledStatements.CompiledCopyToStatement compiledCopyToStatement =
                        new CompiledStatements.CompiledCopyToStatement(copyTo);

                // Query Compilation (happens under the same ongoing metadata transaction)
                final JobSpecification jobSpec = apiFramework.compileQuery(hcc, metadataProvider, copyTo.getQuery(),
                        rewrittenResult.second, null, sessionOutput, compiledCopyToStatement, externalVars,
                        responsePrinter, warningCollector, requestParameters, jobFlags);
                // update stats with count of compile-time warnings. needs to be adapted for multi-statement.
                stats.updateTotalWarningsCount(warningCollector.getTotalWarningsCount());
                afterCompile();
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                stats.setCompileTime(System.nanoTime() - compileStart);
                bActiveTxn = false;
                return isCompileOnly() ? null : jobSpec;
            } catch (Exception e) {
                LOGGER.log(Level.INFO, e.getMessage(), e);
                if (bActiveTxn) {
                    abort(e, e, mdTxnCtx);
                }
                throw e;
            }
        };

        deliverResult(hcc, resultSet, compiler, metadataProvider, locker, resultDelivery, outMetadata, stats,
                requestParameters, true, null);
    }

    public JobSpecification handleInsertUpsertStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IResultSet resultSet, ResultDelivery resultDelivery,
            ResultMetadata outMetadata, Stats stats, IRequestParameters requestParameters,
            Map<String, IAObject> stmtParams, IStatementRewriter stmtRewriter) throws Exception {
        InsertStatement stmtInsertUpsert = (InsertStatement) stmt;
        String datasetName = stmtInsertUpsert.getDatasetName();
        metadataProvider.validateDatabaseObjectName(stmtInsertUpsert.getNamespace(), datasetName,
                stmtInsertUpsert.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(stmtInsertUpsert.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        final IMetadataLocker locker = new IMetadataLocker() {
            @Override
            public void lock() throws AlgebricksException {
                compilationLock.readLock().lock();
                lockUtil.insertDeleteUpsertBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                        datasetName);
            }

            @Override
            public void unlock() {
                metadataProvider.getLocks().unlock();
                compilationLock.readLock().unlock();
            }
        };
        final IStatementCompiler compiler = () -> {
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            boolean bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            try {
                metadataProvider.setWriteTransaction(true);
                final JobSpecification jobSpec =
                        rewriteCompileInsertUpsert(hcc, metadataProvider, stmtInsertUpsert, stmtParams);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                return isCompileOnly() ? null : jobSpec;
            } catch (Exception e) {
                if (bActiveTxn) {
                    abort(e, e, mdTxnCtx);
                }
                throw e;
            }
        };
        if (stmtInsertUpsert.getReturnExpression() != null) {
            deliverResult(hcc, resultSet, compiler, metadataProvider, locker, resultDelivery, outMetadata, stats,
                    requestParameters, false, stmt);
        } else {
            locker.lock();
            JobId jobId = null;
            boolean atomic = false;
            try {
                final JobSpecification jobSpec = compiler.compile();
                if (jobSpec == null) {
                    return jobSpec;
                }
                Dataset ds = metadataProvider.findDataset(databaseName, dataverseName, datasetName);
                atomic = ds.isAtomic();
                if (atomic) {
                    int numParticipatingNodes = appCtx.getNodeJobTracker()
                            .getJobParticipatingNodes(jobSpec, LSMTreeIndexInsertUpdateDeleteOperatorDescriptor.class)
                            .size();
                    int numParticipatingPartitions = appCtx.getNodeJobTracker().getNumParticipatingPartitions(jobSpec,
                            LSMTreeIndexInsertUpdateDeleteOperatorDescriptor.class);
                    List<Integer> participatingDatasetIds = new ArrayList<>();
                    participatingDatasetIds.add(ds.getDatasetId());
                    jobSpec.setProperty(GlobalTxManager.GlOBAL_TX_PROPERTY_NAME, new GlobalTxInfo(
                            participatingDatasetIds, numParticipatingNodes, numParticipatingPartitions));
                }
                jobId = JobUtils.runJob(hcc, jobSpec, jobFlags, false);
                final IRequestTracker requestTracker = appCtx.getRequestTracker();
                final ClientRequest clientRequest =
                        (ClientRequest) requestTracker.get(requestParameters.getRequestReference().getUuid());
                clientRequest.setJobId(jobId);
                String nameBefore = Thread.currentThread().getName();
                try {
                    Thread.currentThread().setName(nameBefore + " : WaitForCompletionForJobId: " + jobId);
                    hcc.waitForCompletion(jobId);
                } finally {
                    Thread.currentThread().setName(nameBefore);
                }
                if (atomic) {
                    globalTxManager.commitTransaction(jobId);
                }
            } catch (Exception e) {
                if (atomic && jobId != null) {
                    globalTxManager.abortTransaction(jobId);
                }
                throw e;
            } finally {
                locker.unlock();
            }
        }
        return null;
    }

    public JobSpecification handleDeleteStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, Map<String, IAObject> stmtParams, IStatementRewriter stmtRewriter,
            IRequestParameters requestParameters) throws Exception {
        DeleteStatement stmtDelete = (DeleteStatement) stmt;
        String datasetName = stmtDelete.getDatasetName();
        metadataProvider.validateDatabaseObjectName(stmtDelete.getNamespace(), datasetName, stmt.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(stmtDelete.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockUtil.insertDeleteUpsertBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                datasetName);
        boolean atomic = false;
        JobId jobId = null;
        try {
            metadataProvider.setWriteTransaction(true);
            CompiledDeleteStatement clfrqs =
                    new CompiledDeleteStatement(stmtDelete.getVariableExpr(), databaseName, dataverseName, datasetName,
                            stmtDelete.getCondition(), stmtDelete.getVarCounter(), stmtDelete.getQuery());
            clfrqs.setSourceLocation(stmt.getSourceLocation());
            JobSpecification jobSpec =
                    rewriteCompileQuery(hcc, metadataProvider, clfrqs.getQuery(), clfrqs, stmtParams, null);
            afterCompile();

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            if (jobSpec != null && !isCompileOnly()) {
                Dataset ds = metadataProvider.findDataset(databaseName, dataverseName, datasetName);
                atomic = ds.isAtomic();
                if (atomic) {
                    int numParticipatingNodes = appCtx.getNodeJobTracker()
                            .getJobParticipatingNodes(jobSpec, LSMTreeIndexInsertUpdateDeleteOperatorDescriptor.class)
                            .size();
                    int numParticipatingPartitions = appCtx.getNodeJobTracker().getNumParticipatingPartitions(jobSpec,
                            LSMTreeIndexInsertUpdateDeleteOperatorDescriptor.class);
                    List<Integer> participatingDatasetIds = new ArrayList<>();
                    participatingDatasetIds.add(ds.getDatasetId());
                    jobSpec.setProperty(GlobalTxManager.GlOBAL_TX_PROPERTY_NAME, new GlobalTxInfo(
                            participatingDatasetIds, numParticipatingNodes, numParticipatingPartitions));
                }
                jobId = JobUtils.runJob(hcc, jobSpec, jobFlags, false);
                final IRequestTracker requestTracker = appCtx.getRequestTracker();
                final ClientRequest clientRequest =
                        (ClientRequest) requestTracker.get(requestParameters.getRequestReference().getUuid());
                clientRequest.setJobId(jobId);
                String nameBefore = Thread.currentThread().getName();
                try {
                    Thread.currentThread().setName(nameBefore + " : WaitForCompletionForJobId: " + jobId);
                    hcc.waitForCompletion(jobId);
                } finally {
                    Thread.currentThread().setName(nameBefore);
                }
                if (atomic) {
                    globalTxManager.commitTransaction(jobId);
                }
            }
            return jobSpec;
        } catch (Exception e) {
            if (atomic && jobId != null) {
                globalTxManager.abortTransaction(jobId);
            }
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
            Map<String, IAObject> stmtParams, IRequestParameters requestParameters)
            throws AlgebricksException, ACIDException {

        Map<VarIdentifier, IAObject> externalVars = createExternalVariables(query, stmtParams);

        // Query Rewriting (happens under the same ongoing metadata transaction)
        LangRewritingContext langRewritingContext = createLangRewritingContext(metadataProvider, declaredFunctions,
                null, warningCollector, query.getVarCounter());
        Pair<IReturningStatement, Integer> rewrittenResult = apiFramework.reWriteQuery(langRewritingContext, query,
                sessionOutput, true, true, externalVars.keySet());

        // Query Compilation (happens under the same ongoing metadata transaction)
        return apiFramework.compileQuery(clusterInfoCollector, metadataProvider, (Query) rewrittenResult.first,
                rewrittenResult.second, stmt == null ? null : stmt.getDatasetName(), sessionOutput, stmt, externalVars,
                responsePrinter, warningCollector, requestParameters, jobFlags);
    }

    protected JobSpecification rewriteCompileInsertUpsert(IClusterInfoCollector clusterInfoCollector,
            MetadataProvider metadataProvider, InsertStatement insertUpsert, Map<String, IAObject> stmtParams)
            throws AlgebricksException, ACIDException {
        SourceLocation sourceLoc = insertUpsert.getSourceLocation();

        Map<VarIdentifier, IAObject> externalVars = createExternalVariables(insertUpsert, stmtParams);

        // Insert/upsert statement rewriting (happens under the same ongoing metadata transaction)
        LangRewritingContext langRewritingContext = createLangRewritingContext(metadataProvider, declaredFunctions,
                null, warningCollector, insertUpsert.getVarCounter());
        Pair<IReturningStatement, Integer> rewrittenResult = apiFramework.reWriteQuery(langRewritingContext,
                insertUpsert, sessionOutput, true, true, externalVars.keySet());

        InsertStatement rewrittenInsertUpsert = (InsertStatement) rewrittenResult.first;
        Namespace stmtActiveNamespace = getActiveNamespace(rewrittenInsertUpsert.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String datasetName = rewrittenInsertUpsert.getDatasetName();
        CompiledInsertStatement clfrqs;
        switch (insertUpsert.getKind()) {
            case INSERT:
                clfrqs = new CompiledInsertStatement(databaseName, dataverseName, datasetName,
                        rewrittenInsertUpsert.getQuery(), rewrittenInsertUpsert.getVarCounter(),
                        rewrittenInsertUpsert.getVar(), rewrittenInsertUpsert.getReturnExpression());
                clfrqs.setSourceLocation(insertUpsert.getSourceLocation());
                break;
            case UPSERT:
                clfrqs = new CompiledUpsertStatement(databaseName, dataverseName, datasetName,
                        rewrittenInsertUpsert.getQuery(), rewrittenInsertUpsert.getVarCounter(),
                        rewrittenInsertUpsert.getVar(), rewrittenInsertUpsert.getReturnExpression());
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
                warningCollector, null, jobFlags);
    }

    protected void handleCreateFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        CreateFeedStatement cfs = (CreateFeedStatement) stmt;
        SourceLocation sourceLoc = cfs.getSourceLocation();
        String feedName = cfs.getFeedName().getValue();
        metadataProvider.validateDatabaseObjectName(cfs.getNamespace(), feedName, sourceLoc);
        Namespace stmtActiveNamespace = getActiveNamespace(cfs.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockUtil.createFeedBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, feedName);
        try {
            Feed feed = MetadataManager.INSTANCE.getFeed(metadataProvider.getMetadataTxnContext(), databaseName,
                    dataverseName, feedName);
            if (feed != null) {
                if (cfs.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "A feed with this name " + feedName + " already exists.");
                }
            }
            Map<String, String> configuration = cfs.getConfiguration();
            ExternalDataUtils.normalize(configuration);
            ExternalDataUtils.validate(configuration);
            feed = new Feed(databaseName, dataverseName, feedName, configuration);
            FeedMetadataUtil.validateFeed(feed, mdTxnCtx, appCtx, warningCollector);
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
        FeedPolicyEntity newPolicy;
        MetadataTransactionContext mdTxnCtx = null;
        CreateFeedPolicyStatement cfps = (CreateFeedPolicyStatement) stmt;
        SourceLocation sourceLoc = cfps.getSourceLocation();
        String policyName = cfps.getPolicyName();
        metadataProvider.validateDatabaseObjectName(null, policyName, sourceLoc);
        DataverseName dataverseName = activeNamespace.getDataverseName();
        String databaseName = activeNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.createFeedPolicyBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                policyName);
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            FeedPolicyEntity feedPolicy = MetadataManager.INSTANCE
                    .getFeedPolicy(metadataProvider.getMetadataTxnContext(), databaseName, dataverseName, policyName);
            if (feedPolicy != null) {
                if (cfps.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "A policy with this name " + policyName + " already exists.");
                }
            }
            boolean extendingExisting = cfps.getSourcePolicyName() != null;
            String description = cfps.getDescription() == null ? "" : cfps.getDescription();
            if (extendingExisting) {
                FeedPolicyEntity sourceFeedPolicy =
                        MetadataManager.INSTANCE.getFeedPolicy(metadataProvider.getMetadataTxnContext(), databaseName,
                                dataverseName, cfps.getSourcePolicyName());
                if (sourceFeedPolicy == null) {
                    sourceFeedPolicy = MetadataManager.INSTANCE.getFeedPolicy(metadataProvider.getMetadataTxnContext(),
                            MetadataConstants.SYSTEM_DATABASE, MetadataConstants.METADATA_DATAVERSE_NAME,
                            cfps.getSourcePolicyName());
                    if (sourceFeedPolicy == null) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                                "Unknown policy " + cfps.getSourcePolicyName());
                    }
                }
                Map<String, String> policyProperties = sourceFeedPolicy.getProperties();
                policyProperties.putAll(cfps.getProperties());
                newPolicy =
                        new FeedPolicyEntity(databaseName, dataverseName, policyName, description, policyProperties);
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
                newPolicy =
                        new FeedPolicyEntity(databaseName, dataverseName, policyName, description, policyProperties);
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
        String feedName = stmtFeedDrop.getFeedName().getValue();
        metadataProvider.validateDatabaseObjectName(stmtFeedDrop.getNamespace(), feedName, sourceLoc);
        Namespace stmtActiveNamespace = getActiveNamespace(stmtFeedDrop.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockUtil.dropFeedBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, feedName);
        try {
            Feed feed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, databaseName, dataverseName, feedName);
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
                    + " is currently active and connected to the following " + dataset(PLURAL) + "\n" + listener);
        } else if (listener != null) {
            listener.unregister();
        }
        JobSpecification spec = FeedOperations.buildRemoveFeedStorageJob(metadataProvider, MetadataManager.INSTANCE
                .getFeed(mdTxnCtx, feedId.getDatabaseName(), feedId.getDataverseName(), feedId.getEntityName()));
        runJob(hcc, spec);
        MetadataManager.INSTANCE.dropFeed(mdTxnCtx, feed.getDatabaseName(), feed.getDataverseName(),
                feed.getFeedName());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Removed feed " + feedId);
        }
    }

    protected void handleDropFeedPolicyStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        FeedPolicyDropStatement stmtFeedPolicyDrop = (FeedPolicyDropStatement) stmt;
        SourceLocation sourceLoc = stmtFeedPolicyDrop.getSourceLocation();
        String policyName = stmtFeedPolicyDrop.getPolicyName().getValue();
        metadataProvider.validateDatabaseObjectName(stmtFeedPolicyDrop.getNamespace(), policyName, sourceLoc);
        Namespace stmtActiveNamespace = getActiveNamespace(stmtFeedPolicyDrop.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        if (isCompileOnly()) {
            return;
        }
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockUtil.dropFeedPolicyBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, policyName);
        try {
            FeedPolicyEntity feedPolicy =
                    MetadataManager.INSTANCE.getFeedPolicy(mdTxnCtx, databaseName, dataverseName, policyName);
            if (feedPolicy == null) {
                if (!stmtFeedPolicyDrop.getIfExists()) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                            "Unknown policy " + policyName + " in " + dataverse() + " " + dataverseName);
                }
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;
            }
            MetadataManager.INSTANCE.dropFeedPolicy(mdTxnCtx, databaseName, dataverseName, policyName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void handleStartFeedStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        StartFeedStatement sfs = (StartFeedStatement) stmt;
        SourceLocation sourceLoc = sfs.getSourceLocation();
        Namespace stmtActiveNamespace = getActiveNamespace(sfs.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String feedName = sfs.getFeedName().getValue();
        if (isCompileOnly()) {
            return;
        }
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean committed = false;
        lockUtil.startFeedBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, feedName);
        try {
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            // Runtime handler
            EntityId entityId = new EntityId(Feed.EXTENSION_NAME, databaseName, dataverseName, feedName);
            // Feed & Feed Connections
            Feed feed = FeedMetadataUtil.validateIfFeedExists(databaseName, dataverseName, feedName,
                    metadataProvider.getMetadataTxnContext());
            List<FeedConnection> feedConnections = MetadataManager.INSTANCE
                    .getFeedConections(metadataProvider.getMetadataTxnContext(), databaseName, dataverseName, feedName);
            if (feedConnections.isEmpty()) {
                throw new CompilationException(ErrorCode.FEED_START_FEED_WITHOUT_CONNECTION, sourceLoc, feedName);
            }
            for (FeedConnection feedConnection : feedConnections) {
                // what if the dataset is in a different dataverse
                lockManager.acquireDatasetReadLock(metadataProvider.getLocks(), feedConnection.getDatabaseName(),
                        feedConnection.getDataverseName(), feedConnection.getDatasetName());
            }
            ActiveNotificationHandler activeEventHandler =
                    (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
            ActiveEntityEventsListener listener = (ActiveEntityEventsListener) activeEventHandler.getListener(entityId);
            if (listener == null) {
                // Prepare policy
                List<Dataset> datasets = new ArrayList<>();
                for (FeedConnection connection : feedConnections) {
                    Dataset ds = metadataProvider.findDataset(connection.getDatabaseName(),
                            connection.getDataverseName(), connection.getDatasetName());
                    datasets.add(ds);
                }
                listener = new FeedEventsListener(this, metadataProvider.getApplicationContext(), hcc, entityId,
                        datasets, null, FeedIntakeOperatorNodePushable.class.getSimpleName(),
                        NoRetryPolicyFactory.INSTANCE, feed, feedConnections, compilationProvider.getLanguage());
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

    protected void handleStopFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        StopFeedStatement sfst = (StopFeedStatement) stmt;
        SourceLocation sourceLoc = sfst.getSourceLocation();
        Namespace stmtActiveNamespace = getActiveNamespace(sfst.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String feedName = sfst.getFeedName().getValue();
        EntityId entityId = new EntityId(Feed.EXTENSION_NAME, databaseName, dataverseName, feedName);
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        // Obtain runtime info from ActiveListener
        ActiveEntityEventsListener listener = (ActiveEntityEventsListener) activeEventHandler.getListener(entityId);
        if (listener == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Feed " + feedName + " is not started.");
        }
        if (isCompileOnly()) {
            return;
        }
        lockUtil.stopFeedBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, feedName);
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
        Namespace stmtActiveNamespace = getActiveNamespace(cfs.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String feedName = cfs.getFeedName();
        String datasetName = cfs.getDatasetName().getValue();
        String policyName = cfs.getPolicy();
        String whereClauseBody = cfs.getWhereClauseBody();
        if (isCompileOnly()) {
            return;
        }
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        // TODO: Check whether we are connecting a change feed to a non-meta dataset
        // Check whether feed is alive
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        // Transaction handling
        lockUtil.connectFeedBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, datasetName,
                feedName);
        try {
            // validation
            Dataset dataset = FeedMetadataUtil.validateIfDatasetExists(metadataProvider, databaseName, dataverseName,
                    datasetName);
            Feed feed = FeedMetadataUtil.validateIfFeedExists(databaseName, dataverseName, feedName,
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
            fc = MetadataManager.INSTANCE.getFeedConnection(metadataProvider.getMetadataTxnContext(), databaseName,
                    dataverseName, feedName, datasetName);
            if (fc != null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Feed" + feedName + " is already connected to " + dataset() + " " + datasetName);
            }
            fc = new FeedConnection(databaseName, dataverseName, feedName, datasetName, appliedFunctions, policyName,
                    whereClauseBody, outputType.getTypeName());
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
        Namespace stmtActiveNamespace = getActiveNamespace(cfs.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String datasetName = cfs.getDatasetName().getValue();
        String feedName = cfs.getFeedName().getValue();
        if (isCompileOnly()) {
            return;
        }
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        lockUtil.disconnectFeedBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, datasetName,
                feedName);
        try {
            ActiveNotificationHandler activeEventHandler =
                    (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
            // Check whether feed is alive
            ActiveEntityEventsListener listener = (ActiveEntityEventsListener) activeEventHandler
                    .getListener(new EntityId(Feed.EXTENSION_NAME, databaseName, dataverseName, feedName));
            if (listener != null && listener.isActive()) {
                throw new CompilationException(ErrorCode.FEED_CHANGE_FEED_CONNECTIVITY_ON_ALIVE_FEED, sourceLoc,
                        feedName);
            }
            FeedMetadataUtil.validateIfDatasetExists(metadataProvider, databaseName, dataverseName,
                    cfs.getDatasetName().getValue());
            FeedMetadataUtil.validateIfFeedExists(databaseName, dataverseName, cfs.getFeedName().getValue(), mdTxnCtx);
            FeedConnection fc = MetadataManager.INSTANCE.getFeedConnection(metadataProvider.getMetadataTxnContext(),
                    databaseName, dataverseName, feedName, datasetName);
            Dataset ds = metadataProvider.findDataset(databaseName, dataverseName, datasetName);
            if (ds == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            if (fc == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc, "Feed " + feedName
                        + " is currently not connected to " + cfs.getDatasetName().getValue() + ". Invalid operation!");
            }
            MetadataManager.INSTANCE.dropFeedConnection(mdTxnCtx, databaseName, dataverseName, feedName, datasetName);
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

    protected void handleAnalyzeStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
        AnalyzeStatement analyzeStatement = (AnalyzeStatement) stmt;
        metadataProvider.validateDatabaseObjectName(analyzeStatement.getNamespace(), analyzeStatement.getDatasetName(),
                analyzeStatement.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(analyzeStatement.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String datasetName = analyzeStatement.getDatasetName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.analyzeDatasetBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                datasetName);
        try {
            doAnalyzeDataset(metadataProvider, analyzeStatement, databaseName, dataverseName, datasetName, hcc,
                    requestParameters);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected void doAnalyzeDataset(MetadataProvider metadataProvider, AnalyzeStatement stmtAnalyze,
            String databaseName, DataverseName dataverseName, String datasetName, IHyracksClientConnection hcc,
            IRequestParameters requestParameters) throws Exception {
        SourceLocation sourceLoc = stmtAnalyze.getSourceLocation();
        ProgressState progressNewIndexCreate = ProgressState.NO_PROGRESS;
        ProgressState progressExistingIndexDrop = ProgressState.NO_PROGRESS;
        Dataset ds = null;
        Index existingIndex = null, newIndexPendingAdd = null;
        JobSpecification existingIndexDropSpec = null;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            // Check if the dataverse exists
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName);
            if (dv == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, sourceLoc,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            // Check if the dataset exists
            ds = metadataProvider.findDataset(databaseName, dataverseName, datasetName);
            if (ds == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                validateDatasetState(metadataProvider, ds, sourceLoc);
            } else {
                throw new CompilationException(ErrorCode.OPERATION_NOT_SUPPORTED, sourceLoc);
            }

            IndexType sampleIndexType = IndexType.SAMPLE;
            Pair<String, String> sampleIndexNames = IndexUtil.getSampleIndexNames(datasetName);
            String newIndexName;
            existingIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), databaseName,
                    dataverseName, datasetName, sampleIndexNames.first);
            if (existingIndex != null) {
                newIndexName = sampleIndexNames.second;
            } else {
                existingIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                        databaseName, dataverseName, datasetName, sampleIndexNames.second);
                newIndexName = sampleIndexNames.first;
            }

            InternalDatasetDetails dsDetails = (InternalDatasetDetails) ds.getDatasetDetails();
            int sampleCardinalityTarget = stmtAnalyze.getSampleSize();
            long sampleSeed = stmtAnalyze.getOrCreateSampleSeed();

            Index.SampleIndexDetails newIndexDetailsPendingAdd = new Index.SampleIndexDetails(dsDetails.getPrimaryKey(),
                    dsDetails.getKeySourceIndicator(), dsDetails.getPrimaryKeyType(), sampleCardinalityTarget, 0, 0,
                    sampleSeed, Collections.emptyMap());
            newIndexPendingAdd = new Index(databaseName, dataverseName, datasetName, newIndexName, sampleIndexType,
                    newIndexDetailsPendingAdd, false, false, MetadataUtil.PENDING_ADD_OP);

            // #. add a new index with PendingAddOp
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), newIndexPendingAdd);
            // #. prepare to create the index artifact in NC.
            JobSpecification spec =
                    IndexUtil.buildSecondaryIndexCreationJobSpec(ds, newIndexPendingAdd, metadataProvider, sourceLoc);
            if (spec == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Failed to create job spec for creating index '" + ds.getDatasetName() + "."
                                + newIndexPendingAdd.getIndexName() + "'");
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progressNewIndexCreate = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            // #. create the index artifact in NC.
            runJob(hcc, spec);

            // #. flush dataset
            FlushDatasetUtil.flushDataset(hcc, metadataProvider, databaseName, dataverseName, datasetName);

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. load data into the index in NC.
            spec = IndexUtil.buildSecondaryIndexLoadingJobSpec(ds, newIndexPendingAdd, metadataProvider, sourceLoc);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            List<IOperatorStats> opStats = runJob(hcc, spec, jobFlags,
                    Collections.singletonList(SampleOperationsHelper.DATASET_STATS_OPERATOR_NAME));
            if (opStats == null || opStats.size() == 0) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, "", sourceLoc);
            }
            DatasetStreamStats stats = new DatasetStreamStats(opStats.get(0));

            Index.SampleIndexDetails newIndexDetailsFinal = new Index.SampleIndexDetails(dsDetails.getPrimaryKey(),
                    dsDetails.getKeySourceIndicator(), dsDetails.getPrimaryKeyType(), sampleCardinalityTarget,
                    stats.getCardinality(), stats.getAvgTupleSize(), sampleSeed, stats.getIndexesStats());
            Index newIndexFinal = new Index(databaseName, dataverseName, datasetName, newIndexName, sampleIndexType,
                    newIndexDetailsFinal, false, false, MetadataUtil.PENDING_NO_OP);

            // #. begin new metadataTxn
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            // #. add same new index with PendingNoOp after deleting its entry with PendingAddOp
            MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(),
                    newIndexPendingAdd.getDatabaseName(), newIndexPendingAdd.getDataverseName(),
                    newIndexPendingAdd.getDatasetName(), newIndexPendingAdd.getIndexName());
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), newIndexFinal);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            progressNewIndexCreate = ProgressState.NO_PROGRESS;

            if (existingIndex != null) {
                // #. set existing index to PendingDropOp because we'll be dropping it next
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(),
                        existingIndex.getDatabaseName(), existingIndex.getDataverseName(),
                        existingIndex.getDatasetName(), existingIndex.getIndexName());
                existingIndex.setPendingOp(MetadataUtil.PENDING_DROP_OP);
                MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), existingIndex);
                existingIndexDropSpec = IndexUtil.buildDropIndexJobSpec(existingIndex, metadataProvider, ds, sourceLoc);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                progressExistingIndexDrop = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;
                bActiveTxn = false;

                // #. drop existing index on NCs
                runJob(hcc, existingIndexDropSpec);

                // #. drop existing index metadata
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(),
                        existingIndex.getDatabaseName(), existingIndex.getDataverseName(),
                        existingIndex.getDatasetName(), existingIndex.getIndexName());
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progressExistingIndexDrop = ProgressState.NO_PROGRESS;
            }

        } catch (Exception e) {
            LOGGER.error("failed to analyze dataset; executing compensating operations", e);
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progressExistingIndexDrop == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // #. execute compensation operations remove the index in NCs
                try {
                    runJob(hcc, existingIndexDropSpec);
                } catch (Exception e2) {
                    // do no throw exception since still the metadata needs to be compensated.
                    e.addSuppressed(e2);
                }
                // #. remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(),
                            existingIndex.getDatabaseName(), existingIndex.getDataverseName(),
                            existingIndex.getDatasetName(), existingIndex.getIndexName());
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is inconsistent state: pending index("
                            + existingIndex.getDataverseName() + "." + existingIndex.getDatasetName() + "."
                            + existingIndex.getIndexName() + ") couldn't be removed from the metadata", e);
                }
            } else if (progressNewIndexCreate == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // #. execute compensation operations remove the index in NCs
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    JobSpecification jobSpec =
                            IndexUtil.buildDropIndexJobSpec(newIndexPendingAdd, metadataProvider, ds, sourceLoc);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;
                    runJob(hcc, jobSpec);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    if (bActiveTxn) {
                        abort(e, e2, mdTxnCtx);
                    }
                }
                // #. remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(),
                            newIndexPendingAdd.getDatabaseName(), newIndexPendingAdd.getDataverseName(),
                            newIndexPendingAdd.getDatasetName(), newIndexPendingAdd.getIndexName());
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is in inconsistent state: pending index("
                            + newIndexPendingAdd.getDataverseName() + "." + newIndexPendingAdd.getDatasetName() + "."
                            + newIndexPendingAdd.getIndexName() + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;
        }
    }

    protected void handleAnalyzeDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IRequestParameters requestParams) throws Exception {
        AnalyzeDropStatement analyzeDropStmt = (AnalyzeDropStatement) stmt;
        metadataProvider.validateDatabaseObjectName(analyzeDropStmt.getNamespace(), analyzeDropStmt.getDatasetName(),
                analyzeDropStmt.getSourceLocation());
        Namespace stmtActiveNamespace = getActiveNamespace(analyzeDropStmt.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String datasetName = analyzeDropStmt.getDatasetName();
        if (isCompileOnly()) {
            return;
        }
        lockUtil.analyzeDatasetDropBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName,
                datasetName);
        try {
            doAnalyzeDatasetDrop(metadataProvider, analyzeDropStmt, databaseName, dataverseName, datasetName, hcc,
                    requestParams);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    protected boolean doAnalyzeDatasetDrop(MetadataProvider metadataProvider, AnalyzeDropStatement stmtIndexDrop,
            String databaseName, DataverseName dataverseName, String datasetName, IHyracksClientConnection hcc,
            IRequestParameters requestParams) throws Exception {
        SourceLocation sourceLoc = stmtIndexDrop.getSourceLocation();
        Pair<String, String> sampleIndexNames = IndexUtil.getSampleIndexNames(datasetName);
        String indexName1 = sampleIndexNames.first;
        String indexName2 = sampleIndexNames.second;
        ProgressState progress = ProgressState.NO_PROGRESS;
        List<JobSpecification> jobsToExecute = new ArrayList<>();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        boolean index1Exists = false, index2Exists = false;
        try {
            Dataset ds = metadataProvider.findDataset(databaseName, dataverseName, datasetName);
            if (ds == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            if (ds.getDatasetType() != DatasetType.INTERNAL) {
                throw new CompilationException(ErrorCode.OPERATION_NOT_SUPPORTED, sourceLoc);
            }
            Index index1 =
                    MetadataManager.INSTANCE.getIndex(mdTxnCtx, databaseName, dataverseName, datasetName, indexName1);
            Index index2 =
                    MetadataManager.INSTANCE.getIndex(mdTxnCtx, databaseName, dataverseName, datasetName, indexName2);
            index1Exists = index1 != null;
            index2Exists = index2 != null;
            if (!index1Exists && !index2Exists) {
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return false;
            }
            ensureNonPrimaryIndexesDrop(index1, index2, sourceLoc);
            validateDatasetState(metadataProvider, ds, sourceLoc);
            prepareIndexDrop(metadataProvider, databaseName, dataverseName, datasetName, sourceLoc, indexName1,
                    jobsToExecute, mdTxnCtx, ds, index1);
            prepareIndexDrop(metadataProvider, databaseName, dataverseName, datasetName, sourceLoc, indexName2,
                    jobsToExecute, mdTxnCtx, ds, index2);

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

            // #. finally, delete the existing indexes
            if (index1Exists) {
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, databaseName, dataverseName, datasetName, indexName1);
            }
            if (index2Exists) {
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, databaseName, dataverseName, datasetName, indexName2);
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }

            if (progress == ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA) {
                // #. execute compensation operations remove the all indexes in NC
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
                    if (index1Exists) {
                        MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), databaseName,
                                dataverseName, datasetName, indexName1);
                    }
                    if (index2Exists) {
                        MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), databaseName,
                                dataverseName, datasetName, indexName2);
                    }
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    String msg = String.format(
                            "System is in inconsistent state: pending index %1$s.%2$s.%3$s and/or %1$s.%2$s.%4$s "
                                    + "couldn't be removed from the metadata",
                            dataverseName, datasetName, indexName1, indexName2);
                    throw new IllegalStateException(msg, e);
                }
            }

            throw e;
        }
    }

    private void ensureNonPrimaryIndexesDrop(Index index1, Index index2, SourceLocation sourceLoc)
            throws AlgebricksException {
        if (index1 != null) {
            ensureNonPrimaryIndexDrop(index1, sourceLoc);
        }
        if (index2 != null) {
            ensureNonPrimaryIndexDrop(index2, sourceLoc);
        }
    }

    private void prepareIndexDrop(MetadataProvider metadataProvider, String databaseName, DataverseName dataverseName,
            String datasetName, SourceLocation sourceLoc, String indexName, List<JobSpecification> jobsToExecute,
            MetadataTransactionContext mdTxnCtx, Dataset ds, Index index) throws AlgebricksException {
        if (index != null) {
            // #. prepare a job to drop the index in NC.
            jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, ds, sourceLoc));

            // #. mark PendingDropOp on the existing index
            MetadataManager.INSTANCE.dropIndex(mdTxnCtx, databaseName, dataverseName, datasetName, indexName);
            MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                    new Index(databaseName, dataverseName, datasetName, indexName, index.getIndexType(),
                            index.getIndexDetails(), index.isEnforced(), index.isPrimaryIndex(),
                            MetadataUtil.PENDING_DROP_OP));
        }
    }

    protected void handleCompactStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        CompactStatement compactStatement = (CompactStatement) stmt;
        SourceLocation sourceLoc = compactStatement.getSourceLocation();
        Namespace stmtActiveNamespace = getActiveNamespace(compactStatement.getNamespace());
        DataverseName dataverseName = stmtActiveNamespace.getDataverseName();
        String databaseName = stmtActiveNamespace.getDatabaseName();
        String datasetName = compactStatement.getDatasetName().getValue();
        if (isCompileOnly()) {
            return;
        }
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        lockUtil.compactBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, datasetName);
        try {
            Dataset ds = metadataProvider.findDataset(databaseName, dataverseName, datasetName);
            if (ds == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            // Prepare jobs to compact the datatset and its indexes
            List<Index> indexes =
                    MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, databaseName, dataverseName, datasetName);
            if (indexes.isEmpty()) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Cannot compact the external " + dataset() + " " + datasetName + " because it has no indexes");
            }
            Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(),
                    databaseName, dataverseName);
            jobsToExecute.add(DatasetUtil.compactDatasetJobSpec(dataverse, datasetName, metadataProvider));

            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                for (Index index : indexes) {
                    if (index.isSecondaryIndex() && !index.isSampleIndex()) {
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
        void lock() throws HyracksDataException, AlgebricksException, InterruptedException;

        void unlock() throws HyracksDataException, AlgebricksException;
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
        final IRequestTracker requestTracker = appCtx.getRequestTracker();
        final ClientRequest clientRequest =
                (ClientRequest) requestTracker.get(requestParameters.getRequestReference().getUuid());
        final IMetadataLocker locker = new IMetadataLocker() {
            @Override
            public void lock() throws RuntimeDataException, InterruptedException {
                try {
                    compilationLock.readLock().lockInterruptibly();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ensureNotCancelled(clientRequest);
                    throw e;
                }
            }

            @Override
            public void unlock() {
                metadataProvider.getLocks().unlock();
                compilationLock.readLock().unlock();
            }
        };
        final IStatementCompiler compiler = () -> {
            long compileStart = System.nanoTime();
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            boolean bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            try {
                final JobSpecification jobSpec =
                        rewriteCompileQuery(hcc, metadataProvider, query, null, stmtParams, requestParameters);
                // update stats with count of compile-time warnings. needs to be adapted for multi-statement.
                stats.updateTotalWarningsCount(warningCollector.getTotalWarningsCount());
                afterCompile();
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                stats.setCompileTime(System.nanoTime() - compileStart);
                bActiveTxn = false;
                return query.isExplain() || isCompileOnly() ? null : jobSpec;
            } catch (Exception e) {
                LOGGER.log(Level.INFO, e.getMessage(), e);
                if (bActiveTxn) {
                    abort(e, e, mdTxnCtx);
                }
                throw e;
            }
        };
        deliverResult(hcc, resultSet, compiler, metadataProvider, locker, resultDelivery, outMetadata, stats,
                requestParameters, true, null);
    }

    private void deliverResult(IHyracksClientConnection hcc, IResultSet resultSet, IStatementCompiler compiler,
            MetadataProvider metadataProvider, IMetadataLocker locker, ResultDelivery resultDelivery,
            ResultMetadata outMetadata, Stats stats, IRequestParameters requestParameters, boolean cancellable,
            Statement atomicStmt) throws Exception {
        final ResultSetId resultSetId = metadataProvider.getResultSetId();
        switch (resultDelivery) {
            case ASYNC:
                MutableBoolean printed = new MutableBoolean(false);
                executorService.submit(() -> asyncCreateAndRunJob(hcc, compiler, locker, resultDelivery,
                        requestParameters, cancellable, resultSetId, printed, metadataProvider, atomicStmt));
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
                }, requestParameters, cancellable, appCtx, metadataProvider, atomicStmt);
                break;
            case DEFERRED:
                createAndRunJob(hcc, jobFlags, null, compiler, locker, resultDelivery, id -> {
                    updateJobStats(id, stats, metadataProvider.getResultSetId());
                    responsePrinter.addResultPrinter(
                            new ResultHandlePrinter(sessionOutput, new ResultHandle(id, resultSetId)));
                    responsePrinter.printResults();
                    if (outMetadata != null) {
                        outMetadata.getResultSets().add(org.apache.commons.lang3.tuple.Triple.of(id, resultSetId,
                                metadataProvider.findOutputRecordType()));
                    }
                }, requestParameters, cancellable, appCtx, metadataProvider, atomicStmt);
                break;
            default:
                break;
        }
    }

    private void updateJobStats(JobId jobId, Stats stats, ResultSetId rsId) throws HyracksDataException {
        final ClusterControllerService controllerService =
                (ClusterControllerService) appCtx.getServiceContext().getControllerService();
        org.apache.asterix.translator.ResultMetadata resultMetadata =
                (org.apache.asterix.translator.ResultMetadata) controllerService.getResultDirectoryService()
                        .getResultMetadata(jobId, rsId);
        stats.setProcessedObjects(resultMetadata.getProcessedObjects());
        stats.setQueueWaitTime(resultMetadata.getQueueWaitTimeInNanos());
        stats.setBufferCacheHitRatio(resultMetadata.getBufferCacheHitRatio());
        stats.setBufferCachePageReadCount(resultMetadata.getBufferCachePageReadCount());
        if (jobFlags.contains(JobFlag.PROFILE_RUNTIME)) {
            stats.setJobProfile(resultMetadata.getJobProfile());
            apiFramework.generateOptimizedLogicalPlanWithProfile(resultMetadata.getJobProfile());
        }
        stats.updateTotalWarningsCount(resultMetadata.getTotalWarningsCount());
        WarningUtil.mergeWarnings(resultMetadata.getWarnings(), warningCollector);
    }

    private void asyncCreateAndRunJob(IHyracksClientConnection hcc, IStatementCompiler compiler, IMetadataLocker locker,
            ResultDelivery resultDelivery, IRequestParameters requestParameters, boolean cancellable,
            ResultSetId resultSetId, MutableBoolean printed, MetadataProvider metadataProvider, Statement atomicStmt) {
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
            }, requestParameters, cancellable, appCtx, metadataProvider, atomicStmt);
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

    private static List<IOperatorStats> runJob(IHyracksClientConnection hcc, JobSpecification jobSpec,
            EnumSet<JobFlag> jobFlags, List<String> statOperatorNames) throws Exception {
        Pair<JobId, List<IOperatorStats>> p = JobUtils.runJob(hcc, jobSpec, jobFlags, true, statOperatorNames);
        return p.second;
    }

    private void createAndRunJob(IHyracksClientConnection hcc, EnumSet<JobFlag> jobFlags, Mutable<JobId> jId,
            IStatementCompiler compiler, IMetadataLocker locker, ResultDelivery resultDelivery, IResultPrinter printer,
            IRequestParameters requestParameters, boolean cancellable, ICcApplicationContext appCtx,
            MetadataProvider metadataProvider, Statement atomicStatement) throws Exception {
        final IRequestTracker requestTracker = appCtx.getRequestTracker();
        final ClientRequest clientRequest =
                (ClientRequest) requestTracker.get(requestParameters.getRequestReference().getUuid());
        if (cancellable) {
            clientRequest.markCancellable();
        }
        locker.lock();
        JobId jobId = null;
        boolean atomic = false;
        try {
            final JobSpecification jobSpec = compiler.compile();
            if (jobSpec == null) {
                return;
            }
            final SchedulableClientRequest schedulableRequest =
                    SchedulableClientRequest.of(clientRequest, requestParameters, metadataProvider, jobSpec);
            appCtx.getReceptionist().ensureSchedulable(schedulableRequest);
            // ensure request not cancelled before running job
            ensureNotCancelled(clientRequest);
            if (atomicStatement != null) {
                Dataset ds = metadataProvider.findDataset(((InsertStatement) atomicStatement).getDatabaseName(),
                        ((InsertStatement) atomicStatement).getDataverseName(),
                        ((InsertStatement) atomicStatement).getDatasetName());
                atomic = ds.isAtomic();
                if (atomic) {
                    int numParticipatingNodes = appCtx.getNodeJobTracker()
                            .getJobParticipatingNodes(jobSpec, LSMTreeIndexInsertUpdateDeleteOperatorDescriptor.class)
                            .size();
                    int numParticipatingPartitions = appCtx.getNodeJobTracker().getNumParticipatingPartitions(jobSpec,
                            LSMTreeIndexInsertUpdateDeleteOperatorDescriptor.class);
                    List<Integer> participatingDatasetIds = new ArrayList<>();
                    participatingDatasetIds.add(ds.getDatasetId());
                    jobSpec.setProperty(GlobalTxManager.GlOBAL_TX_PROPERTY_NAME, new GlobalTxInfo(
                            participatingDatasetIds, numParticipatingNodes, numParticipatingPartitions));
                }
            }
            jobId = JobUtils.runJob(hcc, jobSpec, jobFlags, false);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Created job {} for query uuid:{}, clientContextID:{}", jobId,
                        requestParameters.getRequestReference().getUuid(), requestParameters.getClientContextId());
            }
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
            if (atomic) {
                globalTxManager.commitTransaction(jobId);
            }
        } catch (Exception e) {
            if (atomic && jobId != null) {
                globalTxManager.abortTransaction(jobId);
            }
            if (org.apache.hyracks.api.util.ExceptionUtils.getRootCause(e) instanceof InterruptedException) {
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
        metadataProvider.validateDatabaseObjectName(null, ngName, sourceLoc);

        if (isCompileOnly()) {
            return;
        }
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

    @Override
    public Namespace getActiveNamespace(Namespace namespace) {
        return namespace != null ? namespace : activeNamespace;
    }

    @Override
    public ExecutionPlans getExecutionPlans() {
        return apiFramework.getExecutionPlans();
    }

    @Override
    public IResponsePrinter getResponsePrinter() {
        return responsePrinter;
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

    protected void rewriteStatement(Statement stmt, IStatementRewriter rewriter, MetadataProvider metadataProvider)
            throws CompilationException, RemoteException {
        if (!rewriter.isRewritable(stmt.getKind())) {
            return;
        }
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            rewriter.rewrite(stmt, metadataProvider);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
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

    protected void validateStatements(IRequestParameters requestParameters)
            throws AlgebricksException, HyracksDataException {
        validateStatements(statements, requestParameters.isMultiStatement(),
                requestParameters.getStatementCategoryRestrictionMask());
        DataverseDecl requestDataverseDecl = getRequestDataverseDecl(requestParameters);
        if (requestDataverseDecl != null) {
            statements.add(0, requestDataverseDecl);
        }
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

    private Map<VarIdentifier, IAObject> createExternalVariables(IReturningStatement stmt,
            Map<String, IAObject> stmtParams) throws CompilationException {
        if (!isCompileOnly()) {
            if (stmtParams == null || stmtParams.isEmpty()) {
                return Collections.emptyMap();
            }
            IQueryRewriter queryRewriter = rewriterFactory.createQueryRewriter();
            Map<VarIdentifier, IAObject> result = new HashMap<>();
            for (Map.Entry<String, IAObject> me : stmtParams.entrySet()) {
                result.put(queryRewriter.toExternalVariableName(me.getKey()), me.getValue());
            }
            return result;
        } else {
            // compile only. extract statement parameters from the statement body and bind to NULL
            IQueryRewriter queryRewriter = rewriterFactory.createQueryRewriter();
            Set<VariableExpr> extVars = queryRewriter.getExternalVariables(stmt.getBody());
            if (extVars.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<VarIdentifier, IAObject> result = new HashMap<>();
            for (VariableExpr extVar : extVars) {
                result.put(extVar.getVar(), ANull.NULL);
            }
            return result;
        }
    }

    protected boolean isCompileOnly() {
        return !sessionConfig.isExecuteQuery();
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

    protected void validateExternalDatasetProperties(ExternalDetailsDecl externalDetails,
            Map<String, String> properties, SourceLocation srcLoc, MetadataTransactionContext mdTxnCtx,
            IApplicationContext appCtx) throws AlgebricksException, HyracksDataException {
        // Validate adapter specific properties
        String adapter = externalDetails.getAdapter();
        Map<String, String> details = new HashMap<>(properties);
        details.put(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE, adapter);
        validateAdapterSpecificProperties(details, srcLoc, appCtx);
    }

    protected Map<String, String> createAndValidateAdapterConfigurationForCopyToStmt(
            ExternalDetailsDecl externalDetailsDecl, Set<String> supportedAdapters, SourceLocation sourceLocation,
            MetadataTransactionContext mdTxnCtx, MetadataProvider md) throws AlgebricksException {
        String adapterName = externalDetailsDecl.getAdapter();
        Map<String, String> properties = externalDetailsDecl.getProperties();
        WriterValidationUtil.validateWriterConfiguration(adapterName, supportedAdapters, properties, sourceLocation);
        return properties;
    }

    /**
     * Ensures that the external source container is present
     *
     * @param configuration external source properties
     */
    protected void validateAdapterSpecificProperties(Map<String, String> configuration, SourceLocation srcLoc,
            IApplicationContext appCtx) throws CompilationException {
        ExternalDataUtils.validateAdapterSpecificProperties(configuration, srcLoc, warningCollector, appCtx);
    }

    private List<Dataset> prepareDatabaseDropJobs(MetadataProvider metadataProvider, SourceLocation sourceLoc,
            String databaseName, MetadataTransactionContext mdTxnCtx, List<FeedEventsListener> feedsToStop,
            List<JobSpecification> jobsToExecute) throws AlgebricksException {
        // #. prepare jobs which will drop corresponding feed storage
        addFeedDropJob(metadataProvider, databaseName, feedsToStop, jobsToExecute);

        // #. prepare jobs which will drop corresponding datasets with indexes
        List<Dataset> datasets = addDatasetDropJob(metadataProvider, sourceLoc, databaseName, mdTxnCtx, jobsToExecute);

        // #. prepare jobs which will drop corresponding libraries
        List<Library> libraries = MetadataManager.INSTANCE.getDatabaseLibraries(mdTxnCtx, databaseName);
        for (Library library : libraries) {
            jobsToExecute.add(ExternalLibraryJobUtils.buildDropLibraryJobSpec(
                    new Namespace(library.getDatabaseName(), library.getDataverseName()), library.getName(),
                    metadataProvider));
        }

        // #. prepare jobs which will drop the database
        jobsToExecute.add(DataverseUtil.dropDatabaseJobSpec(databaseName, metadataProvider));

        return datasets;
    }

    private static List<Dataset> addDatasetDropJob(MetadataProvider metadataProvider, SourceLocation sourceLoc,
            String databaseName, MetadataTransactionContext mdTxnCtx, List<JobSpecification> jobsToExecute)
            throws AlgebricksException {
        List<Dataset> datasets = MetadataManager.INSTANCE.getDatabaseDatasets(mdTxnCtx, databaseName);
        for (Dataset dataset : datasets) {
            String datasetName = dataset.getDatasetName();
            DatasetType dsType = dataset.getDatasetType();
            switch (dsType) {
                case INTERNAL:
                    List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, databaseName,
                            dataset.getDataverseName(), datasetName);
                    for (Index index : indexes) {
                        jobsToExecute.add(IndexUtil.buildDropIndexJobSpec(index, metadataProvider, dataset, sourceLoc));
                    }
                    break;
                case EXTERNAL:
                case VIEW:
                    break;
            }
        }
        return datasets;
    }

    private void addFeedDropJob(MetadataProvider metadataProvider, String databaseName,
            List<FeedEventsListener> feedsToStop, List<JobSpecification> jobsToExecute) throws AlgebricksException {
        ActiveNotificationHandler activeEventHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        IActiveEntityEventsListener[] activeListeners = activeEventHandler.getEventListeners();
        for (IActiveEntityEventsListener listener : activeListeners) {
            EntityId activeEntityId = listener.getEntityId();
            if (activeEntityId.getExtensionName().equals(Feed.EXTENSION_NAME)
                    && activeEntityId.getDatabaseName().equals(databaseName)) {
                FeedEventsListener feedListener = (FeedEventsListener) listener;
                feedsToStop.add(feedListener);
                jobsToExecute.add(FeedOperations.buildRemoveFeedStorageJob(metadataProvider, feedListener.getFeed()));
            }
        }
    }

    private void runDropJobs(MetadataProvider mdProvider, IHyracksClientConnection hcc,
            List<FeedEventsListener> feedsToStop, List<JobSpecification> jobsToExecute) throws Exception {
        for (FeedEventsListener feedListener : feedsToStop) {
            if (feedListener.getState() != ActivityState.STOPPED) {
                feedListener.stop(mdProvider);
            }
            feedListener.unregister();
        }

        for (JobSpecification jobSpec : jobsToExecute) {
            runJob(hcc, jobSpec);
        }
    }

    protected enum CreateResult {
        NOOP,
        CREATED,
        REPLACED
    }

}
