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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveJobNotificationHandler;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.active.IActiveEventSubscriber;
import org.apache.asterix.algebra.extension.IExtensionStatement;
import org.apache.asterix.api.common.APIFramework;
import org.apache.asterix.api.http.server.ApiServlet;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.ResultUtil;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.DatasetConfig.TransactionState;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.IDataset;
import org.apache.asterix.common.utils.JobUtils;
import org.apache.asterix.common.utils.JobUtils.ProgressState;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedEventsListener;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.base.IStatementRewriter;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.TypeExpression;
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
import org.apache.asterix.lang.common.statement.IDatasetDetailsDecl;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.InternalDetailsDecl;
import org.apache.asterix.lang.common.statement.LoadStatement;
import org.apache.asterix.lang.common.statement.NodeGroupDropStatement;
import org.apache.asterix.lang.common.statement.NodegroupDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.RefreshExternalDatasetStatement;
import org.apache.asterix.lang.common.statement.RunStatement;
import org.apache.asterix.lang.common.statement.SetStatement;
import org.apache.asterix.lang.common.statement.StartFeedStatement;
import org.apache.asterix.lang.common.statement.StopFeedStatement;
import org.apache.asterix.lang.common.statement.TypeDecl;
import org.apache.asterix.lang.common.statement.TypeDropStatement;
import org.apache.asterix.lang.common.statement.WriteStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataException;
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
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.ExternalDatasetsRegistry;
import org.apache.asterix.metadata.utils.ExternalIndexingOperations;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.metadata.utils.MetadataLockManager;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.asterix.runtime.utils.AppContextInfo;
import org.apache.asterix.transaction.management.service.transaction.DatasetIdFactory;
import org.apache.asterix.translator.AbstractLangTranslator;
import org.apache.asterix.translator.CompiledStatements.CompiledDeleteStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledInsertStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledLoadFromFileStatement;
import org.apache.asterix.translator.CompiledStatements.CompiledUpsertStatement;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.TypeTranslator;
import org.apache.asterix.translator.util.ValidateUtil;
import org.apache.asterix.utils.DataverseUtil;
import org.apache.asterix.utils.FeedOperations;
import org.apache.asterix.utils.FlushDatasetUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
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
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

import com.google.common.collect.Lists;

/*
 * Provides functionality for executing a batch of Query statements (queries included)
 * sequentially.
 */
public class QueryTranslator extends AbstractLangTranslator implements IStatementExecutor {

    private static final Logger LOGGER = Logger.getLogger(QueryTranslator.class.getName());

    public static final boolean IS_DEBUG_MODE = false;// true
    protected final List<Statement> statements;
    protected final SessionConfig sessionConfig;
    protected Dataverse activeDataverse;
    protected final List<FunctionDecl> declaredFunctions;
    protected final APIFramework apiFramework;
    protected final IRewriterFactory rewriterFactory;
    protected final IStorageComponentProvider componentProvider;
    protected final ExecutorService executorService;

    public QueryTranslator(List<Statement> statements, SessionConfig conf,
            ILangCompilationProvider compliationProvider, IStorageComponentProvider componentProvider,
            ExecutorService executorService) {
        this.statements = statements;
        this.sessionConfig = conf;
        this.componentProvider = componentProvider;
        declaredFunctions = getDeclaredFunctions(statements);
        apiFramework = new APIFramework(compliationProvider);
        rewriterFactory = compliationProvider.getRewriterFactory();
        activeDataverse = MetadataBuiltinEntities.DEFAULT_DATAVERSE;
        this.executorService = executorService;
    }

    public SessionConfig getSessionConfig() {
        return sessionConfig;
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

    /**
     * Compiles and submits for execution a list of AQL statements.
     *
     * @param hcc
     *            A Hyracks client connection that is used to submit a jobspec to Hyracks.
     * @param hdc
     *            A Hyracks dataset client object that is used to read the results.
     * @param resultDelivery
     *            True if the results should be read asynchronously or false if we should wait for results to be read.
     * @return A List<QueryResult> containing a QueryResult instance corresponding to each submitted query.
     * @throws Exception
     */
    @Override
    public void compileAndExecute(IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery)
            throws Exception {
        compileAndExecute(hcc, hdc, resultDelivery, new Stats());
    }

    @Override
    public void compileAndExecute(IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery,
            Stats stats) throws Exception {
        int resultSetIdCounter = 0;
        FileSplit outputFile = null;
        IAWriterFactory writerFactory = PrinterBasedWriterFactory.INSTANCE;
        IResultSerializerFactoryProvider resultSerializerFactoryProvider = ResultSerializerFactoryProvider.INSTANCE;
        Map<String, String> config = new HashMap<>();
        /* Since the system runs a large number of threads, when HTTP requests don't return, it becomes difficult to
         * find the thread running the request to determine where it has stopped.
         * Setting the thread name helps make that easier
         */
        String threadName = Thread.currentThread().getName();
        Thread.currentThread().setName(QueryTranslator.class.getSimpleName());
        try {
            for (Statement stmt : statements) {
                if (sessionConfig.is(SessionConfig.FORMAT_HTML)) {
                    sessionConfig.out().println(ApiServlet.HTML_STATEMENT_SEPARATOR);
                }
                validateOperation(activeDataverse, stmt);
                rewriteStatement(stmt); // Rewrite the statement's AST.
                MetadataProvider metadataProvider = new MetadataProvider(activeDataverse, componentProvider);
                metadataProvider.setWriterFactory(writerFactory);
                metadataProvider.setResultSerializerFactoryProvider(resultSerializerFactoryProvider);
                metadataProvider.setOutputFile(outputFile);
                metadataProvider.setConfig(config);
                switch (stmt.getKind()) {
                    case Statement.Kind.SET:
                        handleSetStatement(stmt, config);
                        break;
                    case Statement.Kind.DATAVERSE_DECL:
                        activeDataverse = handleUseDataverseStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.CREATE_DATAVERSE:
                        handleCreateDataverseStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.DATASET_DECL:
                        handleCreateDatasetStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.CREATE_INDEX:
                        handleCreateIndexStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.TYPE_DECL:
                        handleCreateTypeStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.NODEGROUP_DECL:
                        handleCreateNodeGroupStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.DATAVERSE_DROP:
                        handleDataverseDropStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.DATASET_DROP:
                        handleDatasetDropStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.INDEX_DROP:
                        handleIndexDropStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.TYPE_DROP:
                        handleTypeDropStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.NODEGROUP_DROP:
                        handleNodegroupDropStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.CREATE_FUNCTION:
                        handleCreateFunctionStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.FUNCTION_DROP:
                        handleFunctionDropStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.LOAD:
                        handleLoadStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.INSERT:
                    case Statement.Kind.UPSERT:
                        if (((InsertStatement) stmt).getReturnExpression() != null) {
                            metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));
                            metadataProvider.setResultAsyncMode(resultDelivery == ResultDelivery.ASYNC
                                    || resultDelivery == ResultDelivery.DEFERRED);
                        }
                        handleInsertUpsertStatement(metadataProvider, stmt, hcc, hdc, resultDelivery, stats, false);
                        break;
                    case Statement.Kind.DELETE:
                        handleDeleteStatement(metadataProvider, stmt, hcc, false);
                        break;
                    case Statement.Kind.CREATE_FEED:
                        handleCreateFeedStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.DROP_FEED:
                        handleDropFeedStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.DROP_FEED_POLICY:
                        handleDropFeedPolicyStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.CONNECT_FEED:
                        handleConnectFeedStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.DISCONNECT_FEED:
                        handleDisconnectFeedStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.START_FEED:
                        handleStartFeedStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.STOP_FEED:
                        handleStopFeedStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.CREATE_FEED_POLICY:
                        handleCreateFeedPolicyStatement(metadataProvider, stmt);
                        break;
                    case Statement.Kind.QUERY:
                        metadataProvider.setResultSetId(new ResultSetId(resultSetIdCounter++));
                        metadataProvider.setResultAsyncMode(
                                resultDelivery == ResultDelivery.ASYNC || resultDelivery == ResultDelivery.DEFERRED);
                        handleQuery(metadataProvider, (Query) stmt, hcc, hdc, resultDelivery, stats);
                        break;
                    case Statement.Kind.COMPACT:
                        handleCompactStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.EXTERNAL_DATASET_REFRESH:
                        handleExternalDatasetRefreshStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.WRITE:
                        Pair<IAWriterFactory, FileSplit> result = handleWriteStatement(stmt);
                        writerFactory = (result.first != null) ? result.first : writerFactory;
                        outputFile = result.second;
                        break;
                    case Statement.Kind.RUN:
                        handleRunStatement(metadataProvider, stmt, hcc);
                        break;
                    case Statement.Kind.FUNCTION_DECL:
                        // No op
                        break;
                    case Statement.Kind.EXTENSION:
                        ((IExtensionStatement) stmt).handle(this, metadataProvider, hcc, hdc, resultDelivery, stats,
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
        MetadataLockManager.INSTANCE.acquireDataverseReadLock(dvName);
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
            MetadataLockManager.INSTANCE.releaseDataverseReadLock(dvName);
        }
    }

    protected void handleCreateDataverseStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {

        CreateDataverseStatement stmtCreateDataverse = (CreateDataverseStatement) stmt;
        String dvName = stmtCreateDataverse.getDataverseName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        MetadataLockManager.INSTANCE.acquireDataverseReadLock(dvName);
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
            MetadataLockManager.INSTANCE.releaseDataverseReadLock(dvName);
        }
    }

    protected void validateCompactionPolicy(String compactionPolicy, Map<String, String> compactionPolicyProperties,
            MetadataTransactionContext mdTxnCtx, boolean isExternalDataset) throws CompilationException, Exception {
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
            IHyracksClientConnection hcc) throws CompilationException, Exception {
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
        String nodegroupName = getNodeGroupName(ngNameId, dd, dataverseName);
        String compactionPolicy = dd.getCompactionPolicy();
        Map<String, String> compactionPolicyProperties = dd.getCompactionPolicyProperties();
        boolean defaultCompactionPolicy = compactionPolicy == null;
        boolean temp = dd.getDatasetDetailsDecl().isTemp();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        MetadataLockManager.INSTANCE.createDatasetBegin(dataverseName, itemTypeDataverseName,
                itemTypeDataverseName + "." + itemTypeName, metaItemTypeDataverseName,
                metaItemTypeDataverseName + "." + metaItemTypeName, nodegroupName, compactionPolicy,
                dataverseName + "." + datasetName, defaultCompactionPolicy);
        Dataset dataset = null;
        Index primaryIndex = null;
        try {

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
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    itemTypeDataverseName, itemTypeName);
            if (dt == null) {
                throw new AlgebricksException(": type " + itemTypeName + " could not be found.");
            }
            String ngName =
                    ngNameId != null ? ngNameId.getValue() : configureNodegroupForDataset(dd, dataverseName, mdTxnCtx);

            if (compactionPolicy == null) {
                compactionPolicy = GlobalConfig.DEFAULT_COMPACTION_POLICY_NAME;
                compactionPolicyProperties = GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES;
            } else {
                validateCompactionPolicy(compactionPolicy, compactionPolicyProperties, mdTxnCtx, false);
            }
            switch (dd.getDatasetType()) {
                case INTERNAL:
                    IAType itemType = dt.getDatatype();
                    if (itemType.getTypeTag() != ATypeTag.RECORD) {
                        throw new AlgebricksException("Dataset type has to be a record type.");
                    }

                    IAType metaItemType = null;
                    if (metaItemTypeDataverseName != null && metaItemTypeName != null) {
                        metaItemType = metadataProvider.findType(metaItemTypeDataverseName, metaItemTypeName);
                    }
                    if (metaItemType != null && metaItemType.getTypeTag() != ATypeTag.RECORD) {
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
                            keySourceIndicators, partitioningTypes, autogenerated, filterField, temp);
                    break;
                case EXTERNAL:
                    String adapter = ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getAdapter();
                    Map<String, String> properties =
                            ((ExternalDetailsDecl) dd.getDatasetDetailsDecl()).getProperties();

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
            primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, datasetName);
            if (dd.getDatasetType() == DatasetType.INTERNAL) {
                Dataverse dataverse =
                        MetadataManager.INSTANCE.getDataverse(metadataProvider.getMetadataTxnContext(), dataverseName);
                JobSpecification jobSpec = DatasetUtil.createDatasetJobSpec(dataverse, datasetName, metadataProvider);

                // #. make metadataTxn commit before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress.setValue(ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA);

                // #. runJob
                JobUtils.runJob(hcc, jobSpec, true);

                // #. begin new metadataTxn
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
            }

            // #. add a new dataset with PendingNoOp after deleting the dataset with PendingAddOp
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
                // As long as we updated(and committed) metadata, we should remove any effect of the job
                // because an exception occurs during runJob.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    JobSpecification jobSpec = DatasetUtil.dropDatasetJobSpec(dataset, primaryIndex, metadataProvider);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;
                    JobUtils.runJob(hcc, jobSpec, true);
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
            MetadataLockManager.INSTANCE.createDatasetEnd(dataverseName, itemTypeDataverseName,
                    itemTypeDataverseName + "." + itemTypeName, metaItemTypeDataverseName,
                    metaItemTypeDataverseName + "." + metaItemTypeName, nodegroupName, compactionPolicy,
                    dataverseName + "." + datasetName, defaultCompactionPolicy);
        }
    }

    protected void validateIfResourceIsActiveInFeed(Dataset dataset) throws CompilationException {
        StringBuilder builder = null;
        IActiveEntityEventsListener[] listeners = ActiveJobNotificationHandler.INSTANCE.getEventListeners();
        for (IActiveEntityEventsListener listener : listeners) {
            if (listener.isEntityUsingDataset(dataset)) {
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

    protected String getNodeGroupName(Identifier ngNameId, DatasetDecl dd, String dataverse) {
        if (ngNameId != null) {
            return ngNameId.getValue();
        }
        String hintValue = dd.getHints().get(DatasetNodegroupCardinalityHint.NAME);
        if (hintValue == null) {
            return MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME;
        } else {
            return dataverse + ":" + dd.getName().getValue();
        }
    }

    protected String configureNodegroupForDataset(DatasetDecl dd, String dataverse,
            MetadataTransactionContext mdTxnCtx) throws CompilationException {
        int nodegroupCardinality;
        String nodegroupName;
        String hintValue = dd.getHints().get(DatasetNodegroupCardinalityHint.NAME);
        if (hintValue == null) {
            nodegroupName = MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME;
            return nodegroupName;
        } else {
            int numChosen = 0;
            boolean valid = DatasetHints.validate(DatasetNodegroupCardinalityHint.NAME,
                    dd.getHints().get(DatasetNodegroupCardinalityHint.NAME)).first;
            if (!valid) {
                throw new CompilationException("Incorrect use of hint:" + DatasetNodegroupCardinalityHint.NAME);
            } else {
                nodegroupCardinality = Integer.parseInt(dd.getHints().get(DatasetNodegroupCardinalityHint.NAME));
            }
            List<String> nodeNames = AppContextInfo.INSTANCE.getMetadataProperties().getNodeNames();
            List<String> nodeNamesClone = new ArrayList<>(nodeNames);
            String metadataNodeName = AppContextInfo.INSTANCE.getMetadataProperties().getMetadataNodeName();
            List<String> selectedNodes = new ArrayList<>();
            selectedNodes.add(metadataNodeName);
            numChosen++;
            nodeNamesClone.remove(metadataNodeName);

            if (numChosen < nodegroupCardinality) {
                Random random = new Random();
                String[] nodes = nodeNamesClone.toArray(new String[] {});
                int[] b = new int[nodeNamesClone.size()];
                for (int i = 0; i < b.length; i++) {
                    b[i] = i;
                }

                for (int i = 0; i < nodegroupCardinality - numChosen; i++) {
                    int selected = i + random.nextInt(nodeNamesClone.size() - i);
                    int selNodeIndex = b[selected];
                    selectedNodes.add(nodes[selNodeIndex]);
                    int temp = b[0];
                    b[0] = b[selected];
                    b[selected] = temp;
                }
            }
            nodegroupName = dataverse + ":" + dd.getName().getValue();
            MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, new NodeGroup(nodegroupName, selectedNodes));
            return nodegroupName;
        }

    }

    protected void handleCreateIndexStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        ProgressState progress = ProgressState.NO_PROGRESS;
        CreateIndexStatement stmtCreateIndex = (CreateIndexStatement) stmt;
        String dataverseName = getActiveDataverse(stmtCreateIndex.getDataverseName());
        String datasetName = stmtCreateIndex.getDatasetName().getValue();
        List<Integer> keySourceIndicators = stmtCreateIndex.getFieldSourceIndicators();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        MetadataLockManager.INSTANCE.createIndexBegin(dataverseName, dataverseName + "." + datasetName);

        String indexName = null;
        JobSpecification spec = null;
        Dataset ds = null;
        // For external datasets
        List<ExternalFile> externalFilesSnapshot = null;
        boolean firstExternalDatasetIndex = false;
        boolean filesIndexReplicated = false;
        Index filesIndex = null;
        boolean datasetLocked = false;
        Index index = null;
        try {
            ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName);
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName);
            }

            indexName = stmtCreateIndex.getIndexName().getValue();
            index = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName, indexName);
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
            for (Pair<List<String>, TypeExpression> fieldExpr : stmtCreateIndex.getFieldExprs()) {
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
                    if (!stmtCreateIndex.isEnforced()) {
                        throw new AlgebricksException("Cannot create typed index on \"" + fieldExpr.first
                                + "\" field without enforcing it's type");
                    }
                    if (!isOpen) {
                        throw new AlgebricksException("Typed index on \"" + fieldExpr.first
                                + "\" field could be created only for open datatype");
                    }
                    if (stmtCreateIndex.hasMetaField()) {
                        throw new AlgebricksException("Typed open index can only be created on the record part");
                    }
                    Map<TypeSignature, IAType> typeMap =
                            TypeTranslator.computeTypes(mdTxnCtx, fieldExpr.second, indexName, dataverseName);
                    TypeSignature typeSignature = new TypeSignature(dataverseName, indexName);
                    fieldType = typeMap.get(typeSignature);
                }
                if (fieldType == null) {
                    throw new AlgebricksException(
                            "Unknown type " + (fieldExpr.second == null ? fieldExpr.first : fieldExpr.second));
                }

                indexFields.add(fieldExpr.first);
                indexFieldTypes.add(fieldType);
                ++keyIndex;
            }

            ValidateUtil.validateKeyFields(aRecordType, metaRecordType, indexFields, keySourceIndicators,
                    indexFieldTypes, stmtCreateIndex.getIndexType());

            if (index != null) {
                if (stmtCreateIndex.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("An index with this name " + indexName + " already exists.");
                }
            }

            // Checks whether a user is trying to create an inverted secondary index on a dataset
            // with a variable-length primary key.
            // Currently, we do not support this. Therefore, as a temporary solution, we print an
            // error message and stop.
            if (stmtCreateIndex.getIndexType() == IndexType.SINGLE_PARTITION_WORD_INVIX
                    || stmtCreateIndex.getIndexType() == IndexType.SINGLE_PARTITION_NGRAM_INVIX
                    || stmtCreateIndex.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                    || stmtCreateIndex.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
                List<List<String>> partitioningKeys = DatasetUtil.getPartitioningKeys(ds);
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

            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                validateIfResourceIsActiveInFeed(ds);
            } else {
                // External dataset
                // Check if the dataset is indexible
                if (!ExternalIndexingOperations.isIndexible((ExternalDatasetDetails) ds.getDatasetDetails())) {
                    throw new AlgebricksException(
                            "dataset using " + ((ExternalDatasetDetails) ds.getDatasetDetails()).getAdapter()
                                    + " Adapter can't be indexed");
                }
                // Check if the name of the index is valid
                if (!ExternalIndexingOperations.isValidIndexName(datasetName, indexName)) {
                    throw new AlgebricksException("external dataset index name is invalid");
                }

                // Check if the files index exist
                filesIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                        datasetName, IndexingConstants.getFilesIndexName(datasetName));
                firstExternalDatasetIndex = filesIndex == null;
                // Lock external dataset
                ExternalDatasetsRegistry.INSTANCE.buildIndexBegin(ds, firstExternalDatasetIndex);
                datasetLocked = true;
                if (firstExternalDatasetIndex) {
                    // Verify that no one has created an index before we acquire the lock
                    filesIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                            dataverseName, datasetName, IndexingConstants.getFilesIndexName(datasetName));
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
                    filesIndex =
                            new Index(dataverseName, datasetName, IndexingConstants.getFilesIndexName(datasetName),
                                    IndexType.BTREE, ExternalIndexingOperations.FILE_INDEX_FIELD_NAMES, null,
                                    ExternalIndexingOperations.FILE_INDEX_FIELD_TYPES, false, false,
                                    MetadataUtil.PENDING_ADD_OP);
                    MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), filesIndex);
                    // Add files to the external files index
                    for (ExternalFile file : externalFilesSnapshot) {
                        MetadataManager.INSTANCE.addExternalFile(mdTxnCtx, file);
                    }
                    // This is the first index for the external dataset, replicate the files index
                    spec = ExternalIndexingOperations.buildFilesIndexReplicationJobSpec(ds, externalFilesSnapshot,
                            metadataProvider, true);
                    if (spec == null) {
                        throw new CompilationException(
                                "Failed to create job spec for replicating Files Index For external dataset");
                    }
                    filesIndexReplicated = true;
                    JobUtils.runJob(hcc, spec, true);
                }
            }

            // check whether there exists another enforced index on the same field
            if (stmtCreateIndex.isEnforced()) {
                List<Index> indexes = MetadataManager.INSTANCE
                        .getDatasetIndexes(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName);
                for (Index existingIndex : indexes) {
                    if (existingIndex.getKeyFieldNames().equals(indexFields)
                            && !existingIndex.getKeyFieldTypes().equals(indexFieldTypes)
                            && existingIndex.isEnforcingKeyFileds()) {
                        throw new CompilationException("Cannot create index " + indexName + " , enforced index "
                                + existingIndex.getIndexName() + " on field \"" + StringUtils.join(indexFields, ',')
                                + "\" is already defined with type \"" + existingIndex.getKeyFieldTypes() + "\"");
                    }
                }
            }

            // #. add a new index with PendingAddOp
            index = new Index(dataverseName, datasetName, indexName, stmtCreateIndex.getIndexType(), indexFields,
                    keySourceIndicators, indexFieldTypes, stmtCreateIndex.getGramLength(),
                    stmtCreateIndex.isEnforced(), false, MetadataUtil.PENDING_ADD_OP);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);

            ARecordType enforcedType = null;
            ARecordType enforcedMetaType = null;
            if (stmtCreateIndex.isEnforced()) {
                Pair<ARecordType, ARecordType> enforcedTypes =
                        TypeUtil.createEnforcedType(aRecordType, metaRecordType, Lists.newArrayList(index));
                enforcedType = enforcedTypes.first;
                enforcedMetaType = enforcedTypes.second;
            }

            // #. prepare to create the index artifact in NC.
            spec = IndexUtil.buildSecondaryIndexCreationJobSpec(ds, index, aRecordType, metaRecordType, enforcedType,
                    enforcedMetaType, metadataProvider);
            if (spec == null) {
                throw new CompilationException("Failed to create job spec for creating index '"
                        + stmtCreateIndex.getDatasetName() + "." + stmtCreateIndex.getIndexName() + "'");
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

            // #. create the index artifact in NC.
            JobUtils.runJob(hcc, spec, true);

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. load data into the index in NC.
            spec = IndexUtil.buildSecondaryIndexLoadingJobSpec(ds, index, aRecordType, metaRecordType, enforcedType,
                    enforcedMetaType, metadataProvider);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            JobUtils.runJob(hcc, spec, true);

            // #. begin new metadataTxn
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. add another new index with PendingNoOp after deleting the index with PendingAddOp
            MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName, datasetName,
                    indexName);
            index.setPendingOp(MetadataUtil.PENDING_NO_OP);
            MetadataManager.INSTANCE.addIndex(metadataProvider.getMetadataTxnContext(), index);
            // add another new files index with PendingNoOp after deleting the index with
            // PendingAddOp
            if (firstExternalDatasetIndex) {
                MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                        datasetName, filesIndex.getIndexName());
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
            // If files index was replicated for external dataset, it should be cleaned up on NC side
            if (filesIndexReplicated) {
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                bActiveTxn = true;
                try {
                    JobSpecification jobSpec =
                            ExternalIndexingOperations.buildDropFilesIndexJobSpec(metadataProvider, ds);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;
                    JobUtils.runJob(hcc, jobSpec, true);
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
                    JobSpecification jobSpec = IndexUtil.buildDropSecondaryIndexJobSpec(index, metadataProvider, ds);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    bActiveTxn = false;
                    JobUtils.runJob(hcc, jobSpec, true);
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
                        throw new IllegalStateException("System is inconsistent state: pending files for("
                                + dataverseName + "." + datasetName + ") couldn't be removed from the metadata", e);
                    }
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    metadataProvider.setMetadataTxnContext(mdTxnCtx);
                    try {
                        // Drop the files index from metadata
                        MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                                datasetName, IndexingConstants.getFilesIndexName(datasetName));
                        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    } catch (Exception e2) {
                        e.addSuppressed(e2);
                        abort(e, e2, mdTxnCtx);
                        throw new IllegalStateException("System is inconsistent state: pending index(" + dataverseName
                                + "." + datasetName + "." + IndexingConstants.getFilesIndexName(datasetName)
                                + ") couldn't be removed from the metadata", e);
                    }
                }
                // remove the record from the metadata.
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                metadataProvider.setMetadataTxnContext(mdTxnCtx);
                try {
                    MetadataManager.INSTANCE.dropIndex(metadataProvider.getMetadataTxnContext(), dataverseName,
                            datasetName, indexName);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    abort(e, e2, mdTxnCtx);
                    throw new IllegalStateException("System is in inconsistent state: pending index(" + dataverseName
                            + "." + datasetName + "." + indexName + ") couldn't be removed from the metadata", e);
                }
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.createIndexEnd(dataverseName, dataverseName + "." + datasetName);
            if (datasetLocked) {
                ExternalDatasetsRegistry.INSTANCE.buildIndexEnd(ds, firstExternalDatasetIndex);
            }
        }
    }

    protected void handleCreateTypeStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        TypeDecl stmtCreateType = (TypeDecl) stmt;
        String dataverseName = getActiveDataverse(stmtCreateType.getDataverseName());
        String typeName = stmtCreateType.getIdent().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.createTypeBegin(dataverseName, dataverseName + "." + typeName);
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
            MetadataLockManager.INSTANCE.createTypeEnd(dataverseName, dataverseName + "." + typeName);
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
        MetadataLockManager.INSTANCE.acquireDataverseWriteLock(dataverseName);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
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
            // # disconnect all feeds from any datasets in the dataverse.
            IActiveEntityEventsListener[] activeListeners = ActiveJobNotificationHandler.INSTANCE.getEventListeners();
            Identifier dvId = new Identifier(dataverseName);
            for (IActiveEntityEventsListener listener : activeListeners) {
                EntityId activeEntityId = listener.getEntityId();
                if (activeEntityId.getExtensionName().equals(Feed.EXTENSION_NAME)
                        && activeEntityId.getDataverse().equals(dataverseName)) {
                    stopFeedBeforeDelete(new Pair<>(dvId, new Identifier(activeEntityId.getEntityName())),
                            metadataProvider);
                    // prepare job to remove feed log storage
                    jobsToExecute.add(FeedOperations.buildRemoveFeedStorageJob(MetadataManager.INSTANCE
                            .getFeed(mdTxnCtx, dataverseName, activeEntityId.getEntityName())));
                }
            }

            // #. prepare jobs which will drop corresponding datasets with indexes.
            List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dataverseName);
            for (int j = 0; j < datasets.size(); j++) {
                String datasetName = datasets.get(j).getDatasetName();
                DatasetType dsType = datasets.get(j).getDatasetType();
                if (dsType == DatasetType.INTERNAL) {
                    List<Index> indexes =
                            MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
                    for (int k = 0; k < indexes.size(); k++) {
                        if (indexes.get(k).isSecondaryIndex()) {
                            jobsToExecute.add(IndexUtil.buildDropSecondaryIndexJobSpec(indexes.get(k),
                                    metadataProvider, datasets.get(j)));
                        }
                    }
                    Index primaryIndex =
                            MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, datasetName);
                    jobsToExecute.add(DatasetUtil.dropDatasetJobSpec(datasets.get(j), primaryIndex, metadataProvider));
                } else {
                    // External dataset
                    List<Index> indexes =
                            MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
                    for (int k = 0; k < indexes.size(); k++) {
                        if (ExternalIndexingOperations.isFileIndex(indexes.get(k))) {
                            jobsToExecute.add(ExternalIndexingOperations.buildDropFilesIndexJobSpec(metadataProvider,
                                    datasets.get(j)));
                        } else {
                            jobsToExecute.add(IndexUtil.buildDropSecondaryIndexJobSpec(indexes.get(k),
                                    metadataProvider, datasets.get(j)));
                        }
                    }
                    ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(datasets.get(j));
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
                JobUtils.runJob(hcc, jobSpec, true);
            }

            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            bActiveTxn = true;
            metadataProvider.setMetadataTxnContext(mdTxnCtx);

            // #. finally, delete the dataverse.
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
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
                        JobUtils.runJob(hcc, jobSpec, true);
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
            MetadataLockManager.INSTANCE.releaseDataverseWriteLock(dataverseName);
            ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
        }
    }

    protected void stopFeedBeforeDelete(Pair<Identifier, Identifier> feedNameComp, MetadataProvider metadataProvider) {
        StopFeedStatement disStmt = new StopFeedStatement(feedNameComp);
        try {
            handleStopFeedStatement(metadataProvider, disStmt);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Stopped feed " + feedNameComp.second.getValue());
            }
        } catch (Exception exception) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to stop feed " + feedNameComp.second.getValue() + exception);
            }
        }
    }

    public void handleDatasetDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        DropDatasetStatement stmtDelete = (DropDatasetStatement) stmt;
        String dataverseName = getActiveDataverse(stmtDelete.getDataverseName());
        String datasetName = stmtDelete.getDatasetName().getValue();
        MutableObject<ProgressState> progress = new MutableObject<>(ProgressState.NO_PROGRESS);
        MutableObject<MetadataTransactionContext> mdTxnCtx =
                new MutableObject<>(MetadataManager.INSTANCE.beginTransaction());
        MutableBoolean bActiveTxn = new MutableBoolean(true);
        metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
        MetadataLockManager.INSTANCE.dropDatasetBegin(dataverseName, dataverseName + "." + datasetName);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        try {
            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
            if (ds == null) {
                if (stmtDelete.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
                    return;
                } else {
                    throw new AlgebricksException("There is no dataset with this name " + datasetName
                            + " in dataverse " + dataverseName + ".");
                }
            }
            ds.drop(metadataProvider, mdTxnCtx, jobsToExecute, bActiveTxn, progress, hcc);
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
            MetadataLockManager.INSTANCE.dropDatasetEnd(dataverseName, dataverseName + "." + datasetName);
            ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
        }
    }

    protected void doDropDataset(Dataset ds, String datasetName, MetadataProvider metadataProvider,
            MutableObject<MetadataTransactionContext> mdTxnCtx, List<JobSpecification> jobsToExecute,
            String dataverseName, MutableBoolean bActiveTxn, MutableObject<ProgressState> progress,
            IHyracksClientConnection hcc) throws Exception {
        Map<FeedConnectionId, Pair<JobSpecification, Boolean>> disconnectJobList = new HashMap<>();
        if (ds.getDatasetType() == DatasetType.INTERNAL) {
            // prepare job spec(s) that would disconnect any active feeds involving the dataset.
            IActiveEntityEventsListener[] activeListeners = ActiveJobNotificationHandler.INSTANCE.getEventListeners();
            for (IActiveEntityEventsListener listener : activeListeners) {
                if (listener.isEntityUsingDataset(ds)) {
                    throw new CompilationException(
                            "Can't drop dataset since it is connected to active entity: " + listener.getEntityId());
                }
            }

            // #. prepare jobs to drop the datatset and the indexes in NC
            List<Index> indexes =
                    MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx.getValue(), dataverseName, datasetName);
            for (int j = 0; j < indexes.size(); j++) {
                if (indexes.get(j).isSecondaryIndex()) {
                    jobsToExecute.add(IndexUtil.buildDropSecondaryIndexJobSpec(indexes.get(j), metadataProvider, ds));
                }
            }
            Index primaryIndex =
                    MetadataManager.INSTANCE.getIndex(mdTxnCtx.getValue(), dataverseName, datasetName, datasetName);
            jobsToExecute.add(DatasetUtil.dropDatasetJobSpec(ds, primaryIndex, metadataProvider));
            // #. mark the existing dataset as PendingDropOp
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx.getValue(),
                    new Dataset(dataverseName, datasetName, ds.getItemTypeDataverseName(), ds.getItemTypeName(),
                            ds.getMetaItemTypeDataverseName(), ds.getMetaItemTypeName(), ds.getNodeGroupName(),
                            ds.getCompactionPolicy(), ds.getCompactionPolicyProperties(), ds.getDatasetDetails(),
                            ds.getHints(), ds.getDatasetType(), ds.getDatasetId(), MetadataUtil.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
            bActiveTxn.setValue(false);
            progress.setValue(ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA);

            // # disconnect the feeds
            for (Pair<JobSpecification, Boolean> p : disconnectJobList.values()) {
                JobUtils.runJob(hcc, p.first, true);
            }

            // #. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                JobUtils.runJob(hcc, jobSpec, true);
            }

            mdTxnCtx.setValue(MetadataManager.INSTANCE.beginTransaction());
            bActiveTxn.setValue(true);
            metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
        } else {
            // External dataset
            ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(ds);
            // #. prepare jobs to drop the datatset and the indexes in NC
            List<Index> indexes =
                    MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx.getValue(), dataverseName, datasetName);
            for (int j = 0; j < indexes.size(); j++) {
                if (ExternalIndexingOperations.isFileIndex(indexes.get(j))) {
                    jobsToExecute.add(IndexUtil.buildDropSecondaryIndexJobSpec(indexes.get(j), metadataProvider, ds));
                } else {
                    jobsToExecute.add(ExternalIndexingOperations.buildDropFilesIndexJobSpec(metadataProvider, ds));
                }
            }

            // #. mark the existing dataset as PendingDropOp
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx.getValue(),
                    new Dataset(dataverseName, datasetName, ds.getItemTypeDataverseName(), ds.getItemTypeName(),
                            ds.getNodeGroupName(), ds.getCompactionPolicy(), ds.getCompactionPolicyProperties(),
                            ds.getDatasetDetails(), ds.getHints(), ds.getDatasetType(), ds.getDatasetId(),
                            MetadataUtil.PENDING_DROP_OP));

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx.getValue());
            bActiveTxn.setValue(false);
            progress.setValue(ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA);

            // #. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                JobUtils.runJob(hcc, jobSpec, true);
            }
            if (!indexes.isEmpty()) {
                ExternalDatasetsRegistry.INSTANCE.removeDatasetInfo(ds);
            }
            mdTxnCtx.setValue(MetadataManager.INSTANCE.beginTransaction());
            bActiveTxn.setValue(true);
            metadataProvider.setMetadataTxnContext(mdTxnCtx.getValue());
        }

        // #. finally, delete the dataset.
        MetadataManager.INSTANCE.dropDataset(mdTxnCtx.getValue(), dataverseName, datasetName);
        // Drop the associated nodegroup
        String nodegroup = ds.getNodeGroupName();
        if (!nodegroup.equalsIgnoreCase(MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME)) {
            MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx.getValue(), dataverseName + ":" + datasetName);
        }
    }

    protected void handleIndexDropStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {

        IndexDropStatement stmtIndexDrop = (IndexDropStatement) stmt;
        String datasetName = stmtIndexDrop.getDatasetName().getValue();
        String dataverseName = getActiveDataverse(stmtIndexDrop.getDataverseName());
        ProgressState progress = ProgressState.NO_PROGRESS;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        boolean bActiveTxn = true;
        metadataProvider.setMetadataTxnContext(mdTxnCtx);

        MetadataLockManager.INSTANCE.dropIndexBegin(dataverseName, dataverseName + "." + datasetName);

        String indexName = null;
        // For external index
        boolean dropFilesIndex = false;
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        try {

            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName);
            }
            IActiveEntityEventsListener[] listeners = ActiveJobNotificationHandler.INSTANCE.getEventListeners();
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
                // #. prepare a job to drop the index in NC.
                jobsToExecute.add(IndexUtil.buildDropSecondaryIndexJobSpec(index, metadataProvider, ds));

                // #. mark PendingDropOp on the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                        new Index(dataverseName, datasetName, indexName, index.getIndexType(),
                                index.getKeyFieldNames(), index.getKeyFieldSourceIndicators(),
                                index.getKeyFieldTypes(), index.isEnforcingKeyFileds(), index.isPrimaryIndex(),
                                MetadataUtil.PENDING_DROP_OP));

                // #. commit the existing transaction before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                for (JobSpecification jobSpec : jobsToExecute) {
                    JobUtils.runJob(hcc, jobSpec, true);
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
                // #. prepare a job to drop the index in NC.
                jobsToExecute.add(IndexUtil.buildDropSecondaryIndexJobSpec(index, metadataProvider, ds));
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
                                            index.isEnforcingKeyFileds(), externalIndex.isPrimaryIndex(),
                                            MetadataUtil.PENDING_DROP_OP));
                        }
                    }
                }

                // #. mark PendingDropOp on the existing index
                MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                MetadataManager.INSTANCE.addIndex(mdTxnCtx,
                        new Index(dataverseName, datasetName, indexName, index.getIndexType(),
                                index.getKeyFieldNames(), index.getKeyFieldSourceIndicators(),
                                index.getKeyFieldTypes(), index.isEnforcingKeyFileds(), index.isPrimaryIndex(),
                                MetadataUtil.PENDING_DROP_OP));

                // #. commit the existing transaction before calling runJob.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                bActiveTxn = false;
                progress = ProgressState.ADDED_PENDINGOP_RECORD_TO_METADATA;

                for (JobSpecification jobSpec : jobsToExecute) {
                    JobUtils.runJob(hcc, jobSpec, true);
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
                        JobUtils.runJob(hcc, jobSpec, true);
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
                    throw new IllegalStateException("System is inconsistent state: pending index(" + dataverseName
                            + "." + datasetName + "." + indexName + ") couldn't be removed from the metadata", e);
                }
            }

            throw e;

        } finally {
            MetadataLockManager.INSTANCE.dropIndexEnd(dataverseName, dataverseName + "." + datasetName);
            ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
        }
    }

    protected void handleTypeDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {

        TypeDropStatement stmtTypeDrop = (TypeDropStatement) stmt;
        String dataverseName = getActiveDataverse(stmtTypeDrop.getDataverseName());
        String typeName = stmtTypeDrop.getTypeName().getValue();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.dropTypeBegin(dataverseName, dataverseName + "." + typeName);

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
            MetadataLockManager.INSTANCE.dropTypeEnd(dataverseName, dataverseName + "." + typeName);
        }
    }

    protected void handleNodegroupDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        NodeGroupDropStatement stmtDelete = (NodeGroupDropStatement) stmt;
        String nodegroupName = stmtDelete.getNodeGroupName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.acquireNodeGroupWriteLock(nodegroupName);
        try {
            NodeGroup ng = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, nodegroupName);
            if (ng == null) {
                if (!stmtDelete.getIfExists()) {
                    throw new AlgebricksException("There is no nodegroup with this name " + nodegroupName + ".");
                }
            } else {
                MetadataManager.INSTANCE.dropNodegroup(mdTxnCtx, nodegroupName);
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.releaseNodeGroupWriteLock(nodegroupName);
        }
    }

    protected void handleCreateFunctionStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        CreateFunctionStatement cfs = (CreateFunctionStatement) stmt;
        String dataverse = getActiveDataverseName(cfs.getSignature().getNamespace());
        cfs.getSignature().setNamespace(dataverse);
        String functionName = cfs.getaAterixFunction().getName();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.functionStatementBegin(dataverse, dataverse + "." + functionName);
        try {

            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                throw new AlgebricksException("There is no dataverse with this name " + dataverse + ".");
            }
            Function function = new Function(dataverse, functionName, cfs.getaAterixFunction().getArity(),
                    cfs.getParamList(), Function.RETURNTYPE_VOID, cfs.getFunctionBody(), Function.LANGUAGE_AQL,
                    FunctionKind.SCALAR.toString());
            MetadataManager.INSTANCE.addFunction(mdTxnCtx, function);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.functionStatementEnd(dataverse, dataverse + "." + functionName);
        }
    }

    protected void handleFunctionDropStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        FunctionDropStatement stmtDropFunction = (FunctionDropStatement) stmt;
        FunctionSignature signature = stmtDropFunction.getFunctionSignature();
        signature.setNamespace(getActiveDataverseName(signature.getNamespace()));
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.functionStatementBegin(signature.getNamespace(),
                signature.getNamespace() + "." + signature.getName());
        try {
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
            if (function == null) {
                if (!stmtDropFunction.getIfExists()) {
                    throw new AlgebricksException("Unknonw function " + signature);
                }
            } else {
                MetadataManager.INSTANCE.dropFunction(mdTxnCtx, signature);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.functionStatementEnd(signature.getNamespace(),
                    signature.getNamespace() + "." + signature.getName());
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
        MetadataLockManager.INSTANCE.modifyDatasetBegin(dataverseName, dataverseName + "." + datasetName);
        try {
            CompiledLoadFromFileStatement cls =
                    new CompiledLoadFromFileStatement(dataverseName, loadStmt.getDatasetName().getValue(),
                            loadStmt.getAdapter(), loadStmt.getProperties(), loadStmt.dataIsAlreadySorted());
            JobSpecification spec =
                    apiFramework.compileQuery(hcc, metadataProvider, null, 0, null, sessionConfig, cls);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            if (spec != null) {
                JobUtils.runJob(hcc, spec, true);
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.modifyDatasetEnd(dataverseName, dataverseName + "." + datasetName);
        }
    }

    public JobSpecification handleInsertUpsertStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery,
            IStatementExecutor.Stats stats, boolean compileOnly) throws Exception {

        InsertStatement stmtInsertUpsert = (InsertStatement) stmt;
        String dataverseName = getActiveDataverse(stmtInsertUpsert.getDataverseName());
        Query query = stmtInsertUpsert.getQuery();

        final IMetadataLocker locker = new IMetadataLocker() {
            @Override
            public void lock() {
                MetadataLockManager.INSTANCE.insertDeleteUpsertBegin(dataverseName,
                        dataverseName + "." + stmtInsertUpsert.getDatasetName(), query.getDataverses(),
                        query.getDatasets());
            }

            @Override
            public void unlock() {
                MetadataLockManager.INSTANCE.insertDeleteUpsertEnd(dataverseName,
                        dataverseName + "." + stmtInsertUpsert.getDatasetName(), query.getDataverses(),
                        query.getDatasets());
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
            return compiler.compile();
        }

        if (stmtInsertUpsert.getReturnExpression() != null) {
            deliverResult(hcc, hdc, compiler, metadataProvider, locker, resultDelivery, stats);
        } else {
            locker.lock();
            try {
                final JobSpecification jobSpec = compiler.compile();
                if (jobSpec == null) {
                    return jobSpec;
                }
                JobUtils.runJob(hcc, jobSpec, true);
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
        MetadataLockManager.INSTANCE.insertDeleteUpsertBegin(dataverseName,
                dataverseName + "." + stmtDelete.getDatasetName(), stmtDelete.getDataverses(),
                stmtDelete.getDatasets());

        try {
            metadataProvider.setWriteTransaction(true);
            CompiledDeleteStatement clfrqs = new CompiledDeleteStatement(stmtDelete.getVariableExpr(), dataverseName,
                    stmtDelete.getDatasetName().getValue(), stmtDelete.getCondition(), stmtDelete.getVarCounter(),
                    stmtDelete.getQuery());
            JobSpecification jobSpec = rewriteCompileQuery(hcc, metadataProvider, clfrqs.getQuery(), clfrqs);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            if (jobSpec != null && !compileOnly) {
                JobUtils.runJob(hcc, jobSpec, true);
            }
            return jobSpec;

        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.insertDeleteUpsertEnd(dataverseName,
                    dataverseName + "." + stmtDelete.getDatasetName(), stmtDelete.getDataverses(),
                    stmtDelete.getDatasets());
        }
    }

    @Override
    public JobSpecification rewriteCompileQuery(IClusterInfoCollector clusterInfoCollector,
            MetadataProvider metadataProvider, Query query, ICompiledDmlStatement stmt)
            throws RemoteException, AlgebricksException, ACIDException {

        // Query Rewriting (happens under the same ongoing metadata transaction)
        Pair<IReturningStatement, Integer> rewrittenResult =
                apiFramework.reWriteQuery(declaredFunctions, metadataProvider, query, sessionConfig);

        // Query Compilation (happens under the same ongoing metadata transaction)
        return apiFramework.compileQuery(clusterInfoCollector, metadataProvider, (Query) rewrittenResult.first,
                rewrittenResult.second, stmt == null ? null : stmt.getDatasetName(), sessionConfig, stmt);
    }

    private JobSpecification rewriteCompileInsertUpsert(IClusterInfoCollector clusterInfoCollector,
            MetadataProvider metadataProvider, InsertStatement insertUpsert)
            throws RemoteException, AlgebricksException, ACIDException {

        // Insert/upsert statement rewriting (happens under the same ongoing metadata transaction)
        Pair<IReturningStatement, Integer> rewrittenResult =
                apiFramework.reWriteQuery(declaredFunctions, metadataProvider, insertUpsert, sessionConfig);

        InsertStatement rewrittenInsertUpsert = (InsertStatement) rewrittenResult.first;
        String dataverseName = getActiveDataverse(rewrittenInsertUpsert.getDataverseName());
        String datasetName = rewrittenInsertUpsert.getDatasetName().getValue();
        CompiledInsertStatement clfrqs;
        switch (insertUpsert.getKind()) {
            case Statement.Kind.INSERT:
                clfrqs = new CompiledInsertStatement(dataverseName, datasetName, rewrittenInsertUpsert.getQuery(),
                        rewrittenInsertUpsert.getVarCounter(), rewrittenInsertUpsert.getVar(),
                        rewrittenInsertUpsert.getReturnExpression());
                break;
            case Statement.Kind.UPSERT:
                clfrqs = new CompiledUpsertStatement(dataverseName, datasetName, rewrittenInsertUpsert.getQuery(),
                        rewrittenInsertUpsert.getVarCounter(), rewrittenInsertUpsert.getVar(),
                        rewrittenInsertUpsert.getReturnExpression());
                break;
            default:
                throw new AlgebricksException("Unsupported statement type " + rewrittenInsertUpsert.getKind());
        }
        // Insert/upsert statement compilation (happens under the same ongoing metadata transaction)
        return apiFramework.compileQuery(clusterInfoCollector, metadataProvider, rewrittenInsertUpsert.getQuery(),
                rewrittenResult.second, datasetName, sessionConfig, clfrqs);
    }

    protected void handleCreateFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        CreateFeedStatement cfs = (CreateFeedStatement) stmt;
        String dataverseName = getActiveDataverse(cfs.getDataverseName());
        String feedName = cfs.getFeedName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.createFeedBegin(dataverseName, dataverseName + "." + feedName);
        Feed feed = null;
        try {
            feed = MetadataManager.INSTANCE.getFeed(metadataProvider.getMetadataTxnContext(), dataverseName, feedName);
            if (feed != null) {
                if (cfs.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new AlgebricksException("A feed with this name " + feedName + " already exists.");
                }
            }
            String adaptorName = cfs.getAdaptorName();
            feed = new Feed(dataverseName, feedName, adaptorName, cfs.getAdaptorConfiguration());
            FeedMetadataUtil.validateFeed(feed, mdTxnCtx, metadataProvider.getLibraryManager());
            MetadataManager.INSTANCE.addFeed(metadataProvider.getMetadataTxnContext(), feed);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.createFeedEnd(dataverseName, dataverseName + "." + feedName);
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
        MetadataLockManager.INSTANCE.createFeedPolicyBegin(dataverse, dataverse + "." + policy);
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            FeedPolicyEntity feedPolicy = MetadataManager.INSTANCE
                    .getFeedPolicy(metadataProvider.getMetadataTxnContext(), dataverse, policy);
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
                FeedPolicyEntity sourceFeedPolicy = MetadataManager.INSTANCE.getFeedPolicy(
                        metadataProvider.getMetadataTxnContext(), dataverse, cfps.getSourcePolicyName());
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
                for (Entry<Object, Object> entry : prop.entrySet()) {
                    policyProperties.put((String) entry.getKey(), (String) entry.getValue());
                }
                newPolicy = new FeedPolicyEntity(dataverse, policy, description, policyProperties);
            }
            MetadataManager.INSTANCE.addFeedPolicy(mdTxnCtx, newPolicy);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (RemoteException | ACIDException e) {
            abort(e, e, mdTxnCtx);
            throw new HyracksDataException(e);
        } finally {
            MetadataLockManager.INSTANCE.createFeedPolicyEnd(dataverse, dataverse + "." + policy);
        }
    }

    protected void handleDropFeedStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        FeedDropStatement stmtFeedDrop = (FeedDropStatement) stmt;
        String dataverseName = getActiveDataverse(stmtFeedDrop.getDataverseName());
        String feedName = stmtFeedDrop.getFeedName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.dropFeedBegin(dataverseName, dataverseName + "." + feedName);

        try {
            Feed feed = MetadataManager.INSTANCE.getFeed(mdTxnCtx, dataverseName, feedName);
            if (feed == null) {
                if (!stmtFeedDrop.getIfExists()) {
                    throw new AlgebricksException("There is no feed with this name " + feedName + ".");
                }
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;
            }

            EntityId feedId = new EntityId(Feed.EXTENSION_NAME, dataverseName, feedName);
            FeedEventsListener listener =
                    (FeedEventsListener) ActiveJobNotificationHandler.INSTANCE.getActiveEntityListener(feedId);
            if (listener != null) {
                throw new AlgebricksException("Feed " + feedId
                        + " is currently active and connected to the following dataset(s) \n" + listener.toString());
            } else {
                JobSpecification spec = FeedOperations.buildRemoveFeedStorageJob(
                        MetadataManager.INSTANCE.getFeed(mdTxnCtx, feedId.getDataverse(), feedId.getEntityName()));
                JobUtils.runJob(hcc, spec, true);
                MetadataManager.INSTANCE.dropFeed(mdTxnCtx, dataverseName, feedName);
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Removed feed " + feedId);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);

        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.dropFeedEnd(dataverseName, dataverseName + "." + feedName);
        }
    }

    protected void handleDropFeedPolicyStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        FeedPolicyDropStatement stmtFeedPolicyDrop = (FeedPolicyDropStatement) stmt;
        String dataverseName = getActiveDataverse(stmtFeedPolicyDrop.getDataverseName());
        String policyName = stmtFeedPolicyDrop.getPolicyName().getValue();
        MetadataLockManager.INSTANCE.dropFeedPolicyBegin(dataverseName, dataverseName + "." + policyName);

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
            MetadataLockManager.INSTANCE.dropFeedPolicyEnd(dataverseName, dataverseName + "." + policyName);
        }
    }

    private void handleStartFeedStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        StartFeedStatement sfs = (StartFeedStatement) stmt;
        String dataverseName = getActiveDataverse(sfs.getDataverseName());
        String feedName = sfs.getFeedName().getValue();
        // Transcation handler
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        // Runtime handler
        EntityId entityId = new EntityId(Feed.EXTENSION_NAME, dataverseName, feedName);
        // Feed & Feed Connections
        Feed feed = FeedMetadataUtil.validateIfFeedExists(dataverseName, feedName,
                metadataProvider.getMetadataTxnContext());
        List<FeedConnection> feedConnections = MetadataManager.INSTANCE
                .getFeedConections(metadataProvider.getMetadataTxnContext(), dataverseName, feedName);
        ILangCompilationProvider compilationProvider = new AqlCompilationProvider();
        IStorageComponentProvider storageComponentProvider = new StorageComponentProvider();
        DefaultStatementExecutorFactory qtFactory = new DefaultStatementExecutorFactory();
        FeedEventsListener listener =
                (FeedEventsListener) ActiveJobNotificationHandler.INSTANCE.getActiveEntityListener(entityId);
        if (listener != null) {
            throw new AlgebricksException("Feed " + feedName + " is started already.");
        }
        // Start
        try {
            MetadataLockManager.INSTANCE.startFeedBegin(dataverseName, dataverseName + "." + feedName,
                    feedConnections);
            // Prepare policy
            List<IDataset> datasets = new ArrayList<>();
            for (FeedConnection connection : feedConnections) {
                datasets.add(MetadataManager.INSTANCE.getDataset(mdTxnCtx, connection.getDataverseName(),
                        connection.getDatasetName()));
            }

            org.apache.commons.lang3.tuple.Pair<JobSpecification, AlgebricksAbsolutePartitionConstraint> jobInfo =
                    FeedOperations.buildStartFeedJob(sessionConfig, metadataProvider, feed, feedConnections,
                            compilationProvider, storageComponentProvider, qtFactory, hcc);

            JobSpecification feedJob = jobInfo.getLeft();
            listener = new FeedEventsListener(entityId, datasets, jobInfo.getRight().getLocations());
            ActiveJobNotificationHandler.INSTANCE.registerListener(listener);
            IActiveEventSubscriber eventSubscriber = listener.subscribe(ActivityState.STARTED);
            feedJob.setProperty(ActiveJobNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME, entityId);
            JobUtils.runJob(hcc, feedJob,
                    Boolean.valueOf(metadataProvider.getConfig().get(StartFeedStatement.WAIT_FOR_COMPLETION)));
            eventSubscriber.sync();
            LOGGER.log(Level.INFO, "Submitted");
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            if (listener != null) {
                ActiveJobNotificationHandler.INSTANCE.unregisterListener(listener);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.startFeedEnd(dataverseName, dataverseName + "." + feedName, feedConnections);
        }
    }

    private void handleStopFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        StopFeedStatement sfst = (StopFeedStatement) stmt;
        String dataverseName = getActiveDataverse(sfst.getDataverseName());
        String feedName = sfst.getFeedName().getValue();
        EntityId feedId = new EntityId(Feed.EXTENSION_NAME, dataverseName, feedName);
        // Obtain runtime info from ActiveListener
        FeedEventsListener listener =
                (FeedEventsListener) ActiveJobNotificationHandler.INSTANCE.getActiveEntityListener(feedId);
        if (listener == null) {
            throw new AlgebricksException("Feed " + feedName + " is not started.");
        }
        IActiveEventSubscriber eventSubscriber = listener.subscribe(ActivityState.STOPPED);
        // Transaction
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.StopFeedBegin(dataverseName, feedName);
        try {
            // validate
            FeedMetadataUtil.validateIfFeedExists(dataverseName, feedName, mdTxnCtx);
            // Construct ActiveMessage
            for (int i = 0; i < listener.getSources().length; i++) {
                String intakeLocation = listener.getSources()[i];
                FeedOperations.SendStopMessageToNode(feedId, intakeLocation, i);
            }
            eventSubscriber.sync();
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.StopFeedEnd(dataverseName, feedName);
        }
    }

    private void handleConnectFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        FeedConnection fc;
        ConnectFeedStatement cfs = (ConnectFeedStatement) stmt;
        String dataverseName = getActiveDataverse(cfs.getDataverseName());
        String feedName = cfs.getFeedName();
        String datasetName = cfs.getDatasetName().getValue();
        String policyName = cfs.getPolicy();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        // validation
        Feed feed = FeedMetadataUtil.validateIfFeedExists(dataverseName, feedName,
                metadataProvider.getMetadataTxnContext());
        ARecordType outputType = FeedMetadataUtil.getOutputType(feed, feed.getAdapterConfiguration(),
                ExternalDataConstants.KEY_TYPE_NAME);
        List<FunctionSignature> appliedFunctions = cfs.getAppliedFunctions();
        // Transaction handling
        MetadataLockManager.INSTANCE.connectFeedBegin(dataverseName, dataverseName + "." + datasetName,
                dataverseName + "." + feedName);
        try {
            fc = MetadataManager.INSTANCE.getFeedConnection(metadataProvider.getMetadataTxnContext(), dataverseName,
                    feedName, datasetName);
            if (fc != null) {
                throw new AlgebricksException("Feed" + feedName + " is already connected dataset " + datasetName);
            }
            fc = new FeedConnection(dataverseName, feedName, datasetName, appliedFunctions, policyName,
                    outputType.toString());
            MetadataManager.INSTANCE.addFeedConnection(metadataProvider.getMetadataTxnContext(), fc);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.connectFeedEnd(dataverseName, dataverseName + "." + datasetName,
                    dataverseName + "." + feedName);
        }
    }

    protected void handleDisconnectFeedStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {
        DisconnectFeedStatement cfs = (DisconnectFeedStatement) stmt;
        String dataverseName = getActiveDataverse(cfs.getDataverseName());
        String datasetName = cfs.getDatasetName().getValue();
        String feedName = cfs.getFeedName().getValue();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.disconnectFeedBegin(dataverseName, dataverseName + "." + datasetName,
                dataverseName + "." + cfs.getFeedName());
        try {
            FeedMetadataUtil.validateIfDatasetExists(dataverseName, cfs.getDatasetName().getValue(), mdTxnCtx);
            FeedMetadataUtil.validateIfFeedExists(dataverseName, cfs.getFeedName().getValue(), mdTxnCtx);
            FeedConnection fc = MetadataManager.INSTANCE.getFeedConnection(metadataProvider.getMetadataTxnContext(),
                    dataverseName, feedName, datasetName);
            if (fc == null) {
                throw new CompilationException("Feed " + feedName + " is currently not connected to "
                        + cfs.getDatasetName().getValue() + ". Invalid operation!");
            }
            MetadataManager.INSTANCE.dropFeedConnection(mdTxnCtx, dataverseName, feedName, datasetName);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.disconnectFeedEnd(dataverseName, dataverseName + "." + datasetName,
                    dataverseName + "." + cfs.getFeedName());
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
        MetadataLockManager.INSTANCE.compactBegin(dataverseName, dataverseName + "." + datasetName);
        List<JobSpecification> jobsToExecute = new ArrayList<>();
        try {
            Dataset ds = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            if (ds == null) {
                throw new AlgebricksException(
                        "There is no dataset with this name " + datasetName + " in dataverse " + dataverseName + ".");
            }
            String itemTypeName = ds.getItemTypeName();
            Datatype dt = MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                    ds.getItemTypeDataverseName(), itemTypeName);
            ARecordType metaRecordType = null;
            if (ds.hasMetaPart()) {
                metaRecordType =
                        (ARecordType) MetadataManager.INSTANCE.getDatatype(metadataProvider.getMetadataTxnContext(),
                                ds.getMetaItemTypeDataverseName(), ds.getMetaItemTypeName()).getDatatype();
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
            ARecordType aRecordType = (ARecordType) dt.getDatatype();
            Pair<ARecordType, ARecordType> enforcedTypes =
                    TypeUtil.createEnforcedType(aRecordType, metaRecordType, indexes);
            ARecordType enforcedType = enforcedTypes.first;
            ARecordType enforcedMeta = enforcedTypes.second;
            if (ds.getDatasetType() == DatasetType.INTERNAL) {
                for (int j = 0; j < indexes.size(); j++) {
                    if (indexes.get(j).isSecondaryIndex()) {
                        jobsToExecute.add(IndexUtil.buildSecondaryIndexCompactJobSpec(ds, indexes.get(j), aRecordType,
                                metaRecordType, enforcedType, enforcedMeta, metadataProvider));
                    }
                }
            } else {
                prepareCompactJobsForExternalDataset(indexes, ds, jobsToExecute, aRecordType, metaRecordType,
                        metadataProvider, enforcedType, enforcedMeta);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;

            // #. run the jobs
            for (JobSpecification jobSpec : jobsToExecute) {
                JobUtils.runJob(hcc, jobSpec, true);
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.compactEnd(dataverseName, dataverseName + "." + datasetName);
            ExternalDatasetsRegistry.INSTANCE.releaseAcquiredLocks(metadataProvider);
        }
    }

    protected void prepareCompactJobsForExternalDataset(List<Index> indexes, Dataset ds,
            List<JobSpecification> jobsToExecute, ARecordType aRecordType, ARecordType metaRecordType,
            MetadataProvider metadataProvider, ARecordType enforcedType, ARecordType enforcedMeta)
            throws AlgebricksException {
        for (int j = 0; j < indexes.size(); j++) {
            if (!ExternalIndexingOperations.isFileIndex(indexes.get(j))) {
                jobsToExecute.add(IndexUtil.buildSecondaryIndexCompactJobSpec(ds, indexes.get(j), aRecordType,
                        metaRecordType, enforcedType, enforcedMeta, metadataProvider));
            }

        }
        jobsToExecute.add(ExternalIndexingOperations.compactFilesIndexJobSpec(ds, metadataProvider,
                new StorageComponentProvider()));
    }

    private interface IMetadataLocker {
        void lock();

        void unlock();
    }

    private interface IResultPrinter {
        void print(JobId jobId) throws HyracksDataException, AlgebricksException;
    }

    private interface IStatementCompiler {
        JobSpecification compile() throws AlgebricksException, RemoteException, ACIDException;
    }

    protected void handleQuery(MetadataProvider metadataProvider, Query query, IHyracksClientConnection hcc,
            IHyracksDataset hdc, ResultDelivery resultDelivery, Stats stats) throws Exception {
        final IMetadataLocker locker = new IMetadataLocker() {
            @Override
            public void lock() {
                MetadataLockManager.INSTANCE.queryBegin(activeDataverse, query.getDataverses(), query.getDatasets());
            }

            @Override
            public void unlock() {
                MetadataLockManager.INSTANCE.queryEnd(query.getDataverses(), query.getDatasets());
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
        deliverResult(hcc, hdc, compiler, metadataProvider, locker, resultDelivery, stats);
    }

    private void deliverResult(IHyracksClientConnection hcc, IHyracksDataset hdc, IStatementCompiler compiler,
            MetadataProvider metadataProvider, IMetadataLocker locker, ResultDelivery resultDelivery, Stats stats)
            throws Exception {
        final ResultSetId resultSetId = metadataProvider.getResultSetId();
        switch (resultDelivery) {
            case ASYNC:
                MutableBoolean printed = new MutableBoolean(false);
                executorService.submit(() -> {
                    JobId jobId = null;
                    try {
                        jobId = createAndRunJob(hcc, compiler, locker, resultDelivery, id -> {
                            final ResultHandle handle = new ResultHandle(id, resultSetId);
                            ResultUtil.printResultHandle(handle, sessionConfig);
                            synchronized (printed) {
                                printed.setTrue();
                                printed.notify();
                            }
                        });
                    } catch (Exception e) {
                        GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE,
                                resultDelivery.name() + " job " + "with id " + jobId + " failed", e);
                    }
                });
                synchronized (printed) {
                    while (!printed.booleanValue()) {
                        printed.wait();
                    }
                }
                break;
            case IMMEDIATE:
                createAndRunJob(hcc, compiler, locker, resultDelivery, id -> {
                    final ResultReader resultReader = new ResultReader(hdc, id, resultSetId);
                    ResultUtil.printResults(resultReader, sessionConfig, stats,
                            metadataProvider.findOutputRecordType());
                });
                break;
            case DEFERRED:
                createAndRunJob(hcc, compiler, locker, resultDelivery, id -> {
                    ResultUtil.printResultHandle(new ResultHandle(id, resultSetId), sessionConfig);
                });
                break;
            default:
                break;
        }
    }

    private static JobId createAndRunJob(IHyracksClientConnection hcc, IStatementCompiler compiler,
            IMetadataLocker locker, ResultDelivery resultDelivery, IResultPrinter printer) throws Exception {
        locker.lock();
        try {
            final JobSpecification jobSpec = compiler.compile();
            if (jobSpec == null) {
                return JobId.INVALID;
            }
            final JobId jobId = JobUtils.runJob(hcc, jobSpec, false);
            if (ResultDelivery.ASYNC == resultDelivery) {
                printer.print(jobId);
                hcc.waitForCompletion(jobId);
            } else {
                hcc.waitForCompletion(jobId);
                printer.print(jobId);
            }
            return jobId;
        } finally {
            locker.unlock();
        }
    }

    protected void handleCreateNodeGroupStatement(MetadataProvider metadataProvider, Statement stmt) throws Exception {

        NodegroupDecl stmtCreateNodegroup = (NodegroupDecl) stmt;
        String ngName = stmtCreateNodegroup.getNodegroupName().getValue();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        MetadataLockManager.INSTANCE.acquireNodeGroupWriteLock(ngName);

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
            MetadataLockManager.INSTANCE.releaseNodeGroupWriteLock(ngName);
        }
    }

    protected void handleExternalDatasetRefreshStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        RefreshExternalDatasetStatement stmtRefresh = (RefreshExternalDatasetStatement) stmt;
        String dataverseName = getActiveDataverse(stmtRefresh.getDataverseName());
        String datasetName = stmtRefresh.getDatasetName().getValue();
        TransactionState transactionState = TransactionState.COMMIT;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        MetadataLockManager.INSTANCE.refreshDatasetBegin(dataverseName, dataverseName + "." + datasetName);
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
        try {
            ds = MetadataManager.INSTANCE.getDataset(metadataProvider.getMetadataTxnContext(), dataverseName,
                    datasetName);

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
            JobUtils.runJob(hcc, spec, true);

            for (Index index : indexes) {
                if (!ExternalIndexingOperations.isFileIndex(index)) {
                    spec = ExternalIndexingOperations.buildIndexUpdateOp(ds, index, metadataFiles, addedFiles,
                            appendedFiles, metadataProvider);
                    // run the files update job
                    JobUtils.runJob(hcc, spec, true);
                }
            }

            // all index updates has completed successfully, record transaction state
            spec = ExternalIndexingOperations.buildCommitJob(ds, indexes, metadataProvider);

            // Aquire write latch again -> start a transaction and record the decision to commit
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
            JobUtils.runJob(hcc, spec, true);
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
                    JobUtils.runJob(hcc, spec, true);
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
            MetadataLockManager.INSTANCE.refreshDatasetEnd(dataverseName, dataverseName + "." + datasetName);
        }
    }

    protected void handleRunStatement(MetadataProvider metadataProvider, Statement stmt, IHyracksClientConnection hcc)
            throws CompilationException, Exception {
        RunStatement runStmt = (RunStatement) stmt;
        switch (runStmt.getSystem()) {
            case "pregel":
            case "pregelix":
                handlePregelixStatement(metadataProvider, runStmt, hcc);
                break;
            default:
                throw new AlgebricksException("The system \"" + runStmt.getSystem()
                        + "\" specified in your run statement is not supported.");
        }

    }

    protected void handlePregelixStatement(MetadataProvider metadataProvider, Statement stmt,
            IHyracksClientConnection hcc) throws Exception {
        RunStatement pregelixStmt = (RunStatement) stmt;
        boolean bActiveTxn = true;
        String dataverseNameFrom = getActiveDataverse(pregelixStmt.getDataverseNameFrom());
        String dataverseNameTo = getActiveDataverse(pregelixStmt.getDataverseNameTo());
        String datasetNameFrom = pregelixStmt.getDatasetNameFrom().getValue();
        String datasetNameTo = pregelixStmt.getDatasetNameTo().getValue();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        List<String> readDataverses = new ArrayList<>();
        readDataverses.add(dataverseNameFrom);
        List<String> readDatasets = new ArrayList<>();
        readDatasets.add(datasetNameFrom);
        MetadataLockManager.INSTANCE.insertDeleteUpsertBegin(dataverseNameTo, datasetNameTo, readDataverses,
                readDatasets);
        try {
            prepareRunExternalRuntime(metadataProvider, hcc, pregelixStmt, dataverseNameFrom, dataverseNameTo,
                    datasetNameFrom, datasetNameTo, mdTxnCtx);

            String pregelixHomeKey = "PREGELIX_HOME";
            // Finds PREGELIX_HOME in system environment variables.
            String pregelixHome = System.getenv(pregelixHomeKey);
            // Finds PREGELIX_HOME in Java properties.
            if (pregelixHome == null) {
                pregelixHome = System.getProperty(pregelixHomeKey);
            }
            // Finds PREGELIX_HOME in AsterixDB configuration.
            if (pregelixHome == null) {
                // Since there is a default value for PREGELIX_HOME in CompilerProperties,
                // pregelixHome can never be null.
                pregelixHome = AppContextInfo.INSTANCE.getCompilerProperties().getPregelixHome();
            }

            // Constructs the pregelix command line.
            List<String> cmd = constructPregelixCommand(pregelixStmt, dataverseNameFrom, datasetNameFrom,
                    dataverseNameTo, datasetNameTo);
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.directory(new File(pregelixHome));
            pb.redirectErrorStream(true);

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            bActiveTxn = false;
            // Executes the Pregelix command.
            int resultState = executeExternalShellProgram(pb);
            // Checks the return state of the external Pregelix command.
            if (resultState != 0) {
                throw new AlgebricksException(
                        "Something went wrong executing your Pregelix Job. Perhaps the Pregelix cluster "
                                + "needs to be restarted. "
                                + "Check the following things: Are the datatypes of Asterix and Pregelix matching? "
                                + "Is the server configuration correct (node names, buffer sizes, framesize)? "
                                + "Check the logfiles for more details.");
            }
        } catch (Exception e) {
            if (bActiveTxn) {
                abort(e, e, mdTxnCtx);
            }
            throw e;
        } finally {
            MetadataLockManager.INSTANCE.insertDeleteUpsertEnd(dataverseNameTo, datasetNameTo, readDataverses,
                    readDatasets);
        }
    }

    // Prepares to run a program on external runtime.
    protected void prepareRunExternalRuntime(MetadataProvider metadataProvider, IHyracksClientConnection hcc,
            RunStatement pregelixStmt, String dataverseNameFrom, String dataverseNameTo, String datasetNameFrom,
            String datasetNameTo, MetadataTransactionContext mdTxnCtx) throws Exception {
        // Validates the source/sink dataverses and datasets.
        Dataset fromDataset = metadataProvider.findDataset(dataverseNameFrom, datasetNameFrom);
        if (fromDataset == null) {
            throw new CompilationException("The source dataset " + datasetNameFrom + " in dataverse "
                    + dataverseNameFrom + " could not be found for the Run command");
        }
        Dataset toDataset = metadataProvider.findDataset(dataverseNameTo, datasetNameTo);
        if (toDataset == null) {
            throw new CompilationException("The sink dataset " + datasetNameTo + " in dataverse " + dataverseNameTo
                    + " could not be found for the Run command");
        }

        try {
            // Find the primary index of the sink dataset.
            Index toIndex = null;
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseNameTo,
                    pregelixStmt.getDatasetNameTo().getValue());
            for (Index index : indexes) {
                if (index.isPrimaryIndex()) {
                    toIndex = index;
                    break;
                }
            }
            if (toIndex == null) {
                throw new AlgebricksException("Tried to access non-existing dataset: " + datasetNameTo);
            }
            // Cleans up the sink dataset -- Drop and then Create.
            DropDatasetStatement dropStmt =
                    new DropDatasetStatement(new Identifier(dataverseNameTo), pregelixStmt.getDatasetNameTo(), true);
            this.handleDatasetDropStatement(metadataProvider, dropStmt, hcc);
            IDatasetDetailsDecl idd = new InternalDetailsDecl(toIndex.getKeyFieldNames(),
                    toIndex.getKeyFieldSourceIndicators(), false, null, toDataset.getDatasetDetails().isTemp());
            DatasetDecl createToDataset = new DatasetDecl(new Identifier(dataverseNameTo),
                    pregelixStmt.getDatasetNameTo(), new Identifier(toDataset.getItemTypeDataverseName()),
                    new Identifier(toDataset.getItemTypeName()),
                    new Identifier(toDataset.getMetaItemTypeDataverseName()),
                    new Identifier(toDataset.getMetaItemTypeName()), new Identifier(toDataset.getNodeGroupName()),
                    toDataset.getCompactionPolicy(), toDataset.getCompactionPolicyProperties(), toDataset.getHints(),
                    toDataset.getDatasetType(), idd, false);
            this.handleCreateDatasetStatement(metadataProvider, createToDataset, hcc);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
            throw new AlgebricksException("Error cleaning the result dataset. This should not happen.");
        }

        // Flushes source dataset.
        FlushDatasetUtil.flushDataset(hcc, metadataProvider, dataverseNameFrom, datasetNameFrom, datasetNameFrom);
    }

    // Executes external shell commands.
    protected int executeExternalShellProgram(ProcessBuilder pb)
            throws IOException, AlgebricksException, InterruptedException {
        Process process = pb.start();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = in.readLine()) != null) {
                LOGGER.info(line);
                if (line.contains("Exception") || line.contains("Error")) {
                    LOGGER.severe(line);
                    if (line.contains("Connection refused")) {
                        throw new AlgebricksException(
                                "The connection to your Pregelix cluster was refused. Is it running? "
                                        + "Is the port in the query correct?");
                    }
                    if (line.contains("Could not find or load main class")) {
                        throw new AlgebricksException("The main class of your Pregelix query was not found. "
                                + "Is the path to your .jar file correct?");
                    }
                    if (line.contains("ClassNotFoundException")) {
                        throw new AlgebricksException("The vertex class of your Pregelix query was not found. "
                                + "Does it exist? Is the spelling correct?");
                    }
                }
            }
            process.waitFor();
        }
        // Gets the exit value of the program.
        return process.exitValue();
    }

    // Constructs a Pregelix command line.
    protected List<String> constructPregelixCommand(RunStatement pregelixStmt, String fromDataverseName,
            String fromDatasetName, String toDataverseName, String toDatasetName) {
        // Constructs AsterixDB parameters, e.g., URL, source dataset and sink dataset.
        ExternalProperties externalProperties = AppContextInfo.INSTANCE.getExternalProperties();
        String clientIP = ClusterProperties.INSTANCE.getCluster().getMasterNode().getClientIp();
        StringBuilder asterixdbParameterBuilder = new StringBuilder();
        asterixdbParameterBuilder.append(
                "pregelix.asterixdb.url=" + "http://" + clientIP + ":" + externalProperties.getAPIServerPort() + ",");
        asterixdbParameterBuilder.append("pregelix.asterixdb.source=true,");
        asterixdbParameterBuilder.append("pregelix.asterixdb.sink=true,");
        asterixdbParameterBuilder.append("pregelix.asterixdb.input.dataverse=" + fromDataverseName + ",");
        asterixdbParameterBuilder.append("pregelix.asterixdb.input.dataset=" + fromDatasetName + ",");
        asterixdbParameterBuilder.append("pregelix.asterixdb.output.dataverse=" + toDataverseName + ",");
        asterixdbParameterBuilder.append("pregelix.asterixdb.output.dataset=" + toDatasetName + ",");
        asterixdbParameterBuilder.append("pregelix.asterixdb.output.cleanup=false,");

        // construct command
        List<String> cmds = new ArrayList<>();
        cmds.add("bin/pregelix");
        cmds.add(pregelixStmt.getParameters().get(0)); // jar
        cmds.add(pregelixStmt.getParameters().get(1)); // class

        String customizedPregelixProperty = "-cust-prop";
        String inputConverterClassKey = "pregelix.asterixdb.input.converterclass";
        String inputConverterClassValue = "=org.apache.pregelix.example.converter.VLongIdInputVertexConverter,";
        String outputConverterClassKey = "pregelix.asterixdb.output.converterclass";
        String outputConverterClassValue = "=org.apache.pregelix.example.converter.VLongIdOutputVertexConverter,";
        boolean custPropAdded = false;
        boolean meetCustProp = false;
        // User parameters.
        for (String s : pregelixStmt.getParameters().get(2).split(" ")) {
            if (meetCustProp) {
                if (!s.contains(inputConverterClassKey)) {
                    asterixdbParameterBuilder.append(inputConverterClassKey + inputConverterClassValue);
                }
                if (!s.contains(outputConverterClassKey)) {
                    asterixdbParameterBuilder.append(outputConverterClassKey + outputConverterClassValue);
                }
                cmds.add(asterixdbParameterBuilder.toString() + s);
                meetCustProp = false;
                custPropAdded = true;
                continue;
            }
            cmds.add(s);
            if (s.equals(customizedPregelixProperty)) {
                meetCustProp = true;
            }
        }

        if (!custPropAdded) {
            cmds.add(customizedPregelixProperty);
            // Appends default converter classes to asterixdbParameterBuilder.
            asterixdbParameterBuilder.append(inputConverterClassKey + inputConverterClassValue);
            asterixdbParameterBuilder.append(outputConverterClassKey + outputConverterClassValue);
            // Remove the last comma.
            asterixdbParameterBuilder.delete(asterixdbParameterBuilder.length() - 1,
                    asterixdbParameterBuilder.length());
            cmds.add(asterixdbParameterBuilder.toString());
        }
        return cmds;
    }

    @Override
    public String getActiveDataverseName(String dataverse) {
        return (dataverse != null) ? dataverse : activeDataverse.getDataverseName();
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
        try {
            if (IS_DEBUG_MODE) {
                LOGGER.log(Level.SEVERE, rootE.getMessage(), rootE);
            }
            if (mdTxnCtx != null) {
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            }
        } catch (Exception e2) {
            parentE.addSuppressed(e2);
            throw new IllegalStateException(rootE);
        }
    }

    protected void rewriteStatement(Statement stmt) throws CompilationException {
        IStatementRewriter rewriter = rewriterFactory.createStatementRewriter();
        rewriter.rewrite(stmt);
    }
}
