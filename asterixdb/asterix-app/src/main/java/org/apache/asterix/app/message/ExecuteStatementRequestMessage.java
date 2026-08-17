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

package org.apache.asterix.app.message;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.cc.CCExtensionManager;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.Stats.ProfileType;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.ResultProperties;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExecuteStatementRequestMessage implements ICcAddressedMessage {
    private static final long serialVersionUID = 4L;
    private static final Logger LOGGER = LogManager.getLogger();
    //TODO: Make configurable: https://issues.apache.org/jira/browse/ASTERIXDB-2062
    public static final long DEFAULT_NC_TIMEOUT_MILLIS = TimeUnit.MILLISECONDS.toMillis(Long.MAX_VALUE);
    //TODO: Make configurable: https://issues.apache.org/jira/browse/ASTERIXDB-2063
    public static final long DEFAULT_QUERY_CANCELLATION_WAIT_MILLIS = TimeUnit.MINUTES.toMillis(1);
    protected final String requestNodeId;
    protected final long requestMessageId;
    private final ILangExtension.Language lang;
    private final String statementsText;
    private final SessionConfig sessionConfig;
    private final ResultProperties resultProperties;
    private final String clientContextID;
    private final String defaultDataverseName;
    private final String handleUrl;
    private final Map<String, String> optionalParameters;
    private final Map<String, byte[]> statementParameters;
    private final boolean multiStatement;
    private final int statementCategoryRestrictionMask;
    private final ProfileType profileType;
    private final IRequestReference requestReference;
    private final boolean forceDropDataset;
    private final boolean skipAdmissionPolicy;
    private boolean sqlCompatMode;
    private boolean skipQueryPlanCache;

    public ExecuteStatementRequestMessage(String requestNodeId, long requestMessageId, ILangExtension.Language lang,
            String statementsText, SessionConfig sessionConfig, ResultProperties resultProperties,
            String clientContextID, String defaultDataverseName, String handleUrl,
            Map<String, String> optionalParameters, Map<String, byte[]> statementParameters, boolean multiStatement,
            ProfileType profileType, int statementCategoryRestrictionMask, IRequestReference requestReference,
            boolean forceDropDataset) {
        this(requestNodeId, requestMessageId, lang, statementsText, sessionConfig, resultProperties, clientContextID,
                defaultDataverseName, handleUrl, optionalParameters, statementParameters, multiStatement, profileType,
                statementCategoryRestrictionMask, requestReference, forceDropDataset, false);
    }

    protected ExecuteStatementRequestMessage(String requestNodeId, long requestMessageId, ILangExtension.Language lang,
            String statementsText, SessionConfig sessionConfig, ResultProperties resultProperties,
            String clientContextID, String defaultDataverseName, String handleUrl,
            Map<String, String> optionalParameters, Map<String, byte[]> statementParameters, boolean multiStatement,
            ProfileType profileType, int statementCategoryRestrictionMask, IRequestReference requestReference,
            boolean forceDropDataset, boolean skipAdmissionPolicy) {
        this.requestNodeId = requestNodeId;
        this.requestMessageId = requestMessageId;
        this.lang = lang;
        this.statementsText = statementsText;
        this.sessionConfig = sessionConfig;
        this.resultProperties = resultProperties;
        this.clientContextID = clientContextID;
        this.handleUrl = handleUrl;
        this.optionalParameters = optionalParameters;
        this.statementParameters = statementParameters;
        this.multiStatement = multiStatement;
        this.statementCategoryRestrictionMask = statementCategoryRestrictionMask;
        this.profileType = profileType;
        this.requestReference = requestReference;
        this.forceDropDataset = forceDropDataset;
        this.skipAdmissionPolicy = skipAdmissionPolicy;
        this.defaultDataverseName = defaultDataverseName;
    }

    public boolean isSQLCompatMode() {
        return sqlCompatMode;
    }

    public void setSQLCompatMode(boolean sqlCompatMode) {
        this.sqlCompatMode = sqlCompatMode;
    }

    public void setSkipQueryPlanCache(boolean skipQueryPlanCache) {
        this.skipQueryPlanCache = skipQueryPlanCache;
    }

    @Override
    public void handle(ICcApplicationContext ccAppCtx) throws HyracksDataException, InterruptedException {
        ICCServiceContext ccSrvContext = ccAppCtx.getServiceContext();
        ClusterControllerService ccSrv = (ClusterControllerService) ccSrvContext.getControllerService();
        CCApplication ccApp = (CCApplication) ccSrv.getApplication();
        CCMessageBroker messageBroker = (CCMessageBroker) ccSrvContext.getMessageBroker();
        final RuntimeDataException rejectionReason = getRejectionReason(ccSrv, requestNodeId);
        if (rejectionReason != null) {
            sendRejection(rejectionReason, messageBroker);
            return;
        }
        CCExtensionManager ccExtMgr = (CCExtensionManager) ccAppCtx.getExtensionManager();
        ILangCompilationProvider compilationProvider = ccExtMgr.getCompilationProvider(lang);
        IStorageComponentProvider storageComponentProvider = ccAppCtx.getStorageComponentProvider();
        IStatementExecutorFactory statementExecutorFactory = ccApp.getStatementExecutorFactory();
        ExecuteStatementResponseMessage responseMsg =
                new ExecuteStatementResponseMessage(requestMessageId, clientContextID, requestReference.getUuid());
        final IStatementExecutor.StatementProperties statementProperties = new IStatementExecutor.StatementProperties();
        responseMsg.setStatementProperties(statementProperties);
        final IStatementExecutor.ResultMetadata outMetadata = new IStatementExecutor.ResultMetadata();
        responseMsg.setMetadata(outMetadata);
        final IStatementExecutor.Stats stats = new IStatementExecutor.Stats();
        stats.setProfileType(profileType);
        responseMsg.setStats(stats);
        // set before the statements run: a response reporting them is served even when a later one failed
        responseMsg.setExecutionPlans(new ExecutionPlans());
        responseMsg.setWarnings(new ArrayList<>());
        try {
            List<Warning> warnings = new ArrayList<>();
            IParser parser = compilationProvider.getParserFactory().createParser(statementsText);
            List<Statement> statements = parser.parse();
            long maxWarnings = sessionConfig.getMaxWarnings();
            parser.getWarnings(warnings, maxWarnings);
            long parserTotalWarningsCount = parser.getTotalWarningsCount();
            StringWriter outWriter = new StringWriter(256);
            PrintWriter outPrinter = new PrintWriter(outWriter);
            SessionOutput.ResultDecorator resultPrefix = ResultUtil.createPreResultDecorator();
            SessionOutput.ResultDecorator resultPostfix = ResultUtil.createPostResultDecorator();
            SessionOutput.ResultAppender appendHandle = ResultUtil.createResultHandleAppender(handleUrl);
            SessionOutput.ResultAppender appendStatus = ResultUtil.createResultStatusAppender();
            SessionOutput sessionOutput = new SessionOutput(sessionConfig, outPrinter, resultPrefix, resultPostfix,
                    appendHandle, appendStatus);
            MetadataManager.INSTANCE.init();
            IStatementExecutor translator = statementExecutorFactory.create(ccAppCtx, statements, sessionOutput,
                    compilationProvider, storageComponentProvider, new ResponsePrinter(sessionOutput));
            Map<String, IAObject> stmtParams = RequestParameters.deserializeParameterValues(statementParameters);
            final IRequestParameters requestParameters =
                    createRequestParameters(statementProperties, stmtParams, outMetadata, stats);
            try {
                translator.compileAndExecute(ccApp.getHcc(), requestParameters);
            } finally {
                // the parser's warnings belong to their statement whether or not the request went on to fail
                addParserResultsToStatements(outMetadata, parser, maxWarnings);
            }
            translator.getWarnings(warnings, maxWarnings - warnings.size());
            stats.updateTotalWarningsCount(parserTotalWarningsCount);
            outPrinter.close();
            responseMsg.setResult(outWriter.toString());
            responseMsg.setExecutionPlans(translator.getExecutionPlans());
            responseMsg.setWarnings(warnings);
        } catch (AlgebricksException | HyracksException | org.apache.asterix.lang.sqlpp.parser.TokenMgrError pe) {
            // we trust that "our" exceptions are serializable and have a comprehensible error message
            GlobalConfig.ASTERIX_LOGGER.log(Level.WARN, pe.getMessage(), pe);
            responseMsg.setError(pe);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.ERROR, "Unexpected exception", e);
            responseMsg.setError(e);
        }
        sendResponseToNc(messageBroker, responseMsg);
    }

    /**
     * Adds what the parser knows about a statement - its text, and the warnings it raised - to what that statement
     * reports, matching them by position. Every warning is counted, and {@code maxWarnings} of them reported, per
     * statement.
     */
    private static void addParserResultsToStatements(IStatementExecutor.ResultMetadata outMetadata, IParser parser,
            long maxWarnings) {
        if (outMetadata.getStatements().isEmpty()) {
            // nothing reports the statements of this request, so their text is not worth building
            return;
        }
        List<Set<Warning>> warningsPerStatement = parser.getWarningsPerStatement();
        List<String> texts = parser.getStatementTexts();
        for (IStatementExecutor.StatementInfo statement : outMetadata.getStatements()) {
            int parsedAt = statement.getPosition() - 1;
            if (parsedAt < 0) {
                continue;
            }
            if (parsedAt < texts.size()) {
                statement.setText(texts.get(parsedAt));
            }
            if (parsedAt < warningsPerStatement.size()) {
                Set<Warning> parserWarnings = warningsPerStatement.get(parsedAt);
                statement.addWarnings(parserWarnings.stream().limit(maxWarnings).toList());
                if (statement.getStats() != null) {
                    statement.getStats().updateTotalWarningsCount(parserWarnings.size());
                }
            }
        }
    }

    protected IRequestParameters createRequestParameters(IStatementExecutor.StatementProperties statementProperties,
            Map<String, IAObject> stmtParams, IStatementExecutor.ResultMetadata outMetadata,
            IStatementExecutor.Stats stats) {
        RequestParameters requestParameters = new RequestParameters(requestReference, statementsText, null,
                resultProperties, stats, statementProperties, outMetadata, clientContextID, defaultDataverseName,
                optionalParameters, stmtParams, multiStatement, statementCategoryRestrictionMask, forceDropDataset,
                skipAdmissionPolicy);
        requestParameters.setSQLCompatMode(sqlCompatMode);
        requestParameters.setSkipQueryPlanCache(skipQueryPlanCache);
        return requestParameters;
    }

    protected CCMessageBroker getMessageBroker(ICcApplicationContext ccAppCtx) {
        ICCServiceContext ccSrvContext = ccAppCtx.getServiceContext();
        return (CCMessageBroker) ccSrvContext.getMessageBroker();
    }

    static RuntimeDataException getRejectionReason(ClusterControllerService ccSrv, String requestNodeId) {
        if (ccSrv.getNodeManager().getNodeControllerState(requestNodeId) == null) {
            return new RuntimeDataException(ErrorCode.REJECT_NODE_UNREGISTERED);
        }
        ICcApplicationContext appCtx = (ICcApplicationContext) ccSrv.getApplicationContext();
        IClusterStateManager csm = appCtx.getClusterStateManager();
        final IClusterManagementWork.ClusterState clusterState = csm.getState();
        if (clusterState != IClusterManagementWork.ClusterState.ACTIVE) {
            return new RuntimeDataException(ErrorCode.REJECT_BAD_CLUSTER_STATE, clusterState);
        }
        return null;
    }

    protected void sendRejection(Exception reason, CCMessageBroker messageBroker) {
        ExecuteStatementResponseMessage responseMsg = createFailureMessage(reason);
        try {
            messageBroker.sendApplicationMessageToNC(responseMsg, requestNodeId);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, e.toString(), e);
        }
    }

    private void sendResponseToNc(CCMessageBroker messageBroker, ExecuteStatementResponseMessage responseMsg) {
        try {
            messageBroker.sendApplicationMessageToNC(responseMsg, requestNodeId);
        } catch (Exception e) {
            LOGGER.error("unexpected exception sending message to node {}", requestNodeId, e);
            ExecuteStatementResponseMessage failureMessage = createFailureMessage(new Exception(e.getMessage()));
            messageBroker.sendMessageQuietly(failureMessage, requestNodeId);
        }
    }

    private ExecuteStatementResponseMessage createFailureMessage(Exception reason) {
        ExecuteStatementResponseMessage responseMsg =
                new ExecuteStatementResponseMessage(requestMessageId, clientContextID, requestReference.getUuid());
        responseMsg.setError(reason);
        return responseMsg;
    }

    @Override
    public String toString() {
        if (statementsText != null && (statementsText.startsWith("UPSERT") || statementsText.startsWith("INSERT"))) {
            return String.format("%s(id=%s, from=%s, uuid=%s, clientContextID=%s): %s", getClass().getSimpleName(),
                    requestMessageId, requestNodeId, requestReference.getUuid(), clientContextID,
                    "UPSERT/INSERT statement");
        } else {
            return String.format("%s(id=%s, from=%s, uuid=%s, clientContextID=%s): %s", getClass().getSimpleName(),
                    requestMessageId, requestNodeId, requestReference.getUuid(), clientContextID,
                    LogRedactionUtil.statement(statementsText));
        }
    }
}
