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

import static org.apache.asterix.translator.IStatementExecutor.Stats.ProfileType.COUNTS;
import static org.apache.asterix.translator.IStatementExecutor.Stats.ProfileType.FULL;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
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

public final class ExecuteStatementRequestMessage implements ICcAddressedMessage {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    //TODO: Make configurable: https://issues.apache.org/jira/browse/ASTERIXDB-2062
    public static final long DEFAULT_NC_TIMEOUT_MILLIS = TimeUnit.MILLISECONDS.toMillis(Long.MAX_VALUE);
    //TODO: Make configurable: https://issues.apache.org/jira/browse/ASTERIXDB-2063
    public static final long DEFAULT_QUERY_CANCELLATION_WAIT_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private final String requestNodeId;
    private final long requestMessageId;
    private final ILangExtension.Language lang;
    private final String statementsText;
    private final SessionConfig sessionConfig;
    private final ResultProperties resultProperties;
    private final String clientContextID;
    private final String handleUrl;
    private final Map<String, String> optionalParameters;
    private final Map<String, byte[]> statementParameters;
    private final boolean multiStatement;
    private final int statementCategoryRestrictionMask;
    private final boolean profile;
    private final IRequestReference requestReference;

    public ExecuteStatementRequestMessage(String requestNodeId, long requestMessageId, ILangExtension.Language lang,
            String statementsText, SessionConfig sessionConfig, ResultProperties resultProperties,
            String clientContextID, String handleUrl, Map<String, String> optionalParameters,
            Map<String, byte[]> statementParameters, boolean multiStatement, boolean profile,
            int statementCategoryRestrictionMask, IRequestReference requestReference) {
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
        this.profile = profile;
        this.requestReference = requestReference;
    }

    @Override
    public void handle(ICcApplicationContext ccAppCtx) throws HyracksDataException, InterruptedException {
        ICCServiceContext ccSrvContext = ccAppCtx.getServiceContext();
        ClusterControllerService ccSrv = (ClusterControllerService) ccSrvContext.getControllerService();
        CCApplication ccApp = (CCApplication) ccSrv.getApplication();
        CCMessageBroker messageBroker = (CCMessageBroker) ccSrvContext.getMessageBroker();
        final RuntimeDataException rejectionReason = getRejectionReason(ccSrv);
        if (rejectionReason != null) {
            sendRejection(rejectionReason, messageBroker);
            return;
        }
        CCExtensionManager ccExtMgr = (CCExtensionManager) ccAppCtx.getExtensionManager();
        ILangCompilationProvider compilationProvider = ccExtMgr.getCompilationProvider(lang);
        IStorageComponentProvider storageComponentProvider = ccAppCtx.getStorageComponentProvider();
        IStatementExecutorFactory statementExecutorFactory = ccApp.getStatementExecutorFactory();
        ExecuteStatementResponseMessage responseMsg = new ExecuteStatementResponseMessage(requestMessageId);
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
            IStatementExecutor.ResultMetadata outMetadata = new IStatementExecutor.ResultMetadata();
            MetadataManager.INSTANCE.init();
            IStatementExecutor translator = statementExecutorFactory.create(ccAppCtx, statements, sessionOutput,
                    compilationProvider, storageComponentProvider, new ResponsePrinter(sessionOutput));
            final IStatementExecutor.Stats stats = new IStatementExecutor.Stats();
            stats.setType(profile ? FULL : COUNTS);
            Map<String, IAObject> stmtParams = RequestParameters.deserializeParameterValues(statementParameters);
            final IRequestParameters requestParameters = new RequestParameters(requestReference, statementsText, null,
                    resultProperties, stats, outMetadata, clientContextID, optionalParameters, stmtParams,
                    multiStatement, statementCategoryRestrictionMask);
            translator.compileAndExecute(ccApp.getHcc(), requestParameters);
            translator.getWarnings(warnings, maxWarnings - warnings.size());
            stats.updateTotalWarningsCount(parserTotalWarningsCount);
            outPrinter.close();
            responseMsg.setResult(outWriter.toString());
            responseMsg.setMetadata(outMetadata);
            responseMsg.setStats(stats);
            responseMsg.setExecutionPlans(translator.getExecutionPlans());
            responseMsg.setWarnings(warnings);
        } catch (AlgebricksException | HyracksException | TokenMgrError
                | org.apache.asterix.aqlplus.parser.TokenMgrError pe) {
            // we trust that "our" exceptions are serializable and have a comprehensible error message
            GlobalConfig.ASTERIX_LOGGER.log(Level.WARN, pe.getMessage(), pe);
            responseMsg.setError(pe);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.ERROR, "Unexpected exception", e);
            responseMsg.setError(e);
        }
        try {
            messageBroker.sendApplicationMessageToNC(responseMsg, requestNodeId);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, e.toString(), e);
        }
    }

    private RuntimeDataException getRejectionReason(ClusterControllerService ccSrv) {
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

    private void sendRejection(RuntimeDataException reason, CCMessageBroker messageBroker) {
        ExecuteStatementResponseMessage responseMsg = new ExecuteStatementResponseMessage(requestMessageId);
        responseMsg.setError(reason);
        try {
            messageBroker.sendApplicationMessageToNC(responseMsg, requestNodeId);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, e.toString(), e);
        }
    }

    @Override
    public String toString() {
        return String.format("%s(id=%s, from=%s): %s", getClass().getSimpleName(), requestMessageId, requestNodeId,
                LogRedactionUtil.userData(statementsText));
    }
}
