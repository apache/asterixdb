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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.cc.CCExtensionManager;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutorContext;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.cc.ClusterControllerService;

public final class ExecuteStatementRequestMessage implements ICcAddressedMessage {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(ExecuteStatementRequestMessage.class.getName());

    private final String requestNodeId;

    private final long requestMessageId;

    private final ILangExtension.Language lang;

    private final String statementsText;

    private final SessionConfig sessionConfig;

    private final IStatementExecutor.ResultDelivery delivery;

    private final String clientContextID;

    private final String handleUrl;

    public ExecuteStatementRequestMessage(String requestNodeId, long requestMessageId, ILangExtension.Language lang,
            String statementsText, SessionConfig sessionConfig, IStatementExecutor.ResultDelivery delivery,
            String clientContextID, String handleUrl) {
        this.requestNodeId = requestNodeId;
        this.requestMessageId = requestMessageId;
        this.lang = lang;
        this.statementsText = statementsText;
        this.sessionConfig = sessionConfig;
        this.delivery = delivery;
        this.clientContextID = clientContextID;
        this.handleUrl = handleUrl;
    }

    @Override
    public void handle(ICcApplicationContext ccAppCtx) throws HyracksDataException, InterruptedException {
        ICCServiceContext ccSrvContext = ccAppCtx.getServiceContext();
        ClusterControllerService ccSrv = (ClusterControllerService) ccSrvContext.getControllerService();
        CCApplication ccApp = (CCApplication) ccSrv.getApplication();
        CCMessageBroker messageBroker = (CCMessageBroker) ccSrvContext.getMessageBroker();
        CCExtensionManager ccExtMgr = (CCExtensionManager) ccAppCtx.getExtensionManager();
        ILangCompilationProvider compilationProvider = ccExtMgr.getCompilationProvider(lang);
        IStorageComponentProvider storageComponentProvider = ccAppCtx.getStorageComponentProvider();
        IStatementExecutorFactory statementExecutorFactory = ccApp.getStatementExecutorFactory();
        IStatementExecutorContext statementExecutorContext = ccApp.getStatementExecutorContext();

        ccSrv.getExecutor().submit(() -> {
            ExecuteStatementResponseMessage responseMsg = new ExecuteStatementResponseMessage(requestMessageId);

            try {
                final IClusterManagementWork.ClusterState clusterState = ClusterStateManager.INSTANCE.getState();
                if (clusterState != IClusterManagementWork.ClusterState.ACTIVE) {
                    throw new IllegalStateException("Cannot execute request, cluster is " + clusterState);
                }

                IParser parser = compilationProvider.getParserFactory().createParser(statementsText);
                List<Statement> statements = parser.parse();

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
                        compilationProvider, storageComponentProvider);
                translator.compileAndExecute(ccAppCtx.getHcc(), null, delivery, outMetadata,
                        new IStatementExecutor.Stats(), clientContextID, statementExecutorContext);

                outPrinter.close();
                responseMsg.setResult(outWriter.toString());
                responseMsg.setMetadata(outMetadata);
            } catch (TokenMgrError | org.apache.asterix.aqlplus.parser.TokenMgrError pe) {
                GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, pe.getMessage(), pe);
                responseMsg.setError(pe);
            } catch (AsterixException pe) {
                GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, pe.getMessage(), pe);
                responseMsg.setError(new AsterixException(pe.getMessage()));
            } catch (Exception e) {
                String estr = e.toString();
                GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, estr, e);
                responseMsg.setError(new Exception(estr));
            }

            try {
                messageBroker.sendApplicationMessageToNC(responseMsg, requestNodeId);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, e.toString(), e);
            }
        });
    }

    @Override
    public String toString() {
        return String.format("%s(id=%s, from=%s): %s", getClass().getSimpleName(), requestMessageId, requestNodeId,
                statementsText);
    }
}
