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
import java.util.Collections;
import java.util.Map;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.app.cc.CCExtensionManager;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.ResultProperties;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.io.output.NullWriter;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractInternalRequestMessage implements ICcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    final String nodeRequestId;
    final long requestMessageId;
    final IRequestReference requestReference;
    final Map<String, String> additionalParams;

    public AbstractInternalRequestMessage(String nodeRequestId, long requestMessageId,
            IRequestReference requestReference, Map<String, String> additionalParams) {
        this.nodeRequestId = nodeRequestId;
        this.requestMessageId = requestMessageId;
        this.requestReference = requestReference;
        this.additionalParams = additionalParams;
    }

    @Override
    public void handle(ICcApplicationContext ccAppCtx) throws HyracksDataException {
        ICCServiceContext ccSrvContext = ccAppCtx.getServiceContext();
        ClusterControllerService ccSrv = (ClusterControllerService) ccSrvContext.getControllerService();
        CCApplication ccApp = (CCApplication) ccSrv.getApplication();
        CCMessageBroker messageBroker = (CCMessageBroker) ccSrvContext.getMessageBroker();
        final RuntimeDataException rejectionReason =
                ExecuteStatementRequestMessage.getRejectionReason(ccSrv, nodeRequestId);
        if (rejectionReason != null) {
            ExecuteStatementRequestMessage.sendRejection(rejectionReason, messageBroker, requestMessageId,
                    nodeRequestId);
            return;
        }
        CCExtensionManager ccExtMgr = (CCExtensionManager) ccAppCtx.getExtensionManager();
        ILangCompilationProvider compilationProvider = ccExtMgr.getCompilationProvider(ILangExtension.Language.SQLPP);
        IStorageComponentProvider componentProvider = ccAppCtx.getStorageComponentProvider();
        IStatementExecutorFactory statementExecutorFactory = ccApp.getStatementExecutorFactory();
        InternalRequestResponse responseMsg = new InternalRequestResponse(requestMessageId);
        SessionOutput sessionOutput = new SessionOutput(new SessionConfig(SessionConfig.OutputFormat.ADM),
                new PrintWriter(NullWriter.NULL_WRITER));
        ResponsePrinter printer = new ResponsePrinter(sessionOutput);
        ResultProperties resultProperties = new ResultProperties(IStatementExecutor.ResultDelivery.IMMEDIATE, 1);
        IRequestParameters requestParams = new RequestParameters(requestReference, "", null, resultProperties,
                new IStatementExecutor.Stats(), new IStatementExecutor.StatementProperties(), null, null,
                additionalParams, Collections.emptyMap(), false);
        MetadataManager.INSTANCE.init();
        IStatementExecutor translator =
                statementExecutorFactory.create(ccAppCtx, Collections.singletonList(produceStatement()), sessionOutput,
                        compilationProvider, componentProvider, printer);
        try {
            translator.compileAndExecute(ccAppCtx.getHcc(), requestParams);
        } catch (AlgebricksException | HyracksException | org.apache.asterix.lang.sqlpp.parser.TokenMgrError pe) {
            // we trust that "our" exceptions are serializable and have a comprehensible error message
            GlobalConfig.ASTERIX_LOGGER.log(Level.WARN, pe.getMessage(), pe);
            responseMsg.setError(pe);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.ERROR, "Unexpected exception", e);
            responseMsg.setError(e);
        }
        try {
            messageBroker.sendApplicationMessageToNC(responseMsg, nodeRequestId);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, e.toString(), e);
        }

    }

    protected abstract Statement produceStatement();

}
