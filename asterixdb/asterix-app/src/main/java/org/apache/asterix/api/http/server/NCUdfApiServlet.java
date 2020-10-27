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
package org.apache.asterix.api.http.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.message.CreateLibraryRequestMessage;
import org.apache.asterix.app.message.DropLibraryRequestMessage;
import org.apache.asterix.app.message.InternalRequestResponse;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.control.common.context.ServerContext;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;

public class NCUdfApiServlet extends UdfApiServlet {

    INcApplicationContext appCtx;
    INCServiceContext srvCtx;

    public NCUdfApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            ILangCompilationProvider compilationProvider, HttpScheme httpServerProtocol, int httpServerPort) {
        super(ctx, paths, appCtx, compilationProvider, null, null, httpServerProtocol, httpServerPort);
    }

    @Override
    public void init() throws IOException {
        appCtx = (INcApplicationContext) plainAppCtx;
        srvCtx = this.appCtx.getServiceContext();
        workingDir = Paths.get(appCtx.getServiceContext().getServerCtx().getBaseDir().getAbsolutePath()).normalize()
                .resolve(Paths.get(ServerContext.APP_DIR_NAME, ExternalLibraryManager.LIBRARY_MANAGER_BASE_DIR_NAME,
                        "tmp"));
        initAuth();
        initStorage();
    }

    @Override
    protected void doCreate(DataverseName dataverseName, String libraryName, ExternalFunctionLanguage language,
            URI downloadURI, boolean replaceIfExists, String sysAuthHeader, IRequestReference requestReference,
            IServletRequest request, IServletResponse response) throws Exception {
        INCMessageBroker ncMb = (INCMessageBroker) srvCtx.getMessageBroker();
        MessageFuture responseFuture = ncMb.registerMessageFuture();
        CreateLibraryRequestMessage req = new CreateLibraryRequestMessage(srvCtx.getNodeId(),
                responseFuture.getFutureId(), dataverseName, libraryName, language, downloadURI, replaceIfExists,
                sysAuthHeader, requestReference, additionalHttpHeadersFromRequest(request));
        sendMessage(req, responseFuture, requestReference, request, response);
    }

    @Override
    protected void doDrop(DataverseName dataverseName, String libraryName, boolean replaceIfExists,
            IRequestReference requestReference, IServletRequest request, IServletResponse response) throws Exception {
        INCMessageBroker ncMb = (INCMessageBroker) srvCtx.getMessageBroker();
        MessageFuture responseFuture = ncMb.registerMessageFuture();
        DropLibraryRequestMessage req =
                new DropLibraryRequestMessage(srvCtx.getNodeId(), responseFuture.getFutureId(), dataverseName,
                        libraryName, replaceIfExists, requestReference, additionalHttpHeadersFromRequest(request));
        sendMessage(req, responseFuture, requestReference, request, response);
    }

    private void sendMessage(ICcAddressedMessage requestMessage, MessageFuture responseFuture,
            IRequestReference requestReference, IServletRequest request, IServletResponse response) throws Exception {
        // Running on NC -> send 'execute' message to CC
        INCMessageBroker ncMb = (INCMessageBroker) srvCtx.getMessageBroker();
        InternalRequestResponse responseMsg;
        try {
            ncMb.sendMessageToPrimaryCC(requestMessage);
            responseMsg = (InternalRequestResponse) responseFuture.get(120000, TimeUnit.MILLISECONDS);

        } finally {
            ncMb.deregisterMessageFuture(responseFuture.getFutureId());
        }

        Throwable err = responseMsg.getError();
        if (err != null) {
            if (err instanceof Error) {
                throw (Error) err;
            } else if (err instanceof Exception) {
                throw (Exception) err;
            } else {
                throw new Exception(err.toString(), err);
            }
        }
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        String localPath = localPath(request);
        while (localPath.startsWith("/")) {
            localPath = localPath.substring(1);
        }
        if (localPath.isEmpty()) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        Path filePath = workingDir.resolve(localPath).normalize();
        if (!filePath.startsWith(workingDir)) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        readFromFile(filePath, response);
    }

    @Override
    protected void readFromFile(Path filePath, IServletResponse response) throws Exception {
        class InputStreamGetter extends SynchronizableWork {
            private InputStream is;

            @Override
            protected void doRun() throws Exception {
                is = Files.newInputStream(filePath);
            }
        }

        InputStreamGetter r = new InputStreamGetter();
        ((NodeControllerService) srvCtx.getControllerService()).getWorkQueue().scheduleAndSync(r);

        if (r.is == null) {
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return;
        }
        try {
            response.setStatus(HttpResponseStatus.OK);
            HttpUtil.setContentType(response, "application/octet-stream");
            IOUtils.copyLarge(r.is, response.outputStream());
        } finally {
            r.is.close();
        }
    }
}
