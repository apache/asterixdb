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

import static org.apache.asterix.api.http.server.ServletConstants.SYS_AUTH_HEADER;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.message.CreateLibraryRequestMessage;
import org.apache.asterix.app.message.DropLibraryRequestMessage;
import org.apache.asterix.app.message.InternalRequestResponse;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.api.IReceptionist;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.util.ExternalLibraryUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;

public class NCUdfApiServlet extends AbstractNCUdfServlet {

    protected final ILangCompilationProvider compilationProvider;
    protected final IReceptionist receptionist;

    protected Path workingDir;
    protected String sysAuthHeader;

    private static final Logger LOGGER = LogManager.getLogger();

    public NCUdfApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            ILangCompilationProvider compilationProvider, HttpScheme httpServerProtocol, int httpServerPort) {
        super(ctx, paths, appCtx, httpServerProtocol, httpServerPort);
        this.compilationProvider = compilationProvider;
        this.receptionist = appCtx.getReceptionist();
    }

    @Override
    public void init() throws IOException {
        appCtx = (INcApplicationContext) plainAppCtx;
        srvCtx = this.appCtx.getServiceContext();
        workingDir = Paths.get(appCtx.getLibraryManager().getDistributionDir().getAbsolutePath()).normalize();
        initAuth();
        initStorage();
    }

    protected void initAuth() {
        sysAuthHeader = (String) ctx.get(SYS_AUTH_HEADER);
    }

    protected void initStorage() throws IOException {
        // prepare working directory
        if (Files.isDirectory(workingDir)) {
            try {
                FileUtils.cleanDirectory(workingDir.toFile());
            } catch (IOException e) {
                LOGGER.warn("Could not clean directory: " + workingDir, e);
            }
        } else {
            Files.deleteIfExists(workingDir);
            FileUtil.forceMkdirs(workingDir.toFile());
        }
    }

    protected Map<String, String> additionalHttpHeadersFromRequest(IServletRequest request) {
        return Collections.emptyMap();
    }

    private void doCreate(DataverseName dataverseName, String libraryName, ExternalFunctionLanguage language,
            String hash, URI downloadURI, boolean replaceIfExists, String sysAuthHeader,
            IRequestReference requestReference, IServletRequest request, IServletResponse response) throws Exception {
        INCMessageBroker ncMb = (INCMessageBroker) srvCtx.getMessageBroker();
        MessageFuture responseFuture = ncMb.registerMessageFuture();
        CreateLibraryRequestMessage req = new CreateLibraryRequestMessage(srvCtx.getNodeId(),
                responseFuture.getFutureId(), dataverseName, libraryName, language, hash, downloadURI, replaceIfExists,
                sysAuthHeader, requestReference, additionalHttpHeadersFromRequest(request));
        sendMessage(req, responseFuture, requestReference, request, response);
    }

    private void doDrop(DataverseName dataverseName, String libraryName, boolean replaceIfExists,
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
        readFromFile(filePath, response, HttpUtil.ContentType.APPLICATION_OCTET_STREAM, null);
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        HttpRequest httpRequest = request.getHttpRequest();
        Pair<DataverseName, String> libraryName = parseLibraryName(request);
        if (libraryName == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        Path libraryTempFile = null;
        FileOutputStream libTmpOut = null;
        HttpPostRequestDecoder requestDecoder = new HttpPostRequestDecoder(httpRequest);
        try {
            if (!requestDecoder.hasNext() || requestDecoder.getBodyHttpDatas().size() != 1) {
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
                return;
            }
            InterfaceHttpData httpData = requestDecoder.getBodyHttpDatas().get(0);
            if (!httpData.getHttpDataType().equals(InterfaceHttpData.HttpDataType.FileUpload)) {
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
                return;
            }
            FileUpload fileUpload = (FileUpload) httpData;
            String fileExt = FilenameUtils.getExtension(fileUpload.getFilename());
            ExternalFunctionLanguage language = getLanguageByFileExtension(fileExt);
            if (language == null) {
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
                return;
            }
            try {
                IRequestReference requestReference = receptionist.welcome(request);
                libraryTempFile = Files.createTempFile(workingDir, "lib_", '.' + fileExt);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Created temporary file " + libraryTempFile + " for library " + libraryName.first + "."
                            + libraryName.second);
                }
                MessageDigest digest = MessageDigest.getInstance("MD5");
                libTmpOut = new FileOutputStream(libraryTempFile.toFile());
                OutputStream outStream = new DigestOutputStream(libTmpOut, digest);
                InputStream uploadInput = new ByteBufInputStream(((FileUpload) httpData).getByteBuf());
                IOUtils.copyLarge(uploadInput, outStream);
                outStream.close();
                URI downloadURI = createDownloadURI(libraryTempFile);
                doCreate(libraryName.first, libraryName.second, language,
                        ExternalLibraryUtils.digestToHexString(digest), downloadURI, true, sysAuthHeader,
                        requestReference, request, response);
                response.setStatus(HttpResponseStatus.OK);
            } catch (Exception e) {
                response.setStatus(toHttpErrorStatus(e));
                PrintWriter responseWriter = response.writer();
                responseWriter.write(e.getMessage());
                responseWriter.flush();
                LOGGER.error("Error creating/updating library " + libraryName.first + "." + libraryName.second, e);
            }
        } finally {
            requestDecoder.destroy();
            if (libraryTempFile != null) {
                try {
                    if (libTmpOut != null) {
                        libTmpOut.close();
                    }
                    Files.deleteIfExists(libraryTempFile);
                } catch (IOException e) {
                    LOGGER.warn("Could not delete temporary file " + libraryTempFile, e);
                }
            }
        }
    }

    @Override
    protected void delete(IServletRequest request, IServletResponse response) {
        Pair<DataverseName, String> libraryName = parseLibraryName(request);
        if (libraryName == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        try {
            IRequestReference requestReference = receptionist.welcome(request);
            doDrop(libraryName.first, libraryName.second, false, requestReference, request, response);
            response.setStatus(HttpResponseStatus.OK);
        } catch (Exception e) {
            response.setStatus(toHttpErrorStatus(e));
            PrintWriter responseWriter = response.writer();
            responseWriter.write(e.getMessage());
            responseWriter.flush();
            LOGGER.error("Error deleting library " + libraryName.first + "." + libraryName.second, e);
        }
    }

}
