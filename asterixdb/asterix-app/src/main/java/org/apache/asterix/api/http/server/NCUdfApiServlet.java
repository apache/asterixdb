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
import static org.apache.asterix.common.library.LibraryDescriptor.FIELD_HASH;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.util.ExternalLibraryUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;

public class NCUdfApiServlet extends AbstractNCUdfServlet {

    protected final ILangCompilationProvider compilationProvider;
    protected final IReceptionist receptionist;

    protected Path workingDir;
    protected String sysAuthHeader;
    private ILibraryManager libraryManager;
    private int timeout;

    private static final Logger LOGGER = LogManager.getLogger();

    public NCUdfApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            ILangCompilationProvider compilationProvider, HttpScheme httpServerProtocol, int httpServerPort) {
        super(ctx, paths, appCtx, compilationProvider, httpServerProtocol, httpServerPort);
        this.compilationProvider = compilationProvider;
        this.receptionist = appCtx.getReceptionist();
        this.timeout = appCtx.getExternalProperties().getLibraryDeployTimeout();
    }

    @Override
    public void init() throws IOException {
        appCtx = (INcApplicationContext) plainAppCtx;
        this.libraryManager = appCtx.getLibraryManager();
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
            IRequestReference requestReference, IServletRequest request) throws Exception {
        INCMessageBroker ncMb = (INCMessageBroker) srvCtx.getMessageBroker();
        MessageFuture responseFuture = ncMb.registerMessageFuture();
        CreateLibraryRequestMessage req = new CreateLibraryRequestMessage(srvCtx.getNodeId(),
                responseFuture.getFutureId(), dataverseName, libraryName, language, hash, downloadURI, replaceIfExists,
                sysAuthHeader, requestReference, additionalHttpHeadersFromRequest(request));
        sendMessage(req, responseFuture);
    }

    private void doDrop(DataverseName dataverseName, String libraryName, boolean replaceIfExists,
            IRequestReference requestReference, IServletRequest request) throws Exception {
        INCMessageBroker ncMb = (INCMessageBroker) srvCtx.getMessageBroker();
        MessageFuture responseFuture = ncMb.registerMessageFuture();
        DropLibraryRequestMessage req =
                new DropLibraryRequestMessage(srvCtx.getNodeId(), responseFuture.getFutureId(), dataverseName,
                        libraryName, replaceIfExists, requestReference, additionalHttpHeadersFromRequest(request));
        sendMessage(req, responseFuture);
    }

    private void sendMessage(ICcAddressedMessage requestMessage, MessageFuture responseFuture) throws Exception {
        // Running on NC -> send 'execute' message to CC
        INCMessageBroker ncMb = (INCMessageBroker) srvCtx.getMessageBroker();
        InternalRequestResponse responseMsg;
        try {
            ncMb.sendMessageToPrimaryCC(requestMessage);
            responseMsg = (InternalRequestResponse) responseFuture.get(timeout, TimeUnit.SECONDS);
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
        try {
            if (localPath.equals("/") || localPath.equals("")) {
                //TODO: nicer way to get this into display form?
                Map<DataverseName, Map<String, String>> dvToLibHashes =
                        ExternalLibraryUtils.produceLibraryListing(libraryManager);
                List<Map<String, Object>> libraryList = new ArrayList<>();
                for (Map.Entry<DataverseName, Map<String, String>> dvAndLibs : dvToLibHashes.entrySet()) {
                    for (Map.Entry<String, String> libsInDv : dvAndLibs.getValue().entrySet()) {
                        Map<String, Object> libraryEntry = new HashMap<>();
                        List<String> dvParts = dvAndLibs.getKey().getParts();
                        String dvKey = getDisplayFormDataverseParameter() == null ? getDataverseParameter()
                                : getDisplayFormDataverseParameter();
                        libraryEntry.put(dvKey, dvParts.size() > 1 ? dvParts : dvAndLibs.getKey().toString());
                        libraryEntry.put(NAME_PARAMETER, libsInDv.getKey());
                        libraryEntry.put(FIELD_HASH, libsInDv.getValue());
                        libraryList.add(libraryEntry);
                    }
                }
                JsonNode libraryListing = OBJECT_MAPPER.valueToTree(libraryList);
                response.setStatus(HttpResponseStatus.OK);
                HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
                PrintWriter responseWriter = response.writer();
                JSONUtil.writeNode(responseWriter, libraryListing);
                responseWriter.flush();
            } else if (localPath(request).startsWith(GET_UDF_DIST_ENDPOINT)) {
                localPath = localPath(request).substring(GET_UDF_DIST_ENDPOINT.length());
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
            } else {
                response.setStatus(HttpResponseStatus.NOT_FOUND);
            }
        } catch (Exception e) {
            response.setStatus(toHttpErrorStatus(e));
            PrintWriter responseWriter = response.writer();
            Map<String, String> error = Collections.singletonMap("error", e.getMessage());
            String errorJson = "";
            try {
                errorJson = OBJECT_MAPPER.writeValueAsString(error);
            } catch (JsonProcessingException ex) {
                responseWriter.write("{ \"error\": \"Unable to process error message!\" }");
            }
            responseWriter.write(errorJson);
            responseWriter.flush();
            LOGGER.error("Error reading library", e);
        }
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        HttpRequest httpRequest = request.getHttpRequest();
        Path libraryTempFile = null;
        FileOutputStream libTmpOut = null;
        HttpPostRequestDecoder requestDecoder = new HttpPostRequestDecoder(httpRequest);
        try {
            LibraryUploadData uploadData = decodeMultiPartLibraryOptions(requestDecoder);
            IRequestReference requestReference = receptionist.welcome(request);
            if (uploadData.op == LibraryOperation.UPSERT) {
                ExternalFunctionLanguage language = uploadData.type;
                String fileExt = FilenameUtils.getExtension(uploadData.fileUpload.getFilename());
                libraryTempFile = Files.createTempFile(workingDir, "lib_", '.' + fileExt);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Created temporary file " + libraryTempFile + " for library " + uploadData.dataverse
                            + "." + uploadData.name);
                }
                MessageDigest digest = MessageDigest.getInstance("MD5");
                libTmpOut = new FileOutputStream(libraryTempFile.toFile());
                try (OutputStream os = new DigestOutputStream(libTmpOut, digest);
                        InputStream ui = new ByteBufInputStream((uploadData.fileUpload).getByteBuf())) {
                    IOUtils.copyLarge(ui, os);
                }
                URI downloadURI = createDownloadURI(libraryTempFile);
                doCreate(uploadData.dataverse, uploadData.name, language,
                        ExternalLibraryUtils.digestToHexString(digest), downloadURI, true, sysAuthHeader,
                        requestReference, request);
            } else if (uploadData.op == LibraryOperation.DELETE) {
                doDrop(uploadData.dataverse, uploadData.name, uploadData.replaceIfExists, requestReference, request);
            }
            response.setStatus(HttpResponseStatus.OK);
            PrintWriter responseWriter = response.writer();
            String emptyJson = "{}";
            responseWriter.write(emptyJson);
            responseWriter.flush();
        } catch (Exception e) {
            response.setStatus(toHttpErrorStatus(e));
            PrintWriter responseWriter = response.writer();
            Map<String, String> error = Collections.singletonMap("error", e.getMessage());
            String errorJson = "";
            try {
                errorJson = OBJECT_MAPPER.writeValueAsString(error);
            } catch (JsonProcessingException ex) {
                responseWriter.write("{ \"error\": \"Unable to process error message!\" }");
            }
            responseWriter.write(errorJson);
            responseWriter.flush();
            LOGGER.error("Error modifying library", e);
        } finally {
            requestDecoder.destroy();
            try {
                if (libraryTempFile != null) {
                    if (libTmpOut != null) {
                        libTmpOut.close();
                    }
                    Files.deleteIfExists(libraryTempFile);
                }
            } catch (IOException e) {
                LOGGER.warn("Could not delete temporary file " + libraryTempFile, e);
            }
        }
    }

}
