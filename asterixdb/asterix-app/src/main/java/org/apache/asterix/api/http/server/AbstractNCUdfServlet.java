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
import static org.apache.asterix.common.functions.ExternalFunctionLanguage.JAVA;
import static org.apache.asterix.common.functions.ExternalFunctionLanguage.PYTHON;
import static org.apache.asterix.common.library.LibraryDescriptor.FIELD_HASH;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.external.util.ExternalLibraryUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.MixedAttribute;

public abstract class AbstractNCUdfServlet extends AbstractServlet {

    INcApplicationContext appCtx;
    INCServiceContext srvCtx;

    protected final IApplicationContext plainAppCtx;
    protected final IReceptionist receptionist;
    protected final int timeout;
    protected ILibraryManager libraryManager;
    protected Path workingDir;
    protected String sysAuthHeader;

    private static final Logger LOGGER = LogManager.getLogger();

    public static final String GET_UDF_DIST_ENDPOINT = "/dist";
    public static final String TYPE_PARAMETER = "type";
    public static final String DATA_PARAMETER = "data";
    public static final String NAME_KEY = "name";
    public static final String DATAVERSE_KEY = "dataverse";

    protected static final class LibraryUploadData {
        final ExternalFunctionLanguage type;
        final boolean replaceIfExists;
        final FileUpload fileUpload;

        private LibraryUploadData(MixedAttribute type, boolean replaceIfExists, InterfaceHttpData fileUpload)
                throws IOException {
            this.type = type != null ? getLanguageByTypeParameter(type.getValue()) : null;
            this.replaceIfExists = replaceIfExists;
            this.fileUpload = (FileUpload) fileUpload;
        }

        public static LibraryUploadData libraryCreationUploadData(MixedAttribute type, InterfaceHttpData fileUpload)
                throws IOException {
            //POST imples replaceIfExists
            return new LibraryUploadData(type, true, fileUpload);
        }

    }

    public AbstractNCUdfServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx) {
        super(ctx, paths);
        this.plainAppCtx = appCtx;
        this.timeout = appCtx.getExternalProperties().getLibraryDeployTimeout();
        this.receptionist = appCtx.getReceptionist();
    }

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

    protected enum LibraryOperation {
        UPSERT,
        DELETE
    }

    protected Map<String, String> additionalHttpHeadersFromRequest(IServletRequest request) {
        return Collections.emptyMap();
    }

    protected void doCreate(Namespace libNamespace, String libraryName, ExternalFunctionLanguage language, String hash,
            URI downloadURI, boolean replaceIfExists, String sysAuthHeader, IRequestReference requestReference,
            IServletRequest request) throws Exception {
        INCMessageBroker ncMb = (INCMessageBroker) srvCtx.getMessageBroker();
        MessageFuture responseFuture = ncMb.registerMessageFuture();
        CreateLibraryRequestMessage req = new CreateLibraryRequestMessage(srvCtx.getNodeId(),
                responseFuture.getFutureId(), libNamespace, libraryName, language, hash, downloadURI, replaceIfExists,
                sysAuthHeader, requestReference, additionalHttpHeadersFromRequest(request));
        sendMessage(req, responseFuture);
    }

    protected void doDrop(Namespace namespace, String libraryName, boolean replaceIfExists,
            IRequestReference requestReference, IServletRequest request) throws Exception {
        INCMessageBroker ncMb = (INCMessageBroker) srvCtx.getMessageBroker();
        MessageFuture responseFuture = ncMb.registerMessageFuture();
        DropLibraryRequestMessage req = new DropLibraryRequestMessage(srvCtx.getNodeId(), responseFuture.getFutureId(),
                namespace, libraryName, replaceIfExists, requestReference, additionalHttpHeadersFromRequest(request));
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

    protected void handleModification(IServletRequest request, IServletResponse response, LibraryOperation op) {
        HttpRequest httpRequest = request.getHttpRequest();
        Path libraryTempFile = null;
        FileOutputStream libTmpOut = null;
        HttpPostRequestDecoder requestDecoder = null;
        String localPath = localPath(request);
        try {
            Pair<Namespace, String> namespaceAndName = decodeDvAndLibFromLocalPath(localPath);
            String libName = namespaceAndName.second;
            Namespace libNamespace = namespaceAndName.first;
            DataverseName libDv = libNamespace.getDataverseName();
            IRequestReference requestReference = receptionist.welcome(request);
            if (op == LibraryOperation.UPSERT) {
                requestDecoder = new HttpPostRequestDecoder(httpRequest);
                LibraryUploadData uploadData = decodeMultiPartLibraryOptions(requestDecoder);
                ExternalFunctionLanguage language = uploadData.type;
                String fileExt = FilenameUtils.getExtension(uploadData.fileUpload.getFilename());
                MessageDigest digest = MessageDigest.getInstance("MD5");
                distributeLibrary(uploadData, libDv, libName, fileExt, language, digest, libNamespace, requestReference,
                        request);
            } else if (op == LibraryOperation.DELETE) {
                //DELETE semantics imply ifExists
                doDrop(libNamespace, libName, false, requestReference, request);
            }
            response.setStatus(HttpResponseStatus.OK);
            PrintWriter responseWriter = response.writer();
            String emptyJson = "{}";
            responseWriter.write(emptyJson);
            responseWriter.flush();
        } catch (Exception e) {
            writeException(e, response);
            LOGGER.info("Error modifying library", e);
        } finally {
            if (requestDecoder != null) {
                requestDecoder.destroy();
            }
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

    protected void distributeLibrary(LibraryUploadData uploadData, DataverseName libDv, String libName, String fileExt,
            ExternalFunctionLanguage language, MessageDigest digest, Namespace namespace,
            IRequestReference requestReference, IServletRequest request) throws Exception {
    }

    protected void readFromFile(Path filePath, IServletResponse response, String contentType, OpenOption opt)
            throws Exception {
        class InputStreamGetter extends SynchronizableWork {
            private InputStream is;

            @Override
            protected void doRun() throws Exception {
                if (opt != null) {
                    is = Files.newInputStream(filePath, opt);
                } else {
                    is = Files.newInputStream(filePath);
                }
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
            HttpUtil.setContentType(response, contentType);
            IOUtils.copyLarge(r.is, response.outputStream());
        } finally {
            r.is.close();
        }
    }

    protected String getDataverseKey() {
        return DATAVERSE_KEY;
    }

    private boolean isNotAttribute(InterfaceHttpData field) {
        return field == null || !field.getHttpDataType().equals(InterfaceHttpData.HttpDataType.Attribute);
    }

    protected Pair<Namespace, String> decodeDvAndLibFromLocalPath(String localPath)
            throws RuntimeDataException, AlgebricksException {
        String[] pathSegments = StringUtils.split(localPath, '/');
        if (pathSegments.length != 2) {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED,
                    "The URL-encoded " + getDataverseKey() + " name and library name in the request path");
        }
        String namespaceStr = ServletUtil.decodeUriSegment(pathSegments[0]);
        Namespace namespace = plainAppCtx.getNamespaceResolver().resolve(namespaceStr);
        String libName = ServletUtil.decodeUriSegment(pathSegments[1]);
        return new Pair<>(namespace, libName);
    }

    protected LibraryUploadData decodeMultiPartLibraryOptions(HttpPostRequestDecoder requestDecoder)
            throws IOException {
        InterfaceHttpData typeAttribute = requestDecoder.getBodyHttpData(TYPE_PARAMETER);
        if (typeAttribute == null) {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, TYPE_PARAMETER);
        }

        else if (!isNotAttribute(typeAttribute)) {
            InterfaceHttpData libraryData = requestDecoder.getBodyHttpData(DATA_PARAMETER);
            if (libraryData == null) {
                throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, DATA_PARAMETER);
            } else if (!libraryData.getHttpDataType().equals(InterfaceHttpData.HttpDataType.FileUpload)) {
                throw RuntimeDataException.create(ErrorCode.INVALID_REQ_PARAM_VAL, DATA_PARAMETER,
                        libraryData.getHttpDataType());
            }
            LibraryUploadData uploadData =
                    LibraryUploadData.libraryCreationUploadData((MixedAttribute) typeAttribute, libraryData);
            if (uploadData.type == null) {
                throw RuntimeDataException.create(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND,
                        ((MixedAttribute) typeAttribute).getValue());
            }
            return uploadData;
        } else {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, TYPE_PARAMETER);
        }
    }

    static ExternalFunctionLanguage getLanguageByTypeParameter(String lang) {
        if (lang.equalsIgnoreCase(JAVA.name())) {
            return JAVA;
        } else if (lang.equalsIgnoreCase(PYTHON.name())) {
            return PYTHON;
        } else {
            return null;
        }
    }

    HttpResponseStatus toHttpErrorStatus(Exception e) {
        if (IFormattedException.matchesAny(e, ErrorCode.UNKNOWN_DATAVERSE, ErrorCode.UNKNOWN_LIBRARY)) {
            return HttpResponseStatus.NOT_FOUND;
        }
        if (IFormattedException.matchesAny(e, ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNKNOWN_KIND,
                ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND, ErrorCode.INVALID_REQ_PARAM_VAL,
                ErrorCode.PARAMETERS_REQUIRED, ErrorCode.INVALID_DATABASE_OBJECT_NAME)) {
            return HttpResponseStatus.BAD_REQUEST;
        }
        if (e instanceof AlgebricksException) {
            return HttpResponseStatus.BAD_REQUEST;
        }
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        String localPath = localPath(request);
        try {
            if (localPath.equals("/") || localPath.equals("")) {
                //TODO: nicer way to get this into display form?
                Map<Namespace, Map<String, String>> dvToLibHashes =
                        ExternalLibraryUtils.produceLibraryListing(libraryManager);
                List<Map<String, Object>> libraryList = new ArrayList<>();
                for (Map.Entry<Namespace, Map<String, String>> dvAndLibs : dvToLibHashes.entrySet()) {
                    for (Map.Entry<String, String> libsInDv : dvAndLibs.getValue().entrySet()) {
                        Map<String, Object> libraryEntry = new HashMap<>();
                        libraryEntry.put(getDataverseKey(), libraryManager.getNsOrDv(dvAndLibs.getKey()));
                        libraryEntry.put(NAME_KEY, libsInDv.getKey());
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
            writeException(e, response);
            LOGGER.error("Error reading library", e);
        }
    }

    protected void writeException(Exception e, IServletResponse response) {
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
    }

    abstract protected boolean isRequestPermitted(IServletRequest request, IServletResponse response)
            throws IOException;

    protected boolean isRequestOnLoopback(IServletRequest request) {
        if (request.getLocalAddress() != null && request.getRemoteAddress() != null) {
            InetAddress local = request.getLocalAddress().getAddress();
            InetAddress remote = request.getRemoteAddress().getAddress();
            return remote.isLoopbackAddress() && local.isLoopbackAddress();
        } else {
            return false;
        }
    }

    protected void rejectForbidden(IServletResponse response) throws IOException {
        // TODO: why this JSON format, do we use this anywhere else?
        sendError(response, HttpUtil.ContentType.APPLICATION_JSON, HttpResponseStatus.FORBIDDEN,
                "{ \"error\": \"Forbidden\" }");
    }

}
