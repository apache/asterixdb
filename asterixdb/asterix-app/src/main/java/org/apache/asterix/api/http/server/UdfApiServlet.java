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

import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_CONNECTION_ATTR;
import static org.apache.asterix.api.http.server.ServletConstants.SYS_AUTH_HEADER;
import static org.apache.asterix.common.functions.ExternalFunctionLanguage.JAVA;
import static org.apache.asterix.common.functions.ExternalFunctionLanguage.PYTHON;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IReceptionist;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.LibraryDescriptor;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.CreateLibraryStatement;
import org.apache.asterix.lang.common.statement.LibraryDropStatement;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.ResultProperties;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.context.ServerContext;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;

public class UdfApiServlet extends AbstractServlet {

    private static final Logger LOGGER = LogManager.getLogger();

    protected final IApplicationContext plainAppCtx;
    private ICcApplicationContext appCtx;
    private ClusterControllerService ccs;
    private final HttpScheme httpServerProtocol;
    private final int httpServerPort;

    protected final ILangCompilationProvider compilationProvider;
    protected final IStatementExecutorFactory statementExecutorFactory;
    protected final IStorageComponentProvider componentProvider;
    protected final IReceptionist receptionist;
    protected Path workingDir;
    protected String sysAuthHeader;

    public UdfApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            ILangCompilationProvider compilationProvider, IStatementExecutorFactory statementExecutorFactory,
            IStorageComponentProvider componentProvider, HttpScheme httpServerProtocol, int httpServerPort) {
        super(ctx, paths);
        this.plainAppCtx = appCtx;
        this.compilationProvider = compilationProvider;
        this.statementExecutorFactory = statementExecutorFactory;
        this.componentProvider = componentProvider;
        this.receptionist = appCtx.getReceptionist();
        this.httpServerProtocol = httpServerProtocol;
        this.httpServerPort = httpServerPort;
    }

    @Override
    public void init() throws IOException {
        appCtx = (ICcApplicationContext) plainAppCtx;
        ICCServiceContext srvCtx = this.appCtx.getServiceContext();
        this.ccs = (ClusterControllerService) srvCtx.getControllerService();
        File baseDir = srvCtx.getServerCtx().getBaseDir();
        this.workingDir = baseDir.getAbsoluteFile().toPath().normalize().resolve(
                Paths.get(ServerContext.APP_DIR_NAME, ExternalLibraryManager.LIBRARY_MANAGER_BASE_DIR_NAME, "tmp"));
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

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        HttpRequest httpRequest = request.getHttpRequest();
        Pair<DataverseName, String> libraryName = parseLibraryName(request);
        if (libraryName == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        Path libraryTempFile = null;
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
                fileUpload.renameTo(libraryTempFile.toFile());
                URI downloadURI = createDownloadURI(libraryTempFile);
                doCreate(libraryName.first, libraryName.second, language, downloadURI, true, sysAuthHeader,
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
                    Files.deleteIfExists(libraryTempFile);
                } catch (IOException e) {
                    LOGGER.warn("Could not delete temporary file " + libraryTempFile, e);
                }
            }
        }
    }

    protected void doCreate(DataverseName dataverseName, String libraryName, ExternalFunctionLanguage language,
            URI downloadURI, boolean replaceIfExists, String sysAuthHeader, IRequestReference requestReference,
            IServletRequest request, IServletResponse response) throws Exception {
        CreateLibraryStatement stmt = new CreateLibraryStatement(dataverseName, libraryName, language, downloadURI,
                replaceIfExists, sysAuthHeader);
        executeStatement(stmt, requestReference, request, response);
    }

    protected URI createDownloadURI(Path file) throws Exception {
        String path = paths[0].substring(0, trims[0]) + '/' + file.getFileName();
        String host = getHyracksClientConnection().getHost();
        return new URI(httpServerProtocol.toString(), null, host, httpServerPort, path, null, null);
    }

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

    protected void doDrop(DataverseName dataverseName, String libraryName, boolean replaceIfExists,
            IRequestReference requestReference, IServletRequest request, IServletResponse response) throws Exception {
        LibraryDropStatement stmt = new LibraryDropStatement(dataverseName, libraryName, replaceIfExists);
        executeStatement(stmt, requestReference, request, response);
    }

    protected void executeStatement(Statement statement, IRequestReference requestReference, IServletRequest request,
            IServletResponse response) throws Exception {
        IClusterManagementWork.ClusterState clusterState = appCtx.getClusterStateManager().getState();
        if (clusterState != IClusterManagementWork.ClusterState.ACTIVE) {
            response.setStatus(HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        SessionOutput sessionOutput = new SessionOutput(new SessionConfig(SessionConfig.OutputFormat.ADM),
                new PrintWriter(NullWriter.NULL_WRITER));
        ResponsePrinter printer = new ResponsePrinter(sessionOutput);
        ResultProperties resultProperties = new ResultProperties(IStatementExecutor.ResultDelivery.IMMEDIATE, 1);
        IRequestParameters requestParams = new RequestParameters(requestReference, "", null, resultProperties,
                new IStatementExecutor.Stats(), new IStatementExecutor.StatementProperties(), null, null,
                additionalHttpHeadersFromRequest(request), Collections.emptyMap(), false);
        MetadataManager.INSTANCE.init();
        IStatementExecutor translator = statementExecutorFactory.create(appCtx, Collections.singletonList(statement),
                sessionOutput, compilationProvider, componentProvider, printer);
        translator.compileAndExecute(getHyracksClientConnection(), requestParams);
    }

    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        IClusterManagementWork.ClusterState clusterState = appCtx.getClusterStateManager().getState();
        if (clusterState != IClusterManagementWork.ClusterState.ACTIVE) {
            response.setStatus(HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
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

    protected IHyracksClientConnection getHyracksClientConnection() throws Exception { // NOSONAR
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        if (hcc == null) {
            throw new RuntimeDataException(ErrorCode.PROPERTY_NOT_SET, HYRACKS_CONNECTION_ATTR);
        }
        return hcc;
    }

    protected Pair<DataverseName, String> parseLibraryName(IServletRequest request) throws IllegalArgumentException {
        String[] path = StringUtils.split(localPath(request), '/');
        int ln = path.length;
        if (ln < 2) {
            return null;
        }
        String libraryName = path[ln - 1];
        DataverseName dataverseName = DataverseName.create(Arrays.asList(path), 0, ln - 1);
        return new Pair<>(dataverseName, libraryName);
    }

    protected static ExternalFunctionLanguage getLanguageByFileExtension(String fileExtension) {
        switch (fileExtension) {
            case LibraryDescriptor.FILE_EXT_ZIP:
                return JAVA;
            case LibraryDescriptor.FILE_EXT_PYZ:
                return PYTHON;
            default:
                return null;
        }
    }

    protected HttpResponseStatus toHttpErrorStatus(Exception e) {
        if (e instanceof IFormattedException) {
            IFormattedException fe = (IFormattedException) e;
            if (ErrorCode.ASTERIX.equals(fe.getComponent())) {
                switch (fe.getErrorCode()) {
                    case ErrorCode.UNKNOWN_DATAVERSE:
                    case ErrorCode.UNKNOWN_LIBRARY:
                        return HttpResponseStatus.NOT_FOUND;
                }
            }
        }
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }

    protected void readFromFile(Path filePath, IServletResponse response) throws Exception {
        class InputStreamGetter extends SynchronizableWork {
            private InputStream is;

            @Override
            protected void doRun() throws Exception {
                is = Files.newInputStream(filePath);
            }
        }

        InputStreamGetter r = new InputStreamGetter();
        ccs.getWorkQueue().scheduleAndSync(r);

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
