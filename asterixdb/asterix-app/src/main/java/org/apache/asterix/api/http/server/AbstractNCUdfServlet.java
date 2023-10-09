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

import static org.apache.asterix.common.functions.ExternalFunctionLanguage.JAVA;
import static org.apache.asterix.common.functions.ExternalFunctionLanguage.PYTHON;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.MixedAttribute;

public abstract class AbstractNCUdfServlet extends AbstractServlet {

    INcApplicationContext appCtx;
    INCServiceContext srvCtx;

    protected final IApplicationContext plainAppCtx;
    private final HttpScheme httpServerProtocol;
    private final int httpServerPort;

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

    public AbstractNCUdfServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            HttpScheme httpServerProtocol, int httpServerPort) {
        super(ctx, paths);
        this.plainAppCtx = appCtx;
        this.httpServerProtocol = httpServerProtocol;
        this.httpServerPort = httpServerPort;
    }

    void readFromFile(Path filePath, IServletResponse response, String contentType, OpenOption opt) throws Exception {
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

    URI createDownloadURI(Path file) throws Exception {
        String host = appCtx.getServiceContext().getAppConfig().getString(NCConfig.Option.PUBLIC_ADDRESS);
        String path = paths[0].substring(0, servletPathLengths[0]) + GET_UDF_DIST_ENDPOINT + '/' + file.getFileName();
        return new URI(httpServerProtocol.toString(), null, host, httpServerPort, path, null, null);
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

}
