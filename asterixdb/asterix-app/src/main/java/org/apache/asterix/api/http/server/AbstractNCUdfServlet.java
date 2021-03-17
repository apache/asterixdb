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
import static org.apache.asterix.common.functions.ExternalFunctionLanguage.JAVA;
import static org.apache.asterix.common.functions.ExternalFunctionLanguage.PYTHON;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.IFormattedException;
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

    private final IParserFactory parserFactory;
    INcApplicationContext appCtx;
    INCServiceContext srvCtx;

    protected final IApplicationContext plainAppCtx;
    private final HttpScheme httpServerProtocol;
    private final int httpServerPort;

    public static final String GET_UDF_DIST_ENDPOINT = "/dist";
    public static final String DATAVERSE_PARAMETER = "dataverse";
    public static final String NAME_PARAMETER = "name";
    public static final String TYPE_PARAMETER = "type";
    public static final String DELETE_PARAMETER = "delete";
    public static final String IFEXISTS_PARAMETER = "ifexists";
    public static final String DATA_PARAMETER = "data";

    protected enum LibraryOperation {
        UPSERT,
        DELETE
    }

    protected final static class LibraryUploadData {

        final LibraryOperation op;
        final DataverseName dataverse;
        final String name;
        final ExternalFunctionLanguage type;
        final boolean replaceIfExists;
        final FileUpload fileUpload;

        private LibraryUploadData(LibraryOperation op, List<InterfaceHttpData> dataverse, MixedAttribute name,
                MixedAttribute type, boolean replaceIfExists, InterfaceHttpData fileUpload) throws IOException {
            this.op = op;
            List<String> dataverseParts = new ArrayList<>(dataverse.size());
            for (InterfaceHttpData attr : dataverse) {
                dataverseParts.add(((MixedAttribute) attr).getValue());
            }
            this.dataverse = DataverseName.create(dataverseParts);
            this.name = name.getValue();
            this.type = type != null ? getLanguageByTypeParameter(type.getValue()) : null;
            this.replaceIfExists = replaceIfExists;
            this.fileUpload = (FileUpload) fileUpload;
        }

        private LibraryUploadData(LibraryOperation op, DataverseName dataverse, MixedAttribute name,
                MixedAttribute type, boolean replaceIfExists, InterfaceHttpData fileUpload) throws IOException {
            this.op = op;
            this.dataverse = dataverse;
            this.name = name.getValue();
            this.type = type != null ? getLanguageByTypeParameter(type.getValue()) : null;
            this.replaceIfExists = replaceIfExists;
            this.fileUpload = (FileUpload) fileUpload;
        }

        public static LibraryUploadData libraryCreationUploadData(List<InterfaceHttpData> dataverse,
                MixedAttribute name, MixedAttribute type, InterfaceHttpData fileUpload) throws IOException {
            return new LibraryUploadData(LibraryOperation.UPSERT, dataverse, name, type, true, fileUpload);
        }

        public static LibraryUploadData libraryDeletionUploadData(List<InterfaceHttpData> dataverse,
                MixedAttribute name, boolean replaceIfExists) throws IOException {
            return new LibraryUploadData(LibraryOperation.DELETE, dataverse, name, null, replaceIfExists, null);
        }

        public static LibraryUploadData libraryCreationUploadData(DataverseName dataverse, MixedAttribute name,
                MixedAttribute type, InterfaceHttpData fileUpload) throws IOException {
            return new LibraryUploadData(LibraryOperation.UPSERT, dataverse, name, type, true, fileUpload);
        }

        public static LibraryUploadData libraryDeletionUploadData(DataverseName dataverse, MixedAttribute name,
                boolean replaceIfExists) throws IOException {
            return new LibraryUploadData(LibraryOperation.DELETE, dataverse, name, null, replaceIfExists, null);
        }
    }

    public AbstractNCUdfServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            ILangCompilationProvider compilationProvider, HttpScheme httpServerProtocol, int httpServerPort) {

        super(ctx, paths);
        this.plainAppCtx = appCtx;
        this.httpServerProtocol = httpServerProtocol;
        this.httpServerPort = httpServerPort;
        this.parserFactory = compilationProvider.getParserFactory();
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

    URI createDownloadURI(Path file) throws Exception {
        String path = paths[0].substring(0, trims[0]) + GET_UDF_DIST_ENDPOINT + '/' + file.getFileName();
        String host = getHyracksClientConnection().getHost();
        return new URI(httpServerProtocol.toString(), null, host, httpServerPort, path, null, null);
    }

    IHyracksClientConnection getHyracksClientConnection() throws Exception { // NOSONAR
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        if (hcc == null) {
            throw new RuntimeDataException(ErrorCode.PROPERTY_NOT_SET, HYRACKS_CONNECTION_ATTR);
        }
        return hcc;
    }

    protected String getDisplayFormDataverseParameter() {
        return null;
    }

    protected String getDataverseParameter() {
        return DATAVERSE_PARAMETER;
    }

    private boolean isNotAttribute(InterfaceHttpData field) {
        return field == null || !field.getHttpDataType().equals(InterfaceHttpData.HttpDataType.Attribute);
    }

    private boolean areNotAttributes(List<InterfaceHttpData> fields) {
        return fields == null || fields.stream().map(InterfaceHttpData::getHttpDataType)
                .anyMatch(httpDataType -> !httpDataType.equals(InterfaceHttpData.HttpDataType.Attribute));
    }

    protected LibraryUploadData decodeMultiPartLibraryOptions(HttpPostRequestDecoder requestDecoder)
            throws IOException, CompilationException {
        List<InterfaceHttpData> dataverseAttributeParts = requestDecoder.getBodyHttpDatas(DATAVERSE_PARAMETER);
        InterfaceHttpData displayFormDataverseAttribute = null;
        if (getDisplayFormDataverseParameter() != null) {
            displayFormDataverseAttribute = requestDecoder.getBodyHttpData(getDisplayFormDataverseParameter());
        }
        if (displayFormDataverseAttribute != null && dataverseAttributeParts != null) {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_NOT_ALLOWED_AT_SAME_TIME,
                    getDisplayFormDataverseParameter(), getDataverseParameter());
        }
        InterfaceHttpData nameAtrribute = requestDecoder.getBodyHttpData(NAME_PARAMETER);
        InterfaceHttpData typeAttribute = requestDecoder.getBodyHttpData(TYPE_PARAMETER);
        InterfaceHttpData deleteAttribute = requestDecoder.getBodyHttpData(DELETE_PARAMETER);
        InterfaceHttpData replaceIfExistsAttribute = requestDecoder.getBodyHttpData(IFEXISTS_PARAMETER);
        if ((isNotAttribute(displayFormDataverseAttribute)) && (areNotAttributes(dataverseAttributeParts))) {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, getDataverseParameter());
        } else if (isNotAttribute(nameAtrribute)) {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, NAME_PARAMETER);
        } else if ((typeAttribute == null && deleteAttribute == null)) {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED,
                    TYPE_PARAMETER + " or " + DELETE_PARAMETER);
        } else if (typeAttribute != null && deleteAttribute != null) {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_NOT_ALLOWED_AT_SAME_TIME, TYPE_PARAMETER,
                    DELETE_PARAMETER);
        }

        if (!isNotAttribute(deleteAttribute)) {
            boolean replace = false;
            if (replaceIfExistsAttribute != null) {
                replace = Boolean.TRUE.toString()
                        .equalsIgnoreCase(((MixedAttribute) replaceIfExistsAttribute).getValue());
            }
            if (displayFormDataverseAttribute == null) {
                return LibraryUploadData.libraryDeletionUploadData(dataverseAttributeParts,
                        (MixedAttribute) nameAtrribute, replace);
            } else {
                DataverseName dataverseName = DataverseName
                        .create(parserFactory.createParser(((MixedAttribute) displayFormDataverseAttribute).getValue())
                                .parseMultipartIdentifier());
                return LibraryUploadData.libraryDeletionUploadData(dataverseName, (MixedAttribute) nameAtrribute,
                        replace);
            }
        } else if (!isNotAttribute(typeAttribute)) {
            InterfaceHttpData libraryData = requestDecoder.getBodyHttpData(DATA_PARAMETER);
            if (libraryData == null) {
                throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, DATA_PARAMETER);
            } else if (!libraryData.getHttpDataType().equals(InterfaceHttpData.HttpDataType.FileUpload)) {
                throw RuntimeDataException.create(ErrorCode.INVALID_REQ_PARAM_VAL, DATA_PARAMETER,
                        libraryData.getHttpDataType());
            }
            LibraryUploadData uploadData;
            if (displayFormDataverseAttribute == null) {
                uploadData = LibraryUploadData.libraryCreationUploadData(dataverseAttributeParts,
                        (MixedAttribute) nameAtrribute, (MixedAttribute) typeAttribute, libraryData);
            } else {
                DataverseName dataverseName = DataverseName
                        .create(parserFactory.createParser(((MixedAttribute) displayFormDataverseAttribute).getValue())
                                .parseMultipartIdentifier());
                uploadData = LibraryUploadData.libraryCreationUploadData(dataverseName, (MixedAttribute) nameAtrribute,
                        (MixedAttribute) typeAttribute, libraryData);
            }
            if (uploadData.type == null) {
                throw RuntimeDataException.create(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND,
                        ((MixedAttribute) typeAttribute).getValue());
            }
            return uploadData;
        } else {
            if (!typeAttribute.getHttpDataType().equals(InterfaceHttpData.HttpDataType.Attribute)) {
                throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, TYPE_PARAMETER);
            }
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, DELETE_PARAMETER);
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
                ErrorCode.PARAMETERS_REQUIRED)) {
            return HttpResponseStatus.BAD_REQUEST;
        }
        if (e instanceof AlgebricksException) {
            return HttpResponseStatus.BAD_REQUEST;
        }
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }

}
