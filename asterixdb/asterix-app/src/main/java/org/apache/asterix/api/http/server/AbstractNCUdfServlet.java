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

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.LibraryDescriptor;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;

public abstract class AbstractNCUdfServlet extends AbstractServlet {

    INcApplicationContext appCtx;
    INCServiceContext srvCtx;

    protected final IApplicationContext plainAppCtx;
    private final HttpScheme httpServerProtocol;
    private final int httpServerPort;

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

    URI createDownloadURI(Path file) throws Exception {
        String path = paths[0].substring(0, trims[0]) + '/' + file.getFileName();
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

    Pair<DataverseName, String> parseLibraryName(IServletRequest request) throws IllegalArgumentException {
        String[] path = StringUtils.split(localPath(request), '/');
        int ln = path.length;
        if (ln < 2) {
            return null;
        }
        String libraryName = path[ln - 1];
        DataverseName dataverseName = DataverseName.create(Arrays.asList(path), 0, ln - 1);
        return new Pair<>(dataverseName, libraryName);
    }

    static ExternalFunctionLanguage getLanguageByFileExtension(String fileExtension) {
        switch (fileExtension) {
            case LibraryDescriptor.FILE_EXT_ZIP:
                return JAVA;
            case LibraryDescriptor.FILE_EXT_PYZ:
                return PYTHON;
            default:
                return null;
        }
    }

    HttpResponseStatus toHttpErrorStatus(Exception e) {
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
}
