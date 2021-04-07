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

import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.HttpScheme;

public class NCUdfRecoveryServlet extends AbstractNCUdfServlet {

    ExternalLibraryManager libraryManager;

    public static final String GET_ALL_UDF_ENDPOINT = "/all";

    public NCUdfRecoveryServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            HttpScheme httpServerProtocol, int httpServerPort) {
        super(ctx, paths, appCtx, httpServerProtocol, httpServerPort);
    }

    @Override
    public void init() {
        appCtx = (INcApplicationContext) plainAppCtx;
        srvCtx = this.appCtx.getServiceContext();
        this.libraryManager = (ExternalLibraryManager) appCtx.getLibraryManager();
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        String localPath = localPath(request);
        if (localPath.equals(GET_ALL_UDF_ENDPOINT)) {
            Path zippedLibs = libraryManager.zipAllLibs();
            readFromFile(zippedLibs, response, HttpUtil.ContentType.APPLICATION_ZIP,
                    StandardOpenOption.DELETE_ON_CLOSE);
        }
    }
}
