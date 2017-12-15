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
package org.apache.hyracks.control.cc.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ApplicationInstallationHandler extends AbstractServlet {

    private static final Logger LOGGER = LogManager.getLogger();

    private ClusterControllerService ccs;

    public ApplicationInstallationHandler(ConcurrentMap<String, Object> ctx, String[] paths,
            ClusterControllerService ccs) {
        super(ctx, paths);
        this.ccs = ccs;
    }

    @Override
    public void handle(IServletRequest request, IServletResponse response) {
        String localPath = localPath(request);
        while (localPath.startsWith("/")) {
            localPath = localPath.substring(1);
        }
        final String[] params = localPath.split("&");
        if (params.length != 2 || params[0].isEmpty() || params[1].isEmpty()) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        final String deployIdString = params[0];
        final String fileName = params[1];
        final String rootDir = ccs.getServerContext().getBaseDir().toString();

        final String deploymentDir = rootDir.endsWith(File.separator) ? rootDir + "applications/" + deployIdString
                : rootDir + File.separator + "/applications/" + File.separator + deployIdString;
        final HttpMethod method = request.getHttpRequest().method();
        try {
            response.setStatus(HttpResponseStatus.OK);
            if (method == HttpMethod.PUT) {
                final ByteBuf content = request.getHttpRequest().content();
                writeToFile(content, deploymentDir, fileName);
            } else if (method == HttpMethod.GET) {
                readFromFile(fileName, deploymentDir, response);
            } else {
                response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Unhandled exception ", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    protected void readFromFile(final String fileName, final String deploymentDir, IServletResponse response)
            throws Exception {
        class InputStreamGetter extends SynchronizableWork {
            private InputStream is;

            @Override
            protected void doRun() throws Exception {
                File jarFile = new File(deploymentDir, fileName);
                is = new FileInputStream(jarFile);
            }
        }
        InputStreamGetter r = new InputStreamGetter();
        ccs.getWorkQueue().scheduleAndSync(r);
        if (r.is == null) {
            response.setStatus(HttpResponseStatus.NOT_FOUND);
        } else {
            HttpUtil.setContentType(response, "application/octet-stream");
            response.setStatus(HttpResponseStatus.OK);
            try {
                IOUtils.copyLarge(r.is, response.outputStream());
            } finally {
                r.is.close();
            }
        }
    }

    protected void writeToFile(ByteBuf content, final String deploymentDir, final String fileName) throws Exception {
        class OutputStreamGetter extends SynchronizableWork {
            private OutputStream os;

            @Override
            protected void doRun() throws Exception {
                FileUtils.forceMkdir(new File(deploymentDir));
                File jarFile = new File(deploymentDir, fileName);
                os = new FileOutputStream(jarFile);
            }
        }
        OutputStreamGetter r = new OutputStreamGetter();
        ccs.getWorkQueue().scheduleAndSync(r);
        try {
            content.getBytes(0, r.os, content.readableBytes());
        } finally {
            r.os.close();
        }
    }
}
