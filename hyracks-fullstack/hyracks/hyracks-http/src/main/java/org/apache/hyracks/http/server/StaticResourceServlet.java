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
package org.apache.hyracks.http.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.IOUtils;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

public class StaticResourceServlet extends AbstractServlet {

    private static final Logger LOGGER = LogManager.getLogger();

    public StaticResourceServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        String resourcePath = request.getHttpRequest().uri();
        deliverResource(resourcePath, response);
    }

    protected void deliverResource(String resourcePath, IServletResponse response) throws IOException {
        response.setStatus(HttpResponseStatus.OK);
        try (InputStream is = StaticResourceServlet.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return;
            }
            String extension = extension(resourcePath);
            String mime = HttpUtil.mime(extension);
            if (mime == null) {
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
            } else {
                OutputStream out = response.outputStream();
                HttpUtil.setContentType(response, mime);
                try {
                    IOUtils.copy(is, out);
                } catch (IOException e) {
                    LOGGER.log(Level.WARN, "Failure copying response", e);
                } finally {
                    if (out != null) {
                        IOUtils.closeQuietly(out);
                    }
                    IOUtils.closeQuietly(is);
                }
            }
        }
    }

    public static String extension(String path) {
        int i = path.lastIndexOf('.');
        return i < 1 ? "" : path.substring(i);
    }
}
