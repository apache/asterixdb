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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.runtime.utils.AppContextInfo;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryWebInterfaceServlet extends AbstractServlet {
    private static final Logger LOGGER = Logger.getLogger(QueryWebInterfaceServlet.class.getName());

    public QueryWebInterfaceServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    public void handle(IServletRequest request, IServletResponse response) {
        try {
            if (request.getHttpRequest().method() == HttpMethod.GET) {
                doGet(request, response);
            } else if (request.getHttpRequest().method() == HttpMethod.POST) {
                doPost(response);
            } else {
                response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failure setting content type", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }
    }

    private void doGet(IServletRequest request, IServletResponse response) throws IOException {
        String resourcePath = null;
        String requestURI = request.getHttpRequest().uri();
        response.setStatus(HttpResponseStatus.OK);

        if ("/".equals(requestURI)) {
            HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML);
            resourcePath = "/queryui/queryui.html";
        } else {
            resourcePath = requestURI;
        }

        try (InputStream is = QueryWebInterfaceServlet.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return;
            }
            int i = resourcePath.lastIndexOf('.');
            if (i >= 0) {
                String extension = resourcePath.substring(i);
                String mime = HttpUtil.mime(extension);
                if (mime != null) {
                    OutputStream out = response.outputStream();
                    HttpUtil.setContentType(response, mime);
                    try {
                        IOUtils.copy(is, out);
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Failure copying response", e);
                    } finally {
                        if (out != null) {
                            IOUtils.closeQuietly(out);
                        }
                        IOUtils.closeQuietly(is);
                    }
                    return;
                }
            }
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
        }
    }

    private void doPost(IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);
        ExternalProperties externalProperties = AppContextInfo.INSTANCE.getExternalProperties();
        response.setStatus(HttpResponseStatus.OK);
        ObjectMapper om = new ObjectMapper();
        ObjectNode obj = om.createObjectNode();
        try {
            PrintWriter out = response.writer();
            obj.put("api_port", String.valueOf(externalProperties.getAPIServerPort()));
            out.println(obj.toString());
            return;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failure writing response", e);
        }
        try {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failure setting response status", e);
        }
    }

    public static String extension(String path) {
        int i = path.lastIndexOf('.');
        return i < 1 ? "" : path.substring(i);
    }
}
