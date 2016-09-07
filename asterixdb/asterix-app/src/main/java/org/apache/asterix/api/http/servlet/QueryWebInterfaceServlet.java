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
package org.apache.asterix.api.http.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.common.config.AsterixExternalProperties;
import org.apache.asterix.runtime.util.AsterixAppContextInfo;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONObject;

public class QueryWebInterfaceServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final HashMap<String, String> fileMimePair = new HashMap<>();
    private static final Log LOG = LogFactory.getLog(QueryWebInterfaceServlet.class);

    public QueryWebInterfaceServlet() {
        fileMimePair.put("png", "image/png");
        fileMimePair.put("eot", "application/vnd.ms-fontobject");
        fileMimePair.put("svg", "image/svg+xml\t");
        fileMimePair.put("ttf", "application/x-font-ttf");
        fileMimePair.put("woff", "application/x-font-woff");
        fileMimePair.put("woff2", "application/x-font-woff");
        fileMimePair.put("html", "text/html");
        fileMimePair.put("css", "text/css");
        fileMimePair.put("js", "application/javascript");
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        String resourcePath = null;
        String requestURI = request.getRequestURI();

        if ("/".equals(requestURI)) {
            response.setContentType("text/html");
            resourcePath = "/queryui/queryui.html";
        } else {
            resourcePath = requestURI;
        }

        try (InputStream is = APIServlet.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                try {
                    response.sendError(HttpServletResponse.SC_NOT_FOUND);
                } catch (IllegalStateException | IOException e) {
                    LOG.error(e);
                }
                return;
            }
            // Multiple MIME type support
            for (Map.Entry<String, String> entry : fileMimePair.entrySet()) {
                OutputStream out = null;
                if (resourcePath.endsWith(entry.getKey())) {
                    response.setContentType(entry.getValue());
                    try {
                        out = response.getOutputStream();
                        IOUtils.copy(is, out);

                    } catch (IOException e) {
                        LOG.info(e);
                    } finally {

                        if (out != null) {
                            IOUtils.closeQuietly(out);
                        }
                        IOUtils.closeQuietly(is);

                    }
                    return;
                }
            }
            try {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST);
            } catch (IllegalStateException | IOException e) {
                LOG.error(e);
            }
        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setCharacterEncoding("utf-8");
        response.setContentType("application/json");
        AsterixExternalProperties externalProperties = AsterixAppContextInfo.INSTANCE.getExternalProperties();
        JSONObject obj = new JSONObject();
        try {
            PrintWriter out = response.getWriter();
            obj.put("api_port", String.valueOf(externalProperties.getAPIServerPort()));
            out.println(obj.toString());
            return;
        } catch (Exception e) {
            LOG.error(e);
        }

        try {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } catch (IllegalStateException | IOException e) {
            LOG.error(e);
        }
    }

}
