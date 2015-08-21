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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;

import org.apache.asterix.api.common.SessionConfig.OutputFormat;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.result.ResultUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;

public class ShutdownAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "org.apache.asterix.HYRACKS_CONNECTION";

    private static final String HYRACKS_DATASET_ATTR = "org.apache.asterix.HYRACKS_DATASET";

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {

        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");

        PrintWriter out = response.getWriter();
        OutputFormat format = OutputFormat.LOSSLESS_JSON;
        String accept = request.getHeader("Accept");
        if ((accept == null) || (accept.contains("application/x-adm"))) {
            format = OutputFormat.ADM;
        } else if (accept.contains("application/json")) {
            format = OutputFormat.LOSSLESS_JSON;
        }
        StringWriter sw = new StringWriter();
        IOUtils.copy(request.getInputStream(), sw, StandardCharsets.UTF_8.name());

        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        try {
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
                response.setStatus(HttpServletResponse.SC_ACCEPTED);
                hcc.stopCluster();
            }
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            ResultUtils.apiErrorHandler(out, e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    }
}
