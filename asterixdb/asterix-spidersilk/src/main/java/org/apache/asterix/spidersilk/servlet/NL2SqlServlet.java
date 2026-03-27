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
package org.apache.asterix.spidersilk.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.spidersilk.api.INl2SqlTranslator;
import org.apache.asterix.spidersilk.api.Nl2SqlException;
import org.apache.asterix.spidersilk.api.SchemaContext;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * HTTP servlet exposing the NL2SQL++ translation API on the JSON API server.
 *
 * <p><b>Endpoint:</b> {@code POST /query/nl2sql}
 *
 * <p><b>Request parameters (form or JSON body):</b>
 * <ul>
 *   <li>{@code statement} (required) — the natural language query</li>
 *   <li>{@code dataverse} (optional) — target dataverse for schema context</li>
 * </ul>
 *
 * <p><b>Response (JSON):</b>
 * <pre>
 * {
 *   "sqlpp":   "SELECT VALUE t FROM TweetMessages t WHERE ...",
 *   "status":  "success"
 * }
 * </pre>
 *
 * <p>When the {@code INl2SqlTranslator} implementation is not yet available,
 * the endpoint returns HTTP 501 (Not Implemented) with an informative message,
 * allowing the servlet to be registered and tested without a live LLM backend.
 */
public class NL2SqlServlet extends AbstractServlet {

    private static final Logger LOGGER = LogManager.getLogger();
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Request parameter name for the natural language query. */
    public static final String PARAM_STATEMENT = "statement";
    /** Optional request parameter specifying the target dataverse. */
    public static final String PARAM_DATAVERSE = "dataverse";

    /**
     * The translator is injected at construction time and may be {@code null}
     * until a concrete LLM implementation is provided (Phase 2 of development).
     */
    private final INl2SqlTranslator translator;

    public NL2SqlServlet(ConcurrentMap<String, Object> ctx, String[] paths, INl2SqlTranslator translator) {
        super(ctx, paths);
        this.translator = translator;
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws IOException {
        String naturalLanguage = request.getParameter(PARAM_STATEMENT);
        String dataverse = request.getParameter(PARAM_DATAVERSE);

        if (naturalLanguage == null || naturalLanguage.isBlank()) {
            sendError(request, response, HttpResponseStatus.BAD_REQUEST, "Parameter 'statement' is required.");
            return;
        }

        if (translator == null) {
            sendError(request, response, HttpResponseStatus.NOT_IMPLEMENTED,
                    "NL2SQL++ translator is not yet configured. "
                            + "Set nl2sql.model.type and related properties in cc.conf and restart the server.");
            return;
        }

        try {
            // Build schema context from metadata if a dataverse is provided.
            // SchemaContextBuilder integration will be added in the next phase.
            SchemaContext schemaContext = dataverse != null ? new SchemaContext(dataverse, java.util.List.of()) : null;

            String sqlpp = translator.translate(naturalLanguage, schemaContext);

            HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
            response.setStatus(HttpResponseStatus.OK);

            ObjectNode result = OBJECT_MAPPER.createObjectNode();
            result.put("sqlpp", sqlpp);
            result.put("status", "success");

            PrintWriter writer = response.writer();
            writer.write(result.toString());
            writer.flush();

        } catch (Nl2SqlException e) {
            LOGGER.warn("NL2SQL translation failed for query: {}", naturalLanguage, e);
            sendError(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        sendError(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED, "Use POST with parameter 'statement'.");
    }

    private void sendError(IServletRequest request, IServletResponse response, HttpResponseStatus status,
            String message) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        response.setStatus(status);
        ObjectNode error = OBJECT_MAPPER.createObjectNode();
        error.put("status", "error");
        error.put("message", message);
        PrintWriter writer = response.writer();
        writer.write(error.toString());
        writer.flush();
    }
}
