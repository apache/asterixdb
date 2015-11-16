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
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Level;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.api.common.SessionConfig;
import org.apache.asterix.api.common.SessionConfig.OutputFormat;
import org.apache.asterix.aql.translator.AqlTranslator;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.aql.parser.AQLParser;
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.lang.aql.util.AQLFormatPrintUtil;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.base.Statement.Kind;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.result.ResultReader;
import org.apache.asterix.result.ResultUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.json.JSONObject;

abstract class RESTAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    public static final String HYRACKS_CONNECTION_ATTR = "org.apache.asterix.HYRACKS_CONNECTION";

    public static final String HYRACKS_DATASET_ATTR = "org.apache.asterix.HYRACKS_DATASET";

    /**
     * Initialize the Content-Type of the response, and construct a
     * SessionConfig with the appropriate output writer and output-format
     * based on the Accept: header and other servlet parameters.
     */
    static SessionConfig initResponse(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setCharacterEncoding("utf-8");

        // CLEAN_JSON output is the default; most generally useful for a
        // programmatic HTTP API
        OutputFormat format = OutputFormat.CLEAN_JSON;

        // First check the "output" servlet parameter.
        String output = request.getParameter("output");
        String accept = request.getHeader("Accept");
        if (accept == null) {
            accept = "";
        }
        if (output != null) {
            if (output.equals("CSV")) {
                format = OutputFormat.CSV;
            } else if (output.equals("ADM")) {
                format = OutputFormat.ADM;
            }
        } else {
            // Second check the Accept: HTTP header.
            if (accept.contains("application/x-adm")) {
                format = OutputFormat.ADM;
            } else if (accept.contains("text/csv")) {
                format = OutputFormat.CSV;
            }
        }

        // If it's JSON, check for the "lossless" flag
        if (format == OutputFormat.CLEAN_JSON &&
                ("true".equals(request.getParameter("lossless")) || accept.contains("lossless=true")) ) {
            format = OutputFormat.LOSSLESS_JSON;
        }

        SessionConfig sessionConfig = new SessionConfig(response.getWriter(), format);

        // If it's JSON or ADM, check for the "wrapper-array" flag. Default is
        // "true" for JSON and "false" for ADM. (Not applicable for CSV.)
        boolean wrapper_array;
        switch (format) {
            case CLEAN_JSON:
            case LOSSLESS_JSON:
                wrapper_array = true;
                break;
            default:
                wrapper_array = false;
                break;
        }
        String wrapper_param = request.getParameter("wrapper-array");
        if (wrapper_param != null) {
            wrapper_array = Boolean.valueOf(wrapper_param);
        } else if (accept.contains("wrap-array=true")) {
            wrapper_array = true;
        } else if (accept.contains("wrap-array=false")) {
            wrapper_array = false;
        }
        sessionConfig.set(SessionConfig.FORMAT_WRAPPER_ARRAY, wrapper_array);

        // Now that format is set, output the content-type
        switch (format) {
            case ADM:
                response.setContentType("application/x-adm");
                break;
            case CLEAN_JSON:
                // No need to reflect "clean-ness" in output type; fall through
            case LOSSLESS_JSON:
                response.setContentType("application/json");
                break;
            case CSV: {
                // Check for header parameter or in Accept:.
                if ("present".equals(request.getParameter("header")) ||
                    accept.contains("header=present")) {
                    response.setContentType("text/csv; header=present");
                    sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, true);
                } else {
                    response.setContentType("text/csv; header=absent");
                }
            }
        }

        return sessionConfig;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {
        StringWriter sw = new StringWriter();
        IOUtils.copy(request.getInputStream(), sw, StandardCharsets.UTF_8.name());
        String query = sw.toString();
        handleRequest(request, response, query);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String query = getQueryParameter(request);
        handleRequest(request, response, query);
    }

    public void handleRequest(HttpServletRequest request, HttpServletResponse response, String query)
            throws IOException {
        SessionConfig sessionConfig = initResponse(request, response);
        AqlTranslator.ResultDelivery resultDelivery = whichResultDelivery(request);

        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        IHyracksDataset hds;

        try {
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
                hds = (IHyracksDataset) context.getAttribute(HYRACKS_DATASET_ATTR);
                if (hds == null) {
                    hds = new HyracksDataset(hcc, ResultReader.FRAME_SIZE, ResultReader.NUM_READERS);
                    context.setAttribute(HYRACKS_DATASET_ATTR, hds);
                }
            }

            AQLParser parser = new AQLParser(query);
            List<Statement> aqlStatements = parser.parse();
            if (!containsForbiddenStatements(aqlStatements)) {
                MetadataManager.INSTANCE.init();
                AqlTranslator aqlTranslator = new AqlTranslator(aqlStatements, sessionConfig);
                aqlTranslator.compileAndExecute(hcc, hds, resultDelivery);
            }
        } catch (AsterixException | TokenMgrError | org.apache.asterix.aqlplus.parser.TokenMgrError pe) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, pe.getMessage(), pe);
            String errorMessage = ResultUtils.buildParseExceptionMessage(pe, query);
            JSONObject errorResp = ResultUtils.getErrorResponse(2, errorMessage, "", "");
            sessionConfig.out().write(errorResp.toString());
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            ResultUtils.apiErrorHandler(sessionConfig.out(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    private boolean containsForbiddenStatements(List<Statement> aqlStatements) throws AsterixException {
        for (Statement st : aqlStatements) {
            if (!getAllowedStatements().contains(st.getKind())) {
                throw new AsterixException(String.format(getErrorMessage(), st.getKind()));
            }
        }
        return false;
    }

    protected AqlTranslator.ResultDelivery whichResultDelivery(HttpServletRequest request) {
        String mode = request.getParameter("mode");
        if (mode != null) {
            if (mode.equals("asynchronous")) {
                return AqlTranslator.ResultDelivery.ASYNC;
            } else if (mode.equals("asynchronous-deferred")) {
                return AqlTranslator.ResultDelivery.ASYNC_DEFERRED;
            }
        }
        return AqlTranslator.ResultDelivery.SYNC;
    }

    protected abstract String getQueryParameter(HttpServletRequest request);

    protected abstract List<Kind> getAllowedStatements();

    protected abstract String getErrorMessage();
}
