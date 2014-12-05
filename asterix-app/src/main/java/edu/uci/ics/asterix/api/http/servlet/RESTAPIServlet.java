/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.api.http.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Level;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.api.common.APIFramework.OutputFormat;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.base.Statement.Kind;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.aql.parser.TokenMgrError;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.result.ResultReader;
import edu.uci.ics.asterix.result.ResultUtils;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;

abstract class RESTAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    private static final String HYRACKS_DATASET_ATTR = "edu.uci.ics.asterix.HYRACKS_DATASET";

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
        response.setCharacterEncoding("utf-8");

        PrintWriter out = response.getWriter();
        APIFramework.OutputFormat format;
        // QQQ For now switch solely based on Accept header. Later will add
        // an "output" query parameter.
        String accept = request.getHeader("Accept");
        if ((accept == null) || (accept.contains("application/x-adm"))) {
            format = OutputFormat.ADM;
            response.setContentType("application/x-adm");
        } else if (accept.contains("text/html")) {
            format = OutputFormat.HTML;
            response.setContentType("text/html");
        } else if (accept.contains("text/csv")) {
            format = OutputFormat.CSV;
            response.setContentType("text/csv; header=present");
        } else {
            // JSON output is the default; most generally useful for a
            // programmatic HTTP API
            format = APIFramework.OutputFormat.JSON;
            response.setContentType("application/json");
        }
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
                SessionConfig sessionConfig = new SessionConfig(true, false, false, false, false, false, true, true,
                        false);
                MetadataManager.INSTANCE.init();
                AqlTranslator aqlTranslator = new AqlTranslator(aqlStatements, out, sessionConfig, format);
                aqlTranslator.compileAndExecute(hcc, hds, resultDelivery);
            }
        } catch (ParseException | TokenMgrError | edu.uci.ics.asterix.aqlplus.parser.TokenMgrError pe) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, pe.getMessage(), pe);
            String errorMessage = ResultUtils.buildParseExceptionMessage(pe, query);
            JSONObject errorResp = ResultUtils.getErrorResponse(2, errorMessage, "", "");
            out.write(errorResp.toString());
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            ResultUtils.apiErrorHandler(out, e);
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
