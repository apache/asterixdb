/*
 * Copyright 2009-2011 by The Regents of the University of California
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
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.base.Statement.Kind;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
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
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");
        String bufferSizeStr = request.getParameter("chunk_size");
        int bufferSize;
        try {
            bufferSize = Integer.parseInt(bufferSizeStr);
        } catch (Exception e) {
            bufferSize = ResultUtils.DEFAULT_BUFFER_SIZE;
        }
        response.setBufferSize(bufferSize);

        PrintWriter out = response.getWriter();

        DisplayFormat format = DisplayFormat.HTML;

        String contentType = request.getContentType();

        if ((contentType == null) || (contentType.equals("text/plain"))) {
            format = DisplayFormat.TEXT;
        } else if (contentType.equals("application/json")) {
            format = DisplayFormat.JSON;
        }

        String query = getQueryParameter(request);
        boolean asyncResults = isAsync(request);

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
            List<Statement> aqlStatements = parser.Statement();
            if (checkForbiddenStatements(aqlStatements, out)) {
                return;
            }
            SessionConfig sessionConfig = new SessionConfig(true, false, false, false, false, false, true, false,
                    bufferSize);

            MetadataManager.INSTANCE.init();

            AqlTranslator aqlTranslator = new AqlTranslator(aqlStatements, out, sessionConfig, format);

            aqlTranslator.compileAndExecute(hcc, hds, asyncResults);

        } catch (ParseException pe) {
            StringBuilder errorMessage = new StringBuilder();
            String message = pe.getMessage();
            message = message.replace("<", "&lt");
            message = message.replace(">", "&gt");
            errorMessage.append("SyntaxError:" + message + "\n");
            int pos = message.indexOf("line");
            if (pos > 0) {
                int columnPos = message.indexOf(",", pos + 1 + "line".length());
                int lineNo = Integer.parseInt(message.substring(pos + "line".length() + 1, columnPos));
                String line = query.split("\n")[lineNo - 1];
                errorMessage.append("==> " + line + "\n");
            }
            JSONObject errorResp = ResultUtils.getErrorResponse(2, errorMessage.toString());
            out.write(errorResp.toString());
        } catch (Exception e) {
            StringBuilder errorMessage = new StringBuilder();
            errorMessage.append(e.getMessage());
            JSONObject errorResp = ResultUtils.getErrorResponse(99, errorMessage.toString());
            out.write(errorResp.toString());
        }
    }

    private boolean checkForbiddenStatements(List<Statement> aqlStatements, PrintWriter out) {
        for (Statement st : aqlStatements) {
            if (!getAllowedStatements().contains(st.getKind())) {
                JSONObject errorResp = ResultUtils.getErrorResponse(1, String.format(getErrorMessage(), st.getKind()));
                out.write(errorResp.toString());
                return true;
            }
        }
        return false;
    }

    protected boolean isAsync(HttpServletRequest request) {
        String mode = request.getParameter("mode");
        boolean asyncResults = false;
        if (mode != null && mode.equals("asynchronous")) {
            asyncResults = true;
        }
        return asyncResults;
    }

    protected abstract String getQueryParameter(HttpServletRequest request);

    protected abstract List<Kind> getAllowedStatements();

    protected abstract String getErrorMessage();
}
