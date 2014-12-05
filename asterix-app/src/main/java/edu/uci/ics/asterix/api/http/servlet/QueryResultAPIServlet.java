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

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.uci.ics.asterix.api.common.APIFramework;
import edu.uci.ics.asterix.result.ResultReader;
import edu.uci.ics.asterix.result.ResultUtils;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;

public class QueryResultAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    private static final String HYRACKS_DATASET_ATTR = "edu.uci.ics.asterix.HYRACKS_DATASET";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/html");
        response.setCharacterEncoding("utf-8");
        String strHandle = request.getParameter("handle");
        PrintWriter out = response.getWriter();
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        IHyracksDataset hds;

        try {
            HyracksProperties hp = new HyracksProperties();
            String strIP = hp.getHyracksIPAddress();
            int port = hp.getHyracksPort();

            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
                if (hcc == null) {
                    hcc = new HyracksConnection(strIP, port);
                    context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
                }

                hds = (IHyracksDataset) context.getAttribute(HYRACKS_DATASET_ATTR);
                if (hds == null) {
                    hds = new HyracksDataset(hcc, ResultReader.FRAME_SIZE, ResultReader.NUM_READERS);
                    context.setAttribute(HYRACKS_DATASET_ATTR, hds);
                }
            }
            JSONObject handleObj = new JSONObject(strHandle);
            JSONArray handle = handleObj.getJSONArray("handle");
            JobId jobId = new JobId(handle.getLong(0));
            ResultSetId rsId = new ResultSetId(handle.getLong(1));

            ResultReader resultReader = new ResultReader(hcc, hds);
            resultReader.open(jobId, rsId);

            APIFramework.OutputFormat format;
            // QQQ This code is duplicated from RESTAPIServlet, and is
            // erroneous anyway. The output format is determined by
            // the initial query and cannot be modified here, so we need
            // to find a way to send the same OutputFormat value here as
            // was originally determined there. Need to save this value on
            // some object that we can obtain here.
            String accept = request.getHeader("Accept");
            if ((accept == null) || (accept.contains("application/x-adm"))) {
                format = APIFramework.OutputFormat.ADM;
                response.setContentType("application/x-adm");
            } else if (accept.contains("text/html")) {
                format = APIFramework.OutputFormat.HTML;
                response.setContentType("text/html");
            } else if (accept.contains("text/csv")) {
                format = APIFramework.OutputFormat.CSV;
                response.setContentType("text/csv; header=present");
            } else {
                // JSON output is the default; most generally useful for a
                // programmatic HTTP API
                format = APIFramework.OutputFormat.JSON;
                response.setContentType("application/json");
            }
            ResultUtils.displayResults(resultReader, out, format);

        } catch (Exception e) {
            out.println(e.getMessage());
            e.printStackTrace(out);
        }
    }
}
