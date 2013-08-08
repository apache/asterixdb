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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener;
import edu.uci.ics.asterix.metadata.feeds.FeedConnectionId;

public class FeedDataProviderServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

        String feedName = request.getParameter("feed");
        String datasetName = request.getParameter("dataset");
        String dataverseName = request.getParameter("dataverse");

        String report = getFeedReport(feedName, datasetName, dataverseName);
        System.out.println(" RECEIVED REPORT " + report);
        long timestamp = System.currentTimeMillis();
        
        JSONObject obj = new JSONObject();
        JSONArray array = new JSONArray();
        try {
            obj.put("time", timestamp);
            obj.put("value", report);
        } catch (JSONException jsoe) {
            throw new IOException(jsoe);
        }

        PrintWriter out = response.getWriter();
        out.println(obj.toString());
    }

    private String getFeedReport(String feedName, String datasetName, String dataverseName) {
        FeedConnectionId feedId = new FeedConnectionId(dataverseName, feedName, datasetName);
        LinkedBlockingQueue<String> queue = FeedLifecycleListener.INSTANCE.getFeedReportQueue(feedId);
        String report = null;
        try {
            report = queue.poll(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return report;
    }
}
