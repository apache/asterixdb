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

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashSet;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.ActiveObjectId;
import org.apache.asterix.common.active.ActiveObjectId.ActiveObjectType;
import org.apache.asterix.common.feeds.ActiveActivity;
import org.apache.asterix.common.feeds.FeedActivity;
import org.apache.asterix.common.feeds.FeedActivity.FeedActivityDetails;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.api.IActiveLoadManager;
import org.apache.asterix.common.feeds.api.IActiveRuntime.ActiveRuntimeType;
import org.apache.asterix.feeds.CentralActiveManager;

public class FeedServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String resourcePath = null;
        String requestURI = request.getRequestURI();

        if (requestURI.equals("/")) {
            response.setContentType("text/html");
            resourcePath = "/feed/home.html";
        } else {
            resourcePath = requestURI;
        }

        InputStream is = FeedServlet.class.getResourceAsStream(resourcePath);
        if (is == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        // Special handler for font files and .png resources
        if (resourcePath.endsWith(".png")) {

            BufferedImage img = ImageIO.read(is);
            OutputStream outputStream = response.getOutputStream();
            String formatName = "png";
            response.setContentType("image/png");
            ImageIO.write(img, formatName, outputStream);
            outputStream.close();
            return;

        }

        response.setCharacterEncoding("utf-8");
        InputStreamReader isr = new InputStreamReader(is);
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader(isr);
        String line = br.readLine();

        while (line != null) {
            sb.append(line + "\n");
            line = br.readLine();
        }

        String outStr = null;
        if (requestURI.startsWith("/webui/static")) {
            outStr = sb.toString();
        } else {
            Collection<FeedActivity> lfa = new HashSet<FeedActivity>();
            for (ActiveActivity activity : CentralActiveManager.getInstance().getLoadManager().getActivities()) {
                if (activity instanceof FeedActivity) {
                    lfa.add((FeedActivity) activity);
                }
            }
            StringBuilder ldStr = new StringBuilder();
            ldStr.append("<br />");
            ldStr.append("<br />");
            if (lfa == null || lfa.isEmpty()) {
                ldStr.append("Currently there are no active feeds in AsterixDB");
            } else {
                ldStr.append("Active Feeds");
            }
            insertTable(ldStr, lfa);
            outStr = String.format(sb.toString(), ldStr.toString());

        }

        PrintWriter out = response.getWriter();
        out.println(outStr);
    }

    private void insertTable(StringBuilder html, Collection<FeedActivity> list) {
        html.append("<table style=\"width:100%\">");
        html.append("<th>" + FeedServletUtil.Constants.TABLE_HEADER_FEED_NAME + "</th>");
        html.append("<th>" + FeedServletUtil.Constants.TABLE_HEADER_DATASET_NAME + "</th>");
        html.append("<th>" + FeedServletUtil.Constants.TABLE_HEADER_ACTIVE_SINCE + "</th>");
        html.append("<th>" + FeedServletUtil.Constants.TABLE_HEADER_INTAKE_STAGE + "</th>");
        html.append("<th>" + FeedServletUtil.Constants.TABLE_HEADER_COMPUTE_STAGE + "</th>");
        html.append("<th>" + FeedServletUtil.Constants.TABLE_HEADER_STORE_STAGE + "</th>");
        html.append("<th>" + FeedServletUtil.Constants.TABLE_HEADER_INTAKE_RATE + "</th>");
        html.append("<th>" + FeedServletUtil.Constants.TABLE_HEADER_STORE_RATE + "</th>");
        for (FeedActivity activity : list) {
            insertRow(html, activity);
        }
    }

    private void insertRow(StringBuilder html, FeedActivity activity) {
        String intake = activity.getActivityDetails().get(FeedActivityDetails.INTAKE_LOCATIONS);
        String compute = activity.getActivityDetails().get(FeedActivityDetails.COMPUTE_LOCATIONS);
        String store = activity.getActivityDetails().get(FeedActivityDetails.STORAGE_LOCATIONS);

        IActiveLoadManager loadManager = CentralActiveManager.getInstance().getLoadManager();
        ActiveJobId connectionId = new FeedConnectionId(
                new ActiveObjectId(activity.getDataverseName(), activity.getObjectName(), ActiveObjectType.FEED),
                activity.getDatasetName());
        int intakeRate = loadManager.getOutflowRate(connectionId, ActiveRuntimeType.COLLECT) * intake.split(",").length;
        int storeRate = loadManager.getOutflowRate(connectionId, ActiveRuntimeType.STORE) * store.split(",").length;

        html.append("<tr>");
        html.append("<td>" + activity.getObjectName() + "</td>");
        html.append("<td>" + activity.getDatasetName() + "</td>");
        html.append("<td>" + activity.getConnectTimestamp() + "</td>");
        //html.append("<td>" + insertLink(html, FeedDashboardServlet.getParameterizedURL(activity), "Details") + "</td>");
        html.append("<td>" + intake + "</td>");
        html.append("<td>" + compute + "</td>");
        html.append("<td>" + store + "</td>");
        String color = "black";
        if (intakeRate > storeRate) {
            color = "red";
        }
        if (intakeRate < 0) {
            html.append("<td>" + "UNKNOWN" + "</td>");
        } else {
            html.append("<td>" + insertColoredText("" + intakeRate, color) + " rec/sec" + "</td>");
        }
        if (storeRate < 0) {
            html.append("<td>" + "UNKNOWN" + "</td>");
        } else {
            html.append("<td>" + insertColoredText("" + storeRate, color) + " rec/sec" + "</td>");
        }
        html.append("</tr>");
    }

    private String insertColoredText(String s, String color) {
        return "<font color=\"" + color + "\">" + s + "</font>";
    }
}
