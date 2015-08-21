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

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collection;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.uci.ics.asterix.common.feeds.FeedActivity;
import edu.uci.ics.asterix.common.feeds.FeedActivity.FeedActivityDetails;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.api.IFeedLoadManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.feeds.CentralFeedManager;

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
            Collection<FeedActivity> lfa = CentralFeedManager.getInstance().getFeedLoadManager().getFeedActivities();
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
        String intake = activity.getFeedActivityDetails().get(FeedActivityDetails.INTAKE_LOCATIONS);
        String compute = activity.getFeedActivityDetails().get(FeedActivityDetails.COMPUTE_LOCATIONS);
        String store = activity.getFeedActivityDetails().get(FeedActivityDetails.STORAGE_LOCATIONS);

        IFeedLoadManager loadManager = CentralFeedManager.getInstance().getFeedLoadManager();
        FeedConnectionId connectionId = new FeedConnectionId(new FeedId(activity.getDataverseName(),
                activity.getFeedName()), activity.getDatasetName());
        int intakeRate = loadManager.getOutflowRate(connectionId, FeedRuntimeType.COLLECT) * intake.split(",").length;
        int storeRate = loadManager.getOutflowRate(connectionId, FeedRuntimeType.STORE) * store.split(",").length;

        html.append("<tr>");
        html.append("<td>" + activity.getFeedName() + "</td>");
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

    private String insertLink(StringBuilder html, String url, String displayText) {
        return ("<a href=\"" + url + "\">" + displayText + "</a>");
    }

    private String insertColoredText(String s, String color) {
        return "<font color=\"" + color + "\">" + s + "</font>";
    }
}


