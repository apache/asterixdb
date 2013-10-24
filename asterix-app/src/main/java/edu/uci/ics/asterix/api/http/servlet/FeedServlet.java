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
import java.util.List;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;

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

        try {
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
                MetadataManager.INSTANCE.init();
                MetadataTransactionContext ctx = MetadataManager.INSTANCE.beginTransaction();
                List<FeedActivity> lfa = MetadataManager.INSTANCE.getActiveFeeds(ctx, null, null);
                StringBuilder ldStr = new StringBuilder();
                ldStr.append("<br />");
                ldStr.append("<br />");
                if (lfa == null || lfa.isEmpty()) {
                    ldStr.append("Currently there are no active feeds in the Asterix");
                } else {
                    ldStr.append("Active Feeds");
                }
                FeedConnectionId feedId = null;
                for (FeedActivity feedActivity : lfa) {
                    feedId = new FeedConnectionId(feedActivity.getDataverseName(), feedActivity.getFeedName(),
                            feedActivity.getDatasetName());
                    ldStr.append("<br />");
                    ldStr.append("<br />");
                    ldStr.append("<a href=\"/feed/dashboard?dataverse=" + feedActivity.getDataverseName() + "&feed="
                            + feedActivity.getFeedName() + "&dataset=" + feedActivity.getDatasetName() + "\">" + feedId
                            + "</a>");
                    ldStr.append("<br />");
                }

                outStr = String.format(sb.toString(), ldStr.toString());
                MetadataManager.INSTANCE.commitTransaction(ctx);

            }

            PrintWriter out = response.getWriter();
            out.println(outStr);
        } catch (ACIDException | MetadataException e) {

        }
    }
}
